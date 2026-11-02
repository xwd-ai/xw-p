package pipeline

import (
	"strings"
)

// 模板渲染的注入防护。
//
// 模板里的变量值来自触发负载：帖子标题、评论正文、互动者昵称都是终端用户
// 能写的内容。它们进入渠道后会落到几个对格式敏感的位置——邮件头、日志行、
// 收件箱卡片、终端输出。一个带 CR/LF 的标题能往邮件里塞一行 Bcc，一段带
// ESC 的评论能在运维的终端里伪造输出，一条超长评论能把渲染缓冲撑爆。
//
// 因此变量值在拼进模板之前统一清理，而不是指望下游每个渠道各自记得转义：
//
//   - 单行文本（邮件主题、推送/站内信标题、聊天通知预览）：控制字符当分隔符，
//     连续空白压成一个——单行位置上的换行没有任何合法含义；
//   - 多行正文：保留换行与制表符，其余控制字符（含 ANSI 转义序列的前导 ESC）
//     剥掉，CRLF 归一成 LF；
//   - HTML 正文：先清理控制字符再转义，顺序不能反——先转义的话控制字符会以
//     `&#27;` 之类的实体留在页面里。
//
// 清理只作用于变量值与模板产出，模板自身的标记原样保留：模板是工作流作者
// 写的，变量值是终端用户写的，两者信任级别不同。

const (
	// maxRenderedBytes 限制单次渲染的输出字节数。
	//
	// 一条通知的正文远用不到这么大，设这个上限是为了拦住「模板里有一堆表达式、
	// 每个表达式的变量值都很大」的放大效应：64 KiB 的模板配 13k 个表达式，
	// 每个返回 1 MiB 就能把进程内存吃光。超限按渲染失败处理而不是截断，
	// 悄悄丢掉半条评论比发不出去更难排查。
	maxRenderedBytes = 256 * 1024
)

// sanitizeSingleLine 把文本收敛成单行：控制字符与换行当分隔符，
// 连续空白压成一个空格，去掉首尾空白。
//
// 邮件主题与推送标题都只有一行，任何换行都意味着「作者想写第二行」或者
// 「有人在试着加一个头字段」，两种情况下都只能留空格。这个函数是幂等的，
// 渲染阶段与分发阶段各调一次不会把内容再改一遍。
func sanitizeSingleLine(s string) string {
	if s == "" {
		return ""
	}
	s = stripEscapeSequences(s)
	var b strings.Builder
	b.Grow(len(s))
	// pending 表示「刚跳过了分隔符，下一个可见字符前面要补一个空格」。
	pending := false
	for _, r := range s {
		if isControlRune(r) || isLineSeparator(r) || isSpaceRune(r) {
			pending = b.Len() > 0
			continue
		}
		if pending {
			b.WriteByte(' ')
			pending = false
		}
		b.WriteRune(r)
	}
	return b.String()
}

// sanitizeMultiLine 清理多行文本：保留换行与制表符，剥掉其余控制字符。
//
// 正文里的换行是有意义的分段，不能像单行那样压成空格；但 NUL、DEL、ESC
// （终端转义序列的起点）以及 C1 控制字符没有任何显示含义，只会让下游的
// 解析器与终端产生意料之外的行为。
func sanitizeMultiLine(s string) string {
	if s == "" {
		return ""
	}
	s = stripEscapeSequences(s)
	// 先把换行统一成 LF，避免 CRLF 与单独的 CR 在正文里留下孤立回车。
	if strings.ContainsAny(s, "\r\u2028\u2029") {
		s = strings.ReplaceAll(s, "\r\n", "\n")
		s = strings.ReplaceAll(s, "\r", "\n")
		s = strings.ReplaceAll(s, "\u2028", "\n")
		s = strings.ReplaceAll(s, "\u2029", "\n")
	}
	if !hasControlRune(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r == '\n' || r == '\t' {
			b.WriteRune(r)
			continue
		}
		if isControlRune(r) {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// stripEscapeSequences 移除 ANSI 转义序列。
//
// 只剥掉 ESC 会留下一串裸参数：`\x1b[31m` 变成 `[31m`，看起来像正文，
// 在日志与终端里仍然是噪音。这里按序列整体处理：
//
//   - CSI（`ESC [`）：吃掉参数字节直到终止字节（0x40–0x7e），光标移动、
//     清屏、颜色都属于这一类；
//   - OSC（`ESC ]`）：吃掉直到 BEL 或 `ESC \`，终端标题改写属于这一类；
//   - 其它双字节转义：只丢掉 ESC 本身，保留后面的字符——正文里出现孤立的
//     ESC 更可能是编码错误，把下一个字符一起吃掉会误伤真实内容。
func stripEscapeSequences(s string) string {
	if !strings.ContainsRune(s, 0x1b) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != 0x1b {
			b.WriteByte(c)
			continue
		}
		// ESC 之后没有内容：直接丢掉。
		if i+1 >= len(s) {
			break
		}
		switch s[i+1] {
		case '[':
			j := i + 2
			for j < len(s) && (s[j] < 0x40 || s[j] > 0x7e) {
				j++
			}
			i = j // 跳过终止字节（循环体末尾还会自增）
		case ']':
			j := i + 2
			for j < len(s) {
				if s[j] == 0x07 {
					break
				}
				if s[j] == 0x1b && j+1 < len(s) && s[j+1] == '\\' {
					j++
					break
				}
				j++
			}
			i = j
		default:
			// 双字节转义：只丢 ESC，下一个字符按普通文本处理。
		}
	}
	return b.String()
}

// isControlRune 判断是否为不可见控制字符：C0（含 \n、\t、\r，由调用方决定是否保留）、
// DEL 与 C1 控制字符。
func isControlRune(r rune) bool {
	return r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f)
}

// isLineSeparator 判断是否为 Unicode 的行/段分隔符（U+2028 / U+2029），
// 部分客户端会把它们当成换行，因此单行文本里同样不能留。
func isLineSeparator(r rune) bool {
	return r == '\u2028' || r == '\u2029'
}

// isSpaceRune 判断是否为空白字符（含全角空格），单行文本里统一成一个半角空格。
func isSpaceRune(r rune) bool {
	switch r {
	case ' ', '\t', '\v', '\f', '\u0085', '\u00a0', '\u3000':
		return true
	}
	return false
}

// hasControlRune 快速判断字符串里是否有需要清理的控制字符，
// 让常见的「干净文本」直接返回原串，不必重新拷贝。
func hasControlRune(s string) bool {
	for _, r := range s {
		if isControlRune(r) || isLineSeparator(r) {
			return true
		}
	}
	return false
}
