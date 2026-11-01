package mailer

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"strings"
	"time"
)

// Message 是一封待提交的邮件。
//
// 只保留 SMTP 提交必需的三要素（发件人、收件人、主题与正文）；模板渲染与
// 附件属于上层职责，客户端不参与内容拼装之外的决策。正文可同时给出纯文本
// 与 HTML 两种形态：两者都在时按 multipart/alternative 提交，由邮件客户端
// 挑一种能显示的呈现，纯文本只是没写 HTML 时的唯一形态。
type Message struct {
	// From 是发件人，可带显示名：`XW 通知 <notify@example.com>`。
	From string
	// To 是收件人列表，至少一个。
	To []string
	// Subject 是主题，非 ASCII 内容按 RFC 2047 编码。
	Subject string
	// Text 是纯文本正文，按 UTF-8 + base64 传输。
	//
	// 与 HTML 同时存在时它是降级版本：只认纯文本的客户端看到的仍是可读内容。
	Text string
	// HTML 是 HTML 正文，为空时邮件为纯文本单段。
	HTML string
	// ReplyTo 是可选的回信地址。
	ReplyTo string
	// MessageID 为空时自动生成，便于在服务器日志里追踪同一封邮件。
	MessageID string
}

// normalize 校验并清理消息字段，返回信封地址。
//
// 头字段里出现 CR/LF 意味着可以往邮件里塞额外的头（例如悄悄加一个 Bcc），
// 因此在拼接报文之前就拒绝，而不是依赖下游转义。
func (m Message) normalize() (envelopeFrom string, recipients []string, err error) {
	from, err := addrSpec(m.From)
	if err != nil {
		return "", nil, fmt.Errorf("发件人地址不合法：%w", err)
	}
	if err := checkHeaderText("主题", m.Subject); err != nil {
		return "", nil, err
	}
	if m.ReplyTo != "" {
		if _, err := addrSpec(m.ReplyTo); err != nil {
			return "", nil, fmt.Errorf("回信地址不合法：%w", err)
		}
	}

	seen := make(map[string]bool, len(m.To))
	for _, raw := range m.To {
		if err := checkHeaderText("收件人", raw); err != nil {
			return "", nil, err
		}
		addr, err := addrSpec(raw)
		if err != nil {
			return "", nil, fmt.Errorf("收件人地址不合法：%w", err)
		}
		if seen[addr] {
			continue // 同一封邮件里重复的收件人只投一次
		}
		seen[addr] = true
		recipients = append(recipients, addr)
	}
	if len(recipients) == 0 {
		return "", nil, errors.New("没有收件人")
	}
	return from, recipients, nil
}

// build 生成待写入 DATA 段的完整报文（头 + 空行 + 正文）。
//
// 没有 HTML 正文时保持单段 text/plain——既能少一层 MIME 解析，也让「只发
// 纯文本」的老路径在报文层面完全不变。有 HTML 时改为 multipart/alternative，
// 纯文本在前、HTML 在后：按 RFC 2046 的约定，客户端从后往前挑第一个能显示
// 的形态，因此支持 HTML 的客户端看到排版版本，其余客户端看到纯文本。
func (m Message) build(envelopeFrom string, recipients []string, now time.Time) string {
	var b strings.Builder
	writeHeader(&b, "From", formatAddress(m.From))
	writeHeader(&b, "To", joinAddresses(recipients))
	if m.ReplyTo != "" {
		writeHeader(&b, "Reply-To", formatAddress(m.ReplyTo))
	}
	writeHeader(&b, "Subject", mime.QEncoding.Encode("utf-8", m.Subject))
	writeHeader(&b, "Date", now.Format(time.RFC1123Z))
	writeHeader(&b, "Message-ID", m.messageID(envelopeFrom, now))
	writeHeader(&b, "MIME-Version", "1.0")

	html := strings.TrimSpace(m.HTML)
	if html == "" {
		writeHeader(&b, "Content-Type", "text/plain; charset=utf-8")
		writeHeader(&b, "Content-Transfer-Encoding", "base64")
		b.WriteString("\r\n")
		b.WriteString(encodeBase64Body(m.Text))
		return b.String()
	}

	boundary := m.boundary(now)
	writeHeader(&b, "Content-Type", `multipart/alternative; boundary="`+boundary+`"`)
	b.WriteString("\r\n")
	writePart(&b, boundary, "text/plain; charset=utf-8", m.Text)
	writePart(&b, boundary, "text/html; charset=utf-8", html)
	b.WriteString("--")
	b.WriteString(boundary)
	b.WriteString("--\r\n")
	return b.String()
}

// writePart 写一个 base64 编码的 MIME 段（含分隔行与段头）。
func writePart(b *strings.Builder, boundary, contentType, body string) {
	b.WriteString("--")
	b.WriteString(boundary)
	b.WriteString("\r\n")
	writeHeader(b, "Content-Type", contentType)
	writeHeader(b, "Content-Transfer-Encoding", "base64")
	b.WriteString("\r\n")
	// encodeBase64Body 以 CRLF 收尾，下一段的分隔行因此自然另起一行。
	b.WriteString(encodeBase64Body(body))
}

// encodeBase64Body 把正文按 UTF-8 编码成 76 列折行的 base64。
func encodeBase64Body(text string) string {
	return wrapBase64(base64.StdEncoding.EncodeToString([]byte(text)))
}

// boundary 生成多段报文的边界串。
//
// 边界以 `=_` 开头且含有 base64 字母表之外的 `-`，正文又是 base64 编码，
// 因此天然不会与内容撞车；随机部分让同一进程连续发出的邮件边界互不相同。
func (m Message) boundary(now time.Time) string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		// 取不到随机数不影响投递：退化为时间戳，仍带有 `=`/`-` 两个非 base64 字符。
		return fmt.Sprintf("=_xw_%d", now.UnixNano())
	}
	return "=_xw_" + hex.EncodeToString(buf)
}

// messageID 生成 `<时间.随机串@域>` 形式的标识；域取发件人域名，缺省用 localhost。
func (m Message) messageID(envelopeFrom string, now time.Time) string {
	if id := strings.TrimSpace(m.MessageID); id != "" {
		return id
	}
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		// 取不到随机数不影响投递，退化为纳秒时间戳。
		buf = []byte(fmt.Sprintf("%016x", now.UnixNano()))
	}
	domain := "localhost"
	if at := strings.LastIndex(envelopeFrom, "@"); at >= 0 && at < len(envelopeFrom)-1 {
		domain = envelopeFrom[at+1:]
	}
	return fmt.Sprintf("<%d.%s@%s>", now.UnixNano(), hex.EncodeToString(buf), domain)
}

// writeHeader 写一行头字段。
func writeHeader(b *strings.Builder, name, value string) {
	b.WriteString(name)
	b.WriteString(": ")
	b.WriteString(value)
	b.WriteString("\r\n")
}

// checkHeaderText 拒绝头字段里的换行符（防头注入）。
func checkHeaderText(field, value string) error {
	if strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("%s不能包含换行符", field)
	}
	return nil
}

// addrSpec 从 `名字 <a@b>` 或 `a@b` 里取出 addr-spec。
func addrSpec(value string) (string, error) {
	text := strings.TrimSpace(value)
	if text == "" {
		return "", errors.New("地址为空")
	}
	if err := checkHeaderText("地址", text); err != nil {
		return "", err
	}
	if open := strings.LastIndex(text, "<"); open >= 0 {
		rest := text[open+1:]
		closeAt := strings.Index(rest, ">")
		if closeAt < 0 {
			return "", fmt.Errorf("地址缺少右尖括号：%q", value)
		}
		text = strings.TrimSpace(rest[:closeAt])
	}
	if strings.ContainsAny(text, " \t<>\"") {
		return "", fmt.Errorf("地址含非法字符：%q", value)
	}
	at := strings.LastIndex(text, "@")
	if at <= 0 || at == len(text)-1 || strings.Count(text, "@") != 1 {
		return "", fmt.Errorf("地址格式不正确：%q", value)
	}
	return text, nil
}

// formatAddress 输出头字段里的地址；带显示名时按需编码显示名。
func formatAddress(value string) string {
	text := strings.TrimSpace(value)
	open := strings.LastIndex(text, "<")
	if open < 0 {
		return text
	}
	name := strings.TrimSpace(text[:open])
	if name == "" {
		return text
	}
	return mime.QEncoding.Encode("utf-8", strings.Trim(name, `"`)) + " " + text[open:]
}

// joinAddresses 把信封收件人拼成 To 头。
func joinAddresses(recipients []string) string {
	return strings.Join(recipients, ", ")
}

// wrapBase64 按 76 列折行（RFC 2045 对 base64 行长的要求）。
func wrapBase64(encoded string) string {
	const width = 76
	var b strings.Builder
	for len(encoded) > width {
		b.WriteString(encoded[:width])
		b.WriteString("\r\n")
		encoded = encoded[width:]
	}
	b.WriteString(encoded)
	b.WriteString("\r\n")
	return b.String()
}
