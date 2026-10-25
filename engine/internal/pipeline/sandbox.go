package pipeline

import (
	"fmt"
	"strings"
)

// 安全沙箱（Phase 2「变量替换渲染 · 安全沙箱」）：
//
// 模板渲染是用户可控输入进入执行链的关键点，须防止以下风险：
//
//  1. 越权访问：模板变量路径可能试图访问内部字段或非法结构，
//     如 "__class__"、空段（".."）、越界数组下标等。
//  2. 资源耗尽：超大模板或深度嵌套表达式可能导致 CPU/内存被拖垮（DoS）。
//  3. 非法函数：调用未注册的函数或系统命令。
//
// 本文件集中实现路径与表达式级的安全校验，供变量解析与渲染阶段复用。
// 函数白名单由 variableSystem.functions 天然提供（只有注册过的函数才可调用）。

const (
	// maxRenderDepth 限制模板表达式递归求值的最大深度，防止相互引用导致栈溢出。
	maxRenderDepth = 32

	// maxTemplateBytes 限制单条模板的最大字节数，防止超大模板拖垮渲染。
	maxTemplateBytes = 64 * 1024 // 64 KiB

	// maxPathSegments 限制变量路径的最大段数，防止超长路径遍历。
	maxPathSegments = 64
)

// validateTemplate 对模板做渲染前安全检查：非空与大小上限。
func validateTemplate(tmpl string) error {
	if len(tmpl) > maxTemplateBytes {
		return fmt.Errorf("模板超过大小上限 %d 字节", maxTemplateBytes)
	}
	return nil
}

// safePathSegment 判断单个路径段是否安全可访问。
//
// 拒绝规则：
//   - 空段（由 ".." 或前导点产生）；
//   - 以 "__" 开头（内部/魔术字段，如 __class__、__dict__）；
//   - 包含控制字符（防御注入）。
//
// 合法的 payload 字段名通常为字母/数字/下划线/中划线，这里只做「拒绝危险」而非「白名单」，
// 以兼容业务字段的多样性（如 "order-id"、"中文键"）。
func safePathSegment(seg string) error {
	if seg == "" {
		return fmt.Errorf("路径含空段")
	}
	if strings.HasPrefix(seg, "__") {
		return fmt.Errorf("禁止访问内部字段 %q", seg)
	}
	for _, r := range seg {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("路径含非法控制字符")
		}
	}
	return nil
}

// safeSplitPath 与 splitPath 行为一致，但会校验每个段的安全性，
// 并限制段数。返回错误时调用方应拒绝整个路径解析。
func safeSplitPath(path string) ([]string, error) {
	if path == "" {
		return nil, fmt.Errorf("路径为空")
	}
	segs := splitPath(path)
	if len(segs) == 0 {
		return nil, fmt.Errorf("路径无有效段")
	}
	if len(segs) > maxPathSegments {
		return nil, fmt.Errorf("路径段数超过上限 %d", maxPathSegments)
	}
	for _, seg := range segs {
		if err := safePathSegment(seg); err != nil {
			return nil, err
		}
	}
	return segs, nil
}

// safeLookupPath 是对 lookupPath 的安全封装：先校验路径，再取值。
// 校验失败返回 ok=false，调用方按「变量缺失」处理（原样保留占位符）。
func safeLookupPath(root any, path string) (any, bool) {
	segs, err := safeSplitPath(path)
	if err != nil {
		return nil, false
	}
	cur := root
	for _, seg := range segs {
		cur = stepInto(cur, seg)
		if cur == nil {
			return nil, false
		}
	}
	if cur == nil {
		return nil, false
	}
	return cur, true
}
