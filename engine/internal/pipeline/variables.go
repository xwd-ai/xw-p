package pipeline

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// 变量系统（Phase 2）：
//
// 管线中的模板变量分三类：
//
//  1. payload 变量：来自触发消息的原始负载，以 "payload." 前缀访问，
//     支持嵌套字段与数组下标，如 payload.order_id、payload.items[0].name。
//  2. 系统变量：以 "$" 前缀访问，由引擎在 transform 阶段注入，如 $workflow、
//     $subscriber_id、$trigger_id、$timestamp 等。
//  3. 内置函数：以 "{{ fn arg... }}" 形式调用，如 {{ upper payload.customer_name }}、
//     {{ format_amount payload.amount }}，用于对变量做格式化与变换。
//
// 变量解析与函数求值均在此处集中实现，供 render 阶段调用。

// builtinFunc 是内置函数的签名：接收渲染后的字符串参数，返回字符串结果。
type builtinFunc func(args []string) (string, error)

// variableSystem 承载变量解析与内置函数注册表，供管线各阶段复用。
type variableSystem struct {
	functions map[string]builtinFunc
	now       func() time.Time
}

// newVariableSystem 创建变量系统并注册内置函数。
func newVariableSystem() *variableSystem {
	vs := &variableSystem{
		functions: make(map[string]builtinFunc),
		now:       time.Now,
	}
	vs.registerBuiltins()
	return vs
}

// registerBuiltins 注册全部内置函数。
func (vs *variableSystem) registerBuiltins() {
	vs.functions["upper"] = func(args []string) (string, error) {
		return strings.ToUpper(args[0]), nil
	}
	vs.functions["lower"] = func(args []string) (string, error) {
		return strings.ToLower(args[0]), nil
	}
	vs.functions["trim"] = func(args []string) (string, error) {
		return strings.TrimSpace(args[0]), nil
	}
	vs.functions["default"] = func(args []string) (string, error) {
		if args[0] == "" {
			return args[1], nil
		}
		return args[0], nil
	}
	vs.functions["format_amount"] = func(args []string) (string, error) {
		f, err := strconv.ParseFloat(strings.TrimSpace(args[0]), 64)
		if err != nil {
			return args[0], nil // 非数值原样返回
		}
		return fmt.Sprintf("%.2f", f), nil
	}
	vs.functions["format_date"] = func(args []string) (string, error) {
		layout := "2006-01-02 15:04:05"
		if len(args) > 1 && args[1] != "" {
			layout = args[1]
		}
		t, err := parseAnyTime(args[0])
		if err != nil {
			return args[0], nil
		}
		return t.Format(layout), nil
	}
	vs.functions["now"] = func(args []string) (string, error) {
		layout := time.RFC3339
		if len(args) > 0 && args[0] != "" {
			layout = args[0]
		}
		return vs.now().Format(layout), nil
	}
	vs.functions["length"] = func(args []string) (string, error) {
		return strconv.Itoa(len([]rune(args[0]))), nil
	}
}

// resolve 按名称解析变量值。支持：
//   - "$" 前缀：系统变量（直接查变量表）。
//   - "payload." 前缀：嵌套访问原始 payload。
//   - 其他：查变量表（兼容已展开的 payload.<key>）。
func (vs *variableSystem) resolve(ctx *Context, name string) (any, bool) {
	if name == "" {
		return nil, false
	}
	if strings.HasPrefix(name, "payload.") {
		path := strings.TrimPrefix(name, "payload.")
		v, ok := lookupPath(ctx.Msg.Payload, path)
		if !ok {
			// 回退：查已展开的变量表（保持向后兼容）。
			return ctx.Get(name)
		}
		return v, true
	}
	if strings.HasPrefix(name, "$") {
		return ctx.Get(name)
	}
	return ctx.Get(name)
}

// lookupPath 在嵌套结构中按点路径/数组下标取值。
// 支持 map[string]any 与 []any，如 "items[0].name"。
func lookupPath(root any, path string) (any, bool) {
	segments := splitPath(path)
	cur := root
	for _, seg := range segments {
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

// splitPath 把 "items[0].name" 拆成 ["items", "0", "name"]。
func splitPath(path string) []string {
	var out []string
	var buf strings.Builder
	for i := 0; i < len(path); i++ {
		c := path[i]
		switch {
		case c == '.':
			if buf.Len() > 0 {
				out = append(out, buf.String())
				buf.Reset()
			}
		case c == '[':
			if buf.Len() > 0 {
				out = append(out, buf.String())
				buf.Reset()
			}
			j := strings.IndexByte(path[i:], ']')
			if j < 0 {
				return nil
			}
			out = append(out, path[i+1:i+j])
			i += j
		default:
			buf.WriteByte(c)
		}
	}
	if buf.Len() > 0 {
		out = append(out, buf.String())
	}
	return out
}

// stepInto 前进一层：map 按键、slice 按下标。
func stepInto(cur any, seg string) any {
	switch v := cur.(type) {
	case map[string]any:
		val, ok := v[seg]
		if !ok {
			return nil
		}
		return val
	case []any:
		idx, err := strconv.Atoi(seg)
		if err != nil || idx < 0 || idx >= len(v) {
			return nil
		}
		return v[idx]
	default:
		return nil
	}
}

// call 求值一个内置函数调用。
func (vs *variableSystem) call(name string, args []string) (string, error) {
	fn, ok := vs.functions[name]
	if !ok {
		return "", fmt.Errorf("未知内置函数: %s", name)
	}
	return fn(args)
}

// funcNames 返回已注册函数名（排序后），用于诊断与文档。
func (vs *variableSystem) funcNames() []string {
	names := make([]string, 0, len(vs.functions))
	for k := range vs.functions {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// parseAnyTime 宽松解析常见时间字符串，失败返回 error。
func parseAnyTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if ts, err := strconv.ParseInt(s, 10, 64); err == nil {
		// Unix 时间戳（秒）。
		return time.Unix(ts, 0), nil
	}
	for _, layout := range []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("无法解析时间: %s", s)
}
