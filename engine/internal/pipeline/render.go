package pipeline

import (
	"regexp"
	"strings"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// renderStage 是管线第三阶段：用变量渲染步骤模板。
//
// 支持两种模板表达式：
//   - 变量替换「{{ var }}」：var 可为 payload 嵌套变量（payload.items[0].name）、
//     系统变量（$workflow）或已展开的 payload.<key>。
//   - 内置函数「{{ fn arg... }}」：如 {{ upper payload.customer_name }}、
//     {{ format_amount payload.amount }}，参数本身也可为变量。
//
// 缺失变量原样保留占位符（错误策略见 render 实现）。
type renderStage struct {
	// expr 匹配模板表达式：变量或函数调用，允许字母/数字/下划线/点/美元符/方括号。
	expr *regexp.Regexp
	vs   *variableSystem
}

// NewRenderStage 创建渲染阶段。
func NewRenderStage() Stage {
	return &renderStage{
		expr: regexp.MustCompile(`\{\{\s*([^{}]+?)\s*\}\}`),
		vs:   newVariableSystem(),
	}
}

// Apply 渲染当前步骤的模板并写回 Results。
func (r *renderStage) Apply(ctx *Context, step domain.Step) error {
	tmpl, _ := step.Config["template"].(string)
	if tmpl == "" {
		// 无模板步骤（如 delay），渲染结果留空。
		ctx.Results[step.ID] = StepResult{
			StepID:   step.ID,
			StepType: domain.StepType(step.Type),
			Channel:  step.Type,
		}
		return nil
	}

	rendered := r.expr.ReplaceAllStringFunc(tmpl, func(m string) string {
		inner := strings.TrimSpace(r.expr.FindStringSubmatch(m)[1])
		return r.eval(ctx, inner)
	})

	ctx.Results[step.ID] = StepResult{
		StepID:   step.ID,
		StepType: domain.StepType(step.Type),
		Channel:  step.Type,
		Rendered: rendered,
	}
	return nil
}

// eval 求值一个模板表达式：若含空格则视为函数调用，否则视为变量替换。
func (r *renderStage) eval(ctx *Context, inner string) string {
	// 函数调用：第一个 token 为函数名，其余为参数（参数先做变量替换）。
	if tokens := splitExpr(inner); len(tokens) > 1 {
		name := tokens[0]
		args := make([]string, 0, len(tokens)-1)
		for _, arg := range tokens[1:] {
			args = append(args, r.eval(ctx, arg))
		}
		if out, err := r.vs.call(name, args); err == nil {
			return out
		}
		// 未知函数：原样保留。
		return "{{ " + inner + " }}"
	}

	// 变量替换。
	if v, ok := r.vs.resolve(ctx, inner); ok {
		return stringify(v)
	}
	return "{{ " + inner + " }}" // 缺失变量原样保留
}

// splitExpr 按空白切分表达式 token（函数名与参数）。
func splitExpr(inner string) []string {
	return strings.Fields(inner)
}

// stringify 将变量值转成字符串用于模板替换。
func stringify(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	default:
		return strings.TrimSpace(anyToString(t))
	}
}
