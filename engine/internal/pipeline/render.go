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
	return newRenderStage()
}

// newRenderStage 创建渲染阶段并返回具体类型，供管线在编排步骤中复用同一实例
// （digest 需要直接调用它的 Render 渲染卡片文案）。
func newRenderStage() *renderStage {
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

	// 安全沙箱：渲染前校验模板大小，超大模板直接失败。
	rendered, err := r.Render(ctx, tmpl)
	if err != nil {
		ctx.Results[step.ID] = StepResult{
			StepID:   step.ID,
			StepType: domain.StepType(step.Type),
			Channel:  step.Type,
			Error:    err.Error(),
		}
		return err
	}

	ctx.Results[step.ID] = StepResult{
		StepID:   step.ID,
		StepType: domain.StepType(step.Type),
		Channel:  step.Type,
		Rendered: rendered,
	}
	return nil
}

// Render 渲染一段模板文本并返回结果。
//
// 抽成独立方法供编排步骤复用：digest 步骤需要在聚合计数据算完之后，
// 用注入的 digest.* 变量渲染卡片标题与正文，但它的产出并不走
// 「一阶段一结果」的常规流程（否则同一步骤的聚合元数据会被覆盖）。
func (r *renderStage) Render(ctx *Context, tmpl string) (string, error) {
	if tmpl == "" {
		return "", nil
	}
	// 安全沙箱：渲染前校验模板大小，超大模板直接失败。
	if err := validateTemplate(tmpl); err != nil {
		return "", err
	}
	return r.expr.ReplaceAllStringFunc(tmpl, func(m string) string {
		inner := strings.TrimSpace(r.expr.FindStringSubmatch(m)[1])
		return r.eval(ctx, inner, 0)
	}), nil
}

// eval 求值一个模板表达式：若含空格则视为函数调用，否则视为变量替换。
// depth 为递归深度，超过上限则原样保留表达式（防相互引用导致栈溢出）。
func (r *renderStage) eval(ctx *Context, inner string, depth int) string {
	if depth > maxRenderDepth {
		return "{{ " + inner + " }}"
	}

	// 函数调用：第一个 token 为函数名，其余为参数（参数先做变量替换）。
	if tokens := splitExpr(inner); len(tokens) > 1 {
		name := tokens[0]
		args := make([]string, 0, len(tokens)-1)
		for _, arg := range tokens[1:] {
			args = append(args, r.eval(ctx, arg, depth+1))
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
