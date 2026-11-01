package pipeline

import (
	"html"
	"regexp"
	"strings"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
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

// Apply 渲染当前步骤的正文模板与标题模板并写回 Results。
//
// 标题（email 的 subject、push/in_app 的 title）与正文走同一套变量与内置函数：
// 社区场景里标题几乎总要带上帖子名或互动者，只渲染正文会让收件箱里堆满
// 「《{{ payload.post_title }}》有新评论」这种没被替换的占位符。
func (r *renderStage) Apply(ctx *Context, step domain.Step) error {
	res := StepResult{
		StepID:   step.ID,
		StepType: domain.StepType(step.Type),
		Channel:  step.Type,
	}

	subject, err := r.Render(ctx, stepHeaderTemplate(step))
	if err != nil {
		res.Error = err.Error()
		ctx.Results[step.ID] = res
		return err
	}
	res.RenderedSubject = subject

	tmpl, _ := step.Config["template"].(string)
	if tmpl == "" {
		// 无正文模板的步骤（如 delay）只可能有标题，渲染结果留空；
		// 只配了 html 模板的 email 步骤仍要渲染 HTML 正文。
		if err := r.renderEmailHTML(ctx, step, &res); err != nil {
			res.Error = err.Error()
			ctx.Results[step.ID] = res
			return err
		}
		ctx.Results[step.ID] = res
		return nil
	}

	// 安全沙箱：渲染前校验模板大小，超大模板直接失败。
	rendered, err := r.Render(ctx, tmpl)
	if err != nil {
		res.Error = err.Error()
		ctx.Results[step.ID] = res
		return err
	}
	res.Rendered = rendered

	// email 步骤可以额外产出 HTML 正文：要么由步骤的 html 模板渲染，要么把
	// 纯文本结果套进内置版式。两者都失败时按步骤失败处理，不静默降级——
	// 排版丢了大半内容的邮件比没发出去更难排查。
	if err := r.renderEmailHTML(ctx, step, &res); err != nil {
		res.Error = err.Error()
		ctx.Results[step.ID] = res
		return err
	}
	ctx.Results[step.ID] = res
	return nil
}

// renderEmailHTML 为 email 步骤补齐 HTML 正文，非 email 步骤直接跳过。
//
// 两条来源的优先级：步骤配置了 html 模板就用它（原样输出，只转义变量），
// 否则看 format 是否要求 html——要求时把已渲染的纯文本套进内置版式，
// 让「只写了纯文本模板」的工作流也能发出一封有排版的邮件。
func (r *renderStage) renderEmailHTML(ctx *Context, step domain.Step, res *StepResult) error {
	if domain.Channel(step.Type) != domain.ChannelEmail {
		return nil
	}
	// 显式关闭：某些通知（如纯文本回执）不希望被客户端改写版式。
	if isPlainTextFormat(step) {
		return nil
	}
	if tmpl, ok := step.Config["html"].(string); ok && strings.TrimSpace(tmpl) != "" {
		out, err := r.RenderHTML(ctx, tmpl)
		if err != nil {
			return err
		}
		res.RenderedHTML = out
		return nil
	}
	if !wantsHTML(step) || strings.TrimSpace(res.Rendered) == "" {
		return nil
	}
	res.RenderedHTML = emailHTMLLayout(res.RenderedSubject, res.Rendered)
	return nil
}

// isPlainTextFormat 判断步骤是否显式要求纯文本（format=text/plain）。
func isPlainTextFormat(step domain.Step) bool {
	f, ok := step.Config["format"].(string)
	if !ok {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(f)) {
	case "text", "plain", "text/plain":
		return true
	}
	return false
}

// wantsHTML 判断步骤是否要产出 HTML：format=html 时套用内置版式。
func wantsHTML(step domain.Step) bool {
	f, ok := step.Config["format"].(string)
	if !ok {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(f)) {
	case "html", "text/html":
		return true
	}
	return false
}

// stepHeaderTemplate 返回步骤声明的标题模板：email 用 subject、push/in_app 用 title。
func stepHeaderTemplate(step domain.Step) string {
	for _, key := range []string{"subject", "title"} {
		if s, ok := step.Config[key].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

// Render 渲染一段模板文本并返回结果。
//
// 抽成独立方法供编排步骤复用：digest 步骤需要在聚合计数据算完之后，
// 用注入的 digest.* 变量渲染卡片标题与正文，但它的产出并不走
// 「一阶段一结果」的常规流程（否则同一步骤的聚合元数据会被覆盖）。
func (r *renderStage) Render(ctx *Context, tmpl string) (string, error) {
	return r.render(ctx, tmpl, false)
}

// RenderHTML 渲染一段 HTML 模板。
//
// 与 Render 的唯一区别是变量值按 HTML 转义后再拼进去：模板自身的标签原样
// 保留，而 payload 里的用户内容（评论、帖子标题、昵称）即使带 `<script>`
// 也只会显示成文本。帖子标题与评论是最典型的用户可控输入，不转义就等于把
// 用户内容当页面代码执行。
func (r *renderStage) RenderHTML(ctx *Context, tmpl string) (string, error) {
	return r.render(ctx, tmpl, true)
}

// render 按需转义变量值地渲染模板。
func (r *renderStage) render(ctx *Context, tmpl string, escape bool) (string, error) {
	if tmpl == "" {
		return "", nil
	}
	// 安全沙箱：渲染前校验模板大小，超大模板直接失败。
	if err := validateTemplate(tmpl); err != nil {
		return "", err
	}
	return r.expr.ReplaceAllStringFunc(tmpl, func(m string) string {
		inner := strings.TrimSpace(r.expr.FindStringSubmatch(m)[1])
		value, ok := r.eval(ctx, inner, 0)
		// 缺失变量保留占位符原样（其中没有需要转义的字符），由上游决定如何处理。
		if ok && escape {
			return html.EscapeString(value)
		}
		return value
	}), nil
}

// eval 求值一个模板表达式：若含空格则视为函数调用，否则视为变量替换。
// depth 为递归深度，超过上限则原样保留表达式（防相互引用导致栈溢出）。
//
// 第二个返回值表示表达式是否真的解析出了值：模板按 HTML 渲染时只转义
// 解析成功的部分，保留下来的占位符不该被二次转义。
func (r *renderStage) eval(ctx *Context, inner string, depth int) (string, bool) {
	if depth > maxRenderDepth {
		return "{{ " + inner + " }}", false
	}

	// 函数调用：第一个 token 为函数名，其余为参数（参数先做变量替换）。
	if tokens := splitExpr(inner); len(tokens) > 1 {
		name := tokens[0]
		args := make([]string, 0, len(tokens)-1)
		for _, arg := range tokens[1:] {
			value, _ := r.eval(ctx, arg, depth+1)
			args = append(args, value)
		}
		if out, err := r.vs.call(name, args); err == nil {
			return out, true
		}
		// 未知函数：原样保留。
		return "{{ " + inner + " }}", false
	}

	// 变量替换。
	if v, ok := r.vs.resolve(ctx, inner); ok {
		return stringify(v), true
	}
	return "{{ " + inner + " }}", false // 缺失变量原样保留
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
