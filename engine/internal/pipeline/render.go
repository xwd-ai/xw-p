package pipeline

import (
	"fmt"
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
//   - 内置函数「{{ fn arg... }}」：如 {{ upper payload.author }}、
//     {{ format_amount payload.amount }}，参数本身也可为变量。
//
// 缺失变量原样保留占位符（错误策略见 render 实现）。
//
// 替换是单遍的：变量值拼进结果之后不会再被当作模板扫描一遍，因此负载里
// 写着「{{ upper payload.secret }}」的字段只会原样显示成一串花括号，
// 不会被求值。这条性质由 TestRenderDoesNotReexpandValues 锁定。
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

// renderForm 描述一段模板的产出形态，决定变量值如何清理（见 sanitize.go）。
type renderForm int

const (
	// formInline 是单行文本：邮件主题、推送与站内信标题。
	formInline renderForm = iota
	// formBlock 是多行正文。
	formBlock
	// formHTML 是 HTML 正文：变量值先清理控制字符再按 HTML 转义。
	formHTML
)

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

	subject, err := r.RenderInline(ctx, stepHeaderTemplate(step))
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

// Render 渲染一段多行正文模板并返回结果。
//
// 抽成独立方法供编排步骤复用：digest 步骤需要在聚合计数据算完之后，
// 用注入的 digest.* 变量渲染卡片正文，但它的产出并不走
// 「一阶段一结果」的常规流程（否则同一步骤的聚合元数据会被覆盖）。
func (r *renderStage) Render(ctx *Context, tmpl string) (string, error) {
	return r.render(ctx, tmpl, formBlock)
}

// RenderInline 渲染一段单行模板（邮件主题、推送标题、聚合卡片标题）。
//
// 与 Render 的区别只有清理强度：单行文本里的换行一律折成空格，
// 否则一个带换行的帖子标题就能往邮件里塞额外的头字段。
func (r *renderStage) RenderInline(ctx *Context, tmpl string) (string, error) {
	return r.render(ctx, tmpl, formInline)
}

// RenderHTML 渲染一段 HTML 模板。
//
// 与 Render 的第二个区别是变量值按 HTML 转义后再拼进去：模板自身的标签原样
// 保留，而 payload 里的用户内容（评论、帖子标题、昵称）即使带 `<script>`
// 也只会显示成文本。帖子标题与评论是最典型的用户可控输入，不转义就等于把
// 用户内容当页面代码执行。
func (r *renderStage) RenderHTML(ctx *Context, tmpl string) (string, error) {
	return r.render(ctx, tmpl, formHTML)
}

// render 按产出形态清理变量值地渲染模板。
func (r *renderStage) render(ctx *Context, tmpl string, form renderForm) (string, error) {
	if tmpl == "" {
		return "", nil
	}
	// 安全沙箱：渲染前校验模板大小，超大模板直接失败。
	if err := validateTemplate(tmpl); err != nil {
		return "", err
	}

	// 边替换边累计长度：等到拼完再检查，一个被放大的模板已经在内存里了。
	written := 0
	overflow := false
	out := r.expr.ReplaceAllStringFunc(tmpl, func(m string) string {
		if overflow {
			return m
		}
		inner := strings.TrimSpace(r.expr.FindStringSubmatch(m)[1])
		value, ok := r.eval(ctx, inner, 0)
		// 缺失变量保留占位符原样（其中没有需要清理的字符），由上游决定如何处理。
		if !ok {
			return value
		}
		value = cleanValue(value, form)
		written += len(value)
		if written > maxRenderedBytes {
			overflow = true
		}
		return value
	})
	if overflow || len(out) > maxRenderedBytes {
		return "", fmt.Errorf("渲染结果超过上限 %d 字节", maxRenderedBytes)
	}
	// 模板自身的文本也要过一遍：工作流里的标题可能是多行 YAML 写的，
	// 换行同样会破坏单行字段。
	return cleanTemplate(out, form), nil
}

// cleanValue 按产出形态清理一个变量值。
func cleanValue(value string, form renderForm) string {
	switch form {
	case formInline:
		return sanitizeSingleLine(value)
	case formHTML:
		// 先清理再转义：反过来的话控制字符会变成实体留在页面里。
		return html.EscapeString(sanitizeMultiLine(value))
	default:
		return sanitizeMultiLine(value)
	}
}

// cleanTemplate 清理整段渲染结果（模板文本 + 已清理的变量值）。
//
// HTML 形态只清控制字符、不转义：模板里的标签是作者写的，转义等于把
// 排版也一起变成文本。
func cleanTemplate(out string, form renderForm) string {
	if form == formInline {
		return sanitizeSingleLine(out)
	}
	return sanitizeMultiLine(out)
}

// hasTemplateExpr 判断文本里是否还残留模板表达式。
//
// 分发阶段用它拦住「没走渲染就拿来当标题」的兜底路径：把
// `{{ payload.post_title }}` 原样发给用户，等于把模板语法暴露出去。
func hasTemplateExpr(s string) bool {
	return strings.Contains(s, "{{") && strings.Contains(s, "}}")
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
