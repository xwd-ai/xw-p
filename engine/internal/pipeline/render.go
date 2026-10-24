package pipeline

import (
	"regexp"
	"strings"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// renderStage 是管线第三阶段：用变量渲染步骤模板。
//
// 骨架实现为「{{ var }}」占位符的简单替换：从步骤 Config 中读取 "template"
// 字符串，将其中的 {{ var }} 替换为上下文变量值。变量名与 payload 展开后的
// 键一致（如 payload.order_id 或系统变量 $workflow）。
//
// 安全沙箱、内置函数、缺失变量报错策略在 Phase 2 的「变量替换渲染」
// 任务中完善；当前对缺失变量原样保留占位符，便于骨架阶段先行打通链路。
type renderStage struct {
	// placeholders 匹配 {{ 变量名 }}，变量名允许字母/数字/下划线/点/美元符。
	placeholders *regexp.Regexp
}

// NewRenderStage 创建渲染阶段。
func NewRenderStage() Stage {
	return &renderStage{
		placeholders: regexp.MustCompile(`\{\{\s*([A-Za-z0-9_.$]+)\s*\}\}`),
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

	rendered := r.placeholders.ReplaceAllStringFunc(tmpl, func(m string) string {
		name := r.placeholders.FindStringSubmatch(m)[1]
		if v, ok := ctx.Get(name); ok {
			return stringify(v)
		}
		return m // 缺失变量原样保留，待 任务 11 完善错误策略
	})

	ctx.Results[step.ID] = StepResult{
		StepID:   step.ID,
		StepType: domain.StepType(step.Type),
		Channel:  step.Type,
		Rendered: rendered,
	}
	return nil
}

// stringify 将变量值转成字符串用于模板替换。
func stringify(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	default:
		// 数值/布尔等基础类型直接拼接；复杂结构用 fmt 兜底。
		return strings.TrimSpace(anyToString(t))
	}
}
