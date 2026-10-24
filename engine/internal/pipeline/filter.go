package pipeline

import "github.com/cdgjhg/xw-p/engine/internal/domain"

// filterStage 是管线第一阶段的骨架：根据步骤过滤条件决定是否跳过。
//
// 本阶段仅支持「存在性」判断——步骤 Config 中 filters 列表的每一项为一个
// payload 原始字段名（如 order_id），若该字段不存在或为空值则跳过该步骤。
// 完整的条件运算符（等于/包含/正则/数组）在 Phase 2 实现，届时在此扩展。
type filterStage struct{}

// NewFilterStage 创建过滤阶段。
func NewFilterStage() Stage {
	return filterStage{}
}

// Apply 对单个步骤执行过滤；不满足条件时在 Results 中标记 Skipped，
// 并通过返回 errSkipStep 让管线短路跳过后续阶段。
func (f filterStage) Apply(ctx *Context, step domain.Step) error {
	filters, ok := step.Config["filters"].([]any)
	if !ok || len(filters) == 0 {
		return nil // 无过滤条件，放行
	}

	for _, raw := range filters {
		field, ok := raw.(string)
		if !ok {
			continue
		}
		if !hasValue(ctx.Msg.Payload, field) {
			ctx.Results[step.ID] = StepResult{
				StepID:   step.ID,
				StepType: domain.StepType(step.Type),
				Channel:  step.Type,
				Skipped:  true,
			}
			return errSkipStep
		}
	}
	return nil
}

// hasValue 判断 payload 中指定字段是否存在且非空（存在性判断）。
func hasValue(payload map[string]any, field string) bool {
	v, ok := payload[field]
	if !ok {
		return false
	}
	switch t := v.(type) {
	case string:
		return t != ""
	case nil:
		return false
	default:
		return true
	}
}
