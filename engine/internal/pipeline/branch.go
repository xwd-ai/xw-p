package pipeline

import (
	"fmt"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// 条件分支（Phase 2「条件分支与失败短路」）。
//
// 步骤可通过 Config 的 "if" 键声明一个或多个分支条件，语义与 filter 阶段
// 的条件对象一致（field/operator/value）。区别在于：
//   - filter 阶段作用于「过滤」——条件不满足则跳过，且不会影响后续步骤；
//   - 分支条件作用于「是否执行本步骤」——条件为真才执行，为假则跳过本步骤。
//
// "if" 支持三种写法（向后兼容，缺省表示无条件执行）：
//   - 未设置 / 空：无条件执行。
//   - 单个条件对象：map[string]any。
//   - 条件对象数组：[]any，默认按「与」逻辑串联，全部满足才执行。
//
// 所有条件解析复用 filter.go 的 parseCondition 与 filterStage.match，
// 保证两处求值语义一致。

// branchStage 在 filter 阶段之后、transform 之前判定步骤是否满足分支条件。
// 不满足时标记 Skipped（SkipReason="condition"）并通过 errSkipStep 短路本步骤。
type branchStage struct {
	fs *filterStage
}

// NewBranchStage 创建条件分支阶段。
func NewBranchStage() Stage {
	return &branchStage{fs: &filterStage{vs: newVariableSystem()}}
}

// Apply 求值步骤的分支条件；不满足则标记跳过并短路本步骤。
func (b *branchStage) Apply(ctx *Context, step domain.Step) error {
	conds, err := branchConditions(step)
	if err != nil {
		ctx.Results[step.ID] = StepResult{
			StepID:     step.ID,
			StepType:   domain.StepType(step.Type),
			Channel:    step.Type,
			Skipped:    true,
			SkipReason: "condition",
			Error:      err.Error(),
		}
		return errSkipStep
	}
	if len(conds) == 0 {
		return nil // 无分支条件，放行
	}

	for _, cond := range conds {
		if !b.fs.match(ctx, cond) {
			ctx.Results[step.ID] = StepResult{
				StepID:     step.ID,
				StepType:   domain.StepType(step.Type),
				Channel:    step.Type,
				Skipped:    true,
				SkipReason: "condition",
			}
			return errSkipStep
		}
	}
	return nil
}

// branchConditions 从步骤 Config 解析分支条件。
//
// 返回值：
//   - 无 "if" 键或为空 → nil（无条件执行）。
//   - "if" 为 map → 单条件。
//   - "if" 为 []any → 条件列表（与逻辑）。
//   - 其他类型 → 返回错误（视为非法分支声明）。
func branchConditions(step domain.Step) ([]filterCondition, error) {
	raw, ok := step.Config["if"]
	if !ok || raw == nil {
		return nil, nil
	}

	switch v := raw.(type) {
	case map[string]any:
		cond, err := parseCondition(v)
		if err != nil {
			return nil, fmt.Errorf("分支条件解析失败: %w", err)
		}
		return []filterCondition{cond}, nil

	case []any:
		if len(v) == 0 {
			return nil, nil
		}
		conds := make([]filterCondition, 0, len(v))
		for _, item := range v {
			cond, err := parseCondition(item)
			if err != nil {
				return nil, fmt.Errorf("分支条件解析失败: %w", err)
			}
			conds = append(conds, cond)
		}
		return conds, nil

	default:
		return nil, fmt.Errorf("非法分支条件类型: %T", raw)
	}
}
