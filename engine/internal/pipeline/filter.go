package pipeline

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// filterStage 是管线第一阶段：根据步骤的过滤条件决定是否跳过该步骤。
//
// 支持两种过滤条件写法（向后兼容）：
//
//  1. 字符串字段名（如 "order_id"）：等价于 exists 存在性判断——
//     字段存在且非空则放行，否则跳过（Phase 2 的旧行为）。
//
//  2. 条件对象：map[string]any，形如：
//
//     { "field": "payload.order_id", "operator": "eq", "value": "A1001" }
//
//     operator 支持：
//       - "eq"：等于（数值做数值比较，其余转字符串比较）
//       - "ne"：不等于
//       - "contains"：字符串/数组包含
//       - "regex"：正则匹配（value 为 Go 正则表达式）
//       - "in"：字段值属于 value 列表
//       - "exists"：字段存在且非空
//
// field 支持 payload 嵌套路径（点号与数组下标），如 "payload.customer.name"。
// 所有条件默认按「与」逻辑串联：任一条件不满足即跳过该步骤。
type filterStage struct {
	vs *variableSystem
}

// NewFilterStage 创建过滤阶段。
func NewFilterStage() Stage {
	return &filterStage{vs: newVariableSystem()}
}

// Apply 对单个步骤执行过滤；不满足条件时在 Results 中标记 Skipped，
// 并通过返回 errSkipStep 让管线短路跳过后续阶段。
func (f *filterStage) Apply(ctx *Context, step domain.Step) error {
	filters, ok := step.Config["filters"].([]any)
	if !ok || len(filters) == 0 {
		return nil // 无过滤条件，放行
	}

	for _, raw := range filters {
		cond, err := parseCondition(raw)
		if err != nil {
			// 无法解析的过滤条件视为失败，直接跳过并记录错误原因。
			ctx.Results[step.ID] = StepResult{
				StepID:   step.ID,
				StepType: domain.StepType(step.Type),
				Channel:  step.Type,
				Skipped:  true,
				Error:    err.Error(),
			}
			return errSkipStep
		}
		if !f.match(ctx, cond) {
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

// filterCondition 是一次解析完成的过滤条件。
type filterCondition struct {
	field    string
	operator string
	value    any
}

// parseCondition 把 filter 列表中的原始项解析为过滤条件。
//
// 兼容两种写法：
//   - string：字段名 → exists 存在性判断。
//   - map：含 field/operator/value 键的条件对象（operator 缺省为 "eq"）。
func parseCondition(raw any) (filterCondition, error) {
	switch v := raw.(type) {
	case string:
		return filterCondition{field: v, operator: "exists"}, nil

	case map[string]any:
		field, _ := v["field"].(string)
		if field == "" {
			return filterCondition{}, fmt.Errorf("过滤条件缺少 field")
		}
		op, _ := v["operator"].(string)
		if op == "" {
			op = "eq"
		}
		if !validOperator(op) {
			return filterCondition{}, fmt.Errorf("非法过滤运算符: %q", op)
		}
		return filterCondition{field: field, operator: op, value: v["value"]}, nil

	default:
		return filterCondition{}, fmt.Errorf("无法解析过滤条件: %v", raw)
	}
}

// validOperator 判断运算符是否受支持。
func validOperator(op string) bool {
	switch op {
	case "eq", "ne", "contains", "regex", "in", "exists":
		return true
	default:
		return false
	}
}

// match 求值单个过滤条件。
func (f *filterStage) match(ctx *Context, cond filterCondition) bool {
	actual, ok := f.resolveField(ctx, cond.field)

	switch cond.operator {
	case "exists":
		return ok && !isEmptyValue(actual)

	case "eq":
		if !ok {
			return false
		}
		return equalValue(actual, cond.value)

	case "ne":
		if !ok {
			return true // 字段缺失视为「不等于」
		}
		return !equalValue(actual, cond.value)

	case "contains":
		if !ok {
			return false
		}
		return containsValue(actual, cond.value)

	case "regex":
		if !ok {
			return false
		}
		pattern := fmt.Sprintf("%v", cond.value)
		re, err := regexp.Compile(pattern)
		if err != nil {
			return false
		}
		return re.MatchString(stringify(actual))

	case "in":
		if !ok {
			return false
		}
		return inList(actual, cond.value)

	default:
		return false
	}
}

// resolveField 解析字段值：优先按变量系统（payload./$ 前缀）解析，
// 否则回退到 payload 顶层键（兼容旧写法直接写字段名）。
func (f *filterStage) resolveField(ctx *Context, field string) (any, bool) {
	if v, ok := f.vs.resolve(ctx, field); ok {
		return v, true
	}
	// 兼容旧写法：直接按 payload 顶层键查找。
	return lookupPath(ctx.Msg.Payload, field)
}

// isEmptyValue 判断值是否为空（nil、空字符串、空切片视为空）。
func isEmptyValue(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		return t == ""
	case []any:
		return len(t) == 0
	default:
		return false
	}
}

// equalValue 判断实际值是否等于期望值。
// 两侧同为数值类型时做数值比较；否则转字符串比较（大小写不敏感）。
func equalValue(actual, want any) bool {
	if isNumeric(actual) && isNumeric(want) {
		return toFloat(actual) == toFloat(want)
	}
	return strings.EqualFold(stringify(actual), stringify(want))
}

// containsValue 判断实际值是否「包含」期望值。
//   - 字符串：子串包含
//   - 切片：存在任一元素与期望值相等
func containsValue(actual, want any) bool {
	switch t := actual.(type) {
	case string:
		return strings.Contains(t, stringify(want))
	case []any:
		for _, e := range t {
			if equalValue(e, want) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// inList 判断实际值是否属于期望列表（want 为 []any）。
func inList(actual, want any) bool {
	list, ok := want.([]any)
	if !ok {
		return false
	}
	for _, e := range list {
		if equalValue(actual, e) {
			return true
		}
	}
	return false
}

// isNumeric 判断值是否为数值类型。
func isNumeric(v any) bool {
	switch v.(type) {
	case int, int32, int64, float32, float64:
		return true
	default:
		return false
	}
}

// toFloat 将数值类型统一转为 float64。
func toFloat(v any) float64 {
	switch t := v.(type) {
	case int:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case float32:
		return float64(t)
	case float64:
		return t
	default:
		return 0
	}
}
