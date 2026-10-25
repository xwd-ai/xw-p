package pipeline

import (
	"testing"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

// 过滤条件（Phase 2）单元测试：
// 覆盖 eq / ne / contains / regex / in / exists 六种运算符、
// 条件对象写法、旧字符串写法（存在性判断）的向后兼容，以及多条件「与」逻辑。

func filterContext() *Context {
	return NewContext(queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "order_confirmed",
		SubscriberID: "sub_1",
		Payload: map[string]any{
			"order_id":  "A1001",
			"status":    "paid",
			"amount":    199.00,
			"channel":   "email",
			"tags":      []any{"vip", "new"},
			"customer":  map[string]any{"name": "张三"},
			"city":      "上海",
			"empty_str": "",
		},
	})
}

// stepWithFilters 构造一个带给定 filters 的投递步骤。
func stepWithFilters(id string, filters []any) domain.Step {
	return domain.Step{
		ID:     id,
		Type:   domain.ChannelEmail,
		Name:   "过滤测试步骤",
		Config: map[string]any{"filters": filters},
	}
}

// applyFilter 对单步骤执行 filter 阶段，返回是否被跳过及错误。
func applyFilter(ctx *Context, step domain.Step) (skipped bool, err error) {
	f := NewFilterStage()
	e := f.Apply(ctx, step)
	if e == errSkipStep {
		return true, nil
	}
	return false, e
}

func TestFilterEqString(t *testing.T) {
	ctx := filterContext()
	step := stepWithFilters("s1", []any{
		map[string]any{"field": "payload.status", "operator": "eq", "value": "paid"},
	})
	skipped, err := applyFilter(ctx, step)
	if err != nil || skipped {
		t.Errorf("eq 匹配失败: skipped=%v err=%v", skipped, err)
	}
}

func TestFilterEqNumeric(t *testing.T) {
	ctx := filterContext()
	// amount 为 199.00（float64），与数值 199 应做数值比较。
	step := stepWithFilters("s1", []any{
		map[string]any{"field": "payload.amount", "operator": "eq", "value": 199.0},
	})
	skipped, _ := applyFilter(ctx, step)
	if skipped {
		t.Errorf("数值 eq 应匹配")
	}
}

func TestFilterNe(t *testing.T) {
	ctx := filterContext()
	step := stepWithFilters("s1", []any{
		map[string]any{"field": "payload.status", "operator": "ne", "value": "refunded"},
	})
	skipped, _ := applyFilter(ctx, step)
	if skipped {
		t.Errorf("ne 应匹配（paid != refunded）")
	}
}

func TestFilterContainsString(t *testing.T) {
	ctx := filterContext()
	step := stepWithFilters("s1", []any{
		map[string]any{"field": "payload.city", "operator": "contains", "value": "海"},
	})
	skipped, _ := applyFilter(ctx, step)
	if skipped {
		t.Errorf("contains 子串应匹配")
	}
}

func TestFilterContainsArray(t *testing.T) {
	ctx := filterContext()
	step := stepWithFilters("s1", []any{
		map[string]any{"field": "payload.tags", "operator": "contains", "value": "vip"},
	})
	skipped, _ := applyFilter(ctx, step)
	if skipped {
		t.Errorf("数组 contains 应匹配")
	}
}

func TestFilterRegex(t *testing.T) {
	ctx := filterContext()
	step := stepWithFilters("s1", []any{
		map[string]any{"field": "payload.order_id", "operator": "regex", "value": "^A\\d{4}$"},
	})
	skipped, _ := applyFilter(ctx, step)
	if skipped {
		t.Errorf("regex 应匹配 A1001")
	}
}

func TestFilterIn(t *testing.T) {
	ctx := filterContext()
	step := stepWithFilters("s1", []any{
		map[string]any{"field": "payload.channel", "operator": "in", "value": []any{"email", "sms"}},
	})
	skipped, _ := applyFilter(ctx, step)
	if skipped {
		t.Errorf("in 应匹配")
	}
}

func TestFilterExists(t *testing.T) {
	ctx := filterContext()
	step := stepWithFilters("s1", []any{
		map[string]any{"field": "payload.order_id", "operator": "exists"},
	})
	skipped, _ := applyFilter(ctx, step)
	if skipped {
		t.Errorf("exists 应匹配")
	}
}

func TestFilterNestedField(t *testing.T) {
	ctx := filterContext()
	step := stepWithFilters("s1", []any{
		map[string]any{"field": "payload.customer.name", "operator": "eq", "value": "张三"},
	})
	skipped, _ := applyFilter(ctx, step)
	if skipped {
		t.Errorf("嵌套字段应匹配")
	}
}

func TestFilterSkipOnMismatch(t *testing.T) {
	ctx := filterContext()
	step := stepWithFilters("s1", []any{
		map[string]any{"field": "payload.status", "operator": "eq", "value": "refunded"},
	})
	skipped, _ := applyFilter(ctx, step)
	if !skipped {
		t.Errorf("不匹配的条件应跳过步骤")
	}
}

func TestFilterEmptyValueTreatedAsMissing(t *testing.T) {
	ctx := filterContext()
	step := stepWithFilters("s1", []any{
		map[string]any{"field": "payload.empty_str", "operator": "exists"},
	})
	skipped, _ := applyFilter(ctx, step)
	if !skipped {
		t.Errorf("空字符串应视为不存在而跳过")
	}
}

func TestFilterLegacyStringBackwardCompatible(t *testing.T) {
	ctx := filterContext()
	step := stepWithFilters("s1", []any{"order_id"})
	skipped, _ := applyFilter(ctx, step)
	if skipped {
		t.Errorf("旧字符串写法（存在性）应放行存在字段")
	}

	// 缺失字段应跳过。
	missing := stepWithFilters("s2", []any{"missing_field"})
	skipped, _ = applyFilter(ctx, missing)
	if !skipped {
		t.Errorf("旧字符串写法（缺失字段）应跳过")
	}
}

func TestFilterMultiConditionAnd(t *testing.T) {
	ctx := filterContext()
	step := stepWithFilters("s1", []any{
		map[string]any{"field": "payload.status", "operator": "eq", "value": "paid"},
		map[string]any{"field": "payload.amount", "operator": "eq", "value": 199.0},
	})
	skipped, _ := applyFilter(ctx, step)
	if skipped {
		t.Errorf("多条件全满足应放行")
	}

	// 任一不满足即跳过。
	bad := stepWithFilters("s2", []any{
		map[string]any{"field": "payload.status", "operator": "eq", "value": "paid"},
		map[string]any{"field": "payload.city", "operator": "eq", "value": "北京"},
	})
	skipped, _ = applyFilter(ctx, bad)
	if !skipped {
		t.Errorf("多条件任一不满足应跳过")
	}
}

func TestFilterInvalidOperatorSkipsWithError(t *testing.T) {
	ctx := filterContext()
	step := stepWithFilters("s1", []any{
		map[string]any{"field": "payload.status", "operator": "bogus", "value": "x"},
	})
	skipped, err := applyFilter(ctx, step)
	if !skipped {
		t.Errorf("非法运算符应跳过")
	}
	if err != nil {
		t.Errorf("errSkipStep 应归一为 skipped，实际 err=%v", err)
	}
}

func TestFilterInvalidRegexDoesNotPanic(t *testing.T) {
	ctx := filterContext()
	step := stepWithFilters("s1", []any{
		map[string]any{"field": "payload.city", "operator": "regex", "value": "(["},
	})
	skipped, _ := applyFilter(ctx, step)
	if !skipped {
		t.Errorf("非法正则应跳过而非崩溃")
	}
}

func TestParseConditionDefaults(t *testing.T) {
	// operator 缺省应为 eq。
	cond, err := parseCondition(map[string]any{"field": "payload.a", "value": "x"})
	if err != nil {
		t.Fatalf("parseCondition 出错: %v", err)
	}
	if cond.operator != "eq" {
		t.Errorf("缺省 operator = %q, 期望 eq", cond.operator)
	}

	// 缺 field 应报错。
	if _, err := parseCondition(map[string]any{"operator": "eq"}); err == nil {
		t.Errorf("缺 field 应报错")
	}

	// 未知类型应报错。
	if _, err := parseCondition(123); err == nil {
		t.Errorf("非字符串非 map 应报错")
	}
}
