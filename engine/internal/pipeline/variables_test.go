package pipeline

import (
	"strings"
	"testing"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

// 变量系统（Phase 2）单元测试：
// 覆盖 payload 嵌套访问、系统变量、内置函数，以及 render 阶段的集成渲染。

func TestLookupPathNested(t *testing.T) {
	root := map[string]any{
		"order_id": "A1001",
		"customer": map[string]any{
			"name": "张三",
			"address": map[string]any{
				"city": "上海",
			},
		},
		"items": []any{
			map[string]any{"name": "手机", "price": 2999.0},
			map[string]any{"name": "耳机", "price": 199.0},
		},
	}

	cases := []struct {
		path string
		want any
		ok   bool
	}{
		{"order_id", "A1001", true},
		{"customer.name", "张三", true},
		{"customer.address.city", "上海", true},
		{"items[0].name", "手机", true},
		{"items[1].price", 199.0, true},
		{"items[2].name", nil, false},
		{"customer.missing", nil, false},
	}

	for _, c := range cases {
		got, ok := lookupPath(root, c.path)
		if ok != c.ok {
			t.Errorf("lookupPath(%q) ok=%v, 期望 %v", c.path, ok, c.ok)
			continue
		}
		if !c.ok {
			continue
		}
		if got != c.want {
			t.Errorf("lookupPath(%q)=%v, 期望 %v", c.path, got, c.want)
		}
	}
}

func TestSystemVariablesPopulated(t *testing.T) {
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:     "t9",
		TransactionID: "tx9",
		Name:          "order_shipped",
		SubscriberID:  "sub_9",
		Email:         "buyer@example.com",
		Phone:         "13800000000",
	})

	ts := NewTransformStage()
	ts.Apply(ctx, domain.Step{ID: "s1", Type: domain.ChannelEmail, Name: "发货通知"})

	sysChecks := map[string]string{
		"$workflow":       "order_shipped",
		"$subscriber_id":  "sub_9",
		"$trigger_id":     "t9",
		"$transaction_id": "tx9",
		"$step_id":        "s1",
		"$step_name":      "发货通知",
		"$channel":        "email",
		"$email":          "buyer@example.com",
		"$phone":          "13800000000",
	}

	for k, want := range sysChecks {
		if v, ok := ctx.Get(k); !ok || v != want {
			t.Errorf("系统变量 %s = %v(ok=%v), 期望 %q", k, v, ok, want)
		}
	}

	if _, ok := ctx.Get("$timestamp"); !ok {
		t.Errorf("$timestamp 未注入")
	}
	if _, ok := ctx.Get("$timestamp_iso"); !ok {
		t.Errorf("$timestamp_iso 未注入")
	}
}

func TestBuiltinFunctions(t *testing.T) {
	vs := newVariableSystem()

	cases := []struct {
		name string
		args []string
		want string
	}{
		{"upper", []string{"hello"}, "HELLO"},
		{"lower", []string{"HELLO"}, "hello"},
		{"trim", []string{"  pad  "}, "pad"},
		{"default", []string{"", "fallback"}, "fallback"},
		{"default", []string{"keep", "fallback"}, "keep"},
		{"format_amount", []string{"199"}, "199.00"},
		{"format_amount", []string{"42.5"}, "42.50"},
		{"length", []string{"你好"}, "2"},
	}

	for _, c := range cases {
		got, err := vs.call(c.name, c.args)
		if err != nil {
			t.Errorf("内置函数 %s(%v) 出错: %v", c.name, c.args, err)
			continue
		}
		if got != c.want {
			t.Errorf("内置函数 %s(%v) = %q, 期望 %q", c.name, c.args, got, c.want)
		}
	}

	// 未知函数应报错。
	if _, err := vs.call("no_such_fn", []string{"x"}); err == nil {
		t.Errorf("未知函数应报错")
	}
}

func TestRenderWithFunctionsAndNestedVars(t *testing.T) {
	p := newMockPipeline()
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "order_confirmed",
		SubscriberID: "sub_1",
		Payload: map[string]any{
			"order_id": "A1001",
			"amount":   "199.00",
			"customer": map[string]any{
				"name": "张三",
			},
			"items": []any{
				map[string]any{"name": "手机"},
			},
		},
	})

	wf := domain.Workflow{
		ID:      "wf_order",
		Name:    "order_confirmed",
		Version: 1,
		Active:  true,
		Steps: []domain.Step{
			{
				ID:   "s1",
				Type: domain.ChannelEmail,
				Name: "订单确认邮件",
				Config: map[string]any{
					"template": "您好 {{ upper payload.customer.name }}，订单 {{ payload.order_id }} 金额 ¥{{ format_amount payload.amount }}，首件商品 {{ payload.items[0].name }}",
				},
			},
		},
	}

	results := p.Run(ctx, wf)
	if len(results) != 1 {
		t.Fatalf("投递结果数 = %d, 期望 1", len(results))
	}

	got := results[0].Rendered
	want := "您好 张三，订单 A1001 金额 ¥199.00，首件商品 手机"
	if got != want {
		t.Errorf("渲染结果 = %q, 期望 %q", got, want)
	}
}

func TestRenderMissingVariablePreserved(t *testing.T) {
	p := newMockPipeline()
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t2",
		Name:         "order_confirmed",
		SubscriberID: "sub_1",
		Payload:      map[string]any{"order_id": "A1"},
	})

	wf := domain.Workflow{
		ID:   "wf",
		Name: "order_confirmed",
		Steps: []domain.Step{
			{
				ID:   "s1",
				Type: domain.ChannelSMS,
				Config: map[string]any{
					"template": "订单 {{ payload.missing_field }} 已确认",
				},
			},
		},
	}

	results := p.Run(ctx, wf)
	got := results[0].Rendered
	if !strings.Contains(got, "{{ payload.missing_field }}") {
		t.Errorf("缺失变量应原样保留，实际 = %q", got)
	}
}
