package pipeline

import (
	"strings"
	"testing"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

// 安全沙箱（Phase 2）单元测试：
// 覆盖危险路径段拒绝、路径长度限制、模板大小限制、递归深度限制。

func TestSafePathRejectsDangerousSegments(t *testing.T) {
	cases := []string{
		"__class__",
		"payload.__dict__",
		"customer..name",   // 空段
		".order_id",        // 前导点产生空段
		"customer.__name",  // __ 前缀段
		"items[0].__proto", // 数组下标后接内部字段
	}

	for _, p := range cases {
		if _, ok := safeLookupPath(map[string]any{"x": 1}, p); ok {
			t.Errorf("危险路径 %q 不应解析成功", p)
		}
	}
}

func TestSafePathAllowsNormalSegments(t *testing.T) {
	root := map[string]any{
		"order_id": "A1001",
		"customer": map[string]any{
			"name": "张三",
		},
	}
	if v, ok := safeLookupPath(root, "customer.name"); !ok || v != "张三" {
		t.Errorf("正常嵌套路径解析失败: %v, %v", v, ok)
	}
	if v, ok := safeLookupPath(root, "order_id"); !ok || v != "A1001" {
		t.Errorf("顶层路径解析失败: %v, %v", v, ok)
	}
}

func TestSafePathTooManySegments(t *testing.T) {
	path := strings.Repeat("a.", maxPathSegments) + "x"
	if _, ok := safeLookupPath(map[string]any{}, path); ok {
		t.Errorf("超长路径应被拒绝")
	}
}

func TestValidateTemplateSizeLimit(t *testing.T) {
	if err := validateTemplate("正常模板"); err != nil {
		t.Errorf("正常模板不应报错: %v", err)
	}
	big := strings.Repeat("x", maxTemplateBytes+1)
	if err := validateTemplate(big); err == nil {
		t.Errorf("超大模板应被拒绝")
	}
}

func TestRenderRejectsDangerousPathInTemplate(t *testing.T) {
	p := New(nil)
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t3",
		Name:         "order_confirmed",
		SubscriberID: "sub_1",
		Payload: map[string]any{
			"order_id": "A1001",
		},
	})

	wf := domain.Workflow{
		ID:   "wf",
		Name: "order_confirmed",
		Steps: []domain.Step{
			{
				ID:   "s1",
				Type: domain.ChannelEmail,
				Config: map[string]any{
					"template": "订单 {{ payload.__dict__ }} 已确认",
				},
			},
		},
	}

	results := p.Run(ctx, wf)
	got := results[0].Rendered
	if !strings.Contains(got, "{{ payload.__dict__ }}") {
		t.Errorf("危险路径应被拒绝并原样保留，实际 = %q", got)
	}
}

func TestRenderDepthLimit(t *testing.T) {
	// 构造一个通过函数参数自引用的模板不应导致栈溢出：
	// 实际语法无法真正自引用，这里验证 eval 深度上限不 panic。
	r := NewRenderStage().(*renderStage)
	ctx := NewContext(queue.QueuedMessage{
		Payload: map[string]any{"a": "1"},
	})
	// 构造一个深度有限的嵌套调用，确保 eval 能正常返回。
	out := r.eval(ctx, "upper payload.a", 0)
	if out != "1" {
		t.Errorf("深层嵌套求值异常: %q", out)
	}
	// 触发深度上限：直接以超限深度调用，应原样保留。
	out = r.eval(ctx, "upper payload.a", maxRenderDepth+1)
	if out != "{{ upper payload.a }}" {
		t.Errorf("超限深度应原样保留，实际 = %q", out)
	}
}

func TestRenderNormalEcommerceStillWorks(t *testing.T) {
	p := New(nil)
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t4",
		Name:         "order_delivered",
		SubscriberID: "sub_1",
		Payload: map[string]any{
			"order_id":    "A2001",
			"courier":     "顺丰",
			"tracking_no": "SF1234567890",
		},
	})

	wf := domain.Workflow{
		ID:   "wf",
		Name: "order_delivered",
		Steps: []domain.Step{
			{
				ID:   "s1",
				Type: domain.ChannelEmail,
				Config: map[string]any{
					"template": "您的订单 {{ payload.order_id }} 已由 {{ payload.courier }} 送达，运单号 {{ payload.tracking_no }}",
				},
			},
		},
	}

	results := p.Run(ctx, wf)
	want := "您的订单 A2001 已由 顺丰 送达，运单号 SF1234567890"
	if results[0].Rendered != want {
		t.Errorf("电商场景渲染 = %q, 期望 %q", results[0].Rendered, want)
	}
}
