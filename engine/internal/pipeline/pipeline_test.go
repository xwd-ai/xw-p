package pipeline

import (
	"testing"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

func orderWorkflow() domain.Workflow {
	return domain.Workflow{
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
					"template": "订单 {{ payload.order_id }} 已确认，金额 ¥{{ payload.amount }}",
				},
			},
			{
				ID:   "s2",
				Type: domain.ChannelSMS,
				Name: "订单确认短信",
				Config: map[string]any{
					"template": "您的订单 {{ payload.order_id }} 已确认",
					"filters":  []any{"order_id"},
				},
			},
			{
				ID:   "s3",
				Type: domain.ChannelDelay,
				Name: "延迟一步",
			},
		},
	}
}

func TestPipelineRendersAndDispatches(t *testing.T) {
	p := newMockPipeline()
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "order_confirmed",
		SubscriberID: "sub_1",
		Payload: map[string]any{
			"order_id": "A1001",
			"amount":   "199.00",
		},
	})

	results := p.Run(ctx, orderWorkflow())

	// 编排步骤 delay 应被跳过，仅 2 个投递步骤
	if len(results) != 2 {
		t.Fatalf("投递结果数 = %d, 期望 2", len(results))
	}

	// email 步骤应渲染成功
	email := results[0]
	if email.Error != "" {
		t.Errorf("email 步骤出错: %s", email.Error)
	}
	wantRendered := "订单 A1001 已确认，金额 ¥199.00"
	if email.Rendered != wantRendered {
		t.Errorf("email 渲染 = %q, 期望 %q", email.Rendered, wantRendered)
	}

	// sms 步骤应渲染成功（filters 条件满足）
	sms := results[1]
	if sms.Skipped {
		t.Errorf("sms 步骤不应被跳过")
	}
	if sms.Rendered != "您的订单 A1001 已确认" {
		t.Errorf("sms 渲染 = %q", sms.Rendered)
	}
}

func TestPipelineFilterSkipsWhenMissing(t *testing.T) {
	p := newMockPipeline()
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t2",
		Name:         "order_confirmed",
		SubscriberID: "sub_1",
		Payload:      map[string]any{}, // 缺 order_id
	})

	results := p.Run(ctx, orderWorkflow())

	// email 步骤无 filters，仍执行
	if results[0].Skipped {
		t.Errorf("email 步骤不应被跳过")
	}
	// sms 步骤因缺少 order_id 被跳过
	if !results[1].Skipped {
		t.Errorf("sms 步骤应被过滤跳过")
	}
}

func TestContextLoadPayload(t *testing.T) {
	ctx := NewContext(queue.QueuedMessage{
		Payload: map[string]any{
			"order_id": "A2001",
			"amount":   42,
		},
	})
	ctx.LoadPayload()

	if v, ok := ctx.Get("payload.order_id"); !ok || v != "A2001" {
		t.Errorf("payload.order_id = %v, %v", v, ok)
	}
	if v, ok := ctx.Get("payload.amount"); !ok || v != 42 {
		t.Errorf("payload.amount = %v, %v", v, ok)
	}
	if _, ok := ctx.Get("payload.missing"); ok {
		t.Errorf("payload.missing 不应存在")
	}
}
