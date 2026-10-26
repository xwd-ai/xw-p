package pipeline

import (
	"io"
	"log/slog"
	"testing"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

// emailWorkflow 构造一个仅含 email 投递步骤的订单确认工作流。
func emailWorkflow() domain.Workflow {
	return domain.Workflow{
		ID:      "wf_email",
		Name:    "order_confirmed",
		Version: 1,
		Active:  true,
		Steps: []domain.Step{
			{
				ID:   "s1",
				Type: domain.ChannelEmail,
				Name: "订单确认邮件",
				Config: map[string]any{
					"subject":  "您的订单已确认",
					"template": "订单 {{ payload.order_id }} 已确认，金额 ¥{{ payload.amount }}",
				},
			},
		},
	}
}

// queueMessageWithEmail 构造一条带收件邮箱的订单确认队列消息。
func queueMessageWithEmail() queue.QueuedMessage {
	return queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "order_confirmed",
		SubscriberID: "sub_1",
		Email:        "buyer@example.com",
		Channels:     []string{"email"},
		Payload: map[string]any{
			"order_id": "A1001",
			"amount":   "199.00",
		},
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newMockPipeline 返回不含任何 provider 的管线，供验证管线骨架语义
// （filter/transform/render/branch/short-circuit）的测试使用——这些测试只关心
// 骨架行为，不关心渠道是否真正投递，故用空注册表保持纯 mock 分发语义。
func newMockPipeline() *Pipeline {
	return NewWithRegistry(nil, NewProviderRegistry())
}

func TestConsoleEmailProviderSends(t *testing.T) {
	p := NewConsoleEmailProvider(discardLogger())
	if p.Channel() != domain.ChannelEmail {
		t.Errorf("渠道 = %s, 期望 email", p.Channel())
	}

	res := p.Send(Delivery{
		Channel:      domain.ChannelEmail,
		To:           "buyer@example.com",
		Subject:      "订单确认",
		Body:         "订单 A1001 已确认，金额 ¥199.00",
		StepID:       "s1",
		WorkflowName: "order_confirmed",
		SubscriberID: "sub_1",
	})
	if !res.Sent {
		t.Errorf("email 应发送成功, 结果 = %+v", res)
	}
	if res.Reason == "" {
		t.Error("成功结果应携带原因说明")
	}
}

func TestConsoleEmailProviderFailsWithoutRecipient(t *testing.T) {
	p := NewConsoleEmailProvider(discardLogger())
	res := p.Send(Delivery{
		Channel: domain.ChannelEmail,
		To:      "  ",
		Subject: "订单确认",
		Body:    "内容",
	})
	if res.Sent {
		t.Error("缺少收件邮箱应投递失败")
	}
	if res.Reason == "" {
		t.Error("失败结果应携带原因说明")
	}
}

func TestProviderRegistryLookup(t *testing.T) {
	r := NewProviderRegistry()
	if r.Has(domain.ChannelEmail) {
		t.Error("空注册表不应含 email")
	}
	if _, ok := r.Lookup(domain.ChannelEmail); ok {
		t.Error("空注册表查找 email 不应命中")
	}

	ep := NewConsoleEmailProvider(discardLogger())
	r.Register(ep)
	if !r.Has(domain.ChannelEmail) {
		t.Error("注册后应含 email")
	}
	if got, ok := r.Lookup(domain.ChannelEmail); !ok || got != ep {
		t.Error("查找 email 应返回注册的 provider")
	}
	if len(r.Channels()) != 1 {
		t.Errorf("注册表渠道数 = %d, 期望 1", len(r.Channels()))
	}
}

func TestDispatchEmailViaProvider(t *testing.T) {
	registry := NewProviderRegistry()
	registry.Register(NewConsoleEmailProvider(discardLogger()))
	p := NewWithRegistry(nil, registry)

	ctx := NewContext(queueMessageWithEmail())
	results := p.Run(ctx, emailWorkflow())

	if len(results) != 1 {
		t.Fatalf("投递结果数 = %d, 期望 1", len(results))
	}
	res := results[0]
	if res.Error != "" {
		t.Errorf("email 步骤出错: %s", res.Error)
	}
	if !res.Sent {
		t.Error("email 步骤应标记为已发送")
	}
	want := "订单 A1001 已确认，金额 ¥199.00"
	if res.Rendered != want {
		t.Errorf("渲染结果 = %q, 期望 %q", res.Rendered, want)
	}
}

func TestDispatchEmailFailsWithoutRecipient(t *testing.T) {
	registry := NewProviderRegistry()
	registry.Register(NewConsoleEmailProvider(discardLogger()))
	p := NewWithRegistry(nil, registry)

	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "order_confirmed",
		SubscriberID: "sub_1",
		// 未提供 email，订阅者也未绑定邮箱
		Payload: map[string]any{"order_id": "A1001", "amount": "199.00"},
	})
	results := p.Run(ctx, emailWorkflow())

	res := results[0]
	if res.Error == "" {
		t.Error("缺少收件邮箱应导致 email 步骤失败")
	}
	if res.Sent {
		t.Error("失败步骤不应标记已发送")
	}
}
