package pipeline

import (
	"strings"
	"testing"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

func TestConsolePushProviderSends(t *testing.T) {
	p := NewConsolePushProvider(discardLogger())
	if p.Channel() != domain.ChannelPush {
		t.Errorf("渠道 = %s, 期望 push", p.Channel())
	}

	res := p.Send(Delivery{
		Channel:      domain.ChannelPush,
		To:           "fcm-token-9f2c1d7a",
		Subject:      "你的帖子有新评论",
		Body:         "小林 评论了你的帖子 P1001",
		StepID:       "s2",
		WorkflowName: "post_commented",
		SubscriberID: "sub_1",
	})
	if !res.Sent {
		t.Fatalf("push 应投递成功, 结果 = %+v", res)
	}
	if res.Reason == "" {
		t.Error("成功结果应携带原因说明")
	}
	if strings.Contains(res.Reason, "fcm-token-9f2c1d7a") {
		t.Error("结果说明不应包含完整 device token（需脱敏）")
	}
}

func TestConsolePushProviderFailsWithoutToken(t *testing.T) {
	p := NewConsolePushProvider(discardLogger())
	res := p.Send(Delivery{Channel: domain.ChannelPush, To: "  ", Body: "内容"})
	if res.Sent {
		t.Error("缺少 device token 应投递失败")
	}
	if res.Reason == "" {
		t.Error("失败结果应携带原因说明")
	}
}

func TestConsolePushProviderRejectsOversizedPayload(t *testing.T) {
	p := NewConsolePushProvider(discardLogger())
	res := p.Send(Delivery{
		Channel: domain.ChannelPush,
		To:      "fcm-token-9f2c1d7a",
		Subject: "新评论",
		Body:    strings.Repeat("赞", pushPayloadLimit),
	})
	if res.Sent {
		t.Error("超过 4KB 的 payload 应投递失败")
	}
	if res.Reason == "" {
		t.Error("失败结果应携带原因说明")
	}
}

func TestMaskToken(t *testing.T) {
	if got := maskToken("fcm-token-9f2c1d7a"); got != "fcm-…1d7a" {
		t.Errorf("maskToken = %q, 期望 fcm-…1d7a", got)
	}
	if got := maskToken("short"); got != "****" {
		t.Errorf("短 token 应完全遮蔽, 实际 %q", got)
	}
}

func TestPushTitleFallback(t *testing.T) {
	cases := []struct {
		name string
		d    Delivery
		want string
	}{
		{"优先 subject", Delivery{Subject: "新评论", WorkflowName: "post_commented"}, "新评论"},
		{"回退工作流名", Delivery{WorkflowName: "post_liked"}, "post_liked"},
		{"最终兜底", Delivery{}, "新通知"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pushTitle(tc.d); got != tc.want {
				t.Errorf("pushTitle = %q, 期望 %q", got, tc.want)
			}
		})
	}
}

func TestDispatchPushViaProvider(t *testing.T) {
	registry := NewProviderRegistry()
	registry.Register(NewConsolePushProvider(discardLogger()))
	p := NewWithRegistry(nil, registry)

	wf := domain.Workflow{
		ID:     "wf_push",
		Name:   "post_mentioned",
		Active: true,
		Steps: []domain.Step{
			{
				ID:   "s1",
				Type: domain.ChannelPush,
				Name: "被提及推送",
				Config: map[string]any{
					"title":    "有人 @ 了你",
					"template": "{{ payload.actor }} 在帖子 {{ payload.post_id }} 中提到了你",
				},
			},
		},
	}

	// device_token 走队列消息契约字段，验证 dispatch 的 push 收件人回退逻辑。
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "post_mentioned",
		SubscriberID: "sub_1",
		DeviceToken:  "fcm-token-9f2c1d7a",
		Channels:     []string{"push"},
		Payload:      map[string]any{"post_id": "P1001", "actor": "小林"},
	})
	results := p.Run(ctx, wf)

	if len(results) != 1 {
		t.Fatalf("投递结果数 = %d, 期望 1", len(results))
	}
	res := results[0]
	if res.Error != "" {
		t.Errorf("push 步骤出错: %s", res.Error)
	}
	if !res.Sent {
		t.Error("push 步骤应标记为已发送")
	}
	want := "小林 在帖子 P1001 中提到了你"
	if res.Rendered != want {
		t.Errorf("渲染结果 = %q, 期望 %q", res.Rendered, want)
	}
}

func TestDispatchPushFailsWithoutToken(t *testing.T) {
	registry := NewProviderRegistry()
	registry.Register(NewConsolePushProvider(discardLogger()))
	p := NewWithRegistry(nil, registry)

	wf := domain.Workflow{
		ID:     "wf_push",
		Name:   "post_mentioned",
		Active: true,
		Steps:  []domain.Step{{ID: "s1", Type: domain.ChannelPush}},
	}
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "post_mentioned",
		SubscriberID: "sub_1",
		Payload:      map[string]any{"post_id": "P1001"},
	})
	results := p.Run(ctx, wf)

	if results[0].Error == "" {
		t.Error("缺少 device token 应导致 push 步骤失败")
	}
	if results[0].Sent {
		t.Error("失败步骤不应标记已发送")
	}
}

func TestDefaultProviderRegistryCoversAllDeliveryChannels(t *testing.T) {
	r := DefaultProviderRegistry(discardLogger())
	for _, ch := range domain.DeliveryChannels() {
		if !r.Has(ch) {
			t.Errorf("默认注册表应包含 %s", ch)
		}
	}
	// 编排步骤（delay/digest）不是投递渠道，不应有 provider。
	if r.Has(domain.ChannelDelay) || r.Has(domain.ChannelDigest) {
		t.Error("编排步骤不应注册 provider")
	}
}
