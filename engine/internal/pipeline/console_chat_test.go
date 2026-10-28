package pipeline

import (
	"strings"
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

const testChatWebhook = "https://hooks.slack.com/services/T000/B000/XXXXSECRET"

func TestConsoleChatProviderSends(t *testing.T) {
	p := NewConsoleChatProvider(discardLogger())
	if p.Channel() != domain.ChannelChat {
		t.Errorf("渠道 = %s, 期望 chat", p.Channel())
	}

	res := p.Send(Delivery{
		Channel:      domain.ChannelChat,
		To:           testChatWebhook,
		Body:         "帖子 P1001 今日互动 128 次，进入热门榜",
		StepID:       "s1",
		TriggerID:    "t_1",
		WorkflowName: "digest_post_hot",
		SubscriberID: "sub_1",
	})
	if !res.Sent {
		t.Fatalf("chat 应投递成功, 结果 = %+v", res)
	}
	if res.Reason == "" {
		t.Error("成功结果应携带原因说明")
	}
	if strings.Contains(res.Reason, "XXXXSECRET") {
		t.Error("结果说明不应包含 webhook 完整 token（需脱敏）")
	}
}

func TestConsoleChatProviderRejectsBadWebhook(t *testing.T) {
	p := NewConsoleChatProvider(discardLogger())
	cases := []struct {
		name string
		to   string
	}{
		{"缺少地址", "   "},
		{"缺少 scheme", "hooks.slack.com/services/T000"},
		{"非 http(s)", "ftp://hooks.slack.com/services/T000"},
		{"缺少 host", "https:///services/T000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := p.Send(Delivery{To: tc.to, Body: "内容"})
			if res.Sent {
				t.Errorf("webhook %q 应投递失败", tc.to)
			}
			if res.Reason == "" {
				t.Error("失败结果应携带原因说明")
			}
		})
	}
}

func TestConsoleChatProviderRejectsEmptyBody(t *testing.T) {
	p := NewConsoleChatProvider(discardLogger())
	res := p.Send(Delivery{To: testChatWebhook, Body: "  "})
	if res.Sent {
		t.Error("正文为空应投递失败")
	}
}

func TestConsoleChatProviderRejectsOversizedMessage(t *testing.T) {
	p := NewConsoleChatProvider(discardLogger())
	res := p.Send(Delivery{To: testChatWebhook, Body: strings.Repeat("赞", chatMessageLimit+1)})
	if res.Sent {
		t.Error("超过字符上限的消息应投递失败")
	}
	if !strings.Contains(res.Reason, "上限") {
		t.Errorf("失败原因应说明超限, 实际 %q", res.Reason)
	}
}

func TestMaskWebhookHidesPath(t *testing.T) {
	u, err := parseWebhook(testChatWebhook)
	if err != nil {
		t.Fatalf("解析 webhook 失败: %v", err)
	}
	got := maskWebhook(u)
	if got != "https://hooks.slack.com/…" {
		t.Errorf("maskWebhook = %q", got)
	}
	if strings.Contains(got, "B000") || strings.Contains(got, "XXXXSECRET") {
		t.Error("脱敏结果不应保留路径片段")
	}
}

func TestDispatchChatUsesStepWebhook(t *testing.T) {
	t.Setenv("XW_CHAT_WEBHOOK", "")
	registry := NewProviderRegistry()
	registry.Register(NewConsoleChatProvider(discardLogger()))
	p := NewWithRegistry(nil, registry)

	wf := domain.Workflow{
		ID:     "wf_chat",
		Name:   "post_commented",
		Active: true,
		Steps: []domain.Step{
			{
				ID:   "s1",
				Type: domain.ChannelChat,
				Name: "运营群播报",
				Config: map[string]any{
					"webhook":  testChatWebhook,
					"template": "帖子 {{ payload.post_id }} 收到新评论：{{ payload.actor }}",
				},
			},
		},
	}
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t_500",
		Name:         "post_commented",
		SubscriberID: "sub_1",
		Payload:      map[string]any{"post_id": "P1001", "actor": "小林"},
	})

	results := p.Run(ctx, wf)
	if results[0].Error != "" {
		t.Fatalf("chat 步骤出错: %s", results[0].Error)
	}
	if !results[0].Sent {
		t.Error("chat 步骤应标记为已发送")
	}
	if want := "帖子 P1001 收到新评论：小林"; results[0].Rendered != want {
		t.Errorf("渲染结果 = %q, 期望 %q", results[0].Rendered, want)
	}
}

func TestDispatchChatWithoutWebhookFails(t *testing.T) {
	t.Setenv("XW_CHAT_WEBHOOK", "")
	registry := NewProviderRegistry()
	registry.Register(NewConsoleChatProvider(discardLogger()))
	p := NewWithRegistry(nil, registry)

	wf := domain.Workflow{
		ID:     "wf_chat",
		Name:   "post_commented",
		Active: true,
		Steps:  []domain.Step{{ID: "s1", Type: domain.ChannelChat}},
	}
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t_501",
		Name:         "post_commented",
		SubscriberID: "sub_1",
	})

	results := p.Run(ctx, wf)
	if results[0].Error == "" {
		t.Error("未配置 webhook 应导致 chat 步骤失败")
	}
}
