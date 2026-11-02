package pipeline

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/chat"
	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/provider"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// chatHook 是进程内的群机器人 webhook，记录收到的报文。
type chatHook struct {
	mu       sync.Mutex
	payloads []map[string]any
	status   int
	body     string
	server   *httptest.Server
}

func newChatHook(t *testing.T) *chatHook {
	t.Helper()
	hook := &chatHook{status: http.StatusOK, body: "ok"}
	hook.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var payload map[string]any
		_ = json.Unmarshal(raw, &payload)
		hook.mu.Lock()
		hook.payloads = append(hook.payloads, payload)
		status, body := hook.status, hook.body
		hook.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(hook.server.Close)
	return hook
}

// url 返回带路径的 webhook 地址（模拟 Slack 的 /services/T/B/X 形态）。
func (h *chatHook) url() string { return h.server.URL + "/services/T000/B000/XXXXSECRET" }

func (h *chatHook) reply(status int, body string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.status, h.body = status, body
}

func (h *chatHook) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.payloads)
}

func (h *chatHook) last(t *testing.T) map[string]any {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.payloads) == 0 {
		t.Fatal("webhook 未收到消息")
	}
	return h.payloads[len(h.payloads)-1]
}

// blocksText 把报文里的卡片文本拼起来，便于断言内容。
func blocksText(t *testing.T, payload map[string]any) string {
	t.Helper()
	raw, ok := payload["blocks"].([]any)
	if !ok {
		return ""
	}
	var parts []string
	for _, item := range raw {
		block, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if text, ok := block["text"].(map[string]any); ok {
			if value, ok := text["text"].(string); ok {
				parts = append(parts, value)
			}
		}
	}
	return strings.Join(parts, "\n")
}

func newSlackProvider(t *testing.T, creds map[string]string) Provider {
	t.Helper()
	cfg, err := chat.ConfigFromCredentials(creds)
	if err != nil {
		t.Fatalf("构造配置失败：%v", err)
	}
	p, err := NewSlackChatProvider(discardLogger(), cfg)
	if err != nil {
		t.Fatalf("构造 slack provider 失败：%v", err)
	}
	return p
}

// TestSlackChatProviderDelivers 验证渲染结果真的提交到 webhook，
// 并且回执里的地址做了脱敏。
func TestSlackChatProviderDelivers(t *testing.T) {
	hook := newChatHook(t)
	p := newSlackProvider(t, map[string]string{"webhook_url": hook.url()})

	if p.Channel() != domain.ChannelChat {
		t.Errorf("渠道 = %s，期望 chat", p.Channel())
	}
	if id, ok := p.(provider.Identifier); !ok || id.ID() != "slack-chat" {
		t.Errorf("实现标识不正确：%v", p)
	}

	res := p.Send(Delivery{
		Channel:      domain.ChannelChat,
		Subject:      "《周末骑行》有新评论",
		Body:         "阿岚：这条路线我也骑过",
		StepID:       "chat-step",
		WorkflowName: "post_commented",
		SubscriberID: "creator_1001",
	})
	if !res.Sent {
		t.Fatalf("投递应成功，实际失败：%s", res.Reason)
	}
	if strings.Contains(res.Reason, "XXXXSECRET") {
		t.Errorf("回执不应包含 webhook 完整 token：%q", res.Reason)
	}
	if !strings.Contains(res.Reason, "…") {
		t.Errorf("回执应给出脱敏地址：%q", res.Reason)
	}
	if hook.count() != 1 {
		t.Fatalf("webhook 应收到 1 条，实际 %d", hook.count())
	}

	payload := hook.last(t)
	text, _ := payload["text"].(string)
	if !strings.Contains(text, "这条路线上") && !strings.Contains(text, "这条路线我也骑过") {
		t.Errorf("通知预览 = %q", text)
	}
	if body := blocksText(t, payload); !strings.Contains(body, "阿岚：这条路线我也骑过") {
		t.Errorf("卡片内容 = %q", body)
	}
}

// TestSlackChatProviderPrefersStepWebhook 步骤配的 webhook 优先于渠道配置。
func TestSlackChatProviderPrefersStepWebhook(t *testing.T) {
	fallback := newChatHook(t)
	step := newChatHook(t)
	p := newSlackProvider(t, map[string]string{"webhook_url": fallback.url()})

	res := p.Send(Delivery{Channel: domain.ChannelChat, To: step.url(), Body: "互动播报"})
	if !res.Sent {
		t.Fatalf("投递应成功：%s", res.Reason)
	}
	if step.count() != 1 || fallback.count() != 0 {
		t.Errorf("应按步骤 webhook 投递，实际 step=%d fallback=%d", step.count(), fallback.count())
	}
}

// TestSlackChatProviderRejectsBadInput 覆盖三处本地校验：地址、正文与长度。
func TestSlackChatProviderRejectsBadInput(t *testing.T) {
	hook := newChatHook(t)
	p := newSlackProvider(t, map[string]string{"webhook_url": hook.url()})

	cases := []struct {
		name string
		d    Delivery
		want string
	}{
		{"非法 webhook", Delivery{To: "ftp://hooks.slack.com/services/T", Body: "正文"}, "http/https"},
		{"远程 http", Delivery{To: "http://hooks.slack.com/services/T", Body: "正文"}, "https"},
		{"正文为空", Delivery{Body: "   "}, "正文为空"},
		{"超出长度", Delivery{Body: strings.Repeat("赞", chatMessageLimit+1)}, "上限"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := p.Send(tc.d)
			if res.Sent {
				t.Fatalf("%s 应投递失败", tc.name)
			}
			if !strings.Contains(res.Reason, tc.want) {
				t.Errorf("原因 %q 应包含 %q", res.Reason, tc.want)
			}
		})
	}
	if hook.count() != 0 {
		t.Errorf("本地校验失败时不应发出请求，实际 %d", hook.count())
	}
}

// TestSlackChatProviderRequiresWebhook 没有可用的 webhook 时如实判失败。
func TestSlackChatProviderRequiresWebhook(t *testing.T) {
	p := &slackChatProvider{logger: discardLogger()}
	res := p.Send(Delivery{Channel: domain.ChannelChat, Body: "正文"})
	if res.Sent {
		t.Fatal("缺少 webhook 应失败")
	}
	if !strings.Contains(res.Reason, "webhook") {
		t.Errorf("原因 = %q", res.Reason)
	}
}

// TestSlackChatProviderReportsRejection 把 Slack 的拒收原因带回执行结果。
func TestSlackChatProviderReportsRejection(t *testing.T) {
	hook := newChatHook(t)
	hook.reply(http.StatusBadRequest, "invalid_payload")
	p := newSlackProvider(t, map[string]string{"webhook_url": hook.url()})

	res := p.Send(Delivery{Channel: domain.ChannelChat, Body: "正文"})
	if res.Sent {
		t.Fatal("400 应判为投递失败")
	}
	if !strings.Contains(res.Reason, "invalid_payload") || !strings.Contains(res.Reason, "400") {
		t.Errorf("原因 = %q", res.Reason)
	}

	hook.reply(http.StatusTooManyRequests, "rate_limited")
	res = p.Send(Delivery{Channel: domain.ChannelChat, Body: "正文"})
	if res.Sent {
		t.Fatal("429 应判为投递失败")
	}
	if !strings.Contains(res.Reason, "可稍后重试") {
		t.Errorf("限流应提示可重试：%q", res.Reason)
	}
}

// TestSlackChatProviderHonoursCancellation 步骤超时时请求要能中断。
func TestSlackChatProviderHonoursCancellation(t *testing.T) {
	hook := newChatHook(t)
	p := newSlackProvider(t, map[string]string{"webhook_url": hook.url()})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res := p.(ContextSender).SendContext(ctx, Delivery{Channel: domain.ChannelChat, Body: "正文"})
	if res.Sent {
		t.Fatal("已取消的上下文应导致失败")
	}
}

// TestProviderFactoryBuildsSlack 验证工厂按 kind=slack 构造真实实现，
// 并在渠道或凭据不对时拒绝装配。
func TestProviderFactoryBuildsSlack(t *testing.T) {
	hook := newChatHook(t)
	factory := ProviderFactory(discardLogger())

	sender, err := factory.Build(provider.Config{
		ID:      "chat-slack",
		Channel: domain.ChannelChat,
		Kind:    provider.KindSlack,
		Credentials: provider.Credentials{
			"webhook_url": hook.url(),
			"channel":     "#ops",
		},
	})
	if err != nil {
		t.Fatalf("构造失败：%v", err)
	}
	if sender.Channel() != domain.ChannelChat {
		t.Errorf("渠道 = %s，期望 chat", sender.Channel())
	}

	if _, err := factory.Build(provider.Config{
		ID: "chat-slack", Channel: domain.ChannelEmail, Kind: provider.KindSlack,
		Credentials: provider.Credentials{"webhook_url": hook.url()},
	}); err == nil {
		t.Error("slack 用于 email 渠道时应失败")
	}

	if _, err := factory.Build(provider.Config{
		ID: "chat-slack", Channel: domain.ChannelChat, Kind: provider.KindSlack,
		Credentials: provider.Credentials{"webhook_url": "http://hooks.slack.com/services/T"},
	}); err == nil || !strings.Contains(err.Error(), "https") {
		t.Errorf("远程 http 地址应报错，实际 %v", err)
	}

	if _, err := ProviderFactory(discardLogger()).Build(provider.Config{
		ID: "chat-teams", Channel: domain.ChannelChat, Kind: provider.KindTeams,
		Credentials: provider.Credentials{"webhook_url": hook.url()},
	}); err == nil {
		t.Error("尚未接入的种类应返回错误")
	}
}

// TestProviderSetBindsSlack 覆盖「配置文件 → 校验 → 装配」这条路径：
// slack 的凭据要求与可选覆盖项都要被配置模型接受。
func TestProviderSetBindsSlack(t *testing.T) {
	hook := newChatHook(t)
	set, err := provider.NewSet([]provider.Config{{
		ID:      "chat-slack",
		Channel: domain.ChannelChat,
		Kind:    provider.KindSlack,
		Credentials: provider.Credentials{
			"webhook_url": hook.url(),
			"username":    "互动播报",
			"icon_emoji":  ":bell:",
		},
	}})
	if err != nil {
		t.Fatalf("配置应通过校验：%v", err)
	}

	registry := NewProviderRegistry()
	report, err := set.Bind(registry, ProviderFactory(discardLogger()))
	if err != nil {
		t.Fatalf("装配失败：%v", err)
	}
	if len(report.Bound) != 1 || report.Bound[0].ID != "chat-slack" {
		t.Fatalf("装配结果 = %+v", report)
	}
	entry, ok := registry.Select(domain.ChannelChat)
	if !ok || entry.Sender.Channel() != domain.ChannelChat {
		t.Fatal("chat 渠道应能取到实现")
	}

	if _, err := provider.NewSet([]provider.Config{{
		ID: "chat-slack", Channel: domain.ChannelChat, Kind: provider.KindSlack,
		Credentials: provider.Credentials{"webhook": hook.url()},
	}}); err == nil {
		t.Error("缺少必填凭据 webhook_url 时应校验失败")
	}
}

// TestDispatchDeliversChatThroughSlack 覆盖「管线 → 分发阶段 → Slack」整条路径：
// 渲染后的播报内容要真的落到 webhook 上，而不是停在日志里。
func TestDispatchDeliversChatThroughSlack(t *testing.T) {
	t.Setenv("XW_CHAT_WEBHOOK", "")
	hook := newChatHook(t)

	sender := newSlackProvider(t, map[string]string{"webhook_url": "https://hooks.slack.com/services/T000/B000/UNUSED"})
	registry := NewProviderRegistry()
	if err := registry.RegisterEntry(provider.Entry{
		ID:       "chat-slack",
		Channel:  domain.ChannelChat,
		Priority: 10,
		Enabled:  true,
		Sender:   sender,
	}); err != nil {
		t.Fatalf("登记 provider 失败：%v", err)
	}
	pipe := NewWithRegistry(discardLogger(), registry)

	msg := queue.QueuedMessage{
		TriggerID:    "trig-chat-1",
		Name:         "post_commented",
		SubscriberID: "creator_1001",
		Payload: map[string]any{
			"post_id":    "P1001",
			"post_title": "周末骑行",
			"actor":      "阿岚",
		},
	}
	wf := domain.Workflow{
		Name: "post_commented",
		Steps: []domain.Step{{
			ID:   "ops_broadcast",
			Type: domain.ChannelChat,
			Name: "运营群播报",
			Config: map[string]any{
				"webhook":  hook.url(),
				"template": "《{{ payload.post_title }}》收到 {{ payload.actor }} 的新评论",
			},
		}},
	}

	results := pipe.Run(NewContext(msg), wf)
	if len(results) != 1 || !results[0].Sent {
		t.Fatalf("chat 步骤应投递成功，实际 %+v", results)
	}
	if results[0].Error != "" {
		t.Errorf("不应有错误：%s", results[0].Error)
	}

	if hook.count() != 1 {
		t.Fatalf("webhook 应收到 1 条，实际 %d", hook.count())
	}
	body := blocksText(t, hook.last(t))
	if !strings.Contains(body, "《周末骑行》收到 阿岚 的新评论") {
		t.Errorf("卡片内容 = %q，期望渲染后的播报", body)
	}
}
