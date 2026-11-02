package pipeline

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/mailer"
	"github.com/xwd-ai/xw-p/engine/internal/mailer/mailertest"
	"github.com/xwd-ai/xw-p/engine/internal/provider"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// stubProvider 是结果可控的渠道实现，记录每次投递以便断言内容与次数。
type stubProvider struct {
	id  string
	ch  domain.Channel
	res SendResult

	mu         sync.Mutex
	deliveries []Delivery
}

func (s *stubProvider) Channel() domain.Channel { return s.ch }

func (s *stubProvider) ID() string { return s.id }

func (s *stubProvider) Send(d Delivery) SendResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deliveries = append(s.deliveries, d)
	return s.res
}

func (s *stubProvider) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.deliveries)
}

func (s *stubProvider) lastDelivery(t *testing.T) Delivery {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.deliveries) == 0 {
		t.Fatalf("%s 未收到投递", s.id)
	}
	return s.deliveries[len(s.deliveries)-1]
}

// registerStub 按优先级把桩实现登记进注册表。
func registerStub(t *testing.T, reg *ProviderRegistry, p Provider, id string, priority int) {
	t.Helper()
	err := reg.RegisterEntry(provider.Entry{
		ID:       id,
		Channel:  p.Channel(),
		Priority: priority,
		Enabled:  true,
		Sender:   p,
	})
	if err != nil {
		t.Fatalf("登记 %s 失败：%v", id, err)
	}
}

// twoEmailSteps 构造两个 email 步骤的工作流，用于观察失败短路。
func twoEmailSteps() domain.Workflow {
	wf := emailWorkflow()
	wf.Steps = append(wf.Steps, domain.Step{
		ID:   "s2",
		Type: domain.ChannelEmail,
		Name: "抄送运营",
		Config: map[string]any{
			"subject":  "互动汇总",
			"template": "帖子 {{ payload.post_id }} 有新互动",
		},
	})
	return wf
}

func TestDispatchFallsBackToStandbyProvider(t *testing.T) {
	// 主实现连不上中继，备选（console）接着发：这是主备切换要走的主路径。
	primary := &stubProvider{
		id: "smtp-email", ch: domain.ChannelEmail,
		res: SendResult{Reason: "SMTP 连接失败：connection refused"},
	}
	standby := &stubProvider{
		id: "console-email", ch: domain.ChannelEmail,
		res: SendResult{Sent: true, Reason: "email 已发送至 creator@example.com"},
	}
	reg := NewProviderRegistry()
	registerStub(t, reg, primary, "smtp-email", 10)
	registerStub(t, reg, standby, "console-email", 100)

	results := NewWithRegistry(discardLogger(), reg).
		Run(NewContext(queueMessageWithEmail()), emailWorkflow())

	if len(results) != 1 {
		t.Fatalf("步骤结果数 = %d, 期望 1", len(results))
	}
	res := results[0]
	if !res.Sent || res.Error != "" {
		t.Fatalf("回退后应投递成功, 结果 = %+v", res)
	}
	if res.UsedProvider != "console-email" {
		t.Errorf("实际实现 = %q, 期望 console-email", res.UsedProvider)
	}
	if !res.ProviderFallback {
		t.Error("应标记发生过回退")
	}
	if got := strings.Join(res.ProviderAttempts, ","); got != "smtp-email,console-email" {
		t.Errorf("尝试顺序 = %q, 期望 smtp-email,console-email", got)
	}

	// 备选拿到的必须是同一条已渲染的消息，回退不改内容。
	got := standby.lastDelivery(t)
	if got.Body != "帖子 P1001 收到 小林 的评论" {
		t.Errorf("备选收到的正文 = %q", got.Body)
	}
	if got.Subject != "你的帖子有新评论" {
		t.Errorf("备选收到的标题 = %q", got.Subject)
	}
	if got.To != "creator@example.com" || got.TriggerID != "t1" {
		t.Errorf("备选收到的收件人与触发标识不正确：%+v", got)
	}
	if primary.count() != 1 {
		t.Errorf("主实现调用次数 = %d, 期望 1", primary.count())
	}
}

func TestDispatchTerminalFailureDoesNotFallBack(t *testing.T) {
	// 缺收件地址是内容问题：备选实现也发不出去，不该再试一遍。
	primary := &stubProvider{
		id: "smtp-email", ch: domain.ChannelEmail,
		res: SendResult{Reason: "收件邮箱为空", Terminal: true},
	}
	standby := &stubProvider{
		id: "console-email", ch: domain.ChannelEmail,
		res: SendResult{Sent: true, Reason: "不应被调用"},
	}
	reg := NewProviderRegistry()
	registerStub(t, reg, primary, "smtp-email", 10)
	registerStub(t, reg, standby, "console-email", 100)

	msg := queueMessageWithEmail()
	msg.Email = ""
	results := NewWithRegistry(discardLogger(), reg).Run(NewContext(msg), twoEmailSteps())

	res := results[0]
	if res.Sent {
		t.Error("内容校验失败时步骤不应标记为已发送")
	}
	if !strings.Contains(res.Error, "收件邮箱为空") {
		t.Errorf("失败原因 = %q, 期望保留原原因", res.Error)
	}
	if standby.count() != 0 {
		t.Errorf("不可回退的失败后仍调用了备选 %d 次", standby.count())
	}
	if len(res.ProviderAttempts) != 1 || res.ProviderFallback {
		t.Errorf("尝试记录 = %+v, 期望只有一次且不算回退", res)
	}
	// 第一个步骤失败后，第二个步骤短路。
	if len(results) != 2 || !results[1].ShortCircuited {
		t.Errorf("后续步骤应短路, 结果 = %+v", results)
	}
}

func TestDispatchReportsWholeChainWhenEveryProviderFails(t *testing.T) {
	first := &stubProvider{
		id: "smtp-email", ch: domain.ChannelEmail,
		res: SendResult{Reason: "SMTP 连接失败：i/o timeout"},
	}
	second := &stubProvider{
		id: "console-email", ch: domain.ChannelEmail,
		res: SendResult{Reason: "发件队列不可写"},
	}
	reg := NewProviderRegistry()
	registerStub(t, reg, first, "smtp-email", 10)
	registerStub(t, reg, second, "console-email", 100)

	results := NewWithRegistry(discardLogger(), reg).
		Run(NewContext(queueMessageWithEmail()), twoEmailSteps())

	res := results[0]
	if res.Sent {
		t.Error("全部实现失败时步骤不应标记为已发送")
	}
	for _, want := range []string{"尝试 2 个实现均失败", "smtp-email", "console-email", "i/o timeout"} {
		if !strings.Contains(res.Error, want) {
			t.Errorf("失败原因 %q 缺少 %q", res.Error, want)
		}
	}
	if got := strings.Join(res.ProviderAttempts, ","); got != "smtp-email,console-email" {
		t.Errorf("尝试顺序 = %q", got)
	}
	if len(results) != 2 || !results[1].ShortCircuited {
		t.Errorf("后续步骤应短路, 结果 = %+v", results)
	}
}

func TestDispatchFallsBackFromUnreachableSMTP(t *testing.T) {
	// 真实 SMTP 实现连不上中继：失败与实现相关，回退到 console 完成投递。
	emailProvider, err := NewSMTPEmailProvider(discardLogger(), mailer.Config{
		Host:    "127.0.0.1",
		Port:    closedPort(t),
		From:    "互动提醒 <notify@example.com>",
		TLS:     mailer.TLSDisabled,
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("构造 smtp provider 失败：%v", err)
	}
	console := &stubProvider{
		id: "console-email", ch: domain.ChannelEmail,
		res: SendResult{Sent: true, Reason: "email 已发送至 creator@example.com"},
	}
	reg := NewProviderRegistry()
	registerStub(t, reg, emailProvider, "smtp-email", 10)
	registerStub(t, reg, console, "console-email", 100)

	results := NewWithRegistry(discardLogger(), reg).
		Run(NewContext(queueMessageWithEmail()), emailWorkflow())

	res := results[0]
	if !res.Sent || res.UsedProvider != "console-email" || !res.ProviderFallback {
		t.Fatalf("连不上中继时应回退到 console, 结果 = %+v", res)
	}
	if console.count() != 1 {
		t.Errorf("备选调用次数 = %d, 期望 1", console.count())
	}
}

func TestDispatchStopsOnRecipientRejection(t *testing.T) {
	// 服务器用 550 明确拒收该收件人：换实现也是同样的答复，回退链就此终止。
	srv, err := mailertest.New(mailertest.Config{RejectRecipient: "blocked@example.com"})
	if err != nil {
		t.Fatalf("启动测试服务器失败：%v", err)
	}
	defer srv.Close()

	emailProvider, err := NewSMTPEmailProvider(discardLogger(), mailer.Config{
		Host:    srv.Host(),
		Port:    srv.Port(),
		From:    "互动提醒 <notify@example.com>",
		TLS:     mailer.TLSDisabled,
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("构造 smtp provider 失败：%v", err)
	}
	console := &stubProvider{
		id: "console-email", ch: domain.ChannelEmail,
		res: SendResult{Sent: true, Reason: "不应被调用"},
	}
	reg := NewProviderRegistry()
	registerStub(t, reg, emailProvider, "smtp-email", 10)
	registerStub(t, reg, console, "console-email", 100)

	msg := queueMessageWithEmail()
	msg.Email = "blocked@example.com"
	results := NewWithRegistry(discardLogger(), reg).Run(NewContext(msg), emailWorkflow())

	res := results[0]
	if res.Sent {
		t.Error("收件人被拒时步骤不应标记为已发送")
	}
	if !strings.Contains(res.Error, "550") {
		t.Errorf("失败原因 = %q, 应带服务器回复码", res.Error)
	}
	if console.count() != 0 {
		t.Errorf("5xx 拒收后仍调用了备选 %d 次", console.count())
	}
	if len(res.ProviderAttempts) != 1 {
		t.Errorf("尝试记录 = %+v, 期望只有 smtp-email", res.ProviderAttempts)
	}
}

func TestDispatchStopsOnSlackPermissionError(t *testing.T) {
	// webhook 返回 403：地址或频道权限问题，换同渠道的 console 实现没有意义。
	hook := newChatHook(t)
	hook.reply(http.StatusForbidden, "invalid_token")
	slack := newSlackProvider(t, map[string]string{"webhook_url": hook.url()})
	console := &stubProvider{
		id: "console-chat", ch: domain.ChannelChat,
		res: SendResult{Sent: true, Reason: "不应被调用"},
	}
	reg := NewProviderRegistry()
	registerStub(t, reg, slack, "slack-chat", 10)
	registerStub(t, reg, console, "console-chat", 100)

	wf := domain.Workflow{
		ID: "wf_chat", Name: "post_mentioned", Version: 1, Active: true,
		Steps: []domain.Step{{
			ID:   "s1",
			Type: domain.ChannelChat,
			Name: "群播报",
			Config: map[string]any{
				"template": "帖子 {{ payload.post_id }} 提到了你",
				"webhook":  hook.url(),
			},
		}},
	}
	msg := queue.QueuedMessage{
		TriggerID:    "t2",
		Name:         "post_mentioned",
		SubscriberID: "sub_1",
		Channels:     []string{"chat"},
		Payload:      map[string]any{"post_id": "P2002"},
	}
	results := NewWithRegistry(discardLogger(), reg).Run(NewContext(msg), wf)

	res := results[0]
	if res.Sent || res.Error == "" {
		t.Fatalf("403 应判定为失败, 结果 = %+v", res)
	}
	if console.count() != 0 {
		t.Errorf("4xx 拒收后仍调用了备选 %d 次", console.count())
	}
	if len(res.ProviderAttempts) != 1 || res.ProviderAttempts[0] != "slack-chat" {
		t.Errorf("尝试记录 = %+v, 期望只有 slack-chat", res.ProviderAttempts)
	}
}

func TestDispatchFallsBackFromSlackRateLimit(t *testing.T) {
	// 429 是限流：换一个群播报实现可能立刻成功，值得继续回退。
	hook := newChatHook(t)
	hook.reply(http.StatusTooManyRequests, "rate_limited")
	slack := newSlackProvider(t, map[string]string{"webhook_url": hook.url()})
	console := &stubProvider{
		id: "console-chat", ch: domain.ChannelChat,
		res: SendResult{Sent: true, Reason: "chat 已推送"},
	}
	reg := NewProviderRegistry()
	registerStub(t, reg, slack, "slack-chat", 10)
	registerStub(t, reg, console, "console-chat", 100)

	wf := domain.Workflow{
		ID: "wf_chat", Name: "post_mentioned", Version: 1, Active: true,
		Steps: []domain.Step{{
			ID:   "s1",
			Type: domain.ChannelChat,
			Name: "群播报",
			Config: map[string]any{
				"template": "帖子 {{ payload.post_id }} 提到了你",
				"webhook":  hook.url(),
			},
		}},
	}
	msg := queue.QueuedMessage{
		TriggerID:    "t3",
		Name:         "post_mentioned",
		SubscriberID: "sub_1",
		Channels:     []string{"chat"},
		Payload:      map[string]any{"post_id": "P2003"},
	}
	results := NewWithRegistry(discardLogger(), reg).Run(NewContext(msg), wf)

	res := results[0]
	if !res.Sent || res.UsedProvider != "console-chat" || !res.ProviderFallback {
		t.Fatalf("限流时应回退到备选, 结果 = %+v", res)
	}
	if console.count() != 1 {
		t.Errorf("备选调用次数 = %d, 期望 1", console.count())
	}
}

// closedPort 返回一个刚被释放的本机端口：连上去会立即被拒。
func closedPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("申请临时端口失败：%v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatalf("释放临时端口失败：%v", err)
	}
	return port
}
