package provider

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// scriptedSender 是按脚本返回结果的桩实现：第 n 次调用取 results[n]，
// 脚本用完后重复最后一条。calls 记录被调用的次数与收到的投递请求。
type scriptedSender struct {
	ch      domain.Channel
	id      string
	results []Result
	// onSend 在返回结果之前执行，用于模拟「投递途中上下文被取消」。
	onSend func()

	mu         sync.Mutex
	calls      int
	deliveries []Delivery
}

func newScripted(ch domain.Channel, id string, results ...Result) *scriptedSender {
	if len(results) == 0 {
		results = []Result{{Sent: true, Reason: id + " 已接收"}}
	}
	return &scriptedSender{ch: ch, id: id, results: results}
}

func (s *scriptedSender) Channel() domain.Channel { return s.ch }

func (s *scriptedSender) ID() string { return s.id }

func (s *scriptedSender) Send(d Delivery) Result {
	if s.onSend != nil {
		s.onSend()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deliveries = append(s.deliveries, d)
	idx := s.calls
	if idx >= len(s.results) {
		idx = len(s.results) - 1
	}
	s.calls++
	return s.results[idx]
}

func (s *scriptedSender) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// scriptedContextSender 额外实现 ContextSender，用于校验回退链把 ctx 传下去。
type scriptedContextSender struct {
	*scriptedSender
	mu           sync.Mutex
	contextCalls int
}

func (s *scriptedContextSender) SendContext(ctx context.Context, d Delivery) Result {
	s.mu.Lock()
	s.contextCalls++
	s.mu.Unlock()
	return s.scriptedSender.Send(d)
}

func (s *scriptedContextSender) contextCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.contextCalls
}

// registerScripted 把桩实现按给定优先级登记进注册表。
func registerScripted(t *testing.T, reg *Registry, s Sender, id string, priority int) {
	t.Helper()
	if err := reg.RegisterEntry(Entry{
		ID:       id,
		Channel:  s.Channel(),
		Priority: priority,
		Enabled:  true,
		Sender:   s,
	}); err != nil {
		t.Fatalf("登记 %s 失败: %v", id, err)
	}
}

func TestFailoverUsesPrimaryWhenItSucceeds(t *testing.T) {
	primary := newScripted(domain.ChannelEmail, "smtp-email")
	standby := newScripted(domain.ChannelEmail, "console-email")
	reg := NewRegistry()
	registerScripted(t, reg, primary, "smtp-email", 10)
	registerScripted(t, reg, standby, "console-email", 100)

	out := NewFailover(reg).Send(context.Background(), Delivery{
		Channel: domain.ChannelEmail,
		To:      "creator@example.com",
		Body:    "帖子 P1001 收到 小林 的评论",
	})

	if !out.Sent {
		t.Fatalf("投递应成功, 结果 = %+v", out)
	}
	if out.Provider.ID != "smtp-email" {
		t.Errorf("实际实现 = %q, 期望 smtp-email", out.Provider.ID)
	}
	if out.FellBack() || len(out.Attempts) != 1 {
		t.Errorf("默认实现成功时不应回退, attempts = %+v", out.Attempts)
	}
	if standby.callCount() != 0 {
		t.Errorf("备选被调用 %d 次, 期望 0", standby.callCount())
	}
	if out.Candidates != 2 {
		t.Errorf("候选数 = %d, 期望 2", out.Candidates)
	}
}

func TestFailoverFallsBackToStandby(t *testing.T) {
	primary := newScripted(domain.ChannelEmail, "smtp-email",
		Result{Sent: false, Reason: "SMTP 连接失败：connection refused"})
	standby := newScripted(domain.ChannelEmail, "console-email")
	reg := NewRegistry()
	registerScripted(t, reg, primary, "smtp-email", 10)
	registerScripted(t, reg, standby, "console-email", 100)

	out := NewFailover(reg).Send(context.Background(), Delivery{
		Channel: domain.ChannelEmail,
		To:      "creator@example.com",
		Body:    "帖子 P1001 收到 小林 的评论",
	})

	if !out.Sent {
		t.Fatalf("备选投递成功时整体应成功, 结果 = %+v", out)
	}
	if out.Provider.ID != "console-email" {
		t.Errorf("实际实现 = %q, 期望 console-email", out.Provider.ID)
	}
	if got := strings.Join(out.Attempted(), ","); got != "smtp-email,console-email" {
		t.Errorf("尝试顺序 = %q, 期望 smtp-email,console-email", got)
	}
	if len(out.FailedAttempts()) != 1 || out.FailedAttempts()[0].ProviderID != "smtp-email" {
		t.Errorf("失败尝试 = %+v, 期望只有 smtp-email", out.FailedAttempts())
	}
	if out.FailedAttempts()[0].Elapsed <= 0 {
		t.Error("尝试耗时应被记录")
	}
	// 备选拿到的是同一条已渲染消息，回退不应改变投递内容。
	if len(standby.deliveries) != 1 || standby.deliveries[0].Body != "帖子 P1001 收到 小林 的评论" {
		t.Errorf("备选收到的投递 = %+v", standby.deliveries)
	}
}

func TestFailoverStopsOnTerminalFailure(t *testing.T) {
	// 缺收件地址属于内容问题：换哪个实现都会被拒，不该把同一条消息再发一遍。
	primary := newScripted(domain.ChannelEmail, "smtp-email",
		Result{Sent: false, Reason: "收件邮箱为空", Terminal: true})
	standby := newScripted(domain.ChannelEmail, "console-email")
	reg := NewRegistry()
	registerScripted(t, reg, primary, "smtp-email", 10)
	registerScripted(t, reg, standby, "console-email", 100)

	out := NewFailover(reg).Send(context.Background(), Delivery{Channel: domain.ChannelEmail})

	if out.Sent {
		t.Error("内容校验失败时整体应失败")
	}
	if standby.callCount() != 0 {
		t.Errorf("不可回退的失败后仍调用了备选 %d 次", standby.callCount())
	}
	if len(out.Attempts) != 1 {
		t.Errorf("尝试次数 = %d, 期望 1", len(out.Attempts))
	}
	if out.Result.Reason != "收件邮箱为空" {
		t.Errorf("失败原因 = %q, 期望保留原原因", out.Result.Reason)
	}
	if !out.Result.Terminal {
		t.Error("失败结果应带不可回退标记")
	}
}

func TestFailoverAllFailuresReportEveryAttempt(t *testing.T) {
	first := newScripted(domain.ChannelEmail, "smtp-email",
		Result{Sent: false, Reason: "SMTP 连接失败：i/o timeout"})
	second := newScripted(domain.ChannelEmail, "relay-email",
		Result{Sent: false, Reason: "中继返回 451"})
	third := newScripted(domain.ChannelEmail, "console-email",
		Result{Sent: false, Reason: "写入发件队列失败"})
	reg := NewRegistry()
	registerScripted(t, reg, first, "smtp-email", 10)
	registerScripted(t, reg, second, "relay-email", 20)
	registerScripted(t, reg, third, "console-email", 100)

	out := NewFailover(reg).Send(context.Background(), Delivery{Channel: domain.ChannelEmail})

	if out.Sent {
		t.Error("全部实现失败时整体应失败")
	}
	if len(out.Attempts) != 3 {
		t.Fatalf("尝试次数 = %d, 期望 3", len(out.Attempts))
	}
	if out.Provider.ID != "console-email" {
		t.Errorf("最后尝试的实现 = %q, 期望 console-email", out.Provider.ID)
	}
	for _, want := range []string{"尝试 3 个实现均失败", "smtp-email", "relay-email", "console-email", "i/o timeout", "451"} {
		if !strings.Contains(out.Result.Reason, want) {
			t.Errorf("失败原因 %q 缺少 %q", out.Result.Reason, want)
		}
	}
}

func TestFailoverWithoutCandidates(t *testing.T) {
	out := NewFailover(NewRegistry()).Send(context.Background(), Delivery{Channel: domain.ChannelPush})

	if out.Sent || len(out.Attempts) != 0 || out.Candidates != 0 {
		t.Fatalf("空注册表应返回无候选结果, 结果 = %+v", out)
	}
	if !strings.Contains(out.Result.Reason, "push") {
		t.Errorf("失败原因 %q 应指出渠道", out.Result.Reason)
	}
	if !out.Result.Terminal {
		t.Error("没有候选时结果应标记为不可回退")
	}
}

func TestFailoverNilRegistry(t *testing.T) {
	var f *Failover
	out := f.Send(context.Background(), Delivery{Channel: domain.ChannelEmail})
	if out.Sent || out.Candidates != 0 {
		t.Fatalf("未初始化的回退器不应投递, 结果 = %+v", out)
	}
	if out.Result.Reason == "" {
		t.Error("应给出失败原因")
	}
}

func TestFailoverSkipsWhenContextAlreadyCanceled(t *testing.T) {
	primary := newScripted(domain.ChannelEmail, "smtp-email")
	reg := NewRegistry()
	registerScripted(t, reg, primary, "smtp-email", 10)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out := NewFailover(reg).Send(ctx, Delivery{Channel: domain.ChannelEmail})

	if out.Sent {
		t.Error("上下文已取消时不应投递")
	}
	if primary.callCount() != 0 {
		t.Errorf("上下文已取消仍调用了实现 %d 次", primary.callCount())
	}
	if out.Candidates != 1 {
		t.Errorf("候选数 = %d, 期望 1（取消不改变候选集）", out.Candidates)
	}
	if !strings.Contains(out.Result.Reason, "已取消") {
		t.Errorf("失败原因 = %q, 应说明被取消", out.Result.Reason)
	}
}

func TestFailoverStopsChainWhenCanceledMidway(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	primary := newScripted(domain.ChannelEmail, "smtp-email",
		Result{Sent: false, Reason: "SMTP 连接失败：context canceled"})
	primary.onSend = cancel // 模拟步骤超时在投递途中触发
	standby := newScripted(domain.ChannelEmail, "console-email")
	reg := NewRegistry()
	registerScripted(t, reg, primary, "smtp-email", 10)
	registerScripted(t, reg, standby, "console-email", 100)

	out := NewFailover(reg).Send(ctx, Delivery{Channel: domain.ChannelEmail})

	if standby.callCount() != 0 {
		t.Errorf("取消后仍调用了备选 %d 次", standby.callCount())
	}
	if len(out.Attempts) != 1 {
		t.Errorf("尝试次数 = %d, 期望 1", len(out.Attempts))
	}
	if !strings.Contains(out.Result.Reason, "已取消") ||
		!strings.Contains(out.Result.Reason, "连接失败") {
		t.Errorf("失败原因 = %q, 应同时说明尝试结果与取消", out.Result.Reason)
	}
}

func TestFailoverHonorsMaxAttempts(t *testing.T) {
	reg := NewRegistry()
	for i, id := range []string{"email-a", "email-b", "email-c"} {
		registerScripted(t, reg, newScripted(domain.ChannelEmail, id,
			Result{Sent: false, Reason: id + " 不可用"}), id, (i+1)*10)
	}

	f := NewFailover(reg)
	f.MaxAttempts = 2
	out := f.Send(context.Background(), Delivery{Channel: domain.ChannelEmail})

	if len(out.Attempts) != 2 || out.Candidates != 2 {
		t.Fatalf("应只尝试 2 个实现, attempts = %d candidates = %d", len(out.Attempts), out.Candidates)
	}
	if got := strings.Join(out.Attempted(), ","); got != "email-a,email-b" {
		t.Errorf("尝试顺序 = %q, 期望 email-a,email-b", got)
	}
}

func TestFailoverFollowsPriorityNotRegistrationOrder(t *testing.T) {
	// 登记顺序与优先级相反：先登记备选，后登记主实现。
	standby := newScripted(domain.ChannelPush, "console-push")
	primary := newScripted(domain.ChannelPush, "fcm-push",
		Result{Sent: false, Reason: "FCM 返回 503"})
	reg := NewRegistry()
	registerScripted(t, reg, standby, "console-push", 100)
	registerScripted(t, reg, primary, "fcm-push", 5)

	out := NewFailover(reg).Send(context.Background(), Delivery{Channel: domain.ChannelPush})
	if got := strings.Join(out.Attempted(), ","); got != "fcm-push,console-push" {
		t.Errorf("尝试顺序 = %q, 期望按优先级 fcm-push,console-push", got)
	}
	if !out.Sent || out.Provider.ID != "console-push" {
		t.Errorf("应由备选完成投递, 结果 = %+v", out)
	}
}

func TestFailoverSkipsDisabledEntries(t *testing.T) {
	primary := newScripted(domain.ChannelSMS, "twilio-sms")
	standby := newScripted(domain.ChannelSMS, "console-sms")
	reg := NewRegistry()
	registerScripted(t, reg, primary, "twilio-sms", 10)
	registerScripted(t, reg, standby, "console-sms", 100)
	if err := reg.SetEnabled("twilio-sms", false); err != nil {
		t.Fatalf("停用失败: %v", err)
	}

	out := NewFailover(reg).Send(context.Background(), Delivery{Channel: domain.ChannelSMS})
	if out.Candidates != 1 {
		t.Errorf("候选数 = %d, 期望 1（停用的条目被跳过）", out.Candidates)
	}
	if primary.callCount() != 0 {
		t.Errorf("停用的实现被调用 %d 次", primary.callCount())
	}
	if !out.Sent || out.Provider.ID != "console-sms" {
		t.Errorf("应由未停用的实现完成投递, 结果 = %+v", out)
	}
}

func TestFailoverPassesContextDownstream(t *testing.T) {
	sender := &scriptedContextSender{scriptedSender: newScripted(domain.ChannelChat, "slack-chat")}
	reg := NewRegistry()
	registerScripted(t, reg, sender, "slack-chat", 10)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out := NewFailover(reg).Send(ctx, Delivery{
		Channel: domain.ChannelChat,
		To:      "https://hooks.slack.example/services/T/B/X",
		Body:    "帖子 P1001 收到 小林 的评论",
	})

	if !out.Sent {
		t.Fatalf("投递应成功, 结果 = %+v", out)
	}
	if sender.contextCallCount() != 1 {
		t.Errorf("SendContext 调用次数 = %d, 期望 1（应优先走可取消路径）", sender.contextCallCount())
	}
}

func TestFailoverConcurrentSends(t *testing.T) {
	// 主实现每次都返回可回退的失败：并发投递时每条消息都应独立走完
	// 「主失败 → 备选成功」两跳，取用序列不互相干扰。
	primary := newScripted(domain.ChannelEmail, "smtp-email",
		Result{Sent: false, Reason: "连接被拒"})
	standby := newScripted(domain.ChannelEmail, "console-email")
	reg := NewRegistry()
	registerScripted(t, reg, primary, "smtp-email", 10)
	registerScripted(t, reg, standby, "console-email", 100)
	f := NewFailover(reg)

	const workers = 16
	var wg sync.WaitGroup
	fail := make(chan string, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out := f.Send(context.Background(), Delivery{Channel: domain.ChannelEmail})
			if !out.Sent || out.Provider.ID != "console-email" {
				fail <- "并发回退结果不符合预期"
			}
		}()
	}
	wg.Wait()
	close(fail)
	for msg := range fail {
		t.Error(msg)
	}
	if primary.callCount() != workers || standby.callCount() != workers {
		t.Errorf("调用次数 = %d/%d, 期望 %d/%d",
			primary.callCount(), standby.callCount(), workers, workers)
	}
}

func TestOutcomeFailureSummaryFormat(t *testing.T) {
	cases := []struct {
		name string
		out  Outcome
		want string
		ch   domain.Channel
	}{
		{
			name: "没有候选",
			out:  Outcome{},
			ch:   domain.ChannelEmail,
			want: "没有可用的 email 实现",
		},
		{
			name: "单次尝试保留原原因",
			out: Outcome{Attempts: []Attempt{
				{ProviderID: "smtp-email", Reason: "SMTP 收件人 a@b 失败（550）"},
			}},
			ch:   domain.ChannelEmail,
			want: "SMTP 收件人 a@b 失败（550）",
		},
		{
			name: "多次尝试逐个列出",
			out: Outcome{Attempts: []Attempt{
				{ProviderID: "smtp-email", Reason: "连接被拒"},
				{ProviderID: "console-email", Reason: ""},
			}},
			ch:   domain.ChannelEmail,
			want: "尝试 2 个实现均失败：smtp-email：连接被拒；console-email：未给出原因",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.out.FailureSummary(tc.ch); got != tc.want {
				t.Errorf("FailureSummary = %q, 期望 %q", got, tc.want)
			}
		})
	}
}
