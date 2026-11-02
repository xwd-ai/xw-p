package provider

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// retryTable 构造「email 渠道重试 n 次、其余不重试」的策略表。
func retryTable(t *testing.T, ch domain.Channel, policy RetryPolicy) RetryTable {
	t.Helper()
	table, err := NewRetryTable(DefaultRetryPolicy(), map[domain.Channel]RetryPolicy{ch: policy})
	if err != nil {
		t.Fatalf("构造重试表失败：%v", err)
	}
	return table
}

// noWait 让退避等待不真的睡过去，同时记录每次的等待时长。
func noWait(waits *[]time.Duration) func(context.Context, time.Duration) error {
	return func(_ context.Context, d time.Duration) error {
		*waits = append(*waits, d)
		return nil
	}
}

func TestFailoverRetriesSameProviderBeforeSuccess(t *testing.T) {
	primary := newScripted(domain.ChannelEmail, "smtp-email",
		Result{Sent: false, Reason: "连接被重置"},
		Result{Sent: false, Reason: "连接被重置"},
		Result{Sent: true, Reason: "已接收"},
	)
	standby := newScripted(domain.ChannelEmail, "console-email")
	reg := NewRegistry()
	registerScripted(t, reg, primary, "smtp-email", 10)
	registerScripted(t, reg, standby, "console-email", 100)

	var waits []time.Duration
	policy := RetryPolicy{MaxAttempts: 3, Base: 100 * time.Millisecond, Max: 400 * time.Millisecond}
	f := NewFailover(reg).WithRetries(retryTable(t, domain.ChannelEmail, policy)).WithWait(noWait(&waits))
	out := f.Send(context.Background(), Delivery{Channel: domain.ChannelEmail})

	if !out.Sent {
		t.Fatalf("重试后应投递成功：%s", out.Result.Reason)
	}
	if got := primary.callCount(); got != 3 {
		t.Errorf("主实现被调用 %d 次, 期望 3", got)
	}
	if got := standby.callCount(); got != 0 {
		t.Errorf("主实现重试成功时不该动用备选，实际调用 %d 次", got)
	}
	if len(out.Attempts) != 3 {
		t.Fatalf("尝试记录 %d 条, 期望 3", len(out.Attempts))
	}
	for i, a := range out.Attempts {
		if a.Attempt != i+1 {
			t.Errorf("第 %d 条记录的尝试序号 = %d", i+1, a.Attempt)
		}
		if a.ProviderID != "smtp-email" {
			t.Errorf("第 %d 条记录的实现 = %q", i+1, a.ProviderID)
		}
	}
	if out.FellBack() {
		t.Error("同一实现内重试不算回退")
	}
	if got := out.Retries(); got != 2 {
		t.Errorf("重试次数 = %d, 期望 2", got)
	}
	if len(waits) != 2 || waits[0] != 100*time.Millisecond || waits[1] != 200*time.Millisecond {
		t.Errorf("退避等待 = %v, 期望 [100ms 200ms]", waits)
	}
}

func TestFailoverFallsBackAfterRetriesExhausted(t *testing.T) {
	primary := newScripted(domain.ChannelEmail, "smtp-email",
		Result{Sent: false, Reason: "SMTP 连接失败"})
	standby := newScripted(domain.ChannelEmail, "console-email")
	reg := NewRegistry()
	registerScripted(t, reg, primary, "smtp-email", 10)
	registerScripted(t, reg, standby, "console-email", 100)

	policy := RetryPolicy{MaxAttempts: 2, Base: time.Millisecond, Max: time.Millisecond}
	var waits []time.Duration
	out := NewFailover(reg).WithRetries(retryTable(t, domain.ChannelEmail, policy)).
		WithWait(noWait(&waits)).
		Send(context.Background(), Delivery{Channel: domain.ChannelEmail})

	if !out.Sent {
		t.Fatalf("备选应完成投递：%s", out.Result.Reason)
	}
	if got := primary.callCount(); got != 2 {
		t.Errorf("主实现应重试到上限共 2 次，实际 %d", got)
	}
	if got := standby.callCount(); got != 1 {
		t.Errorf("备选调用 %d 次, 期望 1", got)
	}
	if !out.FellBack() {
		t.Error("换了实现应记为回退")
	}
	if got := out.Attempted(); len(got) != 3 || got[0] != "smtp-email" || got[2] != "console-email" {
		t.Errorf("尝试序列 = %v, 期望 [smtp-email smtp-email console-email]", got)
	}
	if out.Provider.ID != "console-email" {
		t.Errorf("最终实现 = %q, 期望 console-email", out.Provider.ID)
	}
}

func TestFailoverDoesNotRetryTerminalFailure(t *testing.T) {
	primary := newScripted(domain.ChannelEmail, "smtp-email",
		Result{Sent: false, Reason: "收件邮箱为空", Terminal: true})
	reg := NewRegistry()
	registerScripted(t, reg, primary, "smtp-email", 10)

	policy := RetryPolicy{MaxAttempts: 5, Base: time.Millisecond, Max: time.Millisecond}
	var waits []time.Duration
	out := NewFailover(reg).WithRetries(retryTable(t, domain.ChannelEmail, policy)).
		WithWait(noWait(&waits)).
		Send(context.Background(), Delivery{Channel: domain.ChannelEmail})

	if out.Sent {
		t.Error("内容校验失败不应被判成功")
	}
	if got := primary.callCount(); got != 1 {
		t.Errorf("不可回退的失败不该重试，实际调用 %d 次", got)
	}
	if len(waits) != 0 {
		t.Errorf("不应产生退避等待，实际 %v", waits)
	}
	if out.Result.Reason != "收件邮箱为空" {
		t.Errorf("失败原因 = %q, 期望原样保留", out.Result.Reason)
	}
}

func TestFailoverNoRetriesWithoutTable(t *testing.T) {
	primary := newScripted(domain.ChannelEmail, "smtp-email",
		Result{Sent: false, Reason: "连接超时"})
	reg := NewRegistry()
	registerScripted(t, reg, primary, "smtp-email", 10)

	out := NewFailover(reg).Send(context.Background(), Delivery{Channel: domain.ChannelEmail})

	if got := primary.callCount(); got != 1 {
		t.Errorf("未配置重试时只应尝试一次，实际 %d", got)
	}
	if out.Retries() != 0 {
		t.Errorf("未配置重试时 Retries = %d, 期望 0", out.Retries())
	}
}

func TestFailoverRetryPolicyIsPerChannel(t *testing.T) {
	email := newScripted(domain.ChannelEmail, "smtp-email",
		Result{Sent: false, Reason: "连接超时"})
	sms := newScripted(domain.ChannelSMS, "console-sms",
		Result{Sent: false, Reason: "号码为空", Terminal: true})
	reg := NewRegistry()
	registerScripted(t, reg, email, "smtp-email", 10)
	registerScripted(t, reg, sms, "console-sms", 10)

	table, err := NewRetryTable(DefaultRetryPolicy(), map[domain.Channel]RetryPolicy{
		domain.ChannelEmail: {MaxAttempts: 3, Base: time.Millisecond, Max: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("构造重试表失败：%v", err)
	}
	var waits []time.Duration
	f := NewFailover(reg).WithRetries(table).WithWait(noWait(&waits))

	f.Send(context.Background(), Delivery{Channel: domain.ChannelEmail})
	f.Send(context.Background(), Delivery{Channel: domain.ChannelSMS})

	if got := email.callCount(); got != 3 {
		t.Errorf("email 应尝试 3 次，实际 %d", got)
	}
	if got := sms.callCount(); got != 1 {
		t.Errorf("sms 未单独配置、默认不重试，应只尝试 1 次，实际 %d", got)
	}
}

func TestFailoverStopsWhenCanceledDuringBackoff(t *testing.T) {
	primary := newScripted(domain.ChannelEmail, "smtp-email",
		Result{Sent: false, Reason: "连接超时"})
	reg := NewRegistry()
	registerScripted(t, reg, primary, "smtp-email", 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	policy := RetryPolicy{MaxAttempts: 4, Base: time.Millisecond, Max: time.Millisecond}
	f := NewFailover(reg).
		WithRetries(retryTable(t, domain.ChannelEmail, policy)).
		WithWait(func(context.Context, time.Duration) error {
			// 模拟在等待退避时上游把步骤预算收回。
			cancel()
			return context.Canceled
		})
	out := f.Send(ctx, Delivery{Channel: domain.ChannelEmail})

	if out.Sent {
		t.Error("等待期间被取消不应判成功")
	}
	if got := primary.callCount(); got != 1 {
		t.Errorf("取消后不该再发起尝试，实际调用 %d 次", got)
	}
	if !out.Result.Terminal {
		t.Error("取消后的结果应标记为终结")
	}
	if !strings.Contains(out.Result.Reason, "投递已取消") {
		t.Errorf("失败原因应说明取消：%q", out.Result.Reason)
	}
}

// sleepCtx 是真实等待：上面用注入的 wait 跳过睡眠，这里确认默认实现真的
// 会在 ctx 到期时立即返回，而不是把整段退避睡完。
func TestSleepCtxReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := sleepCtx(ctx, time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("等待返回 %v, 期望上下文超时", err)
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Errorf("等待用了 %s，应在上限附近就返回", elapsed)
	}
	if err := sleepCtx(context.Background(), 0); err != nil {
		t.Errorf("零时长应直接返回，实际 %v", err)
	}
}

func TestFailureSummaryReportsRetryCounts(t *testing.T) {
	single := Outcome{Attempts: []Attempt{
		{ProviderID: "smtp-email", Attempt: 1, Reason: "第一次：连接超时"},
		{ProviderID: "smtp-email", Attempt: 2, Reason: "第二次：连接超时"},
		{ProviderID: "smtp-email", Attempt: 3, Reason: "连接超时"},
	}}
	reason := single.FailureSummary(domain.ChannelEmail)
	for _, want := range []string{"smtp-email 先后尝试 3 次均失败", "连接超时"} {
		if !strings.Contains(reason, want) {
			t.Errorf("失败原因 %q 缺少 %q", reason, want)
		}
	}

	multi := Outcome{Attempts: []Attempt{
		{ProviderID: "smtp-email", Attempt: 1, Reason: "连接超时"},
		{ProviderID: "smtp-email", Attempt: 2, Reason: "连接超时"},
		{ProviderID: "console-email", Attempt: 1, Reason: "写入失败"},
	}}
	reason = multi.FailureSummary(domain.ChannelEmail)
	for _, want := range []string{"尝试 2 个实现（共 3 次）均失败", "smtp-email（2 次）", "console-email", "写入失败"} {
		if !strings.Contains(reason, want) {
			t.Errorf("失败原因 %q 缺少 %q", reason, want)
		}
	}

	// 没有重试的多实现失败保持原有文案，避免日志格式无谓变化。
	plain := Outcome{Attempts: []Attempt{
		{ProviderID: "smtp-email", Attempt: 1, Reason: "连接超时"},
		{ProviderID: "console-email", Attempt: 1, Reason: "写入失败"},
	}}
	if got := plain.FailureSummary(domain.ChannelEmail); !strings.HasPrefix(got, "尝试 2 个实现均失败") {
		t.Errorf("无重试时的文案 = %q", got)
	}
}

func TestOutcomeGroupsAttemptsPerProvider(t *testing.T) {
	out := Outcome{Attempts: []Attempt{
		{ProviderID: "a", Attempt: 1},
		{ProviderID: "b", Attempt: 1},
		{ProviderID: "a", Attempt: 2},
	}}

	if !out.FellBack() {
		t.Error("尝试过两个实现应记为回退")
	}
	if got := out.Retries(); got != 1 {
		t.Errorf("Retries = %d, 期望 1（3 次尝试 - 2 个实现）", got)
	}
	if got := out.Attempted(); strings.Join(got, ",") != "a,b,a" {
		t.Errorf("尝试序列 = %v, 期望按尝试顺序 a,b,a", got)
	}
}
