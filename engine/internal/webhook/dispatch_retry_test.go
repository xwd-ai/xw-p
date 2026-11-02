package webhook

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/backoff"
)

// fastRetry 是退避极短的策略：用例要的是「重试了几次、等没等」，
// 不关心真实的秒级间隔。
func fastRetry(attempts int) backoff.Policy {
	return backoff.Policy{
		MaxAttempts: attempts,
		Base:        time.Millisecond,
		Max:         5 * time.Millisecond,
	}
}

// 可重试的失败按曲线再试，成功后记一次投递。
func TestDispatcherRetriesUntilSuccess(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	sender := &fakeSender{deliver: func(context.Context, Target, Event) Result {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls < 3 {
			return Result{Status: 503, Err: "回调被拒绝（503）", Retryable: true}
		}
		return Result{Status: 200}
	}}

	opts := quietOptions()
	opts.Retry = fastRetry(3)
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), sender, opts)
	d.Notify(sampleEvent())
	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}

	stats := d.Stats()
	if sender.calls() != 3 {
		t.Errorf("应在第三次尝试成功，实际 %d 次", sender.calls())
	}
	if stats.Delivered != 1 || stats.Failed != 0 {
		t.Errorf("成功后不应记失败: %+v", stats)
	}
	if stats.Retried != 2 {
		t.Errorf("应记 2 次重试，实际 %+v", stats)
	}
}

// 次数用尽仍是失败：计一次失败，重试次数如实记录。
func TestDispatcherExhaustsRetries(t *testing.T) {
	sender := &fakeSender{deliver: func(context.Context, Target, Event) Result {
		return Result{Status: 500, Err: "回调被拒绝（500）", Retryable: true}
	}}

	opts := quietOptions()
	opts.Retry = fastRetry(3)
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), sender, opts)
	d.Notify(sampleEvent())
	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}

	if sender.calls() != 3 {
		t.Errorf("应尝试 3 次，实际 %d", sender.calls())
	}
	stats := d.Stats()
	if stats.Failed != 1 || stats.Delivered != 0 {
		t.Errorf("计数不符: %+v", stats)
	}
	if stats.Retried != 2 {
		t.Errorf("应记 2 次重试，实际 %+v", stats)
	}
}

// 报文与鉴权问题不再重试：同一个报文再发一次也是同一个结果。
func TestDispatcherSkipsRetryForPermanentFailure(t *testing.T) {
	sender := &fakeSender{deliver: func(context.Context, Target, Event) Result {
		return Result{Status: 401, Err: "回调被拒绝（401）"}
	}}

	opts := quietOptions()
	opts.Retry = fastRetry(5)
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), sender, opts)
	d.Notify(sampleEvent())
	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}

	if sender.calls() != 1 {
		t.Errorf("不可重试的失败应只试一次，实际 %d 次", sender.calls())
	}
	if stats := d.Stats(); stats.Failed != 1 || stats.Retried != 0 {
		t.Errorf("计数不符: %+v", stats)
	}
}

// 接收方给的 Retry-After 比退避长时按对方的时间等，但不能超过曲线封顶——
// 否则一条事件能把发送 goroutine 按住几分钟。
func TestDispatcherHonorsRetryAfterWithinCap(t *testing.T) {
	sender := &fakeSender{deliver: func(context.Context, Target, Event) Result {
		return Result{Status: 429, Err: "回调被拒绝（429）", Retryable: true, RetryAfter: 5 * time.Second}
	}}

	opts := quietOptions()
	opts.Retry = backoff.Policy{
		MaxAttempts: 2,
		Base:        time.Millisecond,
		Max:         20 * time.Millisecond,
	}
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), sender, opts)

	start := time.Now()
	d.Notify(sampleEvent())
	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}
	elapsed := time.Since(start)

	if sender.calls() != 2 {
		t.Errorf("应尝试 2 次，实际 %d", sender.calls())
	}
	if elapsed < 20*time.Millisecond {
		t.Errorf("应按对方的 Retry-After 等待（封顶 20ms），实际 %v", elapsed)
	}
	if elapsed > time.Second {
		t.Errorf("等待时间应被封顶，实际 %v", elapsed)
	}
}

// 宽限到期时打断正在退避的重试：退出流程不被一条事件按住，也不再发起
// 新的尝试。
func TestDispatcherAbortsRetryOnClose(t *testing.T) {
	started := make(chan struct{}, 1)
	sender := &fakeSender{deliver: func(context.Context, Target, Event) Result {
		select {
		case started <- struct{}{}:
		default:
		}
		return Result{Status: 500, Err: "回调被拒绝（500）", Retryable: true}
	}}

	opts := quietOptions()
	opts.Workers = 1
	opts.Retry = backoff.Policy{MaxAttempts: 5, Base: 30 * time.Second, Max: 30 * time.Second}
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), sender, opts)
	d.Notify(sampleEvent())
	<-started

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := d.Close(ctx); err == nil {
		t.Fatal("宽限到期应返回错误")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("关闭应打断退避等待，实际耗时 %v", elapsed)
	}
	if sender.calls() != 1 {
		t.Errorf("关闭后不应再发起尝试，实际 %d 次", sender.calls())
	}
	if stats := d.Stats(); stats.Failed != 1 {
		t.Errorf("放弃重试的事件应记一次失败: %+v", stats)
	}
}

// 重试曲线的取值收敛：0 表示没配（用缺省），1 表示只发一次，越界夹到上限。
func TestNormalizeRetryPolicy(t *testing.T) {
	if got := normalizeRetryPolicy(backoff.Policy{}); got.MaxAttempts != DefaultRetryAttempts {
		t.Errorf("零值应补缺省次数，实际 %d", got.MaxAttempts)
	}
	got := normalizeRetryPolicy(backoff.Policy{MaxAttempts: 1})
	if got.MaxAttempts != 1 {
		t.Errorf("显式不重试应保留，实际 %d", got.MaxAttempts)
	}
	if got = normalizeRetryPolicy(backoff.Policy{MaxAttempts: -3}); got.MaxAttempts != 1 {
		t.Errorf("负数应收敛到 1，实际 %d", got.MaxAttempts)
	}
	if got = normalizeRetryPolicy(backoff.Policy{MaxAttempts: 1000}); got.MaxAttempts != MaxRetryAttempts {
		t.Errorf("超上限应收敛到 %d，实际 %d", MaxRetryAttempts, got.MaxAttempts)
	}
	if got = normalizeRetryPolicy(backoff.Policy{MaxAttempts: 3, Base: -1}); got.Base <= 0 {
		t.Errorf("非法退避时长应补默认值: %+v", got)
	}
	// 封顶小于起步时按起步算，避免曲线自相矛盾。
	if got = normalizeRetryPolicy(backoff.Policy{MaxAttempts: 3, Base: time.Second, Max: time.Millisecond}); got.Max < got.Base {
		t.Errorf("封顶不应小于起步: %+v", got)
	}
}

// 分发器暴露生效曲线，装配方才能把它打进启动日志。
func TestDispatcherRetryAccessor(t *testing.T) {
	opts := quietOptions()
	opts.Retry = fastRetry(4)
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), &fakeSender{}, opts)
	defer d.Close(context.Background())

	if got := d.Retry().MaxAttempts; got != 4 {
		t.Errorf("生效次数不符: %d", got)
	}

	var nilDispatcher *Dispatcher
	if got := nilDispatcher.Retry(); got.MaxAttempts != 0 {
		t.Errorf("nil 分发器应返回空策略: %+v", got)
	}
}
