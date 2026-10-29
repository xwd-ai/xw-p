package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// 失败重试测试：验证「失败计数 → 指数退避 → 到期写回 → 次数耗尽进死信」。
// 用进程内 Redis 覆盖脚本逻辑与键布局，时钟通过 consumer.now 注入。

func fixedClock(t time.Time) func() time.Time {
	var mu sync.Mutex
	cur := t
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return cur
	}
}

// advanceClock 返回一个可推进的时钟。
type testClock struct {
	mu  sync.Mutex
	cur time.Time
}

func newTestClock(t time.Time) *testClock { return &testClock{cur: t} }

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cur = c.cur.Add(d)
}

func noJitterPolicy(maxAttempts int) RetryPolicy {
	return RetryPolicy{
		MaxAttempts: maxAttempts,
		Base:        time.Second,
		Max:         time.Minute,
		Jitter:      0,
	}
}

func TestRetryPolicyDelayGrowsAndCaps(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 10, Base: 100 * time.Millisecond, Max: time.Second, Jitter: 0}
	want := map[int]time.Duration{
		1:  100 * time.Millisecond,
		2:  200 * time.Millisecond,
		3:  400 * time.Millisecond,
		4:  800 * time.Millisecond,
		5:  time.Second,
		9:  time.Second,
		30: time.Second,
	}
	for attempt, expected := range want {
		if got := p.Delay(attempt); got != expected {
			t.Errorf("第 %d 次重试等待 %v，期望 %v", attempt, got, expected)
		}
	}
}

func TestRetryPolicyDelayJitterStaysWithinBound(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 5, Base: time.Second, Max: time.Minute, Jitter: 0.5}
	p.rnd = func() float64 { return 1 } // 抖动到下限
	if got := p.Delay(1); got != 500*time.Millisecond {
		t.Errorf("抖动下限应为 500ms，实际 %v", got)
	}
	p.rnd = func() float64 { return 0 } // 不抖动
	if got := p.Delay(1); got != time.Second {
		t.Errorf("无抖动时应为 1s，实际 %v", got)
	}
	// 随机值任意时，等待都不超过理论值、也不低于下界。
	for i := 0; i < 100; i++ {
		p.rnd = nil
		got := p.Delay(3)
		if got < 2*time.Second || got > 4*time.Second {
			t.Fatalf("第 3 次重试等待 %v 超出 [2s,4s]", got)
		}
	}
}

func TestRetryPolicyNormalizeAndStateTTL(t *testing.T) {
	// 延迟类字段为 0 时补默认值；抖动 0 是合法取值（表示不抖动），保持原样。
	p := RetryPolicy{MaxAttempts: 4}
	n := p.Normalize()
	if n.Base != DefaultRetryBase || n.Max != DefaultRetryMax || n.Jitter != 0 {
		t.Fatalf("0 值补全结果不符: %+v", n)
	}
	if DefaultRetryPolicy().Jitter != DefaultRetryJitter {
		t.Error("默认策略应带抖动")
	}
	if !n.Enabled() {
		t.Error("默认策略应处于启用状态")
	}
	if (RetryPolicy{}).Normalize().Enabled() {
		t.Error("MaxAttempts 为 0 时应视为关闭重试")
	}

	// 计数键的存活时间必须覆盖整个退避窗口，否则计数过期会让重试重新从 1 开始。
	p = RetryPolicy{MaxAttempts: 5, Base: time.Second, Max: time.Minute, Jitter: 0}
	window := time.Duration(0)
	for i := 1; i < p.MaxAttempts; i++ {
		window += p.Delay(i)
	}
	if ttl := p.StateTTL(); ttl <= window {
		t.Errorf("状态存活时间 %v 未覆盖退避窗口 %v", ttl, window)
	}
	if ttl := p.StateTTL(); ttl > 24*time.Hour {
		t.Errorf("状态存活时间 %v 超过 24h 上限", ttl)
	}
}

func TestRetrySchedulesBackoffAndPromotes(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	clock := newTestClock(time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC))
	f.consumer.now = clock.Now
	f.consumer.grace = 0

	payload := `{"trigger_id":"t-like-1","name":"post_liked","subscriber_id":"creator_1","payload":{"post_id":"P10293"}}`
	f.push(t, payload)

	d, err := f.consumer.Claim(ctx, time.Second)
	if err != nil || d == nil {
		t.Fatalf("领取失败: %v", err)
	}

	out, err := f.consumer.Retry(ctx, d, errors.New("smtp 超时"), noJitterPolicy(4))
	if err != nil {
		t.Fatalf("登记重试失败: %v", err)
	}
	if out.Dead {
		t.Fatal("首次失败不该直接进死信")
	}
	if out.Attempt != 1 || out.Delay != time.Second {
		t.Fatalf("首次重试应为第 1 次、等待 1s，实际 %+v", out)
	}

	// 重试记录接管了这条消息，本次领取必须从在途记录里消失，
	// 否则租约回收会把同一条消息再投一次。
	if len(f.inflight(t)) != 0 {
		t.Fatal("登记重试后不应残留本次领取的在途记录")
	}
	if n := f.client.ZCard(ctx, f.key+":retry").Val(); n != 1 {
		t.Fatalf("待重试集合应有 1 条，实际 %d", n)
	}

	// 未到时间不提升。
	if promoted, err := f.consumer.PromoteDueRetries(ctx, 10); err != nil || len(promoted) != 0 {
		t.Fatalf("未到期不应提升: %v %v", promoted, err)
	}

	clock.Advance(time.Second)
	promoted, err := f.consumer.PromoteDueRetries(ctx, 10)
	if err != nil {
		t.Fatalf("提升失败: %v", err)
	}
	if len(promoted) != 1 {
		t.Fatalf("应提升 1 条，实际 %d", len(promoted))
	}
	if string(promoted[0].Raw) != payload {
		t.Fatalf("写回的消息体必须与原始消息一致，实际 %s", promoted[0].Raw)
	}
	if promoted[0].Attempts != 1 {
		t.Errorf("写回消息的累计次数应为 1，实际 %d", promoted[0].Attempts)
	}
	// 记录与消息体都要清掉，不能留下无人认领的残片。
	if n := f.client.ZCard(ctx, f.key+":retry").Val(); n != 0 {
		t.Errorf("提升后待重试集合应为空，实际 %d", n)
	}
	if n := f.client.HLen(ctx, f.key+":retry:body").Val(); n != 0 {
		t.Errorf("提升后消息体表应为空，实际 %d", n)
	}

	// 第二次领取同一条消息并再次失败：次数累加、等待翻倍。
	d2, err := f.consumer.Claim(ctx, time.Second)
	if err != nil || d2 == nil {
		t.Fatalf("重新领取失败: %v", err)
	}
	if string(d2.Raw) != payload {
		t.Fatalf("重投的消息体被改写: %s", d2.Raw)
	}
	if d2.ID == d.ID {
		t.Error("重投应生成新的领取凭据")
	}
	out2, err := f.consumer.Retry(ctx, d2, errors.New("smtp 超时"), noJitterPolicy(4))
	if err != nil {
		t.Fatalf("第二次登记重试失败: %v", err)
	}
	if out2.Attempt != 2 || out2.Delay != 2*time.Second {
		t.Fatalf("第二次重试应为第 2 次、等待 2s，实际 %+v", out2)
	}
	if !out2.FirstFailedAt.Equal(out.FirstFailedAt) {
		t.Errorf("首次失败时间应跨重试保持不变: %v vs %v", out2.FirstFailedAt, out.FirstFailedAt)
	}
}

func TestRetryExhaustsIntoDeadLetter(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	clock := newTestClock(time.Date(2026, 10, 29, 9, 30, 0, 0, time.UTC))
	f.consumer.now = clock.Now
	f.consumer.grace = 0

	policy := noJitterPolicy(3) // 最多处理 3 次：两次重试，第 3 次失败进死信
	payload := `{"trigger_id":"t-cmt-9","name":"post_commented"}`
	f.push(t, payload)

	var last RetryOutcome
	for i := 0; i < 3; i++ {
		d, err := f.consumer.Claim(ctx, time.Second)
		if err != nil || d == nil {
			t.Fatalf("第 %d 次领取失败: %v", i+1, err)
		}
		last, err = f.consumer.Retry(ctx, d, fmt.Errorf("下游 502 (第 %d 次)", i+1), policy)
		if err != nil {
			t.Fatalf("第 %d 次登记失败: %v", i+1, err)
		}
		if i < 2 {
			if last.Dead {
				t.Fatalf("第 %d 次失败不该进死信", i+1)
			}
			clock.Advance(last.Delay)
			if _, err := f.consumer.PromoteDueRetries(ctx, 10); err != nil {
				t.Fatalf("提升失败: %v", err)
			}
			continue
		}
		if !last.Dead {
			t.Fatal("最后一次失败应转入死信")
		}
	}

	if last.Attempt != 3 {
		t.Errorf("死信应记录 3 次处理，实际 %d", last.Attempt)
	}
	if n := f.client.ZCard(ctx, f.key+":retry").Val(); n != 0 {
		t.Errorf("进死信后不应残留待重试记录，实际 %d", n)
	}
	if n := f.client.LLen(ctx, f.key).Val(); n != 0 {
		t.Errorf("进死信的消息不应再回到主队列，实际 %d 条", n)
	}

	dead, err := f.consumer.DeadLetters(ctx, 10)
	if err != nil {
		t.Fatalf("读取死信失败: %v", err)
	}
	if len(dead) != 1 {
		t.Fatalf("应有 1 条死信，实际 %d", len(dead))
	}
	if string(dead[0].Payload) != payload {
		t.Errorf("死信应保留原始消息体，实际 %s", dead[0].Payload)
	}
	if dead[0].Attempts != 3 {
		t.Errorf("死信尝试次数应为 3，实际 %d", dead[0].Attempts)
	}
	if dead[0].LastError == "" || dead[0].Queue != f.key {
		t.Errorf("死信应带失败原因与来源队列: %+v", dead[0])
	}
	if !dead[0].FirstFailedAt.Before(dead[0].DeadAt) {
		t.Errorf("首次失败时间应早于入死信时间: %+v", dead[0])
	}
}

func TestRetryRemovesInflightSoReclaimSkipsIt(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	clock := newTestClock(time.Date(2026, 10, 29, 8, 0, 0, 0, time.UTC))
	f.consumer.now = clock.Now

	f.push(t, `{"trigger_id":"t-mention-1"}`)
	d, err := f.consumer.Claim(ctx, time.Second)
	if err != nil || d == nil {
		t.Fatalf("领取失败: %v", err)
	}
	if _, err := f.consumer.Retry(ctx, d, errors.New("boom"), noJitterPolicy(5)); err != nil {
		t.Fatalf("登记重试失败: %v", err)
	}

	// 即使租约早已过期，回收也不该再把它抓起来——否则退避形同虚设。
	clock.Advance(2 * time.Hour)
	report, err := f.consumer.ReclaimExpired(ctx, 10)
	if err != nil {
		t.Fatalf("回收失败: %v", err)
	}
	if len(report.Requeued) != 0 {
		t.Fatalf("已被重试接管的记录不该被回收重投: %+v", report.Requeued)
	}
	if n := f.client.LLen(ctx, f.key).Val(); n != 0 {
		t.Errorf("主队列不应有消息，实际 %d 条", n)
	}
}

func TestPromoteDueRetriesConcurrentNoDuplicate(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	clock := newTestClock(time.Date(2026, 10, 29, 7, 0, 0, 0, time.UTC))
	f.consumer.now = clock.Now

	const total = 24
	policy := RetryPolicy{MaxAttempts: 5, Base: time.Millisecond, Max: time.Millisecond, Jitter: 0}
	for i := 0; i < total; i++ {
		f.push(t, fmt.Sprintf(`{"trigger_id":"t-%02d"}`, i))
	}
	for i := 0; i < total; i++ {
		d, err := f.consumer.Claim(ctx, time.Second)
		if err != nil || d == nil {
			t.Fatalf("第 %d 条领取失败: %v", i, err)
		}
		if _, err := f.consumer.Retry(ctx, d, errors.New("boom"), policy); err != nil {
			t.Fatalf("第 %d 条登记重试失败: %v", i, err)
		}
	}

	clock.Advance(time.Second) // 全部到期

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		promoted  []Promoted
		promoteMu sync.Mutex
	)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := f.consumer.PromoteDueRetries(ctx, total)
			if err != nil {
				return
			}
			promoteMu.Lock()
			promoted = append(promoted, got...)
			promoteMu.Unlock()
		}()
	}
	wg.Wait()
	mu.Lock()
	count := len(promoted)
	mu.Unlock()

	if count != total {
		t.Fatalf("并发提升应恰好提升 %d 条，实际 %d", total, count)
	}
	if n := f.client.LLen(ctx, f.key).Val(); n != total {
		t.Errorf("主队列应有 %d 条，实际 %d", total, n)
	}
	seen := map[string]bool{}
	for _, p := range promoted {
		key := string(p.Raw)
		if seen[key] {
			t.Fatalf("同一条消息被提升两次: %s", key)
		}
		seen[key] = true
	}
	if n := f.client.ZCard(ctx, f.key+":retry").Val(); n != 0 {
		t.Errorf("待重试集合应被清空，实际 %d", n)
	}

	// 提升回来的消息能正常领取。
	d, err := f.consumer.Claim(ctx, time.Second)
	if err != nil || d == nil {
		t.Fatalf("提升后消息应可领取: %v", err)
	}
}

func TestRetryDisabledPolicyRejected(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	if _, err := f.consumer.Retry(ctx, &Delivery{ID: "d", Raw: []byte(`{}`)}, errors.New("x"), RetryPolicy{}); err == nil {
		t.Fatal("关闭重试时应返回错误，由调用方保留在途记录")
	}
}

func TestAttemptKeyIsContentAddressed(t *testing.T) {
	f := newConsumerFixture(t)
	same := []byte(`{"trigger_id":"t-1"}`)
	other := []byte(`{"trigger_id":"t-2"}`)
	if f.consumer.attemptKey(same) != f.consumer.attemptKey([]byte(`{"trigger_id":"t-1"}`)) {
		t.Error("同一消息体的计数键应一致")
	}
	if f.consumer.attemptKey(same) == f.consumer.attemptKey(other) {
		t.Error("不同消息体应使用不同的计数键")
	}
}
