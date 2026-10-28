package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

// chanConsumer 是并发安全的测试消息源：从 channel 取消息，记录领取与确认情况。
type chanConsumer struct {
	items  chan []byte
	seq    int64
	mu     sync.Mutex
	acked  map[string]bool
	claims int
}

func newChanConsumer(n int) *chanConsumer {
	c := &chanConsumer{items: make(chan []byte, n), acked: make(map[string]bool)}
	for i := 0; i < n; i++ {
		c.items <- []byte(fmt.Sprintf(`{"trigger_id":"t-%d"}`, i))
	}
	return c
}

func (c *chanConsumer) Claim(ctx context.Context, timeout time.Duration) (*queue.Delivery, error) {
	if timeout > 20*time.Millisecond {
		timeout = 20 * time.Millisecond
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case raw, ok := <-c.items:
		if !ok {
			return nil, nil
		}
		id := fmt.Sprintf("d-%d", atomic.AddInt64(&c.seq, 1))
		c.mu.Lock()
		c.claims++
		c.mu.Unlock()
		return &queue.Delivery{ID: id, Raw: raw, ClaimedAt: time.Now()}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, nil
	}
}

func (c *chanConsumer) Ack(ctx context.Context, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acked[id] = true
	return nil
}

func (c *chanConsumer) ackedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.acked)
}

func (c *chanConsumer) claimCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.claims
}

func TestNormalizePoolSize(t *testing.T) {
	cases := []struct{ in, want int }{
		{-3, 1}, {0, 1}, {1, 1}, {8, 8}, {MaxPoolSize + 1, MaxPoolSize},
	}
	for _, c := range cases {
		if got := NormalizePoolSize(c.in); got != c.want {
			t.Errorf("NormalizePoolSize(%d) = %d, 期望 %d", c.in, got, c.want)
		}
	}
}

// 消息不重不漏：每条的 trigger_id 恰好处理一次，并且全部确认。
func TestPoolHandlesEveryMessageExactlyOnce(t *testing.T) {
	const total = 240
	consumer := newChanConsumer(total)

	var (
		mu   sync.Mutex
		seen = make(map[string]int)
	)
	pool := NewPool(consumer, 8, time.Millisecond, discardLogger())
	pool.handle = func(raw []byte) error {
		mu.Lock()
		seen[string(raw)]++
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- pool.Run(ctx) }()

	waitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) >= total
	})
	waitFor(t, 5*time.Second, func() bool { return consumer.ackedCount() >= total })
	cancel()

	select {
	case left := <-done:
		if left != 0 {
			t.Errorf("正常退出不应留下未完成消息，实际 %d", left)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("消费者池未在 ctx 取消后退出")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != total {
		t.Fatalf("应处理 %d 条，实际 %d 条", total, len(seen))
	}
	for raw, n := range seen {
		if n != 1 {
			t.Errorf("消息 %s 被处理 %d 次", raw, n)
		}
	}
	if got := consumer.ackedCount(); got != total {
		t.Errorf("应确认 %d 条，实际 %d", total, got)
	}
}

// 并发度生效：同时在处理的消息数不超过池大小，且确实大于 1。
func TestPoolRunsMessagesConcurrently(t *testing.T) {
	const (
		total   = 60
		workers = 4
	)
	consumer := newChanConsumer(total)

	var (
		mu   sync.Mutex
		cur  int
		peak int
	)
	pool := NewPool(consumer, workers, time.Millisecond, discardLogger())
	pool.handle = func(raw []byte) error {
		mu.Lock()
		cur++
		if cur > peak {
			peak = cur
		}
		mu.Unlock()

		time.Sleep(3 * time.Millisecond)

		mu.Lock()
		cur--
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- pool.Run(ctx) }()

	waitFor(t, 5*time.Second, func() bool { return consumer.ackedCount() >= total })
	cancel()
	<-done

	mu.Lock()
	got := peak
	mu.Unlock()
	if got > workers {
		t.Errorf("并发峰值 %d 超过池大小 %d", got, workers)
	}
	if got < 2 {
		t.Errorf("并发峰值仅 %d，消费者池没有并行处理", got)
	}
}

// 关闭时正在处理的消息要跑完并确认，之后不再领取新消息。
func TestPoolWaitsForInflightOnShutdown(t *testing.T) {
	consumer := newChanConsumer(5)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	pool := NewPool(consumer, 2, time.Millisecond, discardLogger())
	pool.handle = func(raw []byte) error {
		once.Do(func() { close(entered) })
		<-release
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- pool.Run(ctx) }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("消息未被领取")
	}
	// 两个消费者各领一条后都卡在处理里，此时再发关闭信号。
	waitFor(t, 2*time.Second, func() bool { return consumer.claimCount() == 2 })

	cancel()

	select {
	case <-done:
		t.Fatal("在途消息处理中不应立即退出")
	case <-time.After(80 * time.Millisecond):
	}

	close(release)
	select {
	case left := <-done:
		if left != 0 {
			t.Errorf("在途消息应处理完毕，实际遗留 %d", left)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("释放后在途消息仍未收尾")
	}

	// 两条被领取的消息都应确认；取消之后不应再有新的领取。
	if got := consumer.ackedCount(); got != 2 {
		t.Errorf("已领取的 2 条都应确认，实际 %d", got)
	}
	if got := consumer.claimCount(); got != 2 {
		t.Errorf("取消后不应继续领取，实际领取 %d 条", got)
	}
}

// 处理卡死超过 grace 时放弃等待，消息不确认（留给租约回收重新投递）。
func TestPoolAbandonsStuckMessageAfterGrace(t *testing.T) {
	consumer := newChanConsumer(1)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	pool := NewPool(consumer, 1, time.Millisecond, discardLogger()).WithShutdownGrace(50 * time.Millisecond)
	pool.handle = func(raw []byte) error {
		once.Do(func() { close(entered) })
		<-release
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- pool.Run(ctx) }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("消息未被领取")
	}

	cancel()

	select {
	case left := <-done:
		if left != 1 {
			t.Errorf("应报告 1 条未完成，实际 %d", left)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("超过 grace 后应放弃等待")
	}

	if got := consumer.ackedCount(); got != 0 {
		t.Errorf("卡死消息不应被确认，实际确认 %d 条", got)
	}

	close(release)
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("等待条件超时")
}
