package bench

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// 批量领取的用例：预取生效时一次脚本取回多条、一次确认整批；
// 读取端不支持批量时仍然逐条领取，行为不能变。

type fakeBatchClaimer struct {
	fakeClaimer

	mu       sync.Mutex
	batches  [][]string
	requests int
}

func (f *fakeBatchClaimer) ClaimBatch(ctx context.Context, max int) ([]*queue.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests++
	if len(f.pending) == 0 {
		return nil, nil
	}
	end := max
	if end > len(f.pending) {
		end = len(f.pending)
	}
	out := append([]*queue.Delivery(nil), f.pending[:end]...)
	f.pending = f.pending[end:]
	return out, nil
}

func (f *fakeBatchClaimer) AckBatch(ctx context.Context, ids []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, append([]string(nil), ids...))
	return nil
}

func (f *fakeBatchClaimer) batchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches)
}

// ackedTotal 统计批量确认覆盖的消息条数。
func (f *fakeBatchClaimer) ackedTotal() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := len(f.acked)
	for _, b := range f.batches {
		n += len(b)
	}
	return n
}

func TestConsumerPrefetchUsesBatchClaimAndAck(t *testing.T) {
	src := &fakeBatchClaimer{fakeClaimer: fakeClaimer{pending: deliveries(40, time.Now())}}
	c := NewConsumer(src, 2, func(ctx context.Context, raw []byte) error { return nil }).WithPrefetch(8)
	c.Start(context.Background())

	waitFor(t, func() bool { return c.Done() >= 40 }, "40 条消息处理完成")
	stats := c.Stop()

	if stats.Handled != 40 {
		t.Fatalf("Handled=%d，期望 40", stats.Handled)
	}
	if got := src.ackedTotal(); got != 40 {
		t.Errorf("确认条数 = %d，期望 40", got)
	}
	// 40 条按每批 8 条领取，确认应当少于 40 次。
	if got := src.batchCount(); got == 0 || got >= 40 {
		t.Errorf("批量确认次数 = %d，期望明显少于消息条数", got)
	}
}

func TestConsumerWithoutPrefetchKeepsSingleAck(t *testing.T) {
	src := &fakeBatchClaimer{fakeClaimer: fakeClaimer{pending: deliveries(6, time.Now())}}
	c := NewConsumer(src, 1, func(ctx context.Context, raw []byte) error { return nil })
	c.Start(context.Background())

	waitFor(t, func() bool { return c.Done() >= 6 }, "6 条消息处理完成")
	c.Stop()

	if got := src.batchCount(); got != 0 {
		t.Errorf("未开启预取时不应走批量确认，实际 %d 次", got)
	}
	if got := src.ackedCount(); got != 6 {
		t.Errorf("逐条确认应覆盖 6 条，实际 %d", got)
	}
}

// 读取端不支持批量时，即使配置了预取也只能逐条领取。
func TestConsumerPrefetchFallsBackWithoutBatchSupport(t *testing.T) {
	src := &fakeClaimer{pending: deliveries(5, time.Now())}
	c := NewConsumer(src, 1, func(ctx context.Context, raw []byte) error { return nil }).WithPrefetch(16)
	c.Start(context.Background())

	waitFor(t, func() bool { return c.Done() >= 5 }, "5 条消息处理完成")
	stats := c.Stop()

	if stats.Handled != 5 {
		t.Fatalf("Handled=%d，期望 5", stats.Handled)
	}
	if got := src.ackedCount(); got != 5 {
		t.Errorf("确认条数 = %d，期望 5", got)
	}
}

func TestPlanNormalizesPrefetch(t *testing.T) {
	if got := (Plan{Prefetch: 0}).Normalize().Prefetch; got != 1 {
		t.Errorf("未配置的预取应回落到 1，实际 %d", got)
	}
	if got := (Plan{Prefetch: -3}).Normalize().Prefetch; got != 1 {
		t.Errorf("负数预取应回落到 1，实际 %d", got)
	}
	if got := (Plan{Prefetch: MaxPrefetch * 2}).Normalize().Prefetch; got != MaxPrefetch {
		t.Errorf("超限预取应收敛到 %d，实际 %d", MaxPrefetch, got)
	}
}

// idleBlockingClaimer 在队列排空后阻塞一段时间，模拟真实消息源等待新消息。
type idleBlockingClaimer struct {
	fakeClaimer
	idle time.Duration
}

func (f *idleBlockingClaimer) Claim(ctx context.Context, timeout time.Duration) (*queue.Delivery, error) {
	f.mu.Lock()
	empty := len(f.pending) == 0
	f.mu.Unlock()
	if empty {
		time.Sleep(f.idle)
		return nil, nil
	}
	return f.fakeClaimer.Claim(ctx, timeout)
}

// 窗口的终点应落在最后一次确认，而不是收尾时仍在等待消息的消费者退出。
// 否则「消费者已经把队列清空、只是还阻塞在等待上」会被算进吞吐的分母。
func TestStopWindowEndsAtLastAck(t *testing.T) {
	src := &idleBlockingClaimer{
		fakeClaimer: fakeClaimer{pending: deliveries(3, time.Now())},
		idle:        200 * time.Millisecond,
	}
	c := NewConsumer(src, 1, func(ctx context.Context, raw []byte) error { return nil })
	c.Start(context.Background())

	waitFor(t, func() bool { return c.Done() >= 3 }, "3 条消息处理完成")
	before := time.Now()
	stats := c.Stop()

	if stats.Finished.After(before) {
		t.Errorf("窗口终点 %v 晚于收尾时刻 %v，说明把等待时间算进了窗口",
			stats.Finished, before)
	}
	if stats.Handled != 3 {
		t.Errorf("Handled=%d，期望 3", stats.Handled)
	}
}
