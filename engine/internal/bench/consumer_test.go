package bench

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// fakeClaimer 是读取端替身：按顺序吐出预置的投递，记录确认情况。
type fakeClaimer struct {
	mu      sync.Mutex
	pending []*queue.Delivery
	acked   []string
	failAck bool
	failOn  bool
}

func (f *fakeClaimer) Claim(ctx context.Context, timeout time.Duration) (*queue.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failOn {
		return nil, errors.New("注入的领取失败")
	}
	if len(f.pending) == 0 {
		return nil, nil
	}
	d := f.pending[0]
	f.pending = f.pending[1:]
	return d, nil
}

func (f *fakeClaimer) Ack(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAck {
		return errors.New("注入的确认失败")
	}
	f.acked = append(f.acked, id)
	return nil
}

func (f *fakeClaimer) ackedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.acked)
}

func (f *fakeClaimer) setFailAck(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAck = v
}

func deliveries(n int, stamp time.Time) []*queue.Delivery {
	out := make([]*queue.Delivery, 0, n)
	for i := 0; i < n; i++ {
		raw, _ := BuildEnvelope(i, stamp, nil).Encode()
		out = append(out, &queue.Delivery{ID: newID(i), Raw: raw})
	}
	return out
}

func newID(i int) string { return fmt.Sprintf("d-%d", i) }

// waitFor 等待条件成立，最多 2 秒。
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}

func TestConsumerHandlesAndAcksAll(t *testing.T) {
	src := &fakeClaimer{pending: deliveries(30, time.Now())}
	c := NewConsumer(src, 4, func(ctx context.Context, raw []byte) error { return nil })
	c.Start(context.Background())

	waitFor(t, func() bool { return c.Done() >= 30 }, "30 条消息处理完成")
	stats := c.Stop()

	if stats.Handled != 30 || stats.Failed != 0 {
		t.Fatalf("Handled=%d Failed=%d, 期望 30/0", stats.Handled, stats.Failed)
	}
	if src.ackedCount() != 30 {
		t.Errorf("确认了 %d 条, 期望 30", src.ackedCount())
	}
	if stats.Latency.Len() != 30 {
		t.Errorf("延迟样本 %d 个, 期望 30", stats.Latency.Len())
	}
	if stats.Finished.Before(stats.Started) {
		t.Error("消费窗口结束时刻早于开始时刻")
	}
}

func TestConsumerRecordsEndToEndLatency(t *testing.T) {
	const lag = 40 * time.Millisecond
	src := &fakeClaimer{pending: deliveries(3, time.Now().Add(-lag))}
	c := NewConsumer(src, 1, func(ctx context.Context, raw []byte) error { return nil }).WithSentAt(StampOf)
	c.Start(context.Background())

	waitFor(t, func() bool { return c.Done() >= 3 }, "3 条消息处理完成")
	stats := c.Stop()

	if got := stats.Latency.Percentile(0.5); got < lag {
		t.Errorf("端到端耗时 %s 应不小于打点间隔 %s", got, lag)
	}
}

func TestConsumerCountsHandlerFailuresAndStillAcks(t *testing.T) {
	src := &fakeClaimer{pending: deliveries(5, time.Now())}
	c := NewConsumer(src, 2, func(ctx context.Context, raw []byte) error {
		return errors.New("处理失败")
	})
	c.Start(context.Background())

	waitFor(t, func() bool { return c.Done() >= 5 }, "5 条消息处理结束")
	stats := c.Stop()

	if stats.Failed != 5 || stats.Handled != 0 {
		t.Fatalf("Failed=%d Handled=%d, 期望 5/0", stats.Failed, stats.Handled)
	}
	if src.ackedCount() != 5 {
		t.Errorf("失败消息也应确认（不重投），实际确认 %d 条", src.ackedCount())
	}
	if len(stats.Errors) == 0 {
		t.Error("应记录失败原因样本")
	}
}

func TestConsumerAckFailureIsCounted(t *testing.T) {
	src := &fakeClaimer{pending: deliveries(2, time.Now())}
	src.setFailAck(true)
	c := NewConsumer(src, 1, func(ctx context.Context, raw []byte) error { return nil })
	c.Start(context.Background())

	waitFor(t, func() bool { return c.Errors() >= 2 || c.ClaimErrors() >= 2 }, "确认失败被记录")
	stats := c.Stop()

	if stats.Handled != 0 {
		t.Errorf("确认失败不应计入处理成功, Handled=%d", stats.Handled)
	}
	if stats.ClaimErrors < 2 {
		t.Errorf("确认失败计数 = %d, 期望至少 2", stats.ClaimErrors)
	}
}

func TestConsumerCountsClaimErrors(t *testing.T) {
	src := &fakeClaimer{failOn: true}
	c := NewConsumer(src, 1, func(ctx context.Context, raw []byte) error { return nil })
	c.Start(context.Background())

	waitFor(t, func() bool { return c.ClaimErrors() > 0 }, "领取失败被记录")
	stats := c.Stop()

	if len(stats.Errors) == 0 {
		t.Error("应记录领取失败原因")
	}
}

func TestConsumerStopIsIdempotent(t *testing.T) {
	src := &fakeClaimer{}
	c := NewConsumer(src, 2, func(ctx context.Context, raw []byte) error { return nil })
	c.Start(context.Background())
	first := c.Stop()
	second := c.Stop() // 第二次调用不应 panic，也不该把样本重复合并
	if second.Latency.Len() != first.Latency.Len() {
		t.Errorf("重复停止后样本数从 %d 变成 %d", first.Latency.Len(), second.Latency.Len())
	}
}
