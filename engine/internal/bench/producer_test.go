package bench

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeSink 是写入端替身：记录条数与顺序，可注入失败。
type fakeSink struct {
	mu     sync.Mutex
	items  [][]byte
	failOn map[int]bool
	calls  atomic.Int64
}

func newFakeSink() *fakeSink { return &fakeSink{failOn: map[int]bool{}} }

func (s *fakeSink) Enqueue(ctx context.Context, payload []byte) error {
	n := int(s.calls.Add(1)) - 1
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failOn[n] {
		return errors.New("注入的写入失败")
	}
	s.items = append(s.items, payload)
	return nil
}

func (s *fakeSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

func TestProducerWritesAllMessages(t *testing.T) {
	sink := newFakeSink()
	plan := Plan{Total: 50, Concurrency: 4}.Normalize()
	p := NewProducer(sink, plan, func(seq int, now time.Time) ([]byte, error) {
		return BuildEnvelope(seq, now, nil).Encode()
	})

	stats := p.Run(context.Background())
	if stats.Sent != 50 || stats.Failed != 0 {
		t.Fatalf("Sent=%d Failed=%d, 期望 50/0", stats.Sent, stats.Failed)
	}
	if sink.count() != 50 {
		t.Errorf("写入端收到 %d 条, 期望 50", sink.count())
	}
	if stats.Latency.Len() != 50 {
		t.Errorf("入队耗时样本 %d 个, 期望 50", stats.Latency.Len())
	}
	if stats.Elapsed() <= 0 {
		t.Error("写入窗口应为正")
	}
}

func TestProducerRespectsRate(t *testing.T) {
	sink := newFakeSink()
	plan := Plan{Total: 10, Concurrency: 2, Rate: 200}.Normalize()
	p := NewProducer(sink, plan, func(seq int, now time.Time) ([]byte, error) {
		return []byte(`{"bench":{"sent_at":1}}`), nil
	})

	start := time.Now()
	stats := p.Run(context.Background())
	elapsed := time.Since(start)

	if stats.Sent != 10 {
		t.Fatalf("Sent=%d, 期望 10", stats.Sent)
	}
	if elapsed < 30*time.Millisecond {
		t.Errorf("限速 200 条/秒 写入 10 条仅耗时 %s，未生效", elapsed)
	}
}

func TestProducerCountsFailures(t *testing.T) {
	sink := newFakeSink()
	for i := 0; i < 3; i++ {
		sink.failOn[i] = true
	}
	plan := Plan{Total: 10, Concurrency: 1}.Normalize()
	p := NewProducer(sink, plan, func(seq int, now time.Time) ([]byte, error) {
		return []byte("x"), nil
	})

	stats := p.Run(context.Background())
	if stats.Sent != 7 || stats.Failed != 3 {
		t.Fatalf("Sent=%d Failed=%d, 期望 7/3", stats.Sent, stats.Failed)
	}
	if len(stats.Errors) != 3 {
		t.Errorf("错误样本 %d 条, 期望 3", len(stats.Errors))
	}
}

func TestProducerErrorMessageSampleIsCapped(t *testing.T) {
	sink := newFakeSink()
	for i := 0; i < 20; i++ {
		sink.failOn[i] = true
	}
	plan := Plan{Total: 20, Concurrency: 2}.Normalize()
	p := NewProducer(sink, plan, func(seq int, now time.Time) ([]byte, error) {
		return []byte("x"), nil
	})

	stats := p.Run(context.Background())
	if stats.Failed != 20 {
		t.Fatalf("Failed=%d, 期望 20", stats.Failed)
	}
	if len(stats.Errors) != errSampleLimit {
		t.Errorf("错误样本应截断到 %d 条, 实际 %d", errSampleLimit, len(stats.Errors))
	}
}

func TestProducerStopsOnCancel(t *testing.T) {
	sink := newFakeSink()
	plan := Plan{Total: 100000, Concurrency: 2, Rate: 1000}.Normalize()
	p := NewProducer(sink, plan, func(seq int, now time.Time) ([]byte, error) {
		return []byte("x"), nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	stats := p.Run(ctx)
	if stats.Sent == 0 {
		t.Fatal("取消前应已写入若干条")
	}
	if stats.Sent >= plan.Total {
		t.Errorf("取消后仍在继续写入: %d 条", stats.Sent)
	}
}

func TestBuildEnvelopeCarriesStamp(t *testing.T) {
	now := time.Unix(1700000000, 123456789)
	raw, err := BuildEnvelope(7, now, nil).Encode()
	if err != nil {
		t.Fatalf("编码失败: %v", err)
	}
	got, ok := StampOf(raw)
	if !ok {
		t.Fatal("应能读回打点时间")
	}
	if !got.Equal(now) {
		t.Errorf("打点时间 = %s, 期望 %s", got, now)
	}
	if _, ok := StampOf([]byte(`{"trigger_id":"x"}`)); ok {
		t.Error("没有 bench 字段的消息不应被当成压测消息")
	}
	if _, ok := StampOf([]byte(`not json`)); ok {
		t.Error("非法 JSON 不应读出打点")
	}
}
