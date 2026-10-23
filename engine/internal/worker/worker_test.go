package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestHandleDispatchesMessage(t *testing.T) {
	w := New(nil, time.Second, discardLogger())
	raw := []byte(`{"trigger_id":"t-1","transaction_id":"x-1","name":"order_confirmed","subscriber_id":"user_1","channels":["in_app","email"],"payload":{"order_id":"A10293"}}`)
	if err := w.Handle(raw); err != nil {
		t.Fatalf("Handle 失败: %v", err)
	}
}

func TestHandleRejectsInvalidJSON(t *testing.T) {
	w := New(nil, time.Second, discardLogger())
	if err := w.Handle([]byte("{not-json")); err == nil {
		t.Error("非法 JSON 应返回错误")
	}
}

// memConsumer 是测试用内存消息源。
type memConsumer struct {
	items [][]byte
	i     int
}

func (m *memConsumer) Next(ctx context.Context, timeout time.Duration) ([]byte, error) {
	if m.i >= len(m.items) {
		return nil, nil
	}
	item := m.items[m.i]
	m.i++
	return item, nil
}

func TestRunConsumesUntilContextCancelled(t *testing.T) {
	consumer := &memConsumer{items: [][]byte{
		[]byte(`{"trigger_id":"t-1","name":"order_confirmed","subscriber_id":"u1"}`),
	}}
	w := New(consumer, time.Millisecond, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker 未在 ctx 取消后退出")
	}
	if consumer.i != 1 {
		t.Errorf("应消费 1 条，实际 %d", consumer.i)
	}
}
