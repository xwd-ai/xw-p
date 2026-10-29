package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// stubRetrier 记录重试请求，并按配置返回结果。
type stubRetrier struct {
	mu       sync.Mutex
	calls    []retryCall
	outcome  queue.RetryOutcome
	err      error
	panicMsg string
}

type retryCall struct {
	deliveryID string
	cause      error
}

func (s *stubRetrier) Retry(ctx context.Context, d *queue.Delivery, cause error) (queue.RetryOutcome, error) {
	if s.panicMsg != "" {
		panic(s.panicMsg)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, retryCall{deliveryID: d.ID, cause: cause})
	return s.outcome, s.err
}

func (s *stubRetrier) called() []retryCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]retryCall(nil), s.calls...)
}

func TestWorkerSchedulesRetryInsteadOfAck(t *testing.T) {
	consumer := &fakeConsumer{items: [][]byte{[]byte(`{"trigger_id":"t-1"}`)}}
	retrier := &stubRetrier{outcome: queue.RetryOutcome{Attempt: 1, Delay: 2 * time.Second}}
	w := New(consumer, time.Second, discardLogger()).WithRetrier(retrier)
	failing := errors.New("渠道超时")
	w.handle = func([]byte) error { return failing }

	d, err := consumer.Claim(context.Background(), time.Second)
	if err != nil || d == nil {
		t.Fatalf("领取失败: %v", err)
	}
	w.process(d)

	calls := retrier.called()
	if len(calls) != 1 {
		t.Fatalf("失败消息应交由重试处理器，实际调用 %d 次", len(calls))
	}
	if !errors.Is(calls[0].cause, failing) {
		t.Errorf("应把原始错误透传给重试处理器，实际 %v", calls[0].cause)
	}
	// 重试处理器已经把这次领取从在途记录摘走，worker 不能再确认一次。
	if acked := consumer.ackedIDs(); len(acked) != 0 {
		t.Fatalf("安排重试后不应确认，实际确认 %v", acked)
	}
}

func TestWorkerAcksUndeliverableMessage(t *testing.T) {
	consumer := &fakeConsumer{items: [][]byte{[]byte(`{"trigger_id":"t-2"}`)}}
	retrier := &stubRetrier{}
	w := New(consumer, time.Second, discardLogger()).WithRetrier(retrier)
	w.handle = func([]byte) error { return fmt.Errorf("%w: 工作流不存在", queue.ErrUndeliverable) }

	d, _ := consumer.Claim(context.Background(), time.Second)
	w.process(d)

	if len(retrier.called()) != 0 {
		t.Error("不可投递的消息不该进入重试")
	}
	if acked := consumer.ackedIDs(); len(acked) != 1 {
		t.Fatalf("不可投递的消息应被确认丢弃，实际确认 %d 条", len(acked))
	}
}

func TestWorkerKeepsInflightWhenRetrierFails(t *testing.T) {
	consumer := &fakeConsumer{items: [][]byte{[]byte(`{"trigger_id":"t-3"}`)}}
	retrier := &stubRetrier{err: errors.New("redis 不可用")}
	w := New(consumer, time.Second, discardLogger()).WithRetrier(retrier)
	w.handle = func([]byte) error { return errors.New("渠道超时") }

	d, _ := consumer.Claim(context.Background(), time.Second)
	w.process(d)

	// 登记重试失败时不能确认：消息要留在在途记录里，等租约回收兜底。
	if acked := consumer.ackedIDs(); len(acked) != 0 {
		t.Fatalf("重试登记失败时不应确认，实际确认 %v", acked)
	}
}

func TestWorkerWithoutRetrierKeepsInflight(t *testing.T) {
	consumer := &fakeConsumer{items: [][]byte{[]byte(`{"trigger_id":"t-4"}`)}}
	w := New(consumer, time.Second, discardLogger())
	w.handle = func([]byte) error { return errors.New("渠道超时") }

	d, _ := consumer.Claim(context.Background(), time.Second)
	w.process(d)

	if acked := consumer.ackedIDs(); len(acked) != 0 {
		t.Fatalf("未配置重试时不应确认，实际确认 %v", acked)
	}
}

// stubRetryQueue 是重试泵的测试消息源。
type stubRetryQueue struct {
	mu       sync.Mutex
	promoted []queue.Promoted
	err      error
	calls    int
}

func (s *stubRetryQueue) PromoteDueRetries(ctx context.Context, batch int) ([]queue.Promoted, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	out := s.promoted
	s.promoted = nil
	return out, nil
}

func TestRetryPumperPromotesDueMessages(t *testing.T) {
	q := &stubRetryQueue{promoted: []queue.Promoted{
		{ID: "d-1", Attempts: 2, Raw: []byte(`{"trigger_id":"t-1"}`)},
	}}
	p := NewRetryPumper(q, time.Hour, discardLogger())
	got, err := p.Pump(context.Background())
	if err != nil {
		t.Fatalf("提升失败: %v", err)
	}
	if len(got) != 1 || got[0].Attempts != 2 {
		t.Fatalf("提升结果不符: %+v", got)
	}
	if q.calls != 1 {
		t.Errorf("应调用一次提升，实际 %d", q.calls)
	}
}

func TestRetryPumperKeepsRunningOnError(t *testing.T) {
	q := &stubRetryQueue{err: errors.New("redis 抖动")}
	p := NewRetryPumper(q, time.Hour, discardLogger())
	if _, err := p.Pump(context.Background()); err == nil {
		t.Fatal("提升失败应返回错误供调用方判断")
	}
	// 单轮失败不该让泵停下：Run 会继续下一轮（此处只验证错误被吞进日志路径）。
	p.pump(context.Background())
	if q.calls != 2 {
		t.Errorf("失败后应仍能继续下一轮，实际调用 %d 次", q.calls)
	}
}

func TestPoolWiresRetrierToConsumers(t *testing.T) {
	consumer := &fakeConsumer{items: [][]byte{[]byte(`{"trigger_id":"t-5"}`)}}
	retrier := &stubRetrier{outcome: queue.RetryOutcome{Attempt: 1, Dead: true}}
	p := NewPool(consumer, 1, time.Millisecond, discardLogger()).WithRetrier(retrier)
	p.handle = func([]byte) error { return errors.New("渠道超时") }

	// 处理完一条后取消，确认池内消费者确实带上了重试处理器。
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			time.Sleep(time.Millisecond)
			if len(retrier.called()) > 0 {
				cancel()
				return
			}
		}
	}()
	p.Run(ctx)

	if len(retrier.called()) != 1 {
		t.Fatalf("池内消费者应使用注入的重试处理器，实际调用 %d 次", len(retrier.called()))
	}
}
