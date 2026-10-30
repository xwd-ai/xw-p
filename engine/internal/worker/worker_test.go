package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeConsumer 是测试用消息源：按顺序发放预置消息并记录确认情况。
type fakeConsumer struct {
	items [][]byte
	i     int
	acked []string
	mu    sync.Mutex
}

func (f *fakeConsumer) Claim(ctx context.Context, timeout time.Duration) (*queue.Delivery, error) {
	if f.i >= len(f.items) {
		return nil, nil
	}
	raw := f.items[f.i]
	f.i++
	return &queue.Delivery{ID: fmt.Sprintf("d-%d", f.i), Raw: raw, ClaimedAt: time.Now()}, nil
}

func (f *fakeConsumer) Ack(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked = append(f.acked, id)
	return nil
}

func (f *fakeConsumer) ackedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.acked...)
}

func TestHandleDispatchesMessage(t *testing.T) {
	w := New(nil, time.Second, discardLogger())
	raw := []byte(`{"trigger_id":"t-1","transaction_id":"x-1","name":"post_commented","subscriber_id":"creator_1","channels":["in_app","email"],"payload":{"post_id":"P10293"}}`)
	if err := w.Handle(raw); err != nil {
		t.Fatalf("Handle 失败: %v", err)
	}
}

func TestHandleRejectsInvalidJSON(t *testing.T) {
	w := New(nil, time.Second, discardLogger())
	err := w.Handle([]byte("{not-json"))
	if err == nil {
		t.Fatal("非法 JSON 应返回错误")
	}
	if !errors.Is(err, ErrInvalidMessage) {
		t.Errorf("应标记为不可解析消息，实际 %v", err)
	}
}

// stubExecutor 记录收到的消息，并按配置返回错误。
type stubExecutor struct {
	mu   sync.Mutex
	msgs []queue.QueuedMessage
	err  error
}

func (s *stubExecutor) Execute(msg queue.QueuedMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append(s.msgs, msg)
	return s.err
}

func (s *stubExecutor) received() []queue.QueuedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]queue.QueuedMessage(nil), s.msgs...)
}

// 注入执行器后，Handle 不再打印 mock 日志，而是把解析好的消息交给执行器。
func TestHandleDelegatesToExecutor(t *testing.T) {
	exec := &stubExecutor{}
	w := New(nil, time.Second, discardLogger()).WithExecutor(exec)

	raw := []byte(`{"trigger_id":"t-1","name":"post_liked","subscriber_id":"creator_1","workflow":{"name":"post_liked","version":2,"steps":[{"id":"step-1","type":"in_app"}]}}`)
	if err := w.Handle(raw); err != nil {
		t.Fatalf("Handle 失败: %v", err)
	}
	msgs := exec.received()
	if len(msgs) != 1 {
		t.Fatalf("执行器应收到 1 条消息，实际 %d", len(msgs))
	}
	if msgs[0].Workflow == nil || len(msgs[0].Workflow.Steps) != 1 {
		t.Errorf("消息应携带步骤快照: %+v", msgs[0].Workflow)
	}
}

// 执行器判定消息不可修复时，消费侧应确认丢弃，而不是留在在途记录里反复重投。
func TestProcessAcksUndeliverableMessage(t *testing.T) {
	consumer := &fakeConsumer{}
	exec := &stubExecutor{err: fmt.Errorf("%w: 工作流不存在", queue.ErrUndeliverable)}
	w := New(consumer, time.Second, discardLogger()).WithExecutor(exec)

	w.process(&queue.Delivery{ID: "d-1", Raw: []byte(`{"trigger_id":"t-1","name":"gone"}`)}, nil)

	got := consumer.ackedIDs()
	if len(got) != 1 || got[0] != "d-1" {
		t.Errorf("不可投递的消息应被确认，实际 %v", got)
	}
}

// 暂时性失败仍然不确认，等待租约回收后重投。
func TestProcessKeepsTransientFailureInFlight(t *testing.T) {
	consumer := &fakeConsumer{}
	exec := &stubExecutor{err: errors.New("Redis 连接超时")}
	w := New(consumer, time.Second, discardLogger()).WithExecutor(exec)

	w.process(&queue.Delivery{ID: "d-1", Raw: []byte(`{"trigger_id":"t-1"}`)}, nil)

	if got := consumer.ackedIDs(); len(got) != 0 {
		t.Errorf("暂时性失败不应确认，实际 %v", got)
	}
}

func TestProcessAcksAfterSuccess(t *testing.T) {
	consumer := &fakeConsumer{}
	w := New(consumer, time.Second, discardLogger())

	w.process(&queue.Delivery{ID: "d-1", Raw: []byte(`{"trigger_id":"t-1","name":"post_liked","subscriber_id":"creator_1"}`)}, nil)

	got := consumer.ackedIDs()
	if len(got) != 1 || got[0] != "d-1" {
		t.Errorf("处理成功应确认该消息，实际确认 %v", got)
	}
}

func TestProcessDoesNotAckOnFailure(t *testing.T) {
	consumer := &fakeConsumer{}
	w := New(consumer, time.Second, discardLogger())
	w.handle = func([]byte) error { return errors.New("邮件渠道超时") }

	w.process(&queue.Delivery{ID: "d-1", Raw: []byte(`{"trigger_id":"t-1"}`)}, nil)

	if got := consumer.ackedIDs(); len(got) != 0 {
		t.Errorf("处理失败不应确认，实际确认 %v", got)
	}
}

// 畸形消息重投多少次都不会变好，确认掉并告警，避免长期占着在途记录。
func TestProcessAcksInvalidMessage(t *testing.T) {
	consumer := &fakeConsumer{}
	w := New(consumer, time.Second, discardLogger())

	w.process(&queue.Delivery{ID: "d-1", Raw: []byte("{not-json")}, nil)

	got := consumer.ackedIDs()
	if len(got) != 1 || got[0] != "d-1" {
		t.Errorf("不可解析的消息应被丢弃（确认），实际确认 %v", got)
	}
}

func TestRunConsumesUntilContextCancelled(t *testing.T) {
	consumer := &fakeConsumer{items: [][]byte{
		[]byte(`{"trigger_id":"t-1","name":"post_commented","subscriber_id":"u1"}`),
		[]byte(`{"trigger_id":"t-2","name":"post_mentioned","subscriber_id":"u2"}`),
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
	if consumer.i != 2 {
		t.Errorf("应消费 2 条，实际 %d", consumer.i)
	}
	if got := consumer.ackedIDs(); len(got) != 2 {
		t.Errorf("成功处理的消息都应确认，实际 %v", got)
	}
}

// 混合场景：成功、失败、畸形三类消息各自走对不同分支。
func TestRunHandlesMixedOutcomes(t *testing.T) {
	consumer := &fakeConsumer{items: [][]byte{
		[]byte(`{"trigger_id":"ok-1"}`),
		[]byte(`{"trigger_id":"boom"}`),
		[]byte(`not-json`),
		[]byte(`{"trigger_id":"ok-2"}`),
	}}
	w := New(consumer, time.Millisecond, discardLogger())
	w.handle = func(raw []byte) error {
		if string(raw) == `{"trigger_id":"boom"}` {
			return errors.New("推送网关 502")
		}
		return w.Handle(raw)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	w.Run(ctx)

	got := consumer.ackedIDs()
	want := map[string]bool{"d-1": false, "d-3": false, "d-4": false}
	for _, id := range got {
		if _, ok := want[id]; !ok {
			t.Errorf("消息 %s 不该被确认", id)
		}
		want[id] = true
	}
	for id, seen := range want {
		if !seen {
			t.Errorf("消息 %s 应被确认", id)
		}
	}
	if len(got) != 3 {
		t.Errorf("应确认 3 条，实际 %v", got)
	}
}
