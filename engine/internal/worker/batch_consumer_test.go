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

// 批量领取路径的用例：预取把多次脚本往返摊成一次，但确认语义不能因此变松——
// 处理成功的才进确认批次，失败进重试的不能被顺手确认掉。

type batchFakeConsumer struct {
	items [][]byte

	mu      sync.Mutex
	i       int
	batches [][]string
	singles []string
}

func (f *batchFakeConsumer) Claim(ctx context.Context, timeout time.Duration) (*queue.Delivery, error) {
	// 批量领完之后的退路：一直等到上下文取消，模拟真实消息源的阻塞等待。
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *batchFakeConsumer) ClaimBatch(ctx context.Context, max int) ([]*queue.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.i >= len(f.items) {
		return nil, nil
	}
	end := f.i + max
	if end > len(f.items) {
		end = len(f.items)
	}
	out := make([]*queue.Delivery, 0, end-f.i)
	for ; f.i < end; f.i++ {
		out = append(out, &queue.Delivery{
			ID:        fmt.Sprintf("d-%d", f.i+1),
			Raw:       f.items[f.i],
			ClaimedAt: time.Now(),
		})
	}
	return out, nil
}

func (f *batchFakeConsumer) Ack(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.singles = append(f.singles, id)
	return nil
}

func (f *batchFakeConsumer) AckBatch(ctx context.Context, ids []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, append([]string(nil), ids...))
	return nil
}

func (f *batchFakeConsumer) ackedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := append([]string(nil), f.singles...)
	for _, b := range f.batches {
		out = append(out, b...)
	}
	return out
}

func (f *batchFakeConsumer) batchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches)
}

func batchItems(n int) [][]byte {
	items := make([][]byte, n)
	for i := range items {
		items[i] = []byte(fmt.Sprintf(`{"trigger_id":"t-%d","name":"post_liked","subscriber_id":"creator_1"}`, i))
	}
	return items
}

func waitAcked(t *testing.T, c *batchFakeConsumer, want int) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := c.ackedIDs(); len(got) >= want {
			return got
		}
		time.Sleep(2 * time.Millisecond)
	}
	got := c.ackedIDs()
	t.Fatalf("等待确认 %d 条超时，实际 %d 条", want, len(got))
	return nil
}

func TestBatchedRunAcksWholeBatch(t *testing.T) {
	const total = 20
	f := &batchFakeConsumer{items: batchItems(total)}

	w := New(f, time.Second, discardLogger()).WithPrefetch(8)
	var handled int
	var mu sync.Mutex
	w.handle = func([]byte) error {
		mu.Lock()
		handled++
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	acked := waitAcked(t, f, total)
	cancel()
	<-done

	if len(acked) != total {
		t.Fatalf("应确认 %d 条，实际 %d 条", total, len(acked))
	}
	seen := map[string]int{}
	for _, id := range acked {
		seen[id]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("凭据 %s 被确认 %d 次", id, n)
		}
	}
	// 20 条按每批 8 条领取，确认次数应明显少于消息条数。
	if got := f.batchCount(); got == 0 || got > total {
		t.Errorf("批量确认次数异常: %d", got)
	}
	if handled != total {
		t.Errorf("应处理 %d 条，实际 %d", total, handled)
	}
}

func TestBatchedRunKeepsFailedMessageUnacked(t *testing.T) {
	f := &batchFakeConsumer{items: [][]byte{
		[]byte(`{"trigger_id":"ok-1","name":"post_liked","subscriber_id":"creator_1"}`),
		[]byte(`{"trigger_id":"boom","name":"post_liked","subscriber_id":"creator_1"}`),
		[]byte(`{"trigger_id":"ok-2","name":"post_liked","subscriber_id":"creator_1"}`),
	}}

	retrier := &stubRetrier{}
	w := New(f, time.Second, discardLogger()).WithPrefetch(8).WithRetrier(retrier)
	w.handle = func(raw []byte) error {
		if string(raw) == string(f.items[1]) {
			return errors.New("渠道暂时不可用")
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	acked := waitAcked(t, f, 2)
	cancel()
	<-done

	for _, id := range acked {
		if id == "d-2" {
			t.Error("交给重试的消息不该被确认")
		}
	}
	calls := retrier.called()
	if len(calls) != 1 || calls[0].deliveryID != "d-2" {
		t.Errorf("失败消息应交给重试处理器，实际 %+v", calls)
	}
}

func TestNormalizePrefetch(t *testing.T) {
	cases := []struct{ in, want int }{
		{0, 1}, {-5, 1}, {1, 1}, {8, 8}, {MaxPrefetch * 2, MaxPrefetch},
	}
	for _, c := range cases {
		if got := NormalizePrefetch(c.in); got != c.want {
			t.Errorf("NormalizePrefetch(%d) = %d，期望 %d", c.in, got, c.want)
		}
	}
}
