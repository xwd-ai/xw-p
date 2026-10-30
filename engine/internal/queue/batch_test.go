package queue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// 批量领取与单条领取是同一套可靠语义：取出即登记在途，确认才删除。
// 这组用例盯着两件事——批量的条数与顺序对不对，以及并发批量会不会重不会漏。

func TestClaimBatchRegistersAllInflight(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	f.push(t, `{"trigger_id":"t-1"}`, `{"trigger_id":"t-2"}`, `{"trigger_id":"t-3"}`)

	batch, err := f.consumer.ClaimBatch(ctx, 2)
	if err != nil {
		t.Fatalf("ClaimBatch 失败: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("应领到 2 条，实际 %d", len(batch))
	}
	// LPUSH 入队、右侧取出：先入队的先出。
	if string(batch[0].Raw) != `{"trigger_id":"t-1"}` || string(batch[1].Raw) != `{"trigger_id":"t-2"}` {
		t.Errorf("批量领取顺序不正确: %s / %s", batch[0].Raw, batch[1].Raw)
	}
	if batch[0].ID == batch[1].ID {
		t.Error("同批次的领取凭据应各不相同")
	}

	inflight := f.inflight(t)
	if len(inflight) != 2 {
		t.Fatalf("在途记录应为 2 条，实际 %d: %v", len(inflight), inflight)
	}
	for _, d := range batch {
		if inflight[d.ID] != string(d.Raw) {
			t.Errorf("在途记录与消息体不一致: %s → %q", d.ID, inflight[d.ID])
		}
	}
	if n, _ := f.client.LLen(ctx, f.key).Result(); n != 1 {
		t.Errorf("主队列应剩 1 条，实际 %d", n)
	}
	if n, _ := f.client.ZCard(ctx, f.key+":processing:lease").Result(); n != 2 {
		t.Errorf("租约应登记 2 条，实际 %d", n)
	}
}

func TestClaimBatchReturnsRemainderAndEmpty(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	f.push(t, `{"trigger_id":"t-1"}`)

	batch, err := f.consumer.ClaimBatch(ctx, 8)
	if err != nil {
		t.Fatalf("ClaimBatch 失败: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("消息不足时应返回实际条数，实际 %d", len(batch))
	}

	empty, err := f.consumer.ClaimBatch(ctx, 8)
	if err != nil {
		t.Fatalf("空队列不应报错: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("空队列应返回空切片，实际 %+v", empty)
	}
	if inflight := f.inflight(t); len(inflight) != 1 {
		t.Errorf("空队列的批量领取不应新增在途记录: %v", inflight)
	}
}

func TestClaimBatchNormalizesSize(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	payloads := make([]string, MaxBatchClaim+8)
	for i := range payloads {
		payloads[i] = fmt.Sprintf(`{"trigger_id":"t-%03d"}`, i)
	}
	f.push(t, payloads...)

	batch, err := f.consumer.ClaimBatch(ctx, MaxBatchClaim*4)
	if err != nil {
		t.Fatalf("ClaimBatch 失败: %v", err)
	}
	if len(batch) != MaxBatchClaim {
		t.Errorf("超限的批量大小应收敛到 %d，实际 %d", MaxBatchClaim, len(batch))
	}

	// 非正数按 1 条处理，避免调用方传 0 时静默不领取。
	one, err := f.consumer.ClaimBatch(ctx, 0)
	if err != nil {
		t.Fatalf("ClaimBatch 失败: %v", err)
	}
	if len(one) != 1 {
		t.Errorf("max<=0 时应收敛为 1，实际 %d", len(one))
	}
}

func TestClaimBatchDrainsStagingFirst(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	if err := f.client.LPush(ctx, f.key+":staging", `{"trigger_id":"staged"}`).Err(); err != nil {
		t.Fatalf("预置 staging 失败: %v", err)
	}
	f.push(t, `{"trigger_id":"queued"}`)

	batch, err := f.consumer.ClaimBatch(ctx, 2)
	if err != nil {
		t.Fatalf("ClaimBatch 失败: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("应领到 2 条，实际 %d", len(batch))
	}
	if string(batch[0].Raw) != `{"trigger_id":"staged"}` {
		t.Errorf("应先取 staging 上的消息，实际 %s", batch[0].Raw)
	}
	if n, _ := f.client.LLen(ctx, f.key+":staging").Result(); n != 0 {
		t.Errorf("staging 应被清空，实际 %d", n)
	}
}

func TestAckBatchRemovesOnlyConfirmed(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	f.push(t, `{"trigger_id":"t-1"}`, `{"trigger_id":"t-2"}`, `{"trigger_id":"t-3"}`)

	batch, err := f.consumer.ClaimBatch(ctx, 3)
	if err != nil || len(batch) != 3 {
		t.Fatalf("ClaimBatch 失败: n=%d err=%v", len(batch), err)
	}
	if err := f.consumer.AckBatch(ctx, []string{batch[0].ID, batch[2].ID}); err != nil {
		t.Fatalf("AckBatch 失败: %v", err)
	}

	inflight := f.inflight(t)
	if len(inflight) != 1 {
		t.Fatalf("应只剩未确认的那条，实际 %d: %v", len(inflight), inflight)
	}
	if _, ok := inflight[batch[1].ID]; !ok {
		t.Error("未确认的消息被误删")
	}
	if n, _ := f.client.ZCard(ctx, f.key+":processing:lease").Result(); n != 1 {
		t.Errorf("租约应剩 1 条，实际 %d", n)
	}

	// 重复确认与空批次都是空操作。
	if err := f.consumer.AckBatch(ctx, []string{batch[0].ID}); err != nil {
		t.Errorf("重复确认应安全，实际 %v", err)
	}
	if err := f.consumer.AckBatch(ctx, nil); err != nil {
		t.Errorf("空批次应直接返回，实际 %v", err)
	}
}

// 并发批量领取：同一批消息不会被两个消费者拿到，也不会漏。
func TestConcurrentClaimBatchDeliversEachMessageOnce(t *testing.T) {
	f := newConsumerFixture(t)
	const (
		total    = 400
		workers  = 8
		batchMax = 16
	)
	payloads := make([]string, total)
	for i := 0; i < total; i++ {
		payloads[i] = fmt.Sprintf(`{"trigger_id":"t-%03d"}`, i)
	}
	f.push(t, payloads...)

	var mu sync.Mutex
	seen := map[string]int{}
	ids := map[string]struct{}{}

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			empty := 0
			for empty < 2 {
				batch, err := f.consumer.ClaimBatch(context.Background(), batchMax)
				if err != nil {
					t.Errorf("ClaimBatch 失败: %v", err)
					return
				}
				if len(batch) == 0 {
					empty++
					// 队列已空，让出一下 CPU 再确认一次，避免各消费者空转。
					time.Sleep(2 * time.Millisecond)
					continue
				}
				empty = 0
				mu.Lock()
				for _, d := range batch {
					seen[string(d.Raw)]++
					ids[d.ID] = struct{}{}
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != total {
		t.Errorf("应领到 %d 条不同消息，实际 %d", total, len(seen))
	}
	for p, n := range seen {
		if n != 1 {
			t.Errorf("消息 %s 被领取 %d 次", p, n)
		}
	}
	if len(ids) != total {
		t.Errorf("领取凭据应两两不同，实际 %d 个", len(ids))
	}
	if inflight := f.inflight(t); len(inflight) != total {
		t.Errorf("在途记录应为 %d 条，实际 %d", total, len(inflight))
	}
}
