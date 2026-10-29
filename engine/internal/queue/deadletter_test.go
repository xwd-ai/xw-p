package queue

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// 死信查询与重放：列表要按最新优先翻页、ID 稳定、重放原样回队且只回一次。

// pushDead 直接往死信列表写一条记录，绕开消费链路以便单独验证读/写语义。
func (f *consumerFixture) pushDead(t *testing.T, payloads ...string) []string {
	t.Helper()
	ctx := context.Background()
	ids := make([]string, 0, len(payloads))
	for i, p := range payloads {
		rec, err := json.Marshal(DeadLetter{
			Payload:       []byte(p),
			Attempts:      3,
			LastError:     "下游不可用",
			FirstFailedAt: time.Date(2026, 10, 29, 8, i, 0, 0, time.UTC),
			DeadAt:        time.Date(2026, 10, 29, 9, i, 0, 0, time.UTC),
			Queue:         f.key,
		})
		if err != nil {
			t.Fatalf("构造死信失败: %v", err)
		}
		if err := f.client.RPush(ctx, f.key+":dead", string(rec)).Err(); err != nil {
			t.Fatalf("写入死信失败: %v", err)
		}
		ids = append(ids, deadLetterID(string(rec)))
	}
	return ids
}

func TestListDeadLettersNewestFirst(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	f.pushDead(t, `{"trigger_id":"t-1"}`, `{"trigger_id":"t-2"}`, `{"trigger_id":"t-3"}`)

	page, err := f.consumer.ListDeadLetters(ctx, DeadLetterQuery{Limit: 2})
	if err != nil {
		t.Fatalf("读取死信失败: %v", err)
	}
	if page.Total != 3 {
		t.Errorf("总数应为 3，实际 %d", page.Total)
	}
	if len(page.Items) != 2 {
		t.Fatalf("应返回 2 条，实际 %d", len(page.Items))
	}
	if string(page.Items[0].Payload) != `{"trigger_id":"t-3"}` || string(page.Items[1].Payload) != `{"trigger_id":"t-2"}` {
		t.Errorf("应按最新在前返回，实际 %s / %s", page.Items[0].Payload, page.Items[1].Payload)
	}
	if page.Items[0].ID == "" || page.Items[0].ID == page.Items[1].ID {
		t.Errorf("每条死信应有互不相同的 ID: %q / %q", page.Items[0].ID, page.Items[1].ID)
	}

	second, err := f.consumer.ListDeadLetters(ctx, DeadLetterQuery{Offset: 2, Limit: 2})
	if err != nil {
		t.Fatalf("读取第二页失败: %v", err)
	}
	if len(second.Items) != 1 || string(second.Items[0].Payload) != `{"trigger_id":"t-1"}` {
		t.Errorf("第二页应只剩最早那条，实际 %+v", second.Items)
	}

	// 越界翻页返回空页而不是报错，前端可以据此停止加载。
	beyond, err := f.consumer.ListDeadLetters(ctx, DeadLetterQuery{Offset: 10, Limit: 2})
	if err != nil {
		t.Fatalf("越界翻页不应报错: %v", err)
	}
	if len(beyond.Items) != 0 || beyond.Total != 3 {
		t.Errorf("越界页应为空且总数不变，实际 %d 条 / 总数 %d", len(beyond.Items), beyond.Total)
	}
}

func TestListDeadLettersLimitClamped(t *testing.T) {
	f := newConsumerFixture(t)
	page, err := f.consumer.ListDeadLetters(context.Background(), DeadLetterQuery{Limit: MaxDeadLetterLimit + 500})
	if err != nil {
		t.Fatalf("读取空死信列表失败: %v", err)
	}
	if page.Limit != MaxDeadLetterLimit {
		t.Errorf("超上限的 limit 应收敛到 %d，实际 %d", MaxDeadLetterLimit, page.Limit)
	}
	if page.Items == nil {
		t.Error("空列表应返回空数组而不是 null，方便前端直接遍历")
	}

	zero, err := f.consumer.ListDeadLetters(context.Background(), DeadLetterQuery{Offset: -3, Limit: 0})
	if err != nil {
		t.Fatalf("非法参数不应报错: %v", err)
	}
	if zero.Offset != 0 || zero.Limit != DefaultDeadLetterLimit {
		t.Errorf("非法分页参数应回退默认值，实际 offset=%d limit=%d", zero.Offset, zero.Limit)
	}
}

func TestListDeadLettersSkipsCorruptRecord(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	f.pushDead(t, `{"trigger_id":"t-1"}`)
	if err := f.client.RPush(ctx, f.key+":dead", "{不是 JSON").Err(); err != nil {
		t.Fatalf("写入脏记录失败: %v", err)
	}

	page, err := f.consumer.ListDeadLetters(ctx, DeadLetterQuery{})
	if err != nil {
		t.Fatalf("脏记录不该让整页读取失败: %v", err)
	}
	if page.Total != 2 {
		t.Errorf("总数应反映列表实际长度 2，实际 %d", page.Total)
	}
	if len(page.Items) != 1 {
		t.Errorf("应跳过脏记录只返回 1 条，实际 %d", len(page.Items))
	}
}

func TestReplayDeadLetterRestoresPayload(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	// 含多字节字符与 emoji：重放必须逐字节还原，不能被编码往返改坏。
	payload := `{"name":"post_liked","payload":{"actor":"小林","comment":"拍得真好👍"}}`
	ids := f.pushDead(t, payload)

	report, err := f.consumer.ReplayDeadLetters(ctx, ids)
	if err != nil {
		t.Fatalf("重放失败: %v", err)
	}
	if len(report.Replayed) != 1 || len(report.Missing) != 0 {
		t.Fatalf("应重放 1 条且没有缺失，实际 %+v", report)
	}
	if n := f.client.LLen(ctx, f.key+":dead").Val(); n != 0 {
		t.Errorf("重放后死信列表应清空，实际 %d 条", n)
	}
	queued, err := f.client.LRange(ctx, f.key, 0, -1).Result()
	if err != nil {
		t.Fatalf("读取主队列失败: %v", err)
	}
	if len(queued) != 1 || queued[0] != payload {
		t.Errorf("重放应把原始消息体写回队列，实际 %v", queued)
	}
}

func TestReplayDeadLetterIsIdempotent(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	ids := f.pushDead(t, `{"trigger_id":"t-1"}`)

	if _, err := f.consumer.ReplayDeadLetters(ctx, ids); err != nil {
		t.Fatalf("首次重放失败: %v", err)
	}
	// 重复提交同一个 ID（例如运维重试、两个实例同时点重放）：不能重复入队。
	again, err := f.consumer.ReplayDeadLetters(ctx, ids)
	if err != nil {
		t.Fatalf("重复重放失败: %v", err)
	}
	if len(again.Replayed) != 0 || len(again.Missing) != 1 {
		t.Errorf("已取走的死信应报告为 missing，实际 %+v", again)
	}
	if n := f.client.LLen(ctx, f.key).Val(); n != 1 {
		t.Errorf("主队列应只有 1 条消息，实际 %d 条", n)
	}
}

func TestReplayDeadLetterUnknownID(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	f.pushDead(t, `{"trigger_id":"t-1"}`)

	report, err := f.consumer.ReplayDeadLetters(ctx, []string{"deadbeef", "  "})
	if err != nil {
		t.Fatalf("未知 ID 不应报错: %v", err)
	}
	if len(report.Replayed) != 0 || len(report.Missing) != 1 {
		t.Errorf("未知 ID 与空串应只报 1 条缺失，实际 %+v", report)
	}
	if n := f.client.LLen(ctx, f.key+":dead").Val(); n != 1 {
		t.Errorf("未知 ID 不该动到已有死信，实际剩 %d 条", n)
	}
}

func TestDiscardDeadLetters(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	ids := f.pushDead(t, `{"trigger_id":"t-1"}`, `{"trigger_id":"t-2"}`)

	removed, err := f.consumer.DiscardDeadLetters(ctx, ids[:1])
	if err != nil {
		t.Fatalf("丢弃失败: %v", err)
	}
	if removed != 1 {
		t.Errorf("应删除 1 条，实际 %d", removed)
	}
	if n := f.client.LLen(ctx, f.key+":dead").Val(); n != 1 {
		t.Errorf("死信列表应剩 1 条，实际 %d", n)
	}
	if n := f.client.LLen(ctx, f.key).Val(); n != 0 {
		t.Errorf("丢弃的消息不应进入主队列，实际 %d 条", n)
	}

	// 重复丢弃同一条返回 0，而不是报错。
	again, err := f.consumer.DiscardDeadLetters(ctx, ids[:1])
	if err != nil {
		t.Fatalf("重复丢弃不应报错: %v", err)
	}
	if again != 0 {
		t.Errorf("重复丢弃应返回 0，实际 %d", again)
	}
}

// 从「处理失败 → 进死信 → 查询 → 重放」走一遍，确认重放后消息能被重新消费，
// 且重新获得完整的重试次数（进死信时计数键已被清除）。
func TestReplayAfterRetryExhausted(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	payload := `{"trigger_id":"t-9","name":"post_commented"}`
	f.push(t, payload)

	d, err := f.consumer.Claim(ctx, 0)
	if err != nil || d == nil {
		t.Fatalf("领取消息失败: %v", err)
	}
	outcome, err := f.consumer.Retry(ctx, d, errBoom{}, RetryPolicy{MaxAttempts: 1})
	if err != nil {
		t.Fatalf("处理失败进死信出错: %v", err)
	}
	if !outcome.Dead {
		t.Fatal("次数耗尽应转入死信")
	}

	page, err := f.consumer.ListDeadLetters(ctx, DeadLetterQuery{})
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("应读到 1 条死信: %v / %+v", err, page.Items)
	}
	entry := page.Items[0]
	if entry.Queue != f.key || entry.Attempts != 1 || entry.LastError == "" {
		t.Errorf("死信应带来源队列、次数与原因，实际 %+v", entry.DeadLetter)
	}
	if string(entry.Payload) != payload {
		t.Errorf("死信应保留原始消息体，实际 %s", entry.Payload)
	}

	if _, err := f.consumer.ReplayDeadLetters(ctx, []string{entry.ID}); err != nil {
		t.Fatalf("重放失败: %v", err)
	}
	again, err := f.consumer.Claim(ctx, 0)
	if err != nil || again == nil {
		t.Fatalf("重放后应能重新领取: %v", err)
	}
	if string(again.Raw) != payload {
		t.Errorf("重新领取的消息体应与原始一致，实际 %s", again.Raw)
	}
	if n := f.client.Get(ctx, f.consumer.attemptKey(again.Raw)).Val(); n != "" {
		t.Errorf("重放后应重新计数，实际残留次数 %s", n)
	}
}

type errBoom struct{}

func (errBoom) Error() string { return "下游暂时不可用" }
