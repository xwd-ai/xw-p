package queue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// 可靠投递测试：验证「取出即登记在途、确认后才删除、未确认不会丢」。
// 用进程内 Redis（miniredis）覆盖脚本逻辑与键布局，不依赖外部服务。

type consumerFixture struct {
	consumer *ReliableConsumer
	client   *redis.Client
	mr       *miniredis.Miniredis
	key      string
}

func newConsumerFixture(t *testing.T) *consumerFixture {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	const key = "xw:queue:events"
	return &consumerFixture{
		consumer: newReliableConsumer(client, key, time.Minute),
		client:   client,
		mr:       mr,
		key:      key,
	}
}

func (f *consumerFixture) push(t *testing.T, payloads ...string) {
	t.Helper()
	ctx := context.Background()
	for _, p := range payloads {
		if err := f.client.LPush(ctx, f.key, p).Err(); err != nil {
			t.Fatalf("LPUSH 失败: %v", err)
		}
	}
}

func (f *consumerFixture) inflight(t *testing.T) map[string]string {
	t.Helper()
	got, err := f.client.HGetAll(context.Background(), f.key+":processing").Result()
	if err != nil {
		t.Fatalf("读取在途记录失败: %v", err)
	}
	return got
}

func TestClaimRegistersInflight(t *testing.T) {
	f := newConsumerFixture(t)
	// Python 侧 LPUSH 入队，本端从右侧取出，先入队的先出。
	f.push(t, `{"trigger_id":"t-1"}`, `{"trigger_id":"t-2"}`)

	d, err := f.consumer.Claim(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("Claim 失败: %v", err)
	}
	if d == nil {
		t.Fatal("队列有消息，不应返回 nil")
	}
	if string(d.Raw) != `{"trigger_id":"t-1"}` {
		t.Errorf("应先取出最早入队的消息，实际 %s", d.Raw)
	}
	if d.ID == "" {
		t.Error("领取凭据不应为空")
	}

	inflight := f.inflight(t)
	if len(inflight) != 1 || inflight[d.ID] != string(d.Raw) {
		t.Errorf("在途记录不正确: %v", inflight)
	}
	if n := f.mr.Exists(f.key + ":processing:lease"); !n {
		t.Error("缺少租约 zset")
	}
	if _, err := f.client.ZScore(context.Background(), f.key+":processing:lease", d.ID).Result(); err != nil {
		t.Errorf("租约未登记: %v", err)
	}
	// 已取出的消息不能还留在主队列里
	if n, _ := f.client.LLen(context.Background(), f.key).Result(); n != 1 {
		t.Errorf("主队列剩余应为 1，实际 %d", n)
	}
	if n, _ := f.client.LLen(context.Background(), f.key+":staging").Result(); n != 0 {
		t.Errorf("staging 应为空，实际 %d", n)
	}
}

func TestLeaseDeadlineFollowsClaimLease(t *testing.T) {
	f := newConsumerFixture(t)
	f.consumer.lease = 30 * time.Second
	f.push(t, `{"trigger_id":"t-1"}`)

	before := time.Now()
	d, err := f.consumer.Claim(context.Background(), time.Second)
	if err != nil || d == nil {
		t.Fatalf("Claim 失败: d=%v err=%v", d, err)
	}
	score, err := f.client.ZScore(context.Background(), f.key+":processing:lease", d.ID).Result()
	if err != nil {
		t.Fatalf("读取租约失败: %v", err)
	}
	deadline := time.UnixMilli(int64(score))
	want := before.Add(30 * time.Second)
	if deadline.Before(want.Add(-time.Second)) || deadline.After(want.Add(2*time.Second)) {
		t.Errorf("租约到期时间 = %v，期望约 %v", deadline, want)
	}
}

func TestAckRemovesInflight(t *testing.T) {
	f := newConsumerFixture(t)
	f.push(t, `{"trigger_id":"t-1"}`)

	d, err := f.consumer.Claim(context.Background(), time.Second)
	if err != nil || d == nil {
		t.Fatalf("Claim 失败: d=%v err=%v", d, err)
	}
	if err := f.consumer.Ack(context.Background(), d.ID); err != nil {
		t.Fatalf("Ack 失败: %v", err)
	}
	if inflight := f.inflight(t); len(inflight) != 0 {
		t.Errorf("确认后不应留有在途记录: %v", inflight)
	}
	if n, _ := f.client.ZCard(context.Background(), f.key+":processing:lease").Result(); n != 0 {
		t.Errorf("确认后租约应清空，实际 %d", n)
	}
}

func TestAckIsIdempotent(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	if err := f.consumer.Ack(ctx, "no-such-id"); err != nil {
		t.Errorf("确认不存在的记录应无副作用，实际 %v", err)
	}

	f.push(t, `{"trigger_id":"t-1"}`)
	d, _ := f.consumer.Claim(ctx, time.Second)
	if err := f.consumer.Ack(ctx, d.ID); err != nil {
		t.Fatalf("首次确认失败: %v", err)
	}
	if err := f.consumer.Ack(ctx, d.ID); err != nil {
		t.Errorf("重复确认应安全，实际 %v", err)
	}
	if err := f.consumer.Ack(ctx, ""); err != nil {
		t.Errorf("空凭据应直接返回，实际 %v", err)
	}
}

// 处理失败（不确认）时消息必须留在在途记录里，后续回收流程才能重新投递。
func TestUnackedMessageStaysInflight(t *testing.T) {
	f := newConsumerFixture(t)
	f.push(t, `{"trigger_id":"t-1"}`, `{"trigger_id":"t-2"}`)

	first, err := f.consumer.Claim(context.Background(), time.Second)
	if err != nil || first == nil {
		t.Fatalf("首次领取失败: %v", err)
	}
	second, err := f.consumer.Claim(context.Background(), time.Second)
	if err != nil || second == nil {
		t.Fatalf("再次领取失败: %v", err)
	}
	if first.ID == second.ID {
		t.Fatal("两条消息应拿到不同的领取凭据")
	}

	inflight := f.inflight(t)
	if len(inflight) != 2 {
		t.Fatalf("未确认的消息都应保留，实际 %d 条: %v", len(inflight), inflight)
	}
	if _, ok := inflight[first.ID]; !ok {
		t.Error("第一条未确认的消息丢失")
	}

	// 只确认第二条，第一条应继续保留
	if err := f.consumer.Ack(context.Background(), second.ID); err != nil {
		t.Fatalf("Ack 失败: %v", err)
	}
	inflight = f.inflight(t)
	if _, ok := inflight[first.ID]; !ok || len(inflight) != 1 {
		t.Errorf("仅确认第二条后应只剩第一条: %v", inflight)
	}
}

// 进程在 BLMOVE 之后、登记之前崩溃时，消息会留在 staging 上；
// 下一次领取应先把它捞出来，而不是让它一直搁着。
func TestClaimDrainsStagingFirst(t *testing.T) {
	f := newConsumerFixture(t)
	ctx := context.Background()
	if err := f.client.LPush(ctx, f.key+":staging", `{"trigger_id":"staged"}`).Err(); err != nil {
		t.Fatalf("预置 staging 失败: %v", err)
	}
	f.push(t, `{"trigger_id":"queued"}`)

	d, err := f.consumer.Claim(ctx, time.Second)
	if err != nil || d == nil {
		t.Fatalf("Claim 失败: %v", err)
	}
	if string(d.Raw) != `{"trigger_id":"staged"}` {
		t.Errorf("应先取 staging 上的消息，实际 %s", d.Raw)
	}
	if n, _ := f.client.LLen(ctx, f.key+":staging").Result(); n != 0 {
		t.Errorf("staging 应被清空，实际 %d", n)
	}
	if inflight := f.inflight(t); len(inflight) != 1 {
		t.Errorf("staging 中的消息应登记在途: %v", inflight)
	}
}

func TestClaimReturnsNilWhenQueueEmpty(t *testing.T) {
	f := newConsumerFixture(t)
	done := make(chan struct{})
	var d *Delivery
	var err error
	go func() {
		d, err = f.consumer.Claim(context.Background(), time.Second)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("空队列时 Claim 未在超时后返回")
	}
	if err != nil {
		t.Fatalf("空队列不应报错: %v", err)
	}
	if d != nil {
		t.Errorf("空队列应返回 nil，实际 %+v", d)
	}
	if inflight := f.inflight(t); len(inflight) != 0 {
		t.Errorf("不应产生在途记录: %v", inflight)
	}
}

func TestClaimStopsOnContextCancel(t *testing.T) {
	f := newConsumerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if _, err := f.consumer.Claim(ctx, 5*time.Second); err == nil {
		t.Error("已取消的上下文应返回错误，而不是空等")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("应在取消后立即返回，实际耗时 %v", elapsed)
	}
}

// 并发领取：每条消息恰好被一个消费者拿到，不会重复登记、不会漏。
func TestConcurrentClaimsDeliverEachMessageOnce(t *testing.T) {
	f := newConsumerFixture(t)
	const total = 120
	payloads := make([]string, total)
	for i := 0; i < total; i++ {
		payloads[i] = fmt.Sprintf(`{"trigger_id":"t-%03d"}`, i)
	}
	f.push(t, payloads...)

	var mu sync.Mutex
	seen := make(map[string]int, total)
	ids := make(map[string]struct{}, total)

	const workers = 8
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			emptyRounds := 0
			for emptyRounds < 2 {
				d, err := f.consumer.Claim(context.Background(), time.Second)
				if err != nil {
					t.Errorf("Claim 失败: %v", err)
					return
				}
				if d == nil {
					emptyRounds++
					continue
				}
				emptyRounds = 0
				mu.Lock()
				seen[string(d.Raw)]++
				ids[d.ID] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != total {
		t.Errorf("应领到 %d 条不同消息，实际 %d", total, len(seen))
	}
	if len(ids) != total {
		t.Errorf("领取凭据应两两不同，实际 %d 个", len(ids))
	}
	for p, n := range seen {
		if n != 1 {
			t.Errorf("消息 %s 被领取 %d 次", p, n)
		}
	}
	if inflight := f.inflight(t); len(inflight) != total {
		t.Errorf("在途记录应为 %d 条，实际 %d", total, len(inflight))
	}
}
