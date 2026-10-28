package queue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// 回收测试：模拟「消息被取出后进程没能确认」的崩溃场景，
// 验证过期租约会被重新入队、未过期与被确认的消息不受影响。

// clockFixture 让测试可以推进消费者的时钟，从而制造「租约已过期」的状态。
type clockFixture struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clockFixture) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clockFixture) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// newReclaimFixture 建一个租约 1 分钟、无宽限期的消费者，时钟由测试控制。
func newReclaimFixture(t *testing.T) (*consumerFixture, *clockFixture) {
	t.Helper()
	f := newConsumerFixture(t)
	clock := &clockFixture{now: time.Now()}
	f.consumer.now = clock.Now
	f.consumer.grace = 0
	return f, clock
}

func TestReclaimRequeuesExpiredDelivery(t *testing.T) {
	f, clock := newReclaimFixture(t)
	ctx := context.Background()
	f.push(t, `{"trigger_id":"t-1"}`)

	claimed, err := f.consumer.Claim(ctx, time.Second)
	if err != nil || claimed == nil {
		t.Fatalf("领取失败: d=%v err=%v", claimed, err)
	}
	// 不确认，直接让租约过期——等价于进程在处理中途消失。
	clock.Advance(2 * time.Minute)

	report, err := f.consumer.ReclaimExpired(ctx, DefaultReclaimBatch)
	if err != nil {
		t.Fatalf("回收失败: %v", err)
	}
	if report.Scanned != 1 || len(report.Requeued) != 1 || report.Orphans != 0 {
		t.Fatalf("回收结果不正确: %+v", report)
	}
	if got := string(report.Requeued[0].Raw); got != `{"trigger_id":"t-1"}` {
		t.Errorf("回收的消息体 = %s", got)
	}
	// 租约以毫秒时间戳保存，比较时按毫秒对齐。
	if want := claimed.ClaimedAt.Add(time.Minute).UnixMilli(); report.Requeued[0].LeaseExpiredAt.UnixMilli() != want {
		t.Errorf("租约到期时刻 = %v，期望 %v", report.Requeued[0].LeaseExpiredAt, time.UnixMilli(want))
	}

	if inflight := f.inflight(t); len(inflight) != 0 {
		t.Errorf("回收后不应留在途记录: %v", inflight)
	}
	if n, _ := f.client.ZCard(ctx, f.key+":processing:lease").Result(); n != 0 {
		t.Errorf("回收后租约应清空，实际 %d", n)
	}
	if n, _ := f.client.LLen(ctx, f.key).Result(); n != 1 {
		t.Errorf("消息应回到主队列，实际队列长度 %d", n)
	}

	// 重新领取：消息体不变，领取凭据是新的（旧的租约已被回收）。
	again, err := f.consumer.Claim(ctx, time.Second)
	if err != nil || again == nil {
		t.Fatalf("回收后重新领取失败: d=%v err=%v", again, err)
	}
	if again.ID == claimed.ID {
		t.Error("重新投递应换一个新的领取凭据")
	}
	if string(again.Raw) != string(claimed.Raw) {
		t.Errorf("消息体在重投中发生了变化: %s", again.Raw)
	}
}

func TestReclaimSkipsFreshLease(t *testing.T) {
	f, clock := newReclaimFixture(t)
	ctx := context.Background()
	f.push(t, `{"trigger_id":"t-1"}`)
	if _, err := f.consumer.Claim(ctx, time.Second); err != nil {
		t.Fatalf("领取失败: %v", err)
	}

	// 租约还早：正在处理的消息不能被别的消费者抢走。
	clock.Advance(10 * time.Second)
	report, err := f.consumer.ReclaimExpired(ctx, DefaultReclaimBatch)
	if err != nil {
		t.Fatalf("回收失败: %v", err)
	}
	if report.Scanned != 0 || len(report.Requeued) != 0 {
		t.Errorf("未过期租约不应被回收: %+v", report)
	}
	if n, _ := f.client.LLen(ctx, f.key).Result(); n != 0 {
		t.Errorf("主队列应为空，实际 %d", n)
	}
}

// 宽限期让逼近租约上限的慢处理不被立刻判死。
func TestReclaimRespectsGrace(t *testing.T) {
	f, clock := newReclaimFixture(t)
	ctx := context.Background()
	f.consumer.grace = 30 * time.Second
	f.push(t, `{"trigger_id":"t-1"}`)
	if _, err := f.consumer.Claim(ctx, time.Second); err != nil {
		t.Fatalf("领取失败: %v", err)
	}

	clock.Advance(70 * time.Second) // 租约(60s)已过，但宽限期(30s)未过
	if report, _ := f.consumer.ReclaimExpired(ctx, DefaultReclaimBatch); len(report.Requeued) != 0 {
		t.Errorf("宽限期内不应回收: %+v", report)
	}

	clock.Advance(30 * time.Second) // 累计 100s，超过 60s + 30s
	report, _ := f.consumer.ReclaimExpired(ctx, DefaultReclaimBatch)
	if len(report.Requeued) != 1 {
		t.Errorf("超过租约与宽限期应回收，实际 %+v", report)
	}
}

func TestReclaimSkipsAckedDelivery(t *testing.T) {
	f, clock := newReclaimFixture(t)
	ctx := context.Background()
	f.push(t, `{"trigger_id":"t-1"}`)
	d, err := f.consumer.Claim(ctx, time.Second)
	if err != nil || d == nil {
		t.Fatalf("领取失败: %v", err)
	}
	if err := f.consumer.Ack(ctx, d.ID); err != nil {
		t.Fatalf("确认失败: %v", err)
	}

	clock.Advance(2 * time.Minute)
	report, _ := f.consumer.ReclaimExpired(ctx, DefaultReclaimBatch)
	if report.Scanned != 0 || len(report.Requeued) != 0 {
		t.Errorf("已确认的消息不应被回收: %+v", report)
	}
	if n, _ := f.client.LLen(ctx, f.key).Result(); n != 0 {
		t.Errorf("主队列应为空，实际 %d", n)
	}
}

// 有租约却没有消息体的残留记录：清理掉并单独计数，不当作消息投递。
func TestReclaimCleansOrphanLease(t *testing.T) {
	f, clock := newReclaimFixture(t)
	ctx := context.Background()
	if err := f.client.ZAdd(ctx, f.key+":processing:lease", redis.Z{Score: 1, Member: "dead-delivery"}).Err(); err != nil {
		t.Fatalf("预置租约失败: %v", err)
	}
	clock.Advance(2 * time.Minute)

	report, err := f.consumer.ReclaimExpired(ctx, DefaultReclaimBatch)
	if err != nil {
		t.Fatalf("回收失败: %v", err)
	}
	if report.Scanned != 1 || len(report.Requeued) != 0 || report.Orphans != 1 {
		t.Errorf("残留记录应计入 Orphans: %+v", report)
	}
	if n, _ := f.client.ZCard(ctx, f.key+":processing:lease").Result(); n != 0 {
		t.Errorf("残留租约应被清理，实际 %d", n)
	}
	if n, _ := f.client.LLen(ctx, f.key).Result(); n != 0 {
		t.Errorf("空消息体不应入队，实际 %d", n)
	}
}

func TestReclaimRespectsBatchLimit(t *testing.T) {
	f, clock := newReclaimFixture(t)
	ctx := context.Background()
	const total = 12
	for i := 0; i < total; i++ {
		f.push(t, fmt.Sprintf(`{"trigger_id":"t-%02d"}`, i))
	}
	for i := 0; i < total; i++ {
		if d, _ := f.consumer.Claim(ctx, time.Second); d == nil {
			t.Fatalf("第 %d 次领取失败", i)
		}
	}
	clock.Advance(2 * time.Minute)

	first, err := f.consumer.ReclaimExpired(ctx, 5)
	if err != nil {
		t.Fatalf("首轮回收失败: %v", err)
	}
	if len(first.Requeued) != 5 || first.Scanned != 5 {
		t.Errorf("首轮应只处理 5 条，实际 %+v", first)
	}

	second, _ := f.consumer.ReclaimExpired(ctx, 100)
	if len(second.Requeued) != total-5 {
		t.Errorf("次轮应处理剩余 7 条，实际 %d", len(second.Requeued))
	}
	if n, _ := f.client.LLen(ctx, f.key).Result(); n != total {
		t.Errorf("消息都应回到队列，实际 %d", n)
	}
}

// 多个实例同时回收：每条消息只回到队列一次，不会重复投递。
func TestConcurrentReclaimRequeuesOnce(t *testing.T) {
	f, clock := newReclaimFixture(t)
	ctx := context.Background()
	const total = 40
	for i := 0; i < total; i++ {
		f.push(t, fmt.Sprintf(`{"trigger_id":"t-%02d"}`, i))
	}
	for i := 0; i < total; i++ {
		if d, _ := f.consumer.Claim(ctx, time.Second); d == nil {
			t.Fatalf("第 %d 次领取失败", i)
		}
	}
	clock.Advance(2 * time.Minute)

	var mu sync.Mutex
	count := 0
	const reclaimers = 6
	var wg sync.WaitGroup
	wg.Add(reclaimers)
	for i := 0; i < reclaimers; i++ {
		go func() {
			defer wg.Done()
			for {
				report, err := f.consumer.ReclaimExpired(ctx, 3)
				if err != nil {
					t.Errorf("回收失败: %v", err)
					return
				}
				mu.Lock()
				count += len(report.Requeued)
				mu.Unlock()
				if report.Scanned == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	if count != total {
		t.Errorf("应回收 %d 条，实际 %d", total, count)
	}
	if n, _ := f.client.LLen(ctx, f.key).Result(); n != total {
		t.Errorf("队列中应恰好 %d 条，实际 %d", total, n)
	}
	if inflight := f.inflight(t); len(inflight) != 0 {
		t.Errorf("回收后在途记录应为空: %d 条", len(inflight))
	}
}

// 回收的消息要能被正常消费到，且只被消费一次。
func TestReclaimedMessageIsConsumed(t *testing.T) {
	f, clock := newReclaimFixture(t)
	ctx := context.Background()
	f.push(t, `{"trigger_id":"t-1"}`, `{"trigger_id":"t-2"}`)

	crashed, _ := f.consumer.Claim(ctx, time.Second)
	if crashed == nil {
		t.Fatal("领取决不应为 nil")
	}
	clock.Advance(2 * time.Minute)
	if _, err := f.consumer.ReclaimExpired(ctx, DefaultReclaimBatch); err != nil {
		t.Fatalf("回收失败: %v", err)
	}

	// 另一条消息一直留在队列里，回收的消息插到消费端优先位置。
	next, _ := f.consumer.Claim(ctx, time.Second)
	if next == nil || string(next.Raw) != string(crashed.Raw) {
		t.Errorf("应优先领到回收的消息，实际 %v", next)
	}
	if err := f.consumer.Ack(ctx, next.ID); err != nil {
		t.Fatalf("确认失败: %v", err)
	}

	rest, _ := f.consumer.Claim(ctx, time.Second)
	if rest == nil || string(rest.Raw) != `{"trigger_id":"t-2"}` {
		t.Errorf("第二条消息应仍在队列里，实际 %v", rest)
	}
	if inflight := f.inflight(t); len(inflight) != 1 {
		t.Errorf("应只剩未确认的一条在途记录，实际 %d", len(inflight))
	}
}
