package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// 可靠性测试：模拟「领到消息 → 处理中途失败 → 进程没确认」的场景，
// 验证消息不会丢、重投后能正常处理完，且历史上不会出现重复确认。

const reliabilityQueueKey = "xw:queue:events"

// 租约取很短，让测试能在百毫秒内制造出「租约已过期」的状态。
const reliabilityLease = 80 * time.Millisecond

type reliabFixture struct {
	consumer *queue.ReliableConsumer
	client   *redis.Client
	mr       *miniredis.Miniredis
}

func newReliabFixture(t *testing.T) *reliabFixture {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	consumer := queue.NewReliableConsumer(mr.Addr(), "", 0, reliabilityQueueKey, reliabilityLease)
	t.Cleanup(func() {
		_ = consumer.Close()
		_ = client.Close()
	})
	return &reliabFixture{consumer: consumer, client: client, mr: mr}
}

func (f *reliabFixture) push(t *testing.T, payloads ...string) {
	t.Helper()
	for _, p := range payloads {
		if err := f.client.LPush(context.Background(), reliabilityQueueKey, p).Err(); err != nil {
			t.Fatalf("入队失败: %v", err)
		}
	}
}

func (f *reliabFixture) queueLen(t *testing.T) int64 {
	t.Helper()
	n, err := f.client.LLen(context.Background(), reliabilityQueueKey).Result()
	if err != nil {
		t.Fatalf("读取队列长度失败: %v", err)
	}
	return n
}

func (f *reliabFixture) inflight(t *testing.T) map[string]string {
	t.Helper()
	got, err := f.client.HGetAll(context.Background(), reliabilityQueueKey+":processing").Result()
	if err != nil {
		t.Fatalf("读取在途记录失败: %v", err)
	}
	return got
}

// waitExpired 让租约失效（含默认宽限期）。
func waitExpired() {
	time.Sleep(reliabilityLease + reliabilityLease/2 + 60*time.Millisecond)
}

// 处理中途失败的消息：既不丢也不被当成已处理，租约过期后重新投递并最终成功。
func TestMessageSurvivesProcessingFailure(t *testing.T) {
	f := newReliabFixture(t)
	ctx := context.Background()
	f.push(t, `{"trigger_id":"t-crash","name":"post_liked","subscriber_id":"creator_1"}`)

	var attempts, success int32
	w := New(f.consumer, 20*time.Millisecond, discardLogger())
	w.handle = func(raw []byte) error {
		atomic.AddInt32(&attempts, 1)
		return errors.New("推送网关连接被重置")
	}

	// 第一次消费：领到消息但处理失败，不确认。
	delivery, err := f.consumer.Claim(ctx, time.Second)
	if err != nil || delivery == nil {
		t.Fatalf("领取失败: d=%v err=%v", delivery, err)
	}
	w.process(delivery, nil)

	if atomic.LoadInt32(&attempts) != 1 {
		t.Fatalf("应尝试处理 1 次，实际 %d", atomic.LoadInt32(&attempts))
	}
	if got := f.inflight(t); len(got) != 1 {
		t.Fatalf("失败的消息应留在在途记录里，实际 %d 条", len(got))
	}
	if n := f.queueLen(t); n != 0 {
		t.Fatalf("消息此刻不在主队列，实际 %d", n)
	}

	// 租约过期后回收，消息回到队列。
	waitExpired()
	report, err := f.consumer.ReclaimExpired(ctx, queue.DefaultReclaimBatch)
	if err != nil {
		t.Fatalf("回收失败: %v", err)
	}
	if len(report.Requeued) != 1 {
		t.Fatalf("应回收 1 条，实际 %+v", report)
	}
	if got := f.queueLen(t); got != 1 {
		t.Fatalf("回收后消息应回到队列，实际 %d", got)
	}

	// 第二次消费：这次成功，确认后不应再留在途记录。
	w.handle = func(raw []byte) error {
		atomic.AddInt32(&attempts, 1)
		atomic.AddInt32(&success, 1)
		return w.Handle(raw)
	}
	retried, err := f.consumer.Claim(ctx, time.Second)
	if err != nil || retried == nil {
		t.Fatalf("重投后领取失败: d=%v err=%v", retried, err)
	}
	w.process(retried, nil)

	if atomic.LoadInt32(&attempts) != 2 || atomic.LoadInt32(&success) != 1 {
		t.Errorf("应共尝试 2 次、成功 1 次，实际 attempts=%d success=%d",
			atomic.LoadInt32(&attempts), atomic.LoadInt32(&success))
	}
	if got := f.inflight(t); len(got) != 0 {
		t.Errorf("成功后不应留有在途记录: %v", got)
	}
	if n := f.queueLen(t); n != 0 {
		t.Errorf("队列应为空，实际 %d", n)
	}
}

// 崩溃恢复发生在启动时：回收器一启动就把上一轮进程留下的消息捡回来。
func TestReclaimerRecoversOnStartup(t *testing.T) {
	f := newReliabFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.push(t, `{"trigger_id":"t-restart","name":"post_commented","subscriber_id":"creator_1"}`)

	// 模拟上一轮进程：领走消息后没有确认就退出。
	crashed, err := f.consumer.Claim(ctx, time.Second)
	if err != nil || crashed == nil {
		t.Fatalf("领取失败: d=%v err=%v", crashed, err)
	}
	if n := f.queueLen(t); n != 0 {
		t.Fatalf("领取后主队列应为空，实际 %d", n)
	}
	waitExpired()

	reclaimer := NewReclaimer(f.consumer, time.Hour, discardLogger())
	go reclaimer.Run(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.queueLen(t) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := f.queueLen(t); n != 1 {
		t.Fatalf("启动回收应把消息放回队列，实际 %d", n)
	}
	if got := f.inflight(t); len(got) != 0 {
		t.Errorf("回收后在途记录应清空，实际 %v", got)
	}

	// 重启后的 worker 能把这条消息正常消费掉。
	var handled int32
	w := New(f.consumer, 20*time.Millisecond, discardLogger())
	w.handle = func(raw []byte) error {
		atomic.AddInt32(&handled, 1)
		return w.Handle(raw)
	}
	delivery, err := f.consumer.Claim(ctx, time.Second)
	if err != nil || delivery == nil {
		t.Fatalf("重启后领取失败: d=%v err=%v", delivery, err)
	}
	w.process(delivery, nil)
	if atomic.LoadInt32(&handled) != 1 {
		t.Errorf("重启后应处理 1 条，实际 %d", atomic.LoadInt32(&handled))
	}
	if got := f.inflight(t); len(got) != 0 {
		t.Errorf("处理完应清空在途记录，实际 %v", got)
	}
}

// 回收失败不应打断回收循环，下一轮仍会重试。
func TestReclaimerKeepsRunningAfterError(t *testing.T) {
	stub := &flakyReclaimer{fail: 2}
	reclaimer := NewReclaimer(stub, 10*time.Millisecond, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		reclaimer.Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if stub.calls() >= 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("回收器未在 ctx 取消后退出")
	}
	if got := stub.calls(); got < 3 {
		t.Errorf("出错后应继续扫描，实际调用 %d 次", got)
	}
}

// flakyReclaimer 前若干次返回错误，用于验证回收循环的容错。
type flakyReclaimer struct {
	mu    sync.Mutex
	fail  int
	count int
}

func (f *flakyReclaimer) ReclaimExpired(ctx context.Context, batch int) (queue.ReclaimReport, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count++
	if f.count <= f.fail {
		return queue.ReclaimReport{}, errors.New("redis: connection refused")
	}
	return queue.ReclaimReport{}, nil
}

func (f *flakyReclaimer) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count
}
