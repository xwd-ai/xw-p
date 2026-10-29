package worker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// 失败重试的端到端验证：真实可靠消费者（进程内 Redis）+ 消费者池 + 重试泵，
// 覆盖「处理失败 → 退避等待 → 写回队列 → 重投成功」与「次数耗尽进死信」。

// retryFixture 组装一套完整的重试链路。
type retryFixture struct {
	client   *redis.Client
	consumer *queue.ReliableConsumer
	key      string
	mu       sync.Mutex
	attempts map[string]int
	// failTimes 是每条消息在前几次处理时失败的次数。
	failTimes int
}

func newRetryFixture(t *testing.T, failTimes int, policy queue.RetryPolicy) *retryFixture {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	const key = "xw:queue:events"
	consumer := queue.NewReliableConsumer(mr.Addr(), "", 0, key, time.Minute)
	t.Cleanup(func() { _ = consumer.Close() })

	return &retryFixture{
		client:    client,
		consumer:  consumer,
		key:       key,
		attempts:  map[string]int{},
		failTimes: failTimes,
	}
}

func (f *retryFixture) push(t *testing.T, payloads ...string) {
	t.Helper()
	for _, p := range payloads {
		if err := f.client.LPush(context.Background(), f.key, p).Err(); err != nil {
			t.Fatalf("入队失败: %v", err)
		}
	}
}

func (f *retryFixture) handle(raw []byte) error {
	f.mu.Lock()
	f.attempts[string(raw)]++
	n := f.attempts[string(raw)]
	f.mu.Unlock()
	if n <= f.failTimes {
		return fmt.Errorf("渠道暂时不可用（第 %d 次）", n)
	}
	return nil
}

func (f *retryFixture) count(raw string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts[raw]
}

// run 启动池与重试泵，直到 ctx 取消。
func (f *retryFixture) run(ctx context.Context, policy queue.RetryPolicy) {
	pool := NewPool(f.consumer, 2, time.Second, discardLogger()).
		WithRetrier(f.consumer.Retrier(policy))
	pool.handle = f.handle

	pumper := NewRetryPumper(f.consumer, 5*time.Millisecond, discardLogger())
	go pumper.Run(ctx)
	pool.Run(ctx)
}

func TestRetryEndToEndRecoversAfterTransientFailure(t *testing.T) {
	// 前两次失败，第三次成功：验证退避重试确实能把消息救回来。
	policy := queue.RetryPolicy{MaxAttempts: 5, Base: 10 * time.Millisecond, Max: 50 * time.Millisecond, Jitter: 0.2}
	f := newRetryFixture(t, 2, policy)
	const payload = `{"trigger_id":"t-like-9","name":"post_liked","subscriber_id":"creator_2"}`
	f.push(t, payload)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.run(ctx, policy)
	}()

	waitFor(t, 5*time.Second, func() bool { return f.count(payload) >= 3 })
	cancel()
	<-done

	if got := f.count(payload); got != 3 {
		t.Errorf("应在第 3 次处理时成功，实际处理 %d 次", got)
	}
	// 成功之后队列、重试集合、在途记录都该是空的。
	for _, k := range []string{f.key, f.key + ":staging"} {
		if n := f.client.LLen(context.Background(), k).Val(); n != 0 {
			t.Errorf("列表 %s 应清空，实际 %d", k, n)
		}
	}
	if n := f.client.ZCard(context.Background(), f.key+":retry").Val(); n != 0 {
		t.Errorf("待重试集合应清空，实际 %d", n)
	}
	if n := f.client.HLen(context.Background(), f.key+":processing").Val(); n != 0 {
		t.Errorf("在途记录应清空，实际 %d", n)
	}
	dead, err := f.consumer.DeadLetters(context.Background(), 10)
	if err != nil {
		t.Fatalf("读取死信失败: %v", err)
	}
	if len(dead) != 0 {
		t.Errorf("重试成功的消息不该进死信，实际 %d 条", len(dead))
	}
}

func TestRetryEndToEndExhaustsIntoDeadLetter(t *testing.T) {
	// 一直失败：处理次数停在配置上限，消息进入死信而不是无限重投。
	policy := queue.RetryPolicy{MaxAttempts: 3, Base: 5 * time.Millisecond, Max: 20 * time.Millisecond, Jitter: 0}
	f := newRetryFixture(t, 100, policy)
	const payload = `{"trigger_id":"t-cmt-bad","name":"post_commented"}`
	f.push(t, payload)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.run(ctx, policy)
	}()

	waitFor(t, 5*time.Second, func() bool {
		dead, err := f.consumer.DeadLetters(context.Background(), 10)
		return err == nil && len(dead) > 0
	})
	// 再等一会，确认没有继续重投。
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if got := f.count(payload); got != policy.MaxAttempts {
		t.Errorf("处理次数应停在上限 %d，实际 %d", policy.MaxAttempts, got)
	}
	dead, err := f.consumer.DeadLetters(context.Background(), 10)
	if err != nil || len(dead) != 1 {
		t.Fatalf("应有 1 条死信，实际 %d（err=%v）", len(dead), err)
	}
	if string(dead[0].Payload) != payload {
		t.Errorf("死信应保留原始消息体，实际 %s", dead[0].Payload)
	}
	if dead[0].LastError == "" {
		t.Error("死信应记录最后一次失败原因")
	}
	if n := f.client.LLen(context.Background(), f.key).Val(); n != 0 {
		t.Errorf("耗尽次数的消息不该回到主队列，实际 %d 条", n)
	}
}
