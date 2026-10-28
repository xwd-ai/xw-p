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

// 并发消费的端到端验证：真实可靠消费者（进程内 Redis）配合消费者池，
// 检验多消费者同时领取时不重复、不丢消息，且处理顺序不影响最终结果。
func TestPoolAgainstReliableConsumer(t *testing.T) {
	const (
		total   = 300
		workers = 8
	)
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	const key = "xw:queue:events"
	ctx := context.Background()
	// LPUSH 入队，消费者从右侧取出，与 Python 侧写入方向一致。
	pipe := client.Pipeline()
	for i := 0; i < total; i++ {
		pipe.LPush(ctx, key, fmt.Sprintf(`{"trigger_id":"t-%d"}`, i))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("入队失败: %v", err)
	}

	consumer := queue.NewReliableConsumer(mr.Addr(), "", 0, key, time.Minute)
	t.Cleanup(func() { _ = consumer.Close() })

	var (
		mu     sync.Mutex
		seen   = make(map[string]int, total)
		orders []string
	)
	pool := NewPool(consumer, workers, time.Second, discardLogger())
	pool.handle = func(raw []byte) error {
		mu.Lock()
		seen[string(raw)]++
		orders = append(orders, string(raw))
		mu.Unlock()
		return nil
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- pool.Run(runCtx) }()

	waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		return n >= total
	})
	// 确认全部回到 Redis 侧完成（在途记录清空）。
	waitFor(t, 10*time.Second, func() bool {
		n, err := client.HLen(ctx, key+":processing").Result()
		return err == nil && n == 0
	})
	cancel()

	select {
	case left := <-done:
		if left != 0 {
			t.Errorf("正常退出不应遗留未完成消息，实际 %d", left)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("消费者池未在 ctx 取消后退出")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != total {
		t.Fatalf("应处理 %d 条不同消息，实际 %d 条", total, len(seen))
	}
	for raw, n := range seen {
		if n != 1 {
			t.Errorf("消息 %s 被处理 %d 次", raw, n)
		}
	}

	// 队列与在途记录都应清空，staging 不应残留搬运中的消息。
	for _, k := range []string{key, key + ":staging"} {
		if n, err := client.LLen(ctx, k).Result(); err != nil || n != 0 {
			t.Errorf("列表 %s 应清空，实际 %d（err=%v）", k, n, err)
		}
	}
	if n, err := client.HLen(ctx, key+":processing").Result(); err != nil || n != 0 {
		t.Errorf("在途记录应清空，实际 %d（err=%v）", n, err)
	}
}
