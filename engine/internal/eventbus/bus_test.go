package eventbus

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func quietOptions() Options {
	return Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

// 主题规则：全等、前缀通配与全量通配。
func TestTopicMatching(t *testing.T) {
	bus := New(quietOptions())
	defer bus.Close(context.Background())

	cases := []struct {
		pattern string
		topic   string
		want    bool
	}{
		{"*", "delivery.sent", true},
		{"*", "", true},
		{"delivery.*", "delivery.sent", true},
		{"delivery.*", "delivery.failed", true},
		{"delivery.*", "digest.updated", false},
		{"delivery.*", "delivery", false},
		{"delivery.result", "delivery.result", true},
		{"delivery.result", "delivery.sent", false},
	}
	for _, tc := range cases {
		sub := bus.Subscribe(tc.pattern, 0, func(Message) {})
		if sub == nil {
			t.Fatalf("规则 %q 应被接受", tc.pattern)
		}
		if got := sub.Matches(tc.topic); got != tc.want {
			t.Errorf("%q 匹配 %q = %v，期望 %v", tc.pattern, tc.topic, got, tc.want)
		}
	}
	for _, bad := range []string{"", "delivery*", "del.*.sent", "*.*"} {
		if sub := bus.Subscribe(bad, 0, func(Message) {}); sub != nil {
			t.Errorf("非法规则 %q 应被拒绝", bad)
		}
	}
}

// 同一事件按规则分发给不同订阅者，互不重复。
func TestPublishFanout(t *testing.T) {
	bus := New(quietOptions())
	var mu sync.Mutex
	got := map[string][]string{}
	record := func(name string) Handler {
		return func(msg Message) {
			mu.Lock()
			got[name] = append(got[name], msg.Topic)
			mu.Unlock()
		}
	}
	bus.Subscribe("*", 0, record("all"))
	bus.Subscribe("delivery.*", 0, record("delivery"))
	bus.Subscribe("digest.updated", 0, record("digest"))

	if n := bus.Publish(Message{ID: "1", Topic: "delivery.sent"}); n != 2 {
		t.Errorf("应有两个订阅者收下，实际 %d", n)
	}
	bus.Publish(Message{ID: "2", Topic: "digest.updated"})
	bus.Publish(Message{ID: "3", Topic: "trigger.received"})

	if err := bus.Close(context.Background()); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got["all"]) != 3 {
		t.Errorf("全量订阅应收到 3 条，实际 %v", got["all"])
	}
	if len(got["delivery"]) != 1 || got["delivery"][0] != "delivery.sent" {
		t.Errorf("前缀订阅结果不符: %v", got["delivery"])
	}
	if len(got["digest"]) != 1 || got["digest"][0] != "digest.updated" {
		t.Errorf("精确订阅结果不符: %v", got["digest"])
	}
}

// 订阅者处理慢时，发布方不被拖住，超出队列的事件被丢弃并计数。
func TestPublishDoesNotBlockOnSlowSubscriber(t *testing.T) {
	bus := New(quietOptions())
	release := make(chan struct{})
	var once sync.Once
	sub := bus.Subscribe("*", 1, func(Message) {
		once.Do(func() { <-release })
	})

	start := time.Now()
	const total = 500
	for i := 0; i < total; i++ {
		bus.Publish(Message{ID: "evt", Topic: "delivery.sent"})
	}
	elapsed := time.Since(start)
	if elapsed > time.Second {
		t.Errorf("发布被慢订阅者拖住：%v", elapsed)
	}

	stats := sub.Stats()
	if stats.Dropped == 0 {
		t.Errorf("队列满时应丢弃并计数: %+v", stats)
	}
	close(release)
	if err := bus.Close(context.Background()); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}
}

// 处理函数 panic 只影响当前这条事件，订阅继续工作。
func TestHandlerPanicIsRecovered(t *testing.T) {
	bus := New(quietOptions())
	var handled int
	var mu sync.Mutex
	sub := bus.Subscribe("*", 4, func(msg Message) {
		if msg.ID == "boom" {
			panic("处理函数写错了")
		}
		mu.Lock()
		handled++
		mu.Unlock()
	})

	bus.Publish(Message{ID: "boom", Topic: "delivery.sent"})
	bus.Publish(Message{ID: "ok", Topic: "delivery.sent"})
	if err := bus.Close(context.Background()); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if handled != 1 {
		t.Errorf("panic 之后应继续处理，实际处理 %d 条", handled)
	}
	stats := sub.Stats()
	if stats.Failed != 1 || stats.Handled != 2 {
		t.Errorf("计数不符: %+v", stats)
	}
}

// 关闭时把队列里已有的事件处理完，再退出。
func TestCloseDrainsQueuedEvents(t *testing.T) {
	bus := New(quietOptions())
	var mu sync.Mutex
	var ids []string
	sub := bus.Subscribe("*", 32, func(msg Message) {
		time.Sleep(time.Millisecond)
		mu.Lock()
		ids = append(ids, msg.ID)
		mu.Unlock()
	})

	const total = 10
	for i := 0; i < total; i++ {
		bus.Publish(Message{ID: string(rune('a' + i)), Topic: "delivery.sent"})
	}
	if err := bus.Close(context.Background()); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(ids) != total {
		t.Errorf("关闭前应收尾 %d 条，实际 %d", total, len(ids))
	}
	if stats := sub.Stats(); stats.Pending != 0 {
		t.Errorf("关闭后队列应为空: %+v", stats)
	}
	// 关闭后再发布不再分发。
	if n := bus.Publish(Message{ID: "late", Topic: "delivery.sent"}); n != 0 {
		t.Errorf("关闭后不应再分发，实际 %d", n)
	}
}

// 最近事件缓冲按容量覆盖最旧的一条，最新在前。
func TestFeedKeepsNewest(t *testing.T) {
	feed := NewFeed(3)
	bus := New(quietOptions())
	bus.Subscribe("*", 8, feed.Handler())
	for i := 0; i < 5; i++ {
		bus.Publish(Message{ID: string(rune('a' + i)), Topic: "delivery.sent"})
	}
	if err := bus.Close(context.Background()); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}

	recent := feed.Recent(2)
	if len(recent) != 2 {
		t.Fatalf("应返回 2 条，实际 %d", len(recent))
	}
	if recent[0].ID != "e" || recent[1].ID != "d" {
		t.Errorf("最新在前不符: %v", []string{recent[0].ID, recent[1].ID})
	}
	if feed.Total() != 5 {
		t.Errorf("总数应为 5，实际 %d", feed.Total())
	}
	if got := feed.Recent(0); len(got) != 3 {
		t.Errorf("不传条数应返回容量内的全部，实际 %d", len(got))
	}
}
