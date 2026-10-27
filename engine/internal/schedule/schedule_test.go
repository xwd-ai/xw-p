package schedule

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fixedTime() time.Time {
	return time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
}

func TestMemoryStoreDueOrderAndLimit(t *testing.T) {
	store := NewMemoryStore()
	now := fixedTime()

	late, _ := store.Add(Item{DueAt: now.Add(3 * time.Minute), Payload: []byte("c")})
	early, _ := store.Add(Item{DueAt: now.Add(-time.Minute), Payload: []byte("a")})
	middle, _ := store.Add(Item{DueAt: now, Payload: []byte("b")})
	if _, err := store.Add(Item{DueAt: now.Add(time.Hour), Payload: []byte("d")}); err != nil {
		t.Fatalf("登记未到期任务失败：%v", err)
	}
	if late == early || early == middle {
		t.Fatalf("自动生成的 id 应唯一：%s %s %s", late, early, middle)
	}

	due, err := store.Due(now, 10)
	if err != nil {
		t.Fatalf("Due 失败：%v", err)
	}
	if len(due) != 2 {
		t.Fatalf("到期任务数 = %d, 期望 2（future 不应到期）", len(due))
	}
	if due[0].ID != early || due[1].ID != middle {
		t.Errorf("排序错误：%s, %s", due[0].ID, due[1].ID)
	}

	// limit 生效
	limited, _ := store.Due(now, 1)
	if len(limited) != 1 {
		t.Errorf("limit 未生效：%d", len(limited))
	}

	// Remove 后不再出现在到期集合里
	if err := store.Remove(early); err != nil {
		t.Fatalf("Remove 失败：%v", err)
	}
	if err := store.Remove(early); !errors.Is(err, ErrNotFound) {
		t.Errorf("重复 Remove 应返回 ErrNotFound，实际 %v", err)
	}
	if store.Len() != 3 {
		t.Errorf("剩余任务数 = %d, 期望 3（含 future）", store.Len())
	}
}

func TestMemoryStoreRejectsMissingDueAt(t *testing.T) {
	if _, err := NewMemoryStore().Add(Item{Payload: []byte("x")}); err == nil {
		t.Fatal("缺到期时刻应报错")
	}
}

func TestSchedulerTickRespectsDueAt(t *testing.T) {
	now := fixedTime()
	var handled atomic.Int32

	s := New(NewMemoryStore(), func(ctx context.Context, item Item) error {
		handled.Add(1)
		return nil
	}, nil, WithNow(func() time.Time { return now }))

	if _, err := s.EnqueueDelay(now.Add(30*time.Second), []byte("resume")); err != nil {
		t.Fatalf("登记失败：%v", err)
	}

	// 未到期：不触发
	if n, err := s.Tick(context.Background(), now); err != nil || n != 0 {
		t.Fatalf("未到期不应执行：n=%d err=%v", n, err)
	}
	if s.Len() != 1 {
		t.Errorf("待执行任务数 = %d, 期望 1", s.Len())
	}

	// 到期：执行一次并清空
	if n, err := s.Tick(context.Background(), now.Add(30*time.Second)); err != nil || n != 1 {
		t.Fatalf("到期应执行一次：n=%d err=%v", n, err)
	}
	if s.Len() != 0 {
		t.Errorf("执行后待执行任务数 = %d, 期望 0", s.Len())
	}
	if handled.Load() != 1 {
		t.Errorf("handler 调用次数 = %d, 期望 1", handled.Load())
	}

	// 再 Tick 不会重复执行
	if n, _ := s.Tick(context.Background(), now.Add(time.Hour)); n != 0 {
		t.Errorf("已执行任务不应重复触发：%d", n)
	}
}

func TestSchedulerRetriesWithBackoffThenDrops(t *testing.T) {
	now := fixedTime()
	var calls atomic.Int32

	s := New(NewMemoryStore(),
		func(ctx context.Context, item Item) error {
			calls.Add(1)
			return errors.New("续跑失败")
		},
		nil,
		WithNow(func() time.Time { return now }),
		WithMaxAttempts(3),
		WithBaseBackoff(time.Second),
	)

	if _, err := s.EnqueueDelay(now, []byte("resume")); err != nil {
		t.Fatalf("登记失败：%v", err)
	}

	// 第 1 次失败 → 退避 1s 后重排
	if n, _ := s.Tick(context.Background(), now); n != 0 {
		t.Fatalf("失败任务不应计入成功数")
	}
	if s.Len() != 1 {
		t.Fatalf("失败任务应重新排期，实际待执行 %d", s.Len())
	}

	// 未到退避时刻：不执行
	if n, _ := s.Tick(context.Background(), now.Add(500*time.Millisecond)); n != 0 {
		t.Errorf("退避期内不应执行")
	}
	// 第 2 次失败（退避 1s 到期）→ 再排 2s 后
	if n, _ := s.Tick(context.Background(), now.Add(time.Second)); n != 0 {
		t.Errorf("第二次失败不应计入成功数")
	}
	// 第 3 次失败 → 达到上限，丢弃
	if n, _ := s.Tick(context.Background(), now.Add(3*time.Second)); n != 0 {
		t.Errorf("第三次失败不应计入成功数")
	}
	if s.Len() != 0 {
		t.Errorf("超限后应丢弃任务，实际待执行 %d", s.Len())
	}
	if calls.Load() != 3 {
		t.Errorf("handler 调用次数 = %d, 期望 3", calls.Load())
	}
}

func TestSchedulerConcurrentTickClaimsOnce(t *testing.T) {
	now := fixedTime()
	store := NewMemoryStore()
	var calls atomic.Int32

	handler := func(ctx context.Context, item Item) error {
		calls.Add(1)
		return nil
	}
	// 两个调度器共享同一存储，模拟多实例并发消费。
	a := New(store, handler, nil, WithNow(func() time.Time { return now }))
	b := New(store, handler, nil, WithNow(func() time.Time { return now }))

	for i := 0; i < 20; i++ {
		if _, err := store.Add(Item{DueAt: now, Payload: []byte("resume")}); err != nil {
			t.Fatalf("登记失败：%v", err)
		}
	}

	var wg sync.WaitGroup
	for _, s := range []*Scheduler{a, b} {
		wg.Add(1)
		go func(s *Scheduler) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				if _, err := s.Tick(context.Background(), now); err != nil {
					t.Errorf("Tick 失败：%v", err)
				}
			}
		}(s)
	}
	wg.Wait()

	if calls.Load() != 20 {
		t.Errorf("handler 调用次数 = %d, 期望 20（每条任务只认领一次）", calls.Load())
	}
	if store.Len() != 0 {
		t.Errorf("剩余任务数 = %d, 期望 0", store.Len())
	}
}

func TestSchedulerSubmitValidatesStore(t *testing.T) {
	s := New(nil, nil, nil)
	if _, err := s.Submit(Item{DueAt: fixedTime()}); err == nil {
		t.Fatal("未配置存储时应报错")
	}
	if s.Len() != 0 {
		t.Errorf("未配置存储时 Len 应为 0")
	}
	if _, err := s.Tick(context.Background(), fixedTime()); err == nil {
		t.Fatal("未配置存储时 Tick 应报错")
	}
}

func TestBackoffCapped(t *testing.T) {
	if got := backoff(time.Second, 1); got != time.Second {
		t.Errorf("首次退避 = %s, 期望 1s", got)
	}
	if got := backoff(time.Second, 3); got != 4*time.Second {
		t.Errorf("第三次退避 = %s, 期望 4s", got)
	}
	if got := backoff(time.Second, 20); got != time.Minute {
		t.Errorf("退避上限 = %s, 期望 1m", got)
	}
	if got := backoff(0, 1); got != time.Second {
		t.Errorf("零基准退避应回退 1s，实际 %s", got)
	}
}
