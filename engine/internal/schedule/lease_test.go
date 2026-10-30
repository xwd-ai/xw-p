package schedule

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// 认领租约测试：任务被认领后就不再出现在时间轴上，但记录仍在存储里；
// 执行成功才清除，认领后进程消失则由下一轮回收放回时间轴重新投递。

// 认领后的任务对 Due 与 Len 都不可见，但记录还在。
func TestClaimHidesTaskUntilAck(t *testing.T) {
	ctx := context.Background()
	now := fixedTime()

	t.Run("内存", func(t *testing.T) {
		store := NewMemoryStore()
		id, err := store.Add(Item{DueAt: now, Payload: []byte("post_liked")})
		if err != nil {
			t.Fatalf("登记失败：%v", err)
		}
		if ok, err := store.Claim(id, now, time.Minute); err != nil || !ok {
			t.Fatalf("认领应成功：ok=%v err=%v", ok, err)
		}
		if due, _ := store.Due(now, 10); len(due) != 0 {
			t.Errorf("已认领的任务不该出现在到期集合里：%+v", due)
		}
		if n := store.Len(); n != 0 {
			t.Errorf("已认领的任务不该计入待执行数：%d", n)
		}
		// 记录还在：租约过期后能被回收回来。
		res, err := store.Reclaim(now.Add(time.Minute), 10, 3)
		if err != nil || len(res.Rewound) != 1 || len(res.Abandoned) != 0 {
			t.Fatalf("租约过期后应回收一条：%+v err=%v", res, err)
		}
		if due, _ := store.Due(now.Add(time.Minute), 10); len(due) != 1 {
			t.Errorf("回收后应重新到期，实际 %d 条", len(due))
		}
		_ = ctx
	})

	t.Run("Redis", func(t *testing.T) {
		store, client, _ := newRedisStoreFixture(t)
		id, err := store.Add(Item{DueAt: now, Payload: []byte("post_liked")})
		if err != nil {
			t.Fatalf("登记失败：%v", err)
		}
		if ok, err := store.Claim(id, now, time.Minute); err != nil || !ok {
			t.Fatalf("认领应成功：ok=%v err=%v", ok, err)
		}
		if due, _ := store.Due(now, 10); len(due) != 0 {
			t.Errorf("已认领的任务不该出现在到期集合里：%+v", due)
		}
		if n := store.Len(); n != 0 {
			t.Errorf("已认领的任务不该计入时间轴：%d", n)
		}
		// 时间轴清了、在途集合记下了、任务体仍在。
		if n, _ := client.ZCard(ctx, store.InflightKey()).Result(); n != 1 {
			t.Errorf("在途记录数 = %d, 期望 1", n)
		}
		if n, _ := client.HLen(ctx, store.ItemsKey()).Result(); n != 1 {
			t.Errorf("任务体数 = %d, 期望 1（认领不等于删除）", n)
		}
		deadline, err := client.ZScore(ctx, store.InflightKey(), id).Result()
		if err != nil {
			t.Fatalf("读取租约截止时刻失败：%v", err)
		}
		if want := float64(now.Add(time.Minute).UnixMilli()); deadline != want {
			t.Errorf("租约截止 = %v, 期望 %v", deadline, want)
		}
	})
}

// 确认之后任务彻底消失：既不回到时间轴，也不留在在途集合。
func TestAckRemovesTaskCompletely(t *testing.T) {
	now := fixedTime()

	t.Run("内存", func(t *testing.T) {
		store := NewMemoryStore()
		id, _ := store.Add(Item{DueAt: now, Payload: []byte("post_liked")})
		if ok, _ := store.Claim(id, now, time.Minute); !ok {
			t.Fatal("认领失败")
		}
		if err := store.Ack(id); err != nil {
			t.Fatalf("确认失败：%v", err)
		}
		// 重复确认是幂等的：租约回收失败时重试确认不该报错。
		if err := store.Ack(id); err != nil {
			t.Errorf("重复确认不该报错：%v", err)
		}
		res, err := store.Reclaim(now.Add(time.Hour), 10, 3)
		if err != nil || len(res.Rewound) != 0 || len(res.Abandoned) != 0 {
			t.Errorf("已确认的任务不该被回收：%+v err=%v", res, err)
		}
		if n := store.Len(); n != 0 {
			t.Errorf("待执行数 = %d, 期望 0", n)
		}
	})

	t.Run("Redis", func(t *testing.T) {
		store, client, _ := newRedisStoreFixture(t)
		id, _ := store.Add(Item{DueAt: now, Payload: []byte("post_liked")})
		if ok, _ := store.Claim(id, now, time.Minute); !ok {
			t.Fatal("认领失败")
		}
		if err := store.Ack(id); err != nil {
			t.Fatalf("确认失败：%v", err)
		}
		if err := store.Ack(id); err != nil {
			t.Errorf("重复确认不该报错：%v", err)
		}
		keys := client.Keys(context.Background(), store.Prefix()+"*").Val()
		if len(keys) != 0 {
			t.Errorf("确认后残留键：%v", keys)
		}
	})
}

// 认领是互斥的：并发认领同一条任务只有一方拿到。
func TestClaimIsExclusive(t *testing.T) {
	now := fixedTime()

	run := func(t *testing.T, store Store) {
		id, err := store.Add(Item{DueAt: now, Payload: []byte("post_liked")})
		if err != nil {
			t.Fatalf("登记失败：%v", err)
		}
		const workers = 16
		var (
			wg    sync.WaitGroup
			mu    sync.Mutex
			wins  int
			fails []error
		)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ok, err := store.Claim(id, now, time.Minute)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					fails = append(fails, err)
				}
				if ok {
					wins++
				}
			}()
		}
		wg.Wait()

		if len(fails) != 0 {
			t.Fatalf("认领报错：%v", fails)
		}
		if wins != 1 {
			t.Errorf("认领成功次数 = %d, 期望 1", wins)
		}
	}

	t.Run("内存", func(t *testing.T) { run(t, NewMemoryStore()) })
	t.Run("Redis", func(t *testing.T) {
		store, _, _ := newRedisStoreFixture(t)
		run(t, store)
	})
}

// 回收按 maxAttempts 分流：还能再试的放回时间轴，次数耗尽的丢弃。
func TestReclaimSplitsRewoundAndAbandoned(t *testing.T) {
	now := fixedTime()

	for _, tc := range []struct {
		name  string
		store func(t *testing.T) Store
	}{
		{name: "内存", store: func(t *testing.T) Store { return NewMemoryStore() }},
		{name: "Redis", store: func(t *testing.T) Store {
			s, _, _ := newRedisStoreFixture(t)
			return s
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.store(t)

			// 一条还能重试（attempts=0），一条已经用完（attempts=2，上限 3）。
			fresh, _ := store.Add(Item{DueAt: now, Payload: []byte("like-1")})
			tired, _ := store.Add(Item{DueAt: now, Payload: []byte("like-2"), Attempts: 2})
			for _, id := range []string{fresh, tired} {
				if ok, err := store.Claim(id, now, 10*time.Second); err != nil || !ok {
					t.Fatalf("认领 %s 失败：ok=%v err=%v", id, ok, err)
				}
			}

			// 租约未过期：一条都不动。
			if res, err := store.Reclaim(now.Add(5*time.Second), 10, 3); err != nil ||
				len(res.Rewound) != 0 || len(res.Abandoned) != 0 {
				t.Fatalf("租约未过期不该回收：%+v err=%v", res, err)
			}

			res, err := store.Reclaim(now.Add(10*time.Second), 10, 3)
			if err != nil {
				t.Fatalf("回收失败：%v", err)
			}
			if len(res.Rewound) != 1 || res.Rewound[0].ID != fresh || res.Rewound[0].Attempts != 1 {
				t.Errorf("应只回收还能重试的那条：%+v", res.Rewound)
			}
			if len(res.Abandoned) != 1 || res.Abandoned[0] != tired {
				t.Errorf("次数耗尽的记录应被丢弃：%+v", res.Abandoned)
			}

			// 回收后的任务立刻到期，且尝试次数已累加。
			due, err := store.Due(now.Add(10*time.Second), 10)
			if err != nil || len(due) != 1 {
				t.Fatalf("回收后到期任务数 = %d, err=%v", len(due), err)
			}
			if due[0].ID != fresh || due[0].Attempts != 1 {
				t.Errorf("回收后任务状态不对：%+v", due[0])
			}
			// 再回收一轮不会有新结果（在途记录已清空）。
			if res, _ := store.Reclaim(now.Add(20*time.Second), 10, 3); len(res.Rewound)+len(res.Abandoned) != 0 {
				t.Errorf("回收应只发生一次：%+v", res)
			}
		})
	}
}

// 多实例并发回收：同一批过期租约里，每条任务只被一方捡回，不重复投递。
func TestReclaimConcurrentInstancesPickEachTaskOnce(t *testing.T) {
	mr := miniredis.RunT(t)
	now := fixedTime()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	storeA := NewRedisStoreWithPrefix(client, "xw:delay:reclaim")
	storeB := NewRedisStoreWithPrefix(client, "xw:delay:reclaim")

	const total = 30
	for i := 0; i < total; i++ {
		id, err := storeA.Add(Item{DueAt: now, Payload: []byte("post_liked")})
		if err != nil {
			t.Fatalf("登记失败：%v", err)
		}
		if ok, err := storeA.Claim(id, now, 10*time.Second); err != nil || !ok {
			t.Fatalf("认领失败：ok=%v err=%v", ok, err)
		}
	}

	var (
		mu   sync.Mutex
		seen = make(map[string]int)
		wg   sync.WaitGroup
	)
	for _, store := range []Store{storeA, storeB} {
		wg.Add(1)
		go func(store Store) {
			defer wg.Done()
			for i := 0; i < 4; i++ {
				res, err := store.Reclaim(now.Add(10*time.Second), 8, 3)
				if err != nil {
					t.Errorf("回收失败：%v", err)
					return
				}
				mu.Lock()
				for _, item := range res.Rewound {
					seen[item.ID]++
				}
				mu.Unlock()
			}
		}(store)
	}
	wg.Wait()

	if len(seen) != total {
		t.Errorf("被回收的任务数 = %d, 期望 %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("任务 %s 被回收 %d 次，期望 1", id, n)
		}
	}
	if n := storeA.Len(); n != total {
		t.Errorf("回收后时间轴上的任务数 = %d, 期望 %d", n, total)
	}
}

// 认领后进程消失（无确认、无重排）：下一个进程在租约过期后补投这一条。
func TestRestartRecoversTaskClaimedBeforeCrash(t *testing.T) {
	mr := miniredis.RunT(t)
	now := fixedTime()
	prefix := "xw:delay:crash"

	clientA := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = clientA.Close() })
	storeA := NewRedisStoreWithPrefix(clientA, prefix)

	id, err := storeA.Add(Item{DueAt: now, Payload: []byte(`{"step":"post_liked"}`)})
	if err != nil {
		t.Fatalf("登记失败：%v", err)
	}
	// 认领之后进程被杀：既没有 Ack 也没有 Requeue。
	if ok, err := storeA.Claim(id, now, time.Minute); err != nil || !ok {
		t.Fatalf("认领失败：ok=%v err=%v", ok, err)
	}

	// 新进程：新连接、新存储、新调度器。
	clientB := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = clientB.Close() })
	storeB := NewRedisStoreWithPrefix(clientB, prefix)

	var (
		mu   sync.Mutex
		got  []string
		atts []int
	)
	scheduler := New(storeB, func(ctx context.Context, item Item) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, string(item.Payload))
		atts = append(atts, item.Attempts)
		return nil
	}, nil, WithClaimLease(time.Minute))

	// 租约还没到期：这条任务仍属于「上一个进程」，不该被抢跑。
	if n, err := scheduler.Tick(context.Background(), now.Add(30*time.Second)); err != nil || n != 0 {
		t.Fatalf("租约有效期内不该重投：n=%d err=%v", n, err)
	}
	if len(got) != 0 {
		t.Fatalf("租约有效期内执行了 %d 条", len(got))
	}

	// 租约到期：回收 → 重新认领 → 执行。
	if n, err := scheduler.Tick(context.Background(), now.Add(time.Minute)); err != nil || n != 1 {
		t.Fatalf("租约到期后应补投一条：n=%d err=%v", n, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != `{"step":"post_liked"}` {
		t.Errorf("补投的载荷 = %v", got)
	}
	if len(atts) != 1 || atts[0] != 1 {
		t.Errorf("补投应是第 2 次尝试（attempts=1），实际 %v", atts)
	}

	// 补投成功后存储里不应留下任何键。
	if keys := clientB.Keys(context.Background(), prefix+"*").Val(); len(keys) != 0 {
		t.Errorf("补投完成后残留键：%v", keys)
	}
}

// 调度器把 Handler 的 panic 当作一次失败：不退出循环，按退避重排。
func TestSchedulerRecoversHandlerPanic(t *testing.T) {
	now := fixedTime()
	store := NewMemoryStore()
	var calls int

	s := New(store, func(ctx context.Context, item Item) error {
		calls++
		panic("续跑时炸了")
	}, nil,
		WithNow(func() time.Time { return now }),
		WithBaseBackoff(time.Second),
		WithMaxAttempts(3),
	)

	if _, err := s.Submit(Item{DueAt: now, Payload: []byte("post_liked")}); err != nil {
		t.Fatalf("登记失败：%v", err)
	}
	if n, err := s.Tick(context.Background(), now); err != nil || n != 0 {
		t.Fatalf("panic 不该计入成功数：n=%d err=%v", n, err)
	}
	if calls != 1 {
		t.Fatalf("Handler 调用次数 = %d, 期望 1", calls)
	}
	// 失败后按退避重排，退避到期再跑一次仍然 panic，最终次数耗尽被丢弃。
	if n := store.Len(); n != 1 {
		t.Fatalf("panic 后应重新排期，待执行 %d", n)
	}
	if _, err := s.Tick(context.Background(), now.Add(time.Second)); err != nil {
		t.Fatalf("第二轮 Tick 失败：%v", err)
	}
	if _, err := s.Tick(context.Background(), now.Add(3*time.Second)); err != nil {
		t.Fatalf("第三轮 Tick 失败：%v", err)
	}
	if calls != 3 {
		t.Errorf("Handler 调用次数 = %d, 期望 3", calls)
	}
	if n := store.Len(); n != 0 {
		t.Errorf("次数耗尽后应丢弃，待执行 %d", n)
	}
}

// 租约时长为 0 时回退默认值，避免任务刚认领就被判为过期。
func TestClaimLeaseFallsBackToDefault(t *testing.T) {
	now := fixedTime()
	store := NewMemoryStore()
	id, _ := store.Add(Item{DueAt: now, Payload: []byte("post_liked")})
	if ok, _ := store.Claim(id, now, 0); !ok {
		t.Fatal("认领失败")
	}
	if res, _ := store.Reclaim(now.Add(time.Second), 10, 3); len(res.Rewound) != 0 {
		t.Errorf("零租约不该被当成立即过期：%+v", res)
	}
	if res, _ := store.Reclaim(now.Add(DefaultClaimLease), 10, 3); len(res.Rewound) != 1 {
		t.Errorf("默认租约到期后应回收：%+v", res)
	}
}

// 真实 Redis 上的租约往返：回收脚本要改写任务体（累加次数、重置到期时刻），
// 用的是服务端 cjson，和 miniredis 的 Lua 实现未必一致，所以单独验一遍。
// 默认跳过，设置 XW_TEST_REDIS_ADDR 后运行。
func TestRedisLeaseAgainstRealRedis(t *testing.T) {
	addr := os.Getenv("XW_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("未设置 XW_TEST_REDIS_ADDR，跳过真实 Redis 冒烟")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("连接 Redis 失败：%v", err)
	}

	prefix := fmt.Sprintf("xw:delay:lease:%d", time.Now().UnixNano())
	store := NewRedisStoreWithPrefix(client, prefix)
	t.Cleanup(func() {
		cleanup, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCleanup()
		_ = client.Del(cleanup, store.DueKey(), store.ItemsKey(), store.InflightKey()).Err()
	})

	now := fixedTime()
	id, err := store.Add(Item{DueAt: now, Payload: []byte(`{"workflow":"post_liked"}`)})
	if err != nil {
		t.Fatalf("登记失败：%v", err)
	}
	if ok, err := store.Claim(id, now, time.Minute); err != nil || !ok {
		t.Fatalf("认领失败：ok=%v err=%v", ok, err)
	}

	// 租约为 1 分钟：30 秒时不动，1 分钟时回收并累加次数。
	if res, err := store.Reclaim(now.Add(30*time.Second), 10, 3); err != nil || len(res.Rewound) != 0 {
		t.Fatalf("租约未到期不该回收：%+v err=%v", res, err)
	}
	res, err := store.Reclaim(now.Add(time.Minute), 10, 3)
	if err != nil {
		t.Fatalf("回收失败：%v", err)
	}
	if len(res.Rewound) != 1 || res.Rewound[0].ID != id || res.Rewound[0].Attempts != 1 {
		t.Fatalf("回收结果不对：%+v", res)
	}

	// 回收后立刻到期，载荷原样，次数已累加。
	due, err := store.Due(now.Add(time.Minute), 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("回收后到期任务数 = %d, err=%v", len(due), err)
	}
	if string(due[0].Payload) != `{"workflow":"post_liked"}` || due[0].Attempts != 1 {
		t.Errorf("回收后任务体不对：%+v", due[0])
	}
	if !due[0].DueAt.Equal(now.Add(time.Minute).UTC()) {
		t.Errorf("回收后的到期时刻 = %s, 期望 %s", due[0].DueAt, now.Add(time.Minute).UTC())
	}

	if ok, err := store.Claim(id, now.Add(time.Minute), time.Minute); err != nil || !ok {
		t.Fatalf("再次认领失败：ok=%v err=%v", ok, err)
	}
	if err := store.Ack(id); err != nil {
		t.Fatalf("确认失败：%v", err)
	}
	if keys := client.Keys(context.Background(), prefix+"*").Val(); len(keys) != 0 {
		t.Errorf("确认后残留键：%v", keys)
	}
}
