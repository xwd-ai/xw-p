package schedule

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Redis 延迟任务存储测试：用进程内 Redis（miniredis）验证键布局、
// 到期取出、认领互斥、任务体往返，以及跨实例共享同一份时间轴。

// newRedisStoreFixture 启动进程内 Redis 并返回存储、原始客户端与实例。
func newRedisStoreFixture(t *testing.T) (*RedisStore, *redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisStore(client), client, mr
}

func TestRedisStoreKeyLayoutAndRoundTrip(t *testing.T) {
	store, client, mr := newRedisStoreFixture(t)
	now := fixedTime()

	id, err := store.Add(Item{
		DueAt:    now.Add(time.Minute),
		Payload:  []byte(`{"workflow":"post_liked"}`),
		Attempts: 2,
	})
	if err != nil {
		t.Fatalf("登记失败：%v", err)
	}
	if !strings.HasPrefix(id, "dl_") {
		t.Errorf("自动生成的 id 形态异常：%s", id)
	}

	// 时间轴是 ZSET，分数字段是可比较的毫秒时间戳。
	if typ := mr.Type(store.DueKey()); typ != "zset" {
		t.Errorf("%s 的类型 = %s, 期望 zset", store.DueKey(), typ)
	}
	if typ := mr.Type(store.ItemsKey()); typ != "hash" {
		t.Errorf("%s 的类型 = %s, 期望 hash", store.ItemsKey(), typ)
	}
	score, err := client.ZScore(context.Background(), store.DueKey(), id).Result()
	if err != nil {
		t.Fatalf("读取分数失败：%v", err)
	}
	if want := float64(now.Add(time.Minute).UnixMilli()); score != want {
		t.Errorf("分数 = %v, 期望 %v（unix 毫秒）", score, want)
	}

	due, err := store.Due(now.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("读取到期任务失败：%v", err)
	}
	if len(due) != 1 {
		t.Fatalf("到期任务数 = %d, 期望 1", len(due))
	}
	got := due[0]
	if got.ID != id || string(got.Payload) != `{"workflow":"post_liked"}` || got.Attempts != 2 {
		t.Errorf("任务往返后不一致：%+v", got)
	}
	if !got.DueAt.Equal(now.Add(time.Minute).UTC()) {
		t.Errorf("到期时刻 = %s, 期望 %s", got.DueAt, now.Add(time.Minute).UTC())
	}
}

func TestRedisStoreDueOrderLimitAndFuture(t *testing.T) {
	store, _, _ := newRedisStoreFixture(t)
	now := fixedTime()

	late, _ := store.Add(Item{DueAt: now.Add(3 * time.Minute), Payload: []byte("c")})
	early, _ := store.Add(Item{DueAt: now.Add(-time.Minute), Payload: []byte("a")})
	middle, _ := store.Add(Item{DueAt: now, Payload: []byte("b")})
	if _, err := store.Add(Item{DueAt: now.Add(time.Hour), Payload: []byte("d")}); err != nil {
		t.Fatalf("登记未到期任务失败：%v", err)
	}

	due, err := store.Due(now, 10)
	if err != nil {
		t.Fatalf("Due 失败：%v", err)
	}
	if len(due) != 2 {
		t.Fatalf("到期任务数 = %d, 期望 2（未到期的不该返回）", len(due))
	}
	if due[0].ID != early || due[1].ID != middle {
		t.Errorf("排序错误：%s, %s", due[0].ID, due[1].ID)
	}

	limited, _ := store.Due(now, 1)
	if len(limited) != 1 || limited[0].ID != early {
		t.Errorf("limit 未按到期时刻取前 N 条：%+v", limited)
	}

	if store.Len() != 4 {
		t.Errorf("待执行任务数 = %d, 期望 4（含未到期）", store.Len())
	}
	_ = late
}

// 认领互斥 + 彻底删除：认领只清时间轴、保留任务体；Remove 才把三个键清干净。
func TestRedisStoreClaimThenRemove(t *testing.T) {
	store, client, _ := newRedisStoreFixture(t)
	now := fixedTime()

	id, err := store.Add(Item{DueAt: now, Payload: []byte("resume")})
	if err != nil {
		t.Fatalf("登记失败：%v", err)
	}
	if ok, err := store.Claim(id, now, time.Minute); err != nil || !ok {
		t.Fatalf("首个认领应成功：ok=%v err=%v", ok, err)
	}
	if ok, err := store.Claim(id, now, time.Minute); err != nil || ok {
		t.Errorf("重复认领应失败：ok=%v err=%v", ok, err)
	}
	// 认领把 id 从时间轴挪到在途集合，任务体必须留着，否则回收时就丢了载荷。
	if store.Len() != 0 {
		t.Errorf("认领后时间轴上的任务数 = %d, 期望 0", store.Len())
	}
	if n, _ := client.HLen(context.Background(), store.ItemsKey()).Result(); n != 1 {
		t.Errorf("认领后任务体数 = %d, 期望 1", n)
	}

	if err := store.Remove(id); err != nil {
		t.Fatalf("删除在途任务失败：%v", err)
	}
	if err := store.Remove(id); !errors.Is(err, ErrNotFound) {
		t.Errorf("重复删除应返回 ErrNotFound，实际 %v", err)
	}
	if n, _ := client.HLen(context.Background(), store.ItemsKey()).Result(); n != 0 {
		t.Errorf("删除后残留任务体 %d 条", n)
	}
	if keys := client.Keys(context.Background(), store.Prefix()+"*").Val(); len(keys) != 0 {
		t.Errorf("删除后残留键：%v", keys)
	}
}

func TestRedisStoreRejectsMissingDueAt(t *testing.T) {
	store, _, _ := newRedisStoreFixture(t)
	if _, err := store.Add(Item{Payload: []byte("x")}); err == nil {
		t.Fatal("缺到期时刻应报错")
	}
}

// 两个实例（各自持有一个存储对象）连同一份 Redis，重启场景下任务仍在。
func TestRedisStoreSurvivesNewInstance(t *testing.T) {
	mr := miniredis.RunT(t)
	clientA := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = clientA.Close() })
	now := fixedTime()

	id, err := NewRedisStore(clientA).Add(Item{DueAt: now.Add(2 * time.Minute), Payload: []byte("resume")})
	if err != nil {
		t.Fatalf("登记失败：%v", err)
	}

	// 新的连接 + 新的存储对象，等价于进程重启后的下一次启动。
	clientB := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = clientB.Close() })
	storeB := NewRedisStore(clientB)

	if storeB.Len() != 1 {
		t.Fatalf("新实例看到的任务数 = %d, 期望 1", storeB.Len())
	}
	due, err := storeB.Due(now.Add(2*time.Minute), 10)
	if err != nil {
		t.Fatalf("新实例读取到期任务失败：%v", err)
	}
	if len(due) != 1 || due[0].ID != id || string(due[0].Payload) != "resume" {
		t.Errorf("新实例读到的任务不一致：%+v", due)
	}
}

// 时间轴上有 id、任务体被外部删掉时，Due 跳过该条且不影响同批其他任务。
func TestRedisStoreSkipsRecordWithoutBody(t *testing.T) {
	store, client, _ := newRedisStoreFixture(t)
	now := fixedTime()

	if err := client.ZAdd(context.Background(), store.DueKey(),
		redis.Z{Score: float64(now.UnixMilli()), Member: "dl_broken"}).Err(); err != nil {
		t.Fatalf("构造脏记录失败：%v", err)
	}
	good, _ := store.Add(Item{DueAt: now, Payload: []byte("resume")})

	due, err := store.Due(now, 10)
	if err != nil {
		t.Fatalf("Due 失败：%v", err)
	}
	if len(due) != 1 || due[0].ID != good {
		t.Fatalf("脏记录应被跳过，实际返回 %+v", due)
	}
}

// 任务体损坏（不是 JSON）时不阻塞整批：丢弃并继续返回其余任务。
func TestRedisStoreDropsCorruptBody(t *testing.T) {
	store, client, _ := newRedisStoreFixture(t)
	now := fixedTime()

	if err := client.ZAdd(context.Background(), store.DueKey(),
		redis.Z{Score: float64(now.UnixMilli()), Member: "dl_corrupt"}).Err(); err != nil {
		t.Fatalf("构造脏记录失败：%v", err)
	}
	if err := client.HSet(context.Background(), store.ItemsKey(), "dl_corrupt", "{not json").Err(); err != nil {
		t.Fatalf("写入坏任务体失败：%v", err)
	}
	good, _ := store.Add(Item{DueAt: now, Payload: []byte("resume")})

	due, err := store.Due(now, 10)
	if err != nil {
		t.Fatalf("Due 失败：%v", err)
	}
	if len(due) != 1 || due[0].ID != good {
		t.Fatalf("损坏任务体应被丢弃，实际返回 %+v", due)
	}
	if store.Len() != 1 {
		t.Errorf("损坏记录应被清理，剩余 %d", store.Len())
	}
}

func TestRedisStorePrefixFallback(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	defer client.Close()
	if got := NewRedisStoreWithPrefix(client, "").Prefix(); got != DefaultRedisPrefix {
		t.Errorf("空前缀应回退默认值，实际 %s", got)
	}
	if got := NewRedisStore(client).DueKey(); got != DefaultRedisPrefix+":due" {
		t.Errorf("默认时间轴键 = %s", got)
	}
}

// 调度器跑在 Redis 存储上：到期任务被执行、键被清空、未到期的留着。
func TestSchedulerWithRedisStore(t *testing.T) {
	store, _, _ := newRedisStoreFixture(t)
	now := fixedTime()

	var handled atomic.Int32
	s := New(store, func(ctx context.Context, item Item) error {
		handled.Add(1)
		if string(item.Payload) != "resume" {
			t.Errorf("载荷 = %q", item.Payload)
		}
		return nil
	}, nil, WithNow(func() time.Time { return now }))

	if _, err := s.EnqueueDelay(now, []byte("resume")); err != nil {
		t.Fatalf("登记失败：%v", err)
	}
	if _, err := s.EnqueueDelay(now.Add(time.Hour), []byte("resume")); err != nil {
		t.Fatalf("登记失败：%v", err)
	}

	if n, err := s.Tick(context.Background(), now); err != nil || n != 1 {
		t.Fatalf("到期任务应执行一条：n=%d err=%v", n, err)
	}
	if handled.Load() != 1 || s.Len() != 1 {
		t.Errorf("执行 %d 条，待执行 %d，期望 1/1", handled.Load(), s.Len())
	}

	// 未到期的那条在到期后仍能被下一轮取到，说明未到期任务没被误清。
	if n, _ := s.Tick(context.Background(), now.Add(time.Hour)); n != 1 {
		t.Errorf("未到期任务到期后应执行：n=%d", n)
	}
	if s.Len() != 0 {
		t.Errorf("全部执行后待执行数 = %d, 期望 0", s.Len())
	}
}

// 两个调度器（模拟两个引擎实例）共享 Redis：每条任务只被执行一次。
func TestRedisStoreConcurrentInstancesClaimOnce(t *testing.T) {
	storeA, client, _ := newRedisStoreFixture(t)
	storeB := NewRedisStore(client)
	now := fixedTime()

	const total = 40
	ids := make([]string, 0, total)
	for i := 0; i < total; i++ {
		id, err := storeA.Add(Item{DueAt: now, Payload: []byte(fmt.Sprintf("resume-%d", i))})
		if err != nil {
			t.Fatalf("登记失败：%v", err)
		}
		ids = append(ids, id)
	}

	var mu sync.Mutex
	seen := make(map[string]int)
	handler := func(ctx context.Context, item Item) error {
		mu.Lock()
		seen[item.ID]++
		mu.Unlock()
		return nil
	}

	schedulers := []*Scheduler{
		New(storeA, handler, nil, WithNow(func() time.Time { return now }), WithBatch(10)),
		New(storeB, handler, nil, WithNow(func() time.Time { return now }), WithBatch(10)),
	}

	var wg sync.WaitGroup
	for _, s := range schedulers {
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

	if len(seen) != total {
		t.Errorf("被执行的任务数 = %d, 期望 %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("任务 %s 被执行 %d 次，期望 1", id, n)
		}
	}
	if storeA.Len() != 0 {
		t.Errorf("全部认领后待执行数 = %d, 期望 0", storeA.Len())
	}

	got := make([]string, 0, len(seen))
	for id := range seen {
		got = append(got, id)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(sortedCopy(ids), ",") {
		t.Error("执行的任务集合与登记的不一致")
	}
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// 真实 Redis 冒烟用例：默认跳过，设置 XW_TEST_REDIS_ADDR（如 127.0.0.1:6379）
// 后才运行。作用是确认脚本在真实服务端（而非 miniredis 的 Lua 实现）上也能跑通，
// 且键布局与预期一致。用独立前缀并在结束时只清理自己的键，不碰同在库里的数据。
func TestRedisStoreAgainstRealRedis(t *testing.T) {
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

	prefix := fmt.Sprintf("xw:delay:test:%d", time.Now().UnixNano())
	store := NewRedisStoreWithPrefix(client, prefix)
	t.Cleanup(func() {
		cleanup, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCleanup()
		_ = client.Del(cleanup, store.DueKey(), store.ItemsKey()).Err()
	})

	now := fixedTime()
	id, err := store.Add(Item{DueAt: now.Add(30 * time.Second), Payload: []byte("resume"), Attempts: 1})
	if err != nil {
		t.Fatalf("登记失败：%v", err)
	}
	if score, err := client.ZScore(ctx, store.DueKey(), id).Result(); err != nil {
		t.Fatalf("读取分数失败：%v", err)
	} else if want := float64(now.Add(30 * time.Second).UnixMilli()); score != want {
		t.Errorf("分数 = %v, 期望 %v", score, want)
	}

	if n := store.Len(); n != 1 {
		t.Errorf("待执行数 = %d, 期望 1", n)
	}
	due, err := store.Due(now, 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("未到期不该取到任务：n=%d err=%v", len(due), err)
	}

	due, err = store.Due(now.Add(30*time.Second), 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("到期应取到 1 条：n=%d err=%v", len(due), err)
	}
	if due[0].ID != id || string(due[0].Payload) != "resume" || due[0].Attempts != 1 {
		t.Errorf("任务往返不一致：%+v", due[0])
	}

	if err := store.Remove(id); err != nil {
		t.Fatalf("认领失败：%v", err)
	}
	if err := store.Remove(id); !errors.Is(err, ErrNotFound) {
		t.Errorf("重复认领应返回 ErrNotFound，实际 %v", err)
	}
	if n := len(client.Keys(ctx, prefix+"*").Val()); n != 0 {
		t.Errorf("清理后仍残留 %d 个键", n)
	}
}
