package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// Redis 聚合卡片存储测试：用进程内 Redis（miniredis）验证
// 「多实例共享同一张卡、并发不丢计数、窗口到期重新开卡」。

// newRedisDigestFixture 启动一个进程内 Redis 并返回存储与其连接。
func newRedisDigestFixture(t *testing.T) (*RedisDigestStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisDigestStore(client), mr
}

// redisScope 构造一张测试卡片的作用域。
func redisScope(key string) DigestScope {
	return DigestScope{Workflow: "post_liked", Subscriber: "creator_1", StepID: "dg1", Key: key}
}

// keysOfPrefix 返回 miniredis 里以指定片段开头的键，便于断言键布局。
func keysOfPrefix(mr *miniredis.Miniredis, fragment string) []string {
	var out []string
	for _, k := range mr.Keys() {
		if strings.Contains(k, fragment) {
			out = append(out, k)
		}
	}
	return out
}

func TestRedisDigestAccumulatesInWindow(t *testing.T) {
	store, _ := newRedisDigestFixture(t)
	scope := redisScope("P1001")
	now := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	window := 10 * time.Minute

	var firstID string
	for i, actor := range []string{"小林", "阿May", "老周"} {
		card, opened, err := store.Accumulate(scope, actor, domain.ChannelInApp, now.Add(time.Duration(i)*time.Minute), window)
		if err != nil {
			t.Fatalf("第 %d 次累加失败: %v", i+1, err)
		}
		if wantOpened := i == 0; opened != wantOpened {
			t.Errorf("第 %d 次 opened = %v, 期望 %v", i+1, opened, wantOpened)
		}
		if card.Count != i+1 {
			t.Errorf("第 %d 次计数 = %d, 期望 %d", i+1, card.Count, i+1)
		}
		if i == 0 {
			firstID = card.EntryID
		} else if card.EntryID != firstID {
			t.Errorf("窗口内条目 ID 应保持不变，%q → %q", firstID, card.EntryID)
		}
	}

	card, ok := store.Snapshot(scope, now.Add(3*time.Minute))
	if !ok {
		t.Fatal("窗口内应读到卡片")
	}
	if card.Count != 3 {
		t.Errorf("快照计数 = %d, 期望 3", card.Count)
	}
	if len(card.Actors) != 3 {
		t.Errorf("互动者应有 3 位，实际 %v", card.Actors)
	}
	if card.Channel != domain.ChannelInApp {
		t.Errorf("渠道 = %q, 期望 in_app", card.Channel)
	}
	if !card.ExpiresAt.Equal(now.Add(window)) {
		t.Errorf("到期时刻 = %s, 期望 %s", card.ExpiresAt, now.Add(window))
	}
}

func TestRedisDigestDeduplicatesActorsAndCapsList(t *testing.T) {
	store, _ := newRedisDigestFixture(t)
	scope := redisScope("P1002")
	now := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)

	card, _, err := store.Accumulate(scope, "小林", domain.ChannelInApp, now, 10*time.Minute)
	if err != nil {
		t.Fatalf("首次累加失败: %v", err)
	}
	if len(card.Actors) != 1 {
		t.Fatalf("互动者应有 1 位，实际 %v", card.Actors)
	}

	// 同一个人反复点赞：计数继续加，互动者列表不重复。
	for i := 0; i < 3; i++ {
		card, _, err = store.Accumulate(scope, "小林", domain.ChannelInApp, now, 10*time.Minute)
		if err != nil {
			t.Fatalf("重复累加失败: %v", err)
		}
	}
	if card.Count != 4 {
		t.Errorf("计数 = %d, 期望 4", card.Count)
	}
	if len(card.Actors) != 1 {
		t.Errorf("互动者列表应去重，实际 %v", card.Actors)
	}

	// 超出上限后截断，但计数照常累加。
	for i := 0; i < maxDigestActors+5; i++ {
		card, _, err = store.Accumulate(scope, fmt.Sprintf("user%d", i), domain.ChannelInApp, now, 10*time.Minute)
		if err != nil {
			t.Fatalf("累加失败: %v", err)
		}
	}
	if len(card.Actors) != maxDigestActors {
		t.Errorf("互动者列表应截断到 %d，实际 %d", maxDigestActors, len(card.Actors))
	}
	if card.Count != 4+maxDigestActors+5 {
		t.Errorf("计数 = %d, 期望 %d", card.Count, 4+maxDigestActors+5)
	}
}

func TestRedisDigestSharedAcrossInstances(t *testing.T) {
	mr := miniredis.RunT(t)
	clientA := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	clientB := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		_ = clientA.Close()
		_ = clientB.Close()
	})
	storeA := NewRedisDigestStore(clientA)
	storeB := NewRedisDigestStore(clientB)

	scope := redisScope("P2001")
	now := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)

	// 两个实例交替处理同一篇帖子的互动：应共用同一张卡。
	cardA, openedA, err := storeA.Accumulate(scope, "小林", domain.ChannelInApp, now, 10*time.Minute)
	if err != nil {
		t.Fatalf("实例 A 累加失败: %v", err)
	}
	if !openedA {
		t.Error("实例 A 应开卡")
	}

	cardB, openedB, err := storeB.Accumulate(scope, "阿May", domain.ChannelInApp, now.Add(time.Second), 10*time.Minute)
	if err != nil {
		t.Fatalf("实例 B 累加失败: %v", err)
	}
	if openedB {
		t.Error("实例 B 应命中实例 A 开出的卡片，不重复开卡")
	}
	if cardB.EntryID != cardA.EntryID {
		t.Errorf("两实例应共用同一张卡，%q ≠ %q", cardA.EntryID, cardB.EntryID)
	}
	if cardB.Count != 2 {
		t.Errorf("跨实例计数 = %d, 期望 2", cardB.Count)
	}

	snapshot, ok := storeA.Snapshot(scope, now.Add(2*time.Second))
	if !ok {
		t.Fatal("实例 A 应读到卡片")
	}
	if snapshot.Count != 2 {
		t.Errorf("快照计数 = %d, 期望 2", snapshot.Count)
	}
	if len(snapshot.Actors) != 2 {
		t.Errorf("互动者应累计两实例的输入，实际 %v", snapshot.Actors)
	}
}

func TestRedisDigestReopensCardAfterWindowExpiry(t *testing.T) {
	store, mr := newRedisDigestFixture(t)
	scope := redisScope("P3001")
	now := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	window := 10 * time.Minute

	first, opened, err := store.Accumulate(scope, "小林", domain.ChannelInApp, now, window)
	if err != nil || !opened {
		t.Fatalf("首卡失败: opened=%v err=%v", opened, err)
	}

	// 窗口内第二次互动：命中既有卡片。
	inner, opened, err := store.Accumulate(scope, "阿May", domain.ChannelInApp, now.Add(5*time.Minute), window)
	if err != nil {
		t.Fatalf("窗口内累加失败: %v", err)
	}
	if opened {
		t.Error("窗口内不应重新开卡")
	}
	if inner.Count != 2 || inner.EntryID != first.EntryID {
		t.Errorf("窗口内应原地累加，实际 %+v", inner)
	}

	// 让 Redis 的 TTL 真实过期（模拟时间推进到窗口之后）。
	mr.FastForward(window)

	if _, ok := store.Snapshot(scope, now.Add(window)); ok {
		t.Error("窗口到期后快照应返回 false")
	}

	second, opened, err := store.Accumulate(scope, "老周", domain.ChannelInApp, now.Add(window+time.Minute), window)
	if err != nil {
		t.Fatalf("过期后累加失败: %v", err)
	}
	if !opened {
		t.Error("窗口过期后应重新开卡")
	}
	if second.Count != 1 {
		t.Errorf("新卡计数 = %d, 期望 1", second.Count)
	}
	if second.EntryID == first.EntryID {
		t.Error("新窗口应生成新的条目 ID，旧卡片留在收件箱作为历史")
	}
	if !second.ExpiresAt.After(first.ExpiresAt) {
		t.Errorf("新卡到期时刻应晚于旧卡：%s vs %s", second.ExpiresAt, first.ExpiresAt)
	}
}

func TestRedisDigestReopensCardWhenClockPassesExpiry(t *testing.T) {
	store, _ := newRedisDigestFixture(t)
	scope := redisScope("P3002")
	now := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	window := 5 * time.Minute

	first, opened, err := store.Accumulate(scope, "小林", domain.ChannelInApp, now, window)
	if err != nil || !opened {
		t.Fatalf("首卡失败: opened=%v err=%v", opened, err)
	}

	// 只推进调用方时钟、不让 Redis 淘汰键（多实例时钟偏差的等价场景）：
	// 仍必须重新开卡，绝不能把新互动算进旧窗口。
	second, opened, err := store.Accumulate(scope, "阿May", domain.ChannelInApp, now.Add(window+time.Second), window)
	if err != nil {
		t.Fatalf("累加失败: %v", err)
	}
	if !opened {
		t.Error("时钟越过窗口后应重新开卡")
	}
	if second.EntryID == first.EntryID {
		t.Error("新窗口应生成新的条目 ID")
	}
	if second.Count != 1 {
		t.Errorf("新卡计数 = %d, 期望 1", second.Count)
	}
}

func TestRedisDigestConcurrentAccumulate(t *testing.T) {
	store, _ := newRedisDigestFixture(t)
	scope := redisScope("P4001")
	now := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)

	const workers = 80
	var openedCount int32
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			card, opened, err := store.Accumulate(scope, fmt.Sprintf("user%d", i), domain.ChannelInApp, now, 10*time.Minute)
			if err != nil {
				t.Errorf("并发累加失败: %v", err)
				return
			}
			if opened {
				atomic.AddInt32(&openedCount, 1)
			}
			if card.Count <= 0 {
				t.Errorf("计数应大于 0，实际 %d", card.Count)
			}
		}(i)
	}
	wg.Wait()

	if got := atomic.LoadInt32(&openedCount); got != 1 {
		t.Errorf("并发下应只开卡 1 次，实际 %d 次", got)
	}
	card, ok := store.Snapshot(scope, now)
	if !ok {
		t.Fatal("并发累加后卡片应存在")
	}
	if card.Count != workers {
		t.Errorf("并发累加计数 = %d, 期望 %d（不能丢计数）", card.Count, workers)
	}
}

func TestRedisDigestRejectsInvalidInput(t *testing.T) {
	store, _ := newRedisDigestFixture(t)

	if _, _, err := store.Accumulate(redisScope("P5001"), "小林", domain.ChannelInApp, time.Now(), 0); err == nil {
		t.Error("窗口为 0 应报错")
	}
	if _, _, err := store.Accumulate(redisScope("  "), "小林", domain.ChannelInApp, time.Now(), time.Minute); err == nil {
		t.Error("空聚合键应报错")
	}
	var nilStore *RedisDigestStore
	if _, _, err := nilStore.Accumulate(redisScope("P5002"), "小林", domain.ChannelInApp, time.Now(), time.Minute); err == nil {
		t.Error("未初始化的存储应报错")
	}
	if _, ok := nilStore.Snapshot(redisScope("P5002"), time.Now()); ok {
		t.Error("未初始化的存储快照应返回 false")
	}
	if err := store.Ping(context.Background()); err != nil {
		t.Errorf("Redis 连通性检查失败: %v", err)
	}
}

func TestRedisDigestRecoversFromCorruptWindow(t *testing.T) {
	store, mr := newRedisDigestFixture(t)
	scope := redisScope("P6001")
	now := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)

	if _, _, err := store.Accumulate(scope, "小林", domain.ChannelInApp, now, 10*time.Minute); err != nil {
		t.Fatalf("首卡失败: %v", err)
	}

	// 人为写坏窗口值：一张坏卡片不能永久挡住这篇帖子的聚合。
	key := store.windowKey(scope)
	mr.Set(key, "这不是合法的窗口数据")

	card, opened, err := store.Accumulate(scope, "阿May", domain.ChannelInApp, now.Add(time.Second), 10*time.Minute)
	if err != nil {
		t.Fatalf("坏数据后累加应自愈，实际报错: %v", err)
	}
	if !opened {
		t.Error("坏数据清理后应按新窗口开卡")
	}
	if card.Count != 1 {
		t.Errorf("计数 = %d, 期望 1", card.Count)
	}
}

func TestRedisDigestKeyLayout(t *testing.T) {
	store, mr := newRedisDigestFixture(t)
	scope := redisScope("P7001")
	now := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)

	card, _, err := store.Accumulate(scope, "小林", domain.ChannelInApp, now, 10*time.Minute)
	if err != nil {
		t.Fatalf("累加失败: %v", err)
	}

	// 窗口键按作用域哈希，不把帖子 ID 原样拼进键名。
	windows := keysOfPrefix(mr, ":window:")
	if len(windows) != 1 {
		t.Fatalf("应有 1 个窗口键，实际 %v", windows)
	}
	if strings.Contains(windows[0], "P7001") {
		t.Errorf("窗口键不应包含原始聚合键：%s", windows[0])
	}
	if !strings.HasPrefix(windows[0], defaultDigestPrefix+":window:") {
		t.Errorf("窗口键前缀不符：%s", windows[0])
	}

	// 卡片与互动者键带上条目 ID，TTL 覆盖窗口与保留期。
	cardKeys := keysOfPrefix(mr, ":card:")
	if len(cardKeys) != 1 || cardKeys[0] != defaultDigestPrefix+":card:"+card.EntryID {
		t.Errorf("卡片键不符：%v", cardKeys)
	}
	if ttl := mr.TTL(cardKeys[0]); ttl <= 0 {
		t.Errorf("卡片键应带过期时间，实际 %s", ttl)
	}
	if ttl := mr.TTL(windows[0]); ttl <= 0 || ttl > 10*time.Minute {
		t.Errorf("窗口键 TTL 应为窗口长度，实际 %s", ttl)
	}
}

func TestDigestPipelineWithRedisStoreWritesSingleCard(t *testing.T) {
	store, _ := newRedisDigestFixture(t)
	inbox := NewMemoryInboxStore()
	registry := NewProviderRegistry()
	registry.Register(NewInAppProvider(discardLogger(), inbox))
	p := NewWithRegistry(discardLogger(), registry).WithDigestStore(store)

	wf := likedDigestWorkflow(map[string]any{
		"key":      "post_id",
		"window":   "10m",
		"template": "{{ digest.count }} 人赞了你的帖子 {{ digest.key }}",
	})

	for i, actor := range []string{"小林", "阿May", "老周"} {
		msg := digestMessage(fmt.Sprintf("t%d", i+1), "P8001", actor)
		res := p.Run(NewContext(msg), wf)[0]
		if res.Error != "" {
			t.Fatalf("第 %d 次聚合失败: %s", i+1, res.Error)
		}
		if res.AggregateCount != i+1 {
			t.Errorf("第 %d 次计数 = %d, 期望 %d", i+1, res.AggregateCount, i+1)
		}
	}

	entries, err := inbox.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("同一篇帖子应只有一条卡片，实际 %d 条", len(entries))
	}
	if !strings.Contains(entries[0].Body, "3 人赞了你的帖子 P8001") {
		t.Errorf("卡片正文未原地累加，实际 %q", entries[0].Body)
	}
}
