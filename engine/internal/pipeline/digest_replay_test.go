package pipeline

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// 聚合事件重放测试：队列重投、崩溃续跑都会让同一个事件再走一遍 digest，
// 这里固定「重放不重复计数、不重复投递，窗口过期后重放按新事件计」的语义，
// 以及「卡片被新互动更新后重新回到未读」的收件箱状态。
//
// 事件 ID 在引擎里就是 trigger_id：一次触发对应一次互动，重投时 ID 不变。

func TestDigestReplayDoesNotDoubleCount(t *testing.T) {
	p, inbox := newDigestPipeline(t)
	wf := likedDigestWorkflow(map[string]any{
		"key":      "post_id",
		"window":   "10m",
		"template": "{{ digest.count }} 人赞了你的帖子 {{ digest.key }}",
	})

	first := p.Run(NewContext(digestMessage("t-replay-1", "P9301", "小林")), wf)[0]
	if first.Error != "" {
		t.Fatalf("首次执行失败: %s", first.Error)
	}
	if first.AggregateCount != 1 || !first.AggregateOpened || first.AggregateReplayed {
		t.Fatalf("首次执行应开卡计 1 条，实际 %+v", first)
	}

	// 同一个 trigger_id 重投：计数保持 1，标记为重放，不重复投递。
	replay := p.Run(NewContext(digestMessage("t-replay-1", "P9301", "小林")), wf)[0]
	if replay.Error != "" {
		t.Fatalf("重放执行失败: %s", replay.Error)
	}
	if !replay.AggregateReplayed {
		t.Error("同一事件 ID 重投应标记为重放")
	}
	if replay.AggregateCount != 1 {
		t.Errorf("重放后计数 = %d, 期望 1（重放不重复计数）", replay.AggregateCount)
	}
	if replay.Sent {
		t.Error("重放不应重复投递")
	}

	// 另一条互动正常累加，说明重放没有把去重集合锁死。
	next := p.Run(NewContext(digestMessage("t-replay-2", "P9301", "阿May")), wf)[0]
	if next.Error != "" {
		t.Fatalf("后续互动失败: %s", next.Error)
	}
	if next.AggregateCount != 2 || next.AggregateReplayed {
		t.Errorf("新事件应正常累加到 2，实际 %+v", next)
	}

	entries, err := inbox.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("窗口内应只有一条卡片，实际 %d 条", len(entries))
	}
	if !strings.Contains(entries[0].Body, "2 人赞了你的帖子 P9301") {
		t.Errorf("卡片正文应按最终计数原地更新，实际 %q", entries[0].Body)
	}
}

func TestDigestReplayScopedToWindow(t *testing.T) {
	p, inbox := newDigestPipeline(t)
	wf := likedDigestWorkflow(map[string]any{
		"key":      "post_id",
		"window":   "10m",
		"template": "{{ digest.count }} 条互动",
	})

	clock := time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC)
	p.WithClock(func() time.Time { return clock })

	msg := digestMessage("t-window-replay", "P9401", "小林")
	if res := p.Run(NewContext(msg), wf)[0]; res.Error != "" {
		t.Fatalf("首次执行失败: %s", res.Error)
	}

	// 窗口内重放：计过数。
	clock = clock.Add(3 * time.Minute)
	if res := p.Run(NewContext(msg), wf)[0]; !res.AggregateReplayed {
		t.Error("窗口内重放应被识别")
	}

	// 越过窗口后再重放：旧卡已结算，这次是一次新的互动。
	clock = clock.Add(8 * time.Minute)
	res := p.Run(NewContext(msg), wf)[0]
	if res.Error != "" {
		t.Fatalf("跨窗口重放失败: %s", res.Error)
	}
	if res.AggregateReplayed {
		t.Error("跨窗口后同一事件 ID 应重新计数，而不是当作重放")
	}
	if !res.AggregateOpened || res.AggregateCount != 1 {
		t.Errorf("跨窗口后应开新卡计 1 条，实际 %+v", res)
	}

	entries, _ := inbox.List("creator_1")
	if len(entries) != 2 {
		t.Errorf("两个窗口应各有一张卡，实际 %d 条", len(entries))
	}
}

func TestDigestReplaySkipsExternalDelivery(t *testing.T) {
	email := &countingProvider{ch: domain.ChannelEmail}
	registry := NewProviderRegistry()
	registry.Register(email)
	p := NewWithRegistry(discardLogger(), registry)

	wf := likedDigestWorkflow(map[string]any{
		"key":      "post_id",
		"window":   "10m",
		"channel":  "email",
		"template": "你的帖子 {{ digest.key }} 累计 {{ digest.count }} 条互动",
	})

	msg := digestMessage("t-mail-replay", "P9501", "小林")
	msg.Email = "creator@example.com"

	first := p.Run(NewContext(msg), wf)[0]
	if first.Error != "" || !first.Sent {
		t.Fatalf("开卡应投递一次，实际 %+v", first)
	}
	// 同一事件重投三次：一次都不能再发信。
	for i := 0; i < 3; i++ {
		res := p.Run(NewContext(msg), wf)[0]
		if res.Error != "" {
			t.Fatalf("第 %d 次重放失败: %s", i+1, res.Error)
		}
		if res.Sent {
			t.Errorf("第 %d 次重放不应重复投递", i+1)
		}
	}
	if got := email.Sends(); got != 1 {
		t.Errorf("重放不应增加外部投递次数，实际 %d 次", got)
	}
}

func TestMemoryDigestStoreReplayByEventID(t *testing.T) {
	store := NewMemoryDigestStore()
	scope := DigestScope{Workflow: "post_liked", Subscriber: "creator_1", StepID: "dg1", Key: "P9601"}
	now := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)

	card, outcome, err := store.Apply(DigestEvent{Scope: scope, EventID: "e1", Actor: "小林", Channel: domain.ChannelInApp, Now: now, Window: 10 * time.Minute})
	if err != nil || !outcome.Opened || outcome.Replayed || card.Count != 1 {
		t.Fatalf("首次累计异常: card=%+v outcome=%+v err=%v", card, outcome, err)
	}

	// 同一事件再来两次：计数不变，互动者不重复。
	for i := 0; i < 2; i++ {
		card, outcome, err = store.Apply(DigestEvent{Scope: scope, EventID: "e1", Actor: "小林", Channel: domain.ChannelInApp, Now: now.Add(time.Second), Window: 10 * time.Minute})
		if err != nil {
			t.Fatalf("重放失败: %v", err)
		}
		if !outcome.Replayed || card.Count != 1 {
			t.Errorf("第 %d 次重放应保持计数 1，实际 %+v", i+1, outcome)
		}
	}
	if len(card.Actors) != 1 {
		t.Errorf("重放不应重复记录互动者，实际 %v", card.Actors)
	}

	// 不带事件 ID 的累计（Accumulate）不做去重：每次都是新互动。
	if _, _, err := store.Accumulate(scope, "阿May", domain.ChannelInApp, now.Add(2*time.Second), 10*time.Minute); err != nil {
		t.Fatalf("累加失败: %v", err)
	}
	if snap, ok := store.Snapshot(scope, now.Add(2*time.Second)); !ok || snap.Count != 2 {
		t.Errorf("无事件 ID 的累计应各计一次，实际 %+v ok=%v", snap, ok)
	}

	// 窗口过期后同一事件 ID 重新计数（去重集合按窗口隔离）。
	later := now.Add(11 * time.Minute)
	card, outcome, err = store.Apply(DigestEvent{Scope: scope, EventID: "e1", Actor: "小林", Channel: domain.ChannelInApp, Now: later, Window: 10 * time.Minute})
	if err != nil || !outcome.Opened || outcome.Replayed || card.Count != 1 {
		t.Errorf("跨窗口的事件应开新卡计 1 条，实际 card=%+v outcome=%+v err=%v", card, outcome, err)
	}
}

func TestRedisDigestApplyReplayDedupe(t *testing.T) {
	store, _ := newRedisDigestFixture(t)
	scope := redisScope("P9701")
	now := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)

	event := DigestEvent{Scope: scope, EventID: "t-1", Actor: "小林", Channel: domain.ChannelInApp, Now: now, Window: 10 * time.Minute}
	card, outcome, err := store.Apply(event)
	if err != nil {
		t.Fatalf("首次累计失败: %v", err)
	}
	if !outcome.Opened || outcome.Replayed || card.Count != 1 {
		t.Fatalf("首次累计异常: card=%+v outcome=%+v", card, outcome)
	}

	for i := 0; i < 4; i++ {
		event.Now = now.Add(time.Duration(i+1) * time.Second)
		card, outcome, err = store.Apply(event)
		if err != nil {
			t.Fatalf("第 %d 次重放失败: %v", i+1, err)
		}
		if !outcome.Replayed {
			t.Errorf("第 %d 次重放应被识别", i+1)
		}
		if card.Count != 1 {
			t.Errorf("第 %d 次重放后计数 = %d, 期望 1", i+1, card.Count)
		}
	}

	// 新事件正常累加。
	card, outcome, err = store.Apply(DigestEvent{Scope: scope, EventID: "t-2", Actor: "阿May", Channel: domain.ChannelInApp, Now: now.Add(5 * time.Second), Window: 10 * time.Minute})
	if err != nil || outcome.Replayed || card.Count != 2 {
		t.Fatalf("新事件应累加到 2，实际 card=%+v outcome=%+v err=%v", card, outcome, err)
	}
	if len(card.Actors) != 2 {
		t.Errorf("互动者应为 2 个，实际 %v", card.Actors)
	}
}

func TestRedisDigestConcurrentReplayCountsOnce(t *testing.T) {
	store, mr := newRedisDigestFixture(t)
	scope := redisScope("P9801")
	now := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)

	// 同一事件被多个副本同时重投：只有一次算数，SADD 是唯一的裁决点。
	const replicas = 40
	var wg sync.WaitGroup
	var replayed int32
	var opened int32
	for i := 0; i < replicas; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			card, outcome, err := store.Apply(DigestEvent{
				Scope: scope, EventID: "dup-event", Actor: "小林",
				Channel: domain.ChannelInApp, Now: now, Window: 10 * time.Minute,
			})
			if err != nil {
				t.Errorf("并发重放失败: %v", err)
				return
			}
			if outcome.Replayed {
				atomic.AddInt32(&replayed, 1)
			}
			if outcome.Opened {
				atomic.AddInt32(&opened, 1)
			}
			if card.Count < 1 {
				t.Errorf("计数应至少为 1，实际 %d", card.Count)
			}
		}()
	}
	wg.Wait()

	if opened != 1 {
		t.Errorf("并发下应只开卡 1 次，实际 %d 次", opened)
	}
	if replayed != replicas-1 {
		t.Errorf("重放数 = %d, 期望 %d", replayed, replicas-1)
	}
	snapshot, ok := store.Snapshot(scope, now.Add(time.Second))
	if !ok {
		t.Fatal("卡片应存在")
	}
	if snapshot.Count != 1 {
		t.Errorf("并发重放后计数 = %d, 期望 1", snapshot.Count)
	}

	// 去重集合落到了独立键上，且带过期时间，不会永久占内存。
	keys := keysOfPrefix(mr, ":events:")
	if len(keys) != 1 {
		t.Fatalf("应只有 1 个事件去重键，实际 %v", keys)
	}
	if ttl := mr.TTL(keys[0]); ttl <= 0 {
		t.Errorf("事件去重键应带过期时间，实际 %s", ttl)
	}
}

func TestDigestCardUpdateKeepsUnreadState(t *testing.T) {
	p, inbox := newDigestPipeline(t)
	wf := likedDigestWorkflow(map[string]any{
		"key":      "post_id",
		"window":   "10m",
		"template": "{{ digest.count }} 条新互动",
	})

	clock := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	p.WithClock(func() time.Time { return clock })

	if res := p.Run(NewContext(digestMessage("t-u1", "P9901", "小林")), wf)[0]; res.Error != "" {
		t.Fatalf("首次执行失败: %s", res.Error)
	}
	entries, _ := inbox.List("creator_1")
	openedAt := entries[0].CreatedAt
	if !entries[0].UpdatedAt.Equal(openedAt) {
		t.Errorf("开卡时 CreatedAt 与 UpdatedAt 应一致，实际 %s / %s", openedAt, entries[0].UpdatedAt)
	}

	clock = clock.Add(2 * time.Minute)
	if res := p.Run(NewContext(digestMessage("t-u2", "P9901", "阿May")), wf)[0]; res.Error != "" {
		t.Fatalf("第二次执行失败: %s", res.Error)
	}
	entries, _ = inbox.List("creator_1")
	if len(entries) != 1 {
		t.Fatalf("应只有一条卡片，实际 %d 条", len(entries))
	}
	entry := entries[0]
	if !entry.CreatedAt.Equal(openedAt) {
		t.Errorf("开卡时刻不应随互动变化：%s → %s", openedAt, entry.CreatedAt)
	}
	if !entry.UpdatedAt.After(openedAt) {
		t.Errorf("新互动应推进 UpdatedAt，实际 %s", entry.UpdatedAt)
	}
	if entry.Read {
		t.Error("被新互动更新后的卡片应重新回到未读")
	}
	if entry.TriggerID != "t-u2" {
		t.Errorf("条目应指向最近一次触发，实际 %q", entry.TriggerID)
	}
}
