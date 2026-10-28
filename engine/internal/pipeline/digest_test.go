package pipeline

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// 摘要聚合（digest）测试：验证「同一篇帖子的互动合并成一条卡片、窗口内原地
// 累加计数、窗口过期重新开卡、并发不丢计数」这些核心语义。

// likedDigestWorkflow 构造一个只含 digest 步骤的点赞聚合工作流。
func likedDigestWorkflow(cfg map[string]any) domain.Workflow {
	if cfg == nil {
		cfg = map[string]any{
			"key":      "post_id",
			"window":   "10m",
			"title":    "你的帖子有新互动",
			"template": "{{ digest.actor }} 等 {{ digest.count }} 人赞了你的帖子 {{ digest.key }}",
		}
	}
	return domain.Workflow{
		ID:      "wf_post_liked",
		Name:    "post_liked",
		Version: 1,
		Active:  true,
		Steps: []domain.Step{
			{ID: "dg1", Type: domain.ChannelDigest, Name: "点赞聚合", Config: cfg},
		},
	}
}

// digestMessage 构造一条点赞触发消息（与 delay_test 的 likedMessage 区分开）。
func digestMessage(triggerID, postID, actor string) queue.QueuedMessage {
	return queue.QueuedMessage{
		TriggerID:    triggerID,
		Name:         "post_liked",
		SubscriberID: "creator_1",
		Channels:     []string{"in_app"},
		Payload: map[string]any{
			"post_id": postID,
			"actor":   actor,
		},
	}
}

// newDigestPipeline 构造一条带内存收件箱的管线，返回管线与收件箱便于断言。
func newDigestPipeline(t *testing.T) (*Pipeline, *MemoryInboxStore) {
	t.Helper()
	store := NewMemoryInboxStore()
	registry := NewProviderRegistry()
	registry.Register(NewInAppProvider(discardLogger(), store))
	return NewWithRegistry(discardLogger(), registry), store
}

func TestParseDigestSpecDefaults(t *testing.T) {
	spec, err := ParseDigestSpec(domain.Step{ID: "dg1", Type: domain.ChannelDigest})
	if err != nil {
		t.Fatalf("默认配置解析失败: %v", err)
	}
	if spec.KeyField != "payload.post_id" {
		t.Errorf("默认聚合键 = %q, 期望 payload.post_id", spec.KeyField)
	}
	if spec.ActorField != "payload.actor" {
		t.Errorf("默认互动者字段 = %q, 期望 payload.actor", spec.ActorField)
	}
	if spec.Window != 10*time.Minute {
		t.Errorf("默认窗口 = %s, 期望 10m", spec.Window)
	}
	if spec.Channel != domain.ChannelInApp {
		t.Errorf("默认渠道 = %q, 期望 in_app", spec.Channel)
	}
	if !strings.Contains(spec.Template, "digest.count") {
		t.Errorf("默认正文模板应含 digest.count，实际 %q", spec.Template)
	}
}

func TestParseDigestSpecCustom(t *testing.T) {
	spec, err := ParseDigestSpec(domain.Step{
		ID:   "dg1",
		Type: domain.ChannelDigest,
		Config: map[string]any{
			"key":      "post_id",
			"window":   "5m",
			"channel":  "email",
			"title":    "帖子互动汇总",
			"template": "{{ digest.count }} 条新互动",
			"actor":    "", // 显式关闭互动者记录
		},
	})
	if err != nil {
		t.Fatalf("自定义配置解析失败: %v", err)
	}
	if spec.KeyField != "payload.post_id" {
		t.Errorf("裸字段名应补 payload. 前缀，实际 %q", spec.KeyField)
	}
	if spec.Window != 5*time.Minute {
		t.Errorf("窗口 = %s, 期望 5m", spec.Window)
	}
	if spec.Channel != domain.ChannelEmail {
		t.Errorf("渠道 = %q, 期望 email", spec.Channel)
	}
	if spec.ActorField != "" {
		t.Errorf("actor 显式置空应关闭互动者记录，实际 %q", spec.ActorField)
	}
}

func TestParseDigestSpecWindowSeconds(t *testing.T) {
	spec, err := ParseDigestSpec(domain.Step{
		ID:     "dg1",
		Type:   domain.ChannelDigest,
		Config: map[string]any{"window": 600},
	})
	if err != nil {
		t.Fatalf("秒数窗口解析失败: %v", err)
	}
	if spec.Window != 10*time.Minute {
		t.Errorf("600 秒窗口 = %s, 期望 10m", spec.Window)
	}
}

func TestParseDigestSpecRejectsInvalid(t *testing.T) {
	cases := []struct {
		name   string
		config map[string]any
	}{
		{"窗口为零", map[string]any{"window": 0}},
		{"窗口为负", map[string]any{"window": "-1m"}},
		{"窗口超上限", map[string]any{"window": "48h"}},
		{"窗口无法解析", map[string]any{"window": "十 分钟"}},
		{"键为空", map[string]any{"key": "   "}},
		{"键类型错误", map[string]any{"key": 42}},
		{"渠道非投递", map[string]any{"channel": "digest"}},
		{"渠道未知", map[string]any{"channel": "telegram"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := ParseDigestSpec(domain.Step{
				ID: "dg1", Type: domain.ChannelDigest, Config: c.config,
			}); err == nil {
				t.Errorf("非法配置应报错：%v", c.config)
			}
		})
	}
}

func TestDigestMergesSamePostIntoOneCard(t *testing.T) {
	p, inbox := newDigestPipeline(t)
	wf := likedDigestWorkflow(nil)

	var last StepResult
	for i, actor := range []string{"小林", "阿May", "老周"} {
		ctx := NewContext(digestMessage(fmt.Sprintf("t%d", i+1), "P1001", actor))
		results := p.Run(ctx, wf)
		if len(results) != 1 {
			t.Fatalf("结果数 = %d, 期望 1", len(results))
		}
		last = results[0]
		if last.Error != "" {
			t.Fatalf("第 %d 次聚合出错: %s", i+1, last.Error)
		}
		if last.AggregateCount != i+1 {
			t.Errorf("第 %d 次聚合计数 = %d, 期望 %d", i+1, last.AggregateCount, i+1)
		}
		// 首次开卡，之后都是命中同一张卡。
		if wantOpened := i == 0; last.AggregateOpened != wantOpened {
			t.Errorf("第 %d 次 opened = %v, 期望 %v", i+1, last.AggregateOpened, wantOpened)
		}
	}

	if last.AggregateKey != "P1001" {
		t.Errorf("聚合键 = %q, 期望 P1001", last.AggregateKey)
	}
	if !strings.Contains(last.Rendered, "3 人赞了你的帖子 P1001") {
		t.Errorf("卡片正文应体现累计 3 次，实际 %q", last.Rendered)
	}

	entries, err := inbox.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("同一篇帖子应只有一条卡片，实际 %d 条: %+v", len(entries), entries)
	}
	entry := entries[0]
	if !strings.Contains(entry.Body, "3 人赞了你的帖子 P1001") {
		t.Errorf("收件箱卡片正文未原地更新，实际 %q", entry.Body)
	}
	if entry.Title != "你的帖子有新互动" {
		t.Errorf("卡片标题 = %q", entry.Title)
	}
	if !strings.HasPrefix(entry.ID, "digest-") {
		t.Errorf("聚合卡片条目 ID 应以 digest- 开头，实际 %q", entry.ID)
	}
}

func TestDigestKeepsEntryIDStableInWindow(t *testing.T) {
	p, inbox := newDigestPipeline(t)
	wf := likedDigestWorkflow(nil)

	ctx1 := NewContext(digestMessage("t1", "P2001", "小林"))
	if res := p.Run(ctx1, wf)[0]; res.Error != "" {
		t.Fatalf("首卡失败: %s", res.Error)
	}
	entries, _ := inbox.List("creator_1")
	firstID := entries[0].ID

	for i := 2; i <= 4; i++ {
		ctx := NewContext(digestMessage(fmt.Sprintf("t%d", i), "P2001", "阿May"))
		if res := p.Run(ctx, wf)[0]; res.Error != "" {
			t.Fatalf("第 %d 次聚合失败: %s", i, res.Error)
		}
	}
	entries, _ = inbox.List("creator_1")
	if len(entries) != 1 {
		t.Fatalf("窗口内应始终只有一条卡片，实际 %d 条", len(entries))
	}
	if entries[0].ID != firstID {
		t.Errorf("卡片条目 ID 应保持不变（原地更新），%q → %q", firstID, entries[0].ID)
	}
}

func TestDigestSeparatesDifferentPosts(t *testing.T) {
	p, inbox := newDigestPipeline(t)
	wf := likedDigestWorkflow(nil)

	for i, post := range []string{"P3001", "P3001", "P3002"} {
		ctx := NewContext(digestMessage(fmt.Sprintf("t%d", i), post, "小林"))
		if res := p.Run(ctx, wf)[0]; res.Error != "" {
			t.Fatalf("聚合失败: %s", res.Error)
		}
	}

	entries, _ := inbox.List("creator_1")
	if len(entries) != 2 {
		t.Fatalf("两篇帖子应对应两张卡片，实际 %d 条", len(entries))
	}
	counts := map[string]string{}
	for _, e := range entries {
		counts[e.Body] = e.ID
	}
	for body := range counts {
		if strings.Contains(body, "P3001") && !strings.Contains(body, "2 人") {
			t.Errorf("P3001 卡片计数应为 2，实际正文 %q", body)
		}
		if strings.Contains(body, "P3002") && !strings.Contains(body, "1 人") {
			t.Errorf("P3002 卡片计数应为 1，实际正文 %q", body)
		}
	}
}

func TestDigestOpensNewCardAfterWindowExpiry(t *testing.T) {
	p, inbox := newDigestPipeline(t)
	wf := likedDigestWorkflow(map[string]any{
		"key":      "post_id",
		"window":   "10m",
		"template": "{{ digest.count }} 人赞了你的帖子 {{ digest.key }}",
	})

	clock := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	p.WithClock(func() time.Time { return clock })

	if res := p.Run(NewContext(digestMessage("t1", "P4001", "小林")), wf)[0]; res.Error != "" {
		t.Fatalf("首卡失败: %s", res.Error)
	}
	clock = clock.Add(9 * time.Minute)
	res := p.Run(NewContext(digestMessage("t2", "P4001", "阿May")), wf)[0]
	if res.AggregateOpened {
		t.Error("窗口内应命中既有卡片")
	}
	if res.AggregateCount != 2 {
		t.Errorf("窗口内计数 = %d, 期望 2", res.AggregateCount)
	}

	// 越过窗口边界：重新开卡。
	clock = clock.Add(2 * time.Minute)
	res = p.Run(NewContext(digestMessage("t3", "P4001", "老周")), wf)[0]
	if !res.AggregateOpened {
		t.Error("窗口过期后应重新开卡")
	}
	if res.AggregateCount != 1 {
		t.Errorf("新卡计数 = %d, 期望 1", res.AggregateCount)
	}

	entries, _ := inbox.List("creator_1")
	if len(entries) != 2 {
		t.Fatalf("窗口过期后应有 2 条卡片，实际 %d 条", len(entries))
	}
}

func TestDigestSkipsWhenFiltered(t *testing.T) {
	p, inbox := newDigestPipeline(t)
	wf := likedDigestWorkflow(map[string]any{
		"key":    "post_id",
		"window": "10m",
		"filters": []any{
			map[string]any{"field": "payload.post_id", "operator": "eq", "value": "P5001"},
		},
	})

	ctx := NewContext(digestMessage("t1", "P9999", "小林"))
	res := p.Run(ctx, wf)[0]
	if !res.Skipped {
		t.Errorf("过滤条件不满足应跳过 digest 步骤，实际 %+v", res)
	}
	if res.AggregateCount != 0 {
		t.Errorf("被跳过的步骤不应计数，实际 %d", res.AggregateCount)
	}
	if entries, _ := inbox.List("creator_1"); len(entries) != 0 {
		t.Errorf("被跳过的步骤不应产生卡片，实际 %d 条", len(entries))
	}
}

func TestDigestFailsWithoutKey(t *testing.T) {
	p, inbox := newDigestPipeline(t)
	wf := likedDigestWorkflow(nil)

	msg := digestMessage("t1", "", "小林")
	delete(msg.Payload, "post_id")
	res := p.Run(NewContext(msg), wf)[0]
	if res.Error == "" {
		t.Fatal("缺少聚合键应使 digest 步骤失败")
	}
	if !strings.Contains(res.Error, "payload.post_id") {
		t.Errorf("错误信息应指明缺失字段，实际 %q", res.Error)
	}
	if entries, _ := inbox.List("creator_1"); len(entries) != 0 {
		t.Errorf("失败时不应产生卡片，实际 %d 条", len(entries))
	}
}

// countingProvider 是测试用的计数 provider，记录投递次数与最后一次投递内容。
type countingProvider struct {
	ch    domain.Channel
	mu    sync.Mutex
	sends int
	last  Delivery
}

func (c *countingProvider) Channel() domain.Channel { return c.ch }

func (c *countingProvider) Send(d Delivery) SendResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sends++
	c.last = d
	return SendResult{Sent: true, Reason: "测试渠道已接收"}
}

func (c *countingProvider) Sends() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sends
}

func TestDigestSendsExternalChannelOncePerWindow(t *testing.T) {
	email := &countingProvider{ch: domain.ChannelEmail}
	registry := NewProviderRegistry()
	registry.Register(email)
	p := NewWithRegistry(discardLogger(), registry)

	wf := likedDigestWorkflow(map[string]any{
		"key":      "post_id",
		"window":   "10m",
		"channel":  "email",
		"title":    "帖子互动汇总",
		"template": "你的帖子 {{ digest.key }} 累计 {{ digest.count }} 条互动",
	})

	msg := digestMessage("t1", "P6001", "小林")
	msg.Email = "creator@example.com"

	first := p.Run(NewContext(msg), wf)[0]
	if first.Error != "" {
		t.Fatalf("开卡投递失败: %s", first.Error)
	}
	if !first.Sent {
		t.Error("开卡时应真实投递一次")
	}

	for i := 2; i <= 3; i++ {
		m := digestMessage(fmt.Sprintf("t%d", i), "P6001", "阿May")
		m.Email = "creator@example.com"
		res := p.Run(NewContext(m), wf)[0]
		if res.Error != "" {
			t.Fatalf("第 %d 次聚合失败: %s", i, res.Error)
		}
		if res.Sent {
			t.Errorf("窗口内命中不应重复投递（第 %d 次）", i)
		}
		if res.AggregateCount != i {
			t.Errorf("第 %d 次计数 = %d, 期望 %d", i, res.AggregateCount, i)
		}
	}

	if got := email.Sends(); got != 1 {
		t.Errorf("窗口内应只投递 1 次，实际 %d 次", got)
	}
	if email.last.Subject != "帖子互动汇总" {
		t.Errorf("投递标题 = %q", email.last.Subject)
	}
	if !strings.Contains(email.last.Body, "P6001") {
		t.Errorf("投递正文应含帖子 ID，实际 %q", email.last.Body)
	}
}

func TestMemoryDigestStoreConcurrentAccumulate(t *testing.T) {
	store := NewMemoryDigestStore()
	scope := DigestScope{Workflow: "post_liked", Subscriber: "creator_1", StepID: "dg1", Key: "P7001"}
	now := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)

	const workers = 200
	var wg sync.WaitGroup
	var openedCount int32
	var mu sync.Mutex
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
				mu.Lock()
				openedCount++
				mu.Unlock()
			}
			if card.Count <= 0 {
				t.Errorf("计数应大于 0，实际 %d", card.Count)
			}
		}(i)
	}
	wg.Wait()

	card, ok := store.Snapshot(scope, now)
	if !ok {
		t.Fatal("并发累加后卡片应存在")
	}
	if card.Count != workers {
		t.Errorf("并发累加计数 = %d, 期望 %d（不能丢计数）", card.Count, workers)
	}
	if openedCount != 1 {
		t.Errorf("并发下应只开卡 1 次，实际 %d 次（不能重复开卡）", openedCount)
	}
	if len(card.Actors) != maxDigestActors {
		t.Errorf("互动者列表应截断到 %d，实际 %d", maxDigestActors, len(card.Actors))
	}
}

func TestDigestStoreSnapshotAfterExpiry(t *testing.T) {
	store := NewMemoryDigestStore()
	scope := DigestScope{Workflow: "post_liked", Subscriber: "creator_1", StepID: "dg1", Key: "P8001"}
	now := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	if _, _, err := store.Accumulate(scope, "小林", domain.ChannelInApp, now, time.Minute); err != nil {
		t.Fatalf("累加失败: %v", err)
	}
	if _, ok := store.Snapshot(scope, now.Add(30*time.Second)); !ok {
		t.Error("窗口内应能读到卡片")
	}
	if _, ok := store.Snapshot(scope, now.Add(2*time.Minute)); ok {
		t.Error("窗口过期后快照应返回 false")
	}
	if _, _, err := store.Accumulate(scope, "", domain.ChannelInApp, now, 0); err == nil {
		t.Error("窗口为 0 应报错")
	}
}

func TestJSONLInboxStoreUpdateInPlace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "inbox.jsonl")
	store, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开 JSONL 收件箱失败: %v", err)
	}
	entry := InboxEntry{
		ID:           "digest-abc",
		TriggerID:    "t1",
		SubscriberID: "creator_1",
		Workflow:     "post_liked",
		StepID:       "dg1",
		Title:        "你的帖子有新互动",
		Body:         "1 人赞了你的帖子 P9001",
		CreatedAt:    time.Now().UTC(),
	}
	if inserted, err := store.Save(entry); err != nil || !inserted {
		t.Fatalf("首次写卡失败: inserted=%v err=%v", inserted, err)
	}

	updated := entry
	updated.Body = "3 人赞了你的帖子 P9001"
	if ok, err := store.Update(updated); err != nil || !ok {
		t.Fatalf("原地更新失败: ok=%v err=%v", ok, err)
	}

	// 重新打开文件：应只有一条，且正文已更新（说明重写生效、没有追加重复行）。
	reopened, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("重新打开失败: %v", err)
	}
	list, err := reopened.List("creator_1")
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("应只有 1 条卡片，实际 %d 条", len(list))
	}
	if list[0].Body != "3 人赞了你的帖子 P9001" {
		t.Errorf("卡片正文未原地更新，实际 %q", list[0].Body)
	}

	// 更新不存在的条目应返回 false，且不影响其它行。
	if ok, err := reopened.Update(InboxEntry{ID: "digest-missing"}); err != nil || ok {
		t.Errorf("更新不存在的条目应返回 false，实际 ok=%v err=%v", ok, err)
	}

	// 文件内容不应包含临时文件残留。
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("读取目录失败: %v", err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp") {
			t.Errorf("残留临时文件: %s", e.Name())
		}
	}
}

func TestMemoryInboxStoreUpdateMissing(t *testing.T) {
	store := NewMemoryInboxStore()
	if ok, err := store.Update(InboxEntry{ID: "digest-none"}); err != nil || ok {
		t.Errorf("更新不存在的条目应返回 false，实际 ok=%v err=%v", ok, err)
	}
	if _, err := store.Update(InboxEntry{}); err == nil {
		t.Error("缺少 ID 应报错")
	}
}

func TestDigestStepDoesNotShortCircuitFollowingSteps(t *testing.T) {
	inbox := NewMemoryInboxStore()
	registry := NewProviderRegistry()
	registry.Register(NewInAppProvider(discardLogger(), inbox))
	p := NewWithRegistry(discardLogger(), registry)

	wf := domain.Workflow{
		ID:   "wf_post_liked",
		Name: "post_liked",
		Steps: []domain.Step{
			{ID: "dg1", Type: domain.ChannelDigest, Config: map[string]any{"key": "post_id", "window": "10m"}},
			{ID: "s1", Type: domain.ChannelInApp, Config: map[string]any{"title": "互动提醒", "template": "帖子 {{ payload.post_id }} 有新互动"}},
		},
	}

	results := p.Run(NewContext(digestMessage("t1", "P9101", "小林")), wf)
	if len(results) != 2 {
		t.Fatalf("结果数 = %d, 期望 2", len(results))
	}
	if results[0].Error != "" {
		t.Errorf("digest 步骤出错: %s", results[0].Error)
	}
	if results[1].ShortCircuited {
		t.Error("digest 成功时后续步骤不应被短路")
	}
	if !results[1].Sent {
		t.Error("后续 in_app 步骤应投递成功")
	}
}

func TestDigestWritesCardToJSONLInbox(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.jsonl")
	store, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开 JSONL 收件箱失败: %v", err)
	}
	registry := NewProviderRegistry()
	registry.Register(NewInAppProvider(discardLogger(), store))
	p := NewWithRegistry(discardLogger(), registry)

	wf := likedDigestWorkflow(map[string]any{
		"key":      "post_id",
		"window":   "10m",
		"template": "{{ digest.count }} 人赞了你的帖子 {{ digest.key }}",
	})
	for i, actor := range []string{"小林", "阿May", "老周"} {
		msg := digestMessage(fmt.Sprintf("t%d", i+1), "P9201", actor)
		if res := p.Run(NewContext(msg), wf)[0]; res.Error != "" {
			t.Fatalf("第 %d 次聚合失败: %s", i+1, res.Error)
		}
	}

	// 落盘结果应只有一行（卡片被原地改写而非追加三行）。
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取收件箱文件失败: %v", err)
	}
	var lines []string
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("收件箱文件应只有 1 行卡片，实际 %d 行", len(lines))
	}
	if !strings.Contains(lines[0], "3 人赞了你的帖子 P9201") {
		t.Errorf("落盘卡片正文未原地累加，实际 %s", lines[0])
	}

	// 重启后回放：文件索引仍认得这张卡（再次写同 ID 会被幂等去重）。
	reopened, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("重新打开失败: %v", err)
	}
	registry2 := NewProviderRegistry()
	registry2.Register(NewInAppProvider(discardLogger(), reopened))
	p2 := NewWithRegistry(discardLogger(), registry2)

	// 已知限制：聚合卡片状态目前在进程内存中，新进程会按新卡落库（条目 +1）。
	// 多实例共享窗口状态由 Redis 实现接管（后续阶段），这里把这个行为固定下来，
	// 避免以后悄悄改变语义。
	p2.Run(NewContext(digestMessage("t4", "P9201", "小林")), wf)
	list, err := reopened.List("creator_1")
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if len(list) != 2 {
		t.Errorf("新进程应另开一张卡片（窗口状态不跨进程），实际 %d 条", len(list))
	}
}
