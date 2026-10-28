package pipeline

import (
	"encoding/json"
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

// 聚合卡片集成测试：从「队列里的原始 JSON」一路走到「收件箱文件里的卡片」。
//
// 与 digest_test.go 的区别在于入口与出口都取真实边界：
//   - 入口是 Python 侧 LPUSH 进 Redis 的那段 JSON（经 queue.Decode 还原），
//     而不是手工构造的 QueuedMessage，字段名漂移会直接被这里抓到；
//   - 出口是磁盘上的 JSONL 收件箱文件，由另一个存储实例重新打开读取，
//     相当于 API 进程读取引擎写下的文件，而不是读回同一份内存状态。
//
// 覆盖三种真实形态：单实例连续互动（含自赞过滤与重投重放）、
// 多实例共享聚合窗口、并发投递同一篇帖子的互动洪峰。

// likedEventJSON 返回一条点赞事件的队列 JSON，字段与 Python 侧 QueuedMessage 一致。
func likedEventJSON(triggerID, postID, postTitle, actor string) string {
	return fmt.Sprintf(
		`{"trigger_id":%q,"transaction_id":"tx-%s","name":"post_liked",`+
			`"subscriber_id":"creator_1","channels":["in_app"],`+
			`"payload":{"post_id":%q,"post_title":%q,"actor":%q}}`,
		triggerID, triggerID, postID, postTitle, actor,
	)
}

// decodeEvent 把队列 JSON 还原为消息，走与引擎消费相同的解码路径。
func decodeEvent(t *testing.T, raw string) queue.QueuedMessage {
	t.Helper()
	msg, err := queue.Decode([]byte(raw))
	if err != nil {
		t.Fatalf("队列消息解码失败: %v", err)
	}
	return msg
}

// postLikedTree 构造点赞聚合工作流：过滤掉创作者自己的点赞，再按帖子聚合。
func postLikedTree() domain.Workflow {
	return domain.Workflow{
		ID:      "wf_post_liked",
		Name:    "post_liked",
		Version: 3,
		Active:  true,
		Steps: []domain.Step{
			{
				ID:   "dg1",
				Type: domain.ChannelDigest,
				Name: "按帖子聚合点赞",
				Config: map[string]any{
					"key":      "post_id",
					"window":   "10m",
					"channel":  "in_app",
					"title":    "你的帖子有新互动",
					"template": "{{ digest.actors }} 等 {{ digest.count }} 人赞了你的帖子《{{ payload.post_title }}》",
					// 自己给自己的帖子点赞不该打扰创作者。
					"filters": []any{
						map[string]any{"field": "payload.actor", "operator": "ne", "value": "creator_1"},
					},
				},
			},
		},
	}
}

// newJSONLPipeline 构造一条把聚合卡片写进指定 JSONL 文件的管线。
func newJSONLPipeline(t *testing.T, path string) (*Pipeline, *JSONLInboxStore) {
	t.Helper()
	store, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开收件箱文件失败: %v", err)
	}
	registry := NewProviderRegistry()
	registry.Register(NewInAppProvider(discardLogger(), store))
	return NewWithRegistry(discardLogger(), registry), store
}

// readInboxRaw 原样读取收件箱文件的每一行，用于断言落盘结构。
func readInboxRaw(t *testing.T, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取收件箱文件失败: %v", err)
	}
	var out []map[string]any
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("收件箱文件出现不可解析的行: %v（行内容 %q）", err, line)
		}
		out = append(out, row)
	}
	return out
}

// TestDigestIntegrationFromQueueJSONToInboxFile 覆盖单实例的完整互动链路：
// 多篇帖子的点赞进来 → 自赞被过滤 → 同一篇帖子的互动合并成一张卡片 →
// 重投不重复计数 → 落盘结果按订阅者重新读取仍然只有两条卡片。
func TestDigestIntegrationFromQueueJSONToInboxFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.jsonl")
	p, _ := newJSONLPipeline(t, path)
	wf := postLikedTree()

	const (
		postA = "P-2026-1001"
		postB = "P-2026-2002"
	)
	titleA := "深夜写代码是一种什么体验"
	titleB := "第一次跑通 CI 的记录"

	events := []struct {
		trigger, post, title, actor string
	}{
		{"t-like-1", postA, titleA, "小林"},
		{"t-like-2", postA, titleA, "阿May"},
		{"t-like-3", postA, titleA, "creator_1"}, // 自赞：应被过滤
		{"t-like-2", postA, titleA, "阿May"},      // 队列重投：应识别为重放
		{"t-like-4", postA, titleA, "老周"},
		{"t-like-5", postB, titleB, "小林"},
	}

	var lastA StepResult
	for i, e := range events {
		res := p.Run(NewContext(decodeEvent(t, likedEventJSON(e.trigger, e.post, e.title, e.actor))), wf)[0]
		if res.Error != "" {
			t.Fatalf("第 %d 条互动执行失败: %s", i+1, res.Error)
		}
		switch e.trigger {
		case "t-like-3":
			if !res.Skipped {
				t.Errorf("自赞应被过滤条件跳过，实际 %+v", res)
			}
			if res.AggregateCount != 0 {
				t.Errorf("被跳过的互动不应计数，实际 %d", res.AggregateCount)
			}
		case "t-like-2":
			if i == 3 {
				if !res.AggregateReplayed {
					t.Error("重投的互动应识别为重放")
				}
				if res.AggregateCount != 2 {
					t.Errorf("重放后计数 = %d, 期望保持 2", res.AggregateCount)
				}
			}
		default:
			if e.post == postA {
				lastA = res
			}
		}
	}

	if lastA.AggregateKey != postA {
		t.Errorf("聚合键 = %q, 期望 %q", lastA.AggregateKey, postA)
	}
	if lastA.AggregateCount != 3 {
		t.Errorf("帖子 %s 的累计互动 = %d, 期望 3（自赞与重投都不计）", postA, lastA.AggregateCount)
	}
	if !strings.Contains(lastA.Rendered, "《"+titleA+"》") {
		t.Errorf("卡片正文应带上帖子标题，实际 %q", lastA.Rendered)
	}

	// 落盘：两篇帖子各一条卡片，没有因为重投或原地更新产生多余行。
	rows := readInboxRaw(t, path)
	if len(rows) != 2 {
		t.Fatalf("收件箱应有 2 条卡片，实际 %d 条", len(rows))
	}

	// 契约：字段集合与 API 侧读取逻辑（api/app/inbox.py）保持一致。
	wantKeys := map[string]bool{
		"id": true, "trigger_id": true, "subscriber_id": true, "workflow": true,
		"step_id": true, "title": true, "body": true,
		"created_at": true, "updated_at": true, "read": true,
	}
	for _, row := range rows {
		if len(row) != len(wantKeys) {
			t.Errorf("收件箱条目字段数 = %d, 期望 %d: %v", len(row), len(wantKeys), row)
		}
		for k := range row {
			if !wantKeys[k] {
				t.Errorf("收件箱条目出现了 API 不认识的字段 %q", k)
			}
		}
		id, _ := row["id"].(string)
		if !strings.HasPrefix(id, "digest-") {
			t.Errorf("聚合卡片条目 ID 应以 digest- 开头，实际 %q", id)
		}
		if row["subscriber_id"] != "creator_1" || row["workflow"] != "post_liked" || row["step_id"] != "dg1" {
			t.Errorf("条目归属字段不正确: %v", row)
		}
		if row["read"] != false {
			t.Errorf("引擎写入的条目应为未读，实际 %v", row["read"])
		}
		for _, field := range []string{"created_at", "updated_at"} {
			ts, _ := row[field].(string)
			if !strings.HasSuffix(ts, "Z") {
				t.Errorf("%s 应为 UTC 时间（以 Z 结尾），实际 %q", field, ts)
			}
			if _, err := time.Parse(time.RFC3339Nano, ts); err != nil {
				t.Errorf("%s 不是合法 RFC3339 时间: %q", field, ts)
			}
		}
	}

	// 另一个存储实例读取（相当于 API 进程读引擎写下的文件）。
	reader, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("重新打开收件箱失败: %v", err)
	}
	entries, err := reader.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("订阅者收件箱应只有 2 条卡片，实际 %d 条", len(entries))
	}
	bodies := map[string]InboxEntry{}
	for _, e := range entries {
		bodies[e.Body] = e
	}
	var cardA InboxEntry
	for body, entry := range bodies {
		if strings.Contains(body, titleA) {
			cardA = entry
		}
	}
	if cardA.ID == "" {
		t.Fatalf("未找到帖子 %s 的卡片: %v", postA, bodies)
	}
	if !strings.Contains(cardA.Body, "3 人赞了你的帖子") {
		t.Errorf("卡片正文应累计 3 条互动，实际 %q", cardA.Body)
	}
	if cardA.Title != "你的帖子有新互动" {
		t.Errorf("卡片标题 = %q", cardA.Title)
	}
	if !cardA.UpdatedAt.After(cardA.CreatedAt) {
		t.Errorf("新互动应推进更新时间: created=%s updated=%s", cardA.CreatedAt, cardA.UpdatedAt)
	}
}

// TestDigestIntegrationMultiInstanceSharedWindow 模拟两个引擎实例共享聚合窗口：
// 各自持有聚合存储的客户端与收件箱文件的存储实例，交替处理同一篇帖子的互动，
// 最终收件箱里仍只有一张卡片，计数为全部互动之和。
func TestDigestIntegrationMultiInstanceSharedWindow(t *testing.T) {
	shared, _ := newRedisDigestFixture(t)
	path := filepath.Join(t.TempDir(), "inbox.jsonl")

	// 两个实例各自打开自己的收件箱存储实例（共享卷），聚合状态走 Redis。
	pA, _ := newJSONLPipeline(t, path)
	pA.WithDigestStore(shared)
	pB, _ := newJSONLPipeline(t, path)
	pB.WithDigestStore(shared)

	wf := postLikedTree()
	// 6 位不同读者的点赞，按实例交替投递到同一个聚合窗口。
	actors := []string{"小林", "阿May", "老周", "阿树", "Momo", "Kai"}
	var opened int
	for i, actor := range actors {
		instance := pA
		if i%2 == 1 {
			instance = pB
		}
		trigger := fmt.Sprintf("t-multi-%d", i+1)
		res := instance.Run(NewContext(decodeEvent(t, likedEventJSON(trigger, "P-2026-7001", "多实例下的互动聚合", actor))), wf)[0]
		if res.Error != "" {
			t.Fatalf("第 %d 条互动执行失败: %s", i+1, res.Error)
		}
		if res.AggregateOpened {
			opened++
		}
		if res.AggregateCount != i+1 {
			t.Errorf("第 %d 条互动后计数 = %d, 期望 %d", i+1, res.AggregateCount, i+1)
		}
	}
	if opened != 1 {
		t.Errorf("共享窗口下应只开卡 1 次，实际 %d 次", opened)
	}

	rows := readInboxRaw(t, path)
	if len(rows) != 1 {
		t.Fatalf("多实例应共享同一张卡片，实际落盘 %d 条", len(rows))
	}

	reader, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("重新打开收件箱失败: %v", err)
	}
	entries, err := reader.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("应只有 1 条卡片，实际 %d 条", len(entries))
	}
	if !strings.Contains(entries[0].Body, "6 人赞了你的帖子") {
		t.Errorf("卡片应累计 6 条互动，实际 %q", entries[0].Body)
	}
}

// TestDigestIntegrationConcurrentLikeStorm 模拟互动洪峰：同一篇帖子被并发点赞，
// 每个事件都必须计入，收件箱里始终只有一张卡片。
func TestDigestIntegrationConcurrentLikeStorm(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.jsonl")
	p, _ := newJSONLPipeline(t, path)
	wf := postLikedTree()

	const (
		workers     = 8
		perWorker   = 8
		totalEvents = workers * perWorker
	)

	var wg sync.WaitGroup
	var mu sync.Mutex
	maxSeen := 0
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				trigger := fmt.Sprintf("t-storm-%d-%d", w, i)
				actor := fmt.Sprintf("user%d", w)
				res := p.Run(NewContext(decodeEvent(t, likedEventJSON(trigger, "P-2026-8001", "并发点赞洪峰", actor))), wf)[0]
				if res.Error != "" {
					t.Errorf("%s 执行失败: %s", trigger, res.Error)
					return
				}
				mu.Lock()
				if res.AggregateCount > maxSeen {
					maxSeen = res.AggregateCount
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	if maxSeen != totalEvents {
		t.Errorf("并发累计峰值 = %d, 期望 %d（不能丢计数）", maxSeen, totalEvents)
	}

	rows := readInboxRaw(t, path)
	if len(rows) != 1 {
		t.Fatalf("并发互动应只留下 1 条卡片，实际 %d 条", len(rows))
	}
	body, _ := rows[0]["body"].(string)
	if !strings.Contains(body, fmt.Sprintf("%d 人赞了你的帖子", totalEvents)) {
		t.Errorf("卡片应累计 %d 条互动，实际 %q", totalEvents, body)
	}

	reader, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("重新打开收件箱失败: %v", err)
	}
	entries, err := reader.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("应只有 1 条卡片，实际 %d 条", len(entries))
	}
}

// TestJSONLInboxStoreDetectsOtherProcessWrites 固定跨进程写入的语义：
// 一个存储实例写下的卡片，另一个实例在索引为空时也要认出来，
// 否则同一张聚合卡片会在文件里出现两行。
func TestJSONLInboxStoreDetectsOtherProcessWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.jsonl")
	writer, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开收件箱失败: %v", err)
	}

	entry := InboxEntry{
		ID:           "digest-shared-1",
		TriggerID:    "t-1",
		SubscriberID: "creator_1",
		Workflow:     "post_liked",
		StepID:       "dg1",
		Title:        "互动汇总",
		Body:         "1 人赞了你的帖子",
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}
	if inserted, err := writer.Save(entry); err != nil || !inserted {
		t.Fatalf("写入卡片失败: inserted=%v err=%v", inserted, err)
	}

	// 另一个进程（索引在启动时为空）要能原地改写这条卡片。
	other, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开收件箱失败: %v", err)
	}
	entry.Body = "2 人赞了你的帖子"
	entry.UpdatedAt = time.Now().UTC()
	if ok, err := other.Update(entry); err != nil || !ok {
		t.Fatalf("跨实例原地更新应成功: ok=%v err=%v", ok, err)
	}

	// 索引未命中的重复写入也应被判为已存在，而不是追加一行。
	if inserted, err := other.Save(entry); err != nil || inserted {
		t.Errorf("同 ID 写入应幂等，实际 inserted=%v err=%v", inserted, err)
	}

	rows := readInboxRaw(t, path)
	if len(rows) != 1 {
		t.Fatalf("同一张卡片只应有一行，实际 %d 行", len(rows))
	}
	if body, _ := rows[0]["body"].(string); body != "2 人赞了你的帖子" {
		t.Errorf("卡片正文未更新，实际 %q", body)
	}
}
