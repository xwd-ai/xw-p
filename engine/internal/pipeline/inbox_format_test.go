package pipeline

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// 收件箱文件由引擎与 API 两侧共同写入，字段与时间格式必须对得上。
// 这组用例把「API 侧写出的行」当成固定输入，锁住跨进程的格式约定。

// apiWrittenEntry 是 API 侧站内信实现写出的行（时间带 +00:00 偏移与微秒）。
const apiWrittenEntry = `{"id":"t-like-1:in_app","trigger_id":"t-like-1",` +
	`"subscriber_id":"creator_1001","workflow":"post_liked","step_id":"in_app",` +
	`"title":"有人赞了你的帖子","body":"小林 赞了你的《我的第一支 Vlog》",` +
	`"created_at":"2026-03-01T09:00:00.123456+00:00","updated_at":"2026-03-01T09:00:00.123456+00:00",` +
	`"read":false}`

func TestJSONLInboxStoreReadsAPIWrittenEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.jsonl")
	if err := os.WriteFile(path, []byte(apiWrittenEntry+"\n"), 0o644); err != nil {
		t.Fatalf("写入准备数据失败: %v", err)
	}

	store, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开收件箱失败: %v", err)
	}
	entries, err := store.List("creator_1001")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("期望 1 条条目，实际 %d 条", len(entries))
	}
	entry := entries[0]
	if entry.ID != "t-like-1:in_app" || entry.Body == "" || entry.Title == "" {
		t.Fatalf("条目内容未正确解析: %+v", entry)
	}
	if entry.CreatedAt.IsZero() || entry.UpdatedAt.IsZero() {
		t.Fatalf("时间未解析: created=%v updated=%v", entry.CreatedAt, entry.UpdatedAt)
	}
	if got := entry.CreatedAt.UTC().Format(time.RFC3339Nano); got != "2026-03-01T09:00:00.123456Z" {
		t.Fatalf("时间偏移解析不正确: %s", got)
	}

	// 同一标识再写一次应判为已存在，不会在共享文件里出现第二条。
	inserted, err := store.Save(entry)
	if err != nil {
		t.Fatalf("重复写入失败: %v", err)
	}
	if inserted {
		t.Fatal("同一 ID 的条目应判为已存在")
	}
}

func TestInboxEntryJSONFieldsMatchAPIModel(t *testing.T) {
	raw, err := json.Marshal(InboxEntry{ID: "m1", SubscriberID: "u1", Body: "x"})
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}
	keys := make([]string, 0, len(decoded))
	for key := range decoded {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	want := []string{
		"body", "created_at", "id", "read", "step_id",
		"subscriber_id", "title", "trigger_id", "updated_at", "workflow",
	}
	if len(keys) != len(want) {
		t.Fatalf("字段数量不一致: %v", keys)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("字段名不一致: 期望 %v，实际 %v", want, keys)
		}
	}
}
