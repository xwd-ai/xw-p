package pipeline

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// 收件箱文件是「一行一个版本」的追加流，这组用例钉住三件事：
// 改写不重写整文件、废弃行到比例后会被压缩、多进程追加只做增量读取。

func newTestInboxEntry(id, body string) InboxEntry {
	now := time.Now().UTC()
	return InboxEntry{
		ID:           id,
		TriggerID:    "t-" + id,
		SubscriberID: "creator_1",
		Workflow:     "post_liked",
		StepID:       "dg1",
		Title:        "互动汇总",
		Body:         body,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
}

// inboxFileLines 返回文件里的非空行。
func inboxFileLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取收件箱文件失败: %v", err)
	}
	var out []string
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) != "" {
			out = append(out, line)
		}
	}
	return out
}

// 改写只追加一版，旧版本留在文件里；读侧仍只看到一条。
func TestJSONLInboxStoreUpdateAppendsVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.jsonl")
	store, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开收件箱失败: %v", err)
	}

	if inserted, err := store.Save(newTestInboxEntry("digest-1", "1 人赞了你的帖子")); err != nil || !inserted {
		t.Fatalf("写入首版失败: inserted=%v err=%v", inserted, err)
	}
	if updated, err := store.Update(newTestInboxEntry("digest-1", "2 人赞了你的帖子")); err != nil || !updated {
		t.Fatalf("改写失败: updated=%v err=%v", updated, err)
	}

	lines := inboxFileLines(t, path)
	if len(lines) != 2 {
		t.Fatalf("一次改写应追加一版，实际 %d 行", len(lines))
	}
	if !strings.Contains(lines[1], "2 人赞了你的帖子") {
		t.Errorf("追加的应是新版本，实际 %s", lines[1])
	}

	entries, err := store.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 1 || entries[0].Body != "2 人赞了你的帖子" {
		t.Fatalf("读侧应只看到最新版本，实际 %+v", entries)
	}
	if store.Len() != 1 {
		t.Errorf("条目数应为 1，实际 %d", store.Len())
	}
}

// 废弃行超过比例且文件超过阈值时压缩：文件回落成「一条一行」。
func TestJSONLInboxStoreCompactsSupersededVersions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.jsonl")
	store, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开收件箱失败: %v", err)
	}
	// 把阈值调到最小，让十来次改写就足以触发压缩。
	store.compactMinBytes = 1

	if inserted, err := store.Save(newTestInboxEntry("digest-1", "0 人赞了")); err != nil || !inserted {
		t.Fatalf("写入失败: inserted=%v err=%v", inserted, err)
	}
	for i := 1; i <= 10; i++ {
		entry := newTestInboxEntry("digest-1", fmt.Sprintf("%d 人赞了你的帖子", i))
		if updated, err := store.Update(entry); err != nil || !updated {
			t.Fatalf("第 %d 次改写失败: updated=%v err=%v", i, updated, err)
		}
	}

	lines := inboxFileLines(t, path)
	if len(lines) != 1 {
		t.Fatalf("压缩后应只剩最新版本，实际 %d 行", len(lines))
	}
	if !strings.Contains(lines[0], "10 人赞了你的帖子") {
		t.Errorf("压缩保留了错误版本: %s", lines[0])
	}

	entries, err := store.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 1 || entries[0].Body != "10 人赞了你的帖子" {
		t.Fatalf("压缩后读到的条目不正确: %+v", entries)
	}
}

// 小文件不做压缩：重复行留着比反复重写更划算，但读侧语义不受影响。
func TestJSONLInboxStoreKeepsSmallFileUncompacted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.jsonl")
	store, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开收件箱失败: %v", err)
	}
	if _, err := store.Save(newTestInboxEntry("digest-1", "0 人赞了")); err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	for i := 1; i <= 20; i++ {
		if _, err := store.Update(newTestInboxEntry("digest-1", fmt.Sprintf("%d 人赞了", i))); err != nil {
			t.Fatalf("第 %d 次改写失败: %v", i, err)
		}
	}

	if lines := inboxFileLines(t, path); len(lines) != 21 {
		t.Fatalf("未达阈值不应压缩，实际 %d 行", len(lines))
	}
	entries, err := store.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 1 || entries[0].Body != "20 人赞了" {
		t.Fatalf("读侧应只看到最新版本，实际 %+v", entries)
	}
}

// 别的进程追加后，本进程的幂等判断要能看到那一行（增量刷新，不整文件重扫）。
func TestJSONLInboxStoreSeesAppendsFromOtherProcess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.jsonl")
	writer, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开收件箱失败: %v", err)
	}
	if _, err := writer.Save(newTestInboxEntry("digest-1", "1 人赞了")); err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	// 第二个实例在启动时读到的是同一份文件，它写入的新条目对第一个实例可见。
	other, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开第二个实例失败: %v", err)
	}
	if _, err := other.Save(newTestInboxEntry("digest-2", "另一张卡片")); err != nil {
		t.Fatalf("第二个实例写入失败: %v", err)
	}

	if inserted, err := writer.Save(newTestInboxEntry("digest-2", "重复写入")); err != nil || inserted {
		t.Fatalf("应识别出别的实例已写过同一条目: inserted=%v err=%v", inserted, err)
	}
	entries, err := writer.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("应读到 2 条条目，实际 %d 条", len(entries))
	}
}

// 文件被整体替换（压缩或别的实例重写）后索引必须重建，不能沿用旧偏移。
func TestJSONLInboxStoreReloadsAfterFileReplaced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.jsonl")
	store, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开收件箱失败: %v", err)
	}
	if _, err := store.Save(newTestInboxEntry("digest-1", "1 人赞了")); err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	// 模拟别的进程压缩：整份文件换成一条更短的记录。
	if err := writeAtomic(path, []byte(`{"id":"digest-9","subscriber_id":"creator_1","body":"替换后的卡片"}`+"\n")); err != nil {
		t.Fatalf("替换文件失败: %v", err)
	}

	entries, err := store.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 1 || entries[0].ID != "digest-9" {
		t.Fatalf("应读到替换后的内容，实际 %+v", entries)
	}

	// 旧条目已不在文件里，按其 ID 改写应判为「不存在」，由调用方重新落库。
	if updated, err := store.Update(newTestInboxEntry("digest-1", "2 人赞了")); err != nil || updated {
		t.Fatalf("替换后旧条目的改写应判为不存在: updated=%v err=%v", updated, err)
	}
}

// 末尾没有换行的残片可能是别的进程正在写的半行，先不解析。
func TestJSONLInboxStoreIgnoresPartialTrailingLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.jsonl")
	first := `{"id":"digest-1","subscriber_id":"creator_1","body":"1 人赞了"}` + "\n"
	partial := `{"id":"digest-2","subscriber_id":"creator_1","body":"2 人`
	if err := os.WriteFile(path, []byte(first+partial), 0o644); err != nil {
		t.Fatalf("准备数据失败: %v", err)
	}

	store, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开收件箱失败: %v", err)
	}
	entries, err := store.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 1 || entries[0].ID != "digest-1" {
		t.Fatalf("半行不应被当成条目，实际 %+v", entries)
	}

	// 半行补齐后应被读到。
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("打开文件失败: %v", err)
	}
	if _, err := f.WriteString("赞了\"}" + "\n"); err != nil {
		f.Close()
		t.Fatalf("补全半行失败: %v", err)
	}
	f.Close()

	entries, err = store.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("补全后应读到 2 条条目，实际 %d 条", len(entries))
	}
}

// 文件末尾残留半行（别的进程写到一半退出）时，新的追加要先把它收尾，
// 否则自己的整行会和残片粘成一条坏记录，连这次写入一起丢掉。
func TestJSONLInboxStoreTerminatesPartialTailBeforeAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.jsonl")
	first := `{"id":"digest-1","subscriber_id":"creator_1","body":"1 人赞了"}` + "\n"
	fragment := `{"id":"digest-2","body":"写到一半`
	if err := os.WriteFile(path, []byte(first+fragment), 0o644); err != nil {
		t.Fatalf("准备数据失败: %v", err)
	}

	store, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开收件箱失败: %v", err)
	}
	if inserted, err := store.Save(newTestInboxEntry("digest-9", "新卡片")); err != nil || !inserted {
		t.Fatalf("写入失败: inserted=%v err=%v", inserted, err)
	}

	entries, err := store.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		ids = append(ids, e.ID)
	}
	if len(entries) != 2 || ids[0] != "digest-1" || ids[1] != "digest-9" {
		t.Fatalf("残片不应吃掉后续写入，实际读到 %v", ids)
	}
	// 残片单独占一行，没有和新条目粘在一起。
	if lines := inboxFileLines(t, path); len(lines) != 3 {
		t.Fatalf("应留下 3 行（有效条目 + 残片 + 新条目），实际 %d 行", len(lines))
	}
}

// 文件带着残片时压缩不能停摆：残片本来就会被丢掉，不该被当成「有人正在写」。
func TestJSONLInboxStoreCompactsFileWithPartialTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.jsonl")
	line := `{"id":"digest-1","subscriber_id":"creator_1","body":"1 人赞了"}`
	if err := os.WriteFile(path, []byte(line+"\n"+line+"\n"+`{"id":"digest-2","body":"半`), 0o644); err != nil {
		t.Fatalf("准备数据失败: %v", err)
	}

	store, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开收件箱失败: %v", err)
	}
	store.compactMinBytes = 1
	store.compactDeadRatio = 0.1

	if inserted, err := store.Save(newTestInboxEntry("digest-3", "新卡片")); err != nil || !inserted {
		t.Fatalf("写入失败: inserted=%v err=%v", inserted, err)
	}

	lines := inboxFileLines(t, path)
	if len(lines) != 2 {
		t.Fatalf("压缩后应只剩两条有效条目，实际 %d 行", len(lines))
	}
	entries, err := store.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 2 || entries[0].ID != "digest-1" || entries[1].ID != "digest-3" {
		t.Fatalf("压缩后条目不正确: %+v", entries)
	}
}

// 并发改写同一张卡片：只追加，不丢条目，读侧仍只有一条。
func TestJSONLInboxStoreConcurrentUpdates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.jsonl")
	store, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开收件箱失败: %v", err)
	}
	if _, err := store.Save(newTestInboxEntry("digest-1", "0 人赞了")); err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	const workers, perWorker = 8, 25
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				body := fmt.Sprintf("%d-%d", worker, i)
				if _, err := store.Update(newTestInboxEntry("digest-1", body)); err != nil {
					t.Errorf("并发改写失败: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	if lines := inboxFileLines(t, path); len(lines) != workers*perWorker+1 {
		t.Fatalf("每次改写应追加一版，实际 %d 行", len(lines))
	}
	entries, err := store.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("并发改写后应只有 1 条条目，实际 %d 条", len(entries))
	}
	// 并发下最后落盘的是哪一次不确定，只要求读到其中一次写出的版本。
	if entries[0].Body == "" || !strings.Contains(entries[0].Body, "-") {
		t.Fatalf("读到的正文不是任何一次写入的版本: %q", entries[0].Body)
	}
}
