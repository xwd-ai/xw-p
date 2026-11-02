package execution

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func sampleRecord(trigger string) Record {
	return Record{
		ID:         "id-" + trigger,
		TriggerID:  trigger,
		Channel:    "in_app",
		Status:     StatusSent,
		DurationMS: 7,
		CreatedAt:  NewTimestamp(time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)),
	}
}

// 目录不存在时一并创建：容器里挂载的空卷第一次启动时目录往往还没有。
func TestJSONLStoreCreatesDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "data", "executions.jsonl")
	store, err := NewJSONLStore(path)
	if err != nil {
		t.Fatalf("创建存储失败: %v", err)
	}
	defer store.Close()

	if store.Path() != path {
		t.Errorf("路径不符：%s", store.Path())
	}
	if err := store.Append(sampleRecord("t-1")); err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("文件未落盘: %v", err)
	}
}

func TestJSONLStoreAppendsAndReadsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "executions.jsonl")
	store, err := NewJSONLStore(path)
	if err != nil {
		t.Fatalf("创建存储失败: %v", err)
	}
	defer store.Close()

	for _, trigger := range []string{"t-1", "t-2"} {
		if err := store.Append(sampleRecord(trigger)); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
	}

	records, err := store.Records()
	if err != nil {
		t.Fatalf("读回失败: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("应有 2 条记录，实际 %d", len(records))
	}
	if records[0].TriggerID != "t-1" || records[1].TriggerID != "t-2" {
		t.Errorf("记录顺序应保持写入顺序: %+v", records)
	}
	if !records[0].CreatedAt.Equal(time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)) {
		t.Errorf("时间戳读回后偏移不一致: %s", records[0].CreatedAt)
	}

	// 文件是一行一条：不能被序列化时的换行拆成多行。
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取文件失败: %v", err)
	}
	if lines := strings.Count(string(raw), "\n"); lines != 2 {
		t.Errorf("应有 2 行，实际 %d 行：%q", lines, raw)
	}
}

func TestJSONLStoreSkipsCorruptLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "executions.jsonl")
	// 模拟进程被杀时写了一半的行：读取时跳过，不能让整份明细读不出来。
	content := "{\"trigger_id\":\"t-1\"" + "\n" + "\n" + `{"id":"x","trigger_id":"t-2","channel":"email","status":"sent","duration_ms":1,"created_at":"2026-11-02T10:00:00.000000+00:00"}` + "\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("准备文件失败: %v", err)
	}

	store, err := NewJSONLStore(path)
	if err != nil {
		t.Fatalf("创建存储失败: %v", err)
	}
	defer store.Close()

	records, err := store.Records()
	if err != nil {
		t.Fatalf("读回失败: %v", err)
	}
	if len(records) != 1 || records[0].TriggerID != "t-2" {
		t.Fatalf("应只读出 1 条有效记录，实际 %+v", records)
	}
}

// 并发追加：每条记录必须是完整的一行，不能出现两条记录交错。
func TestJSONLStoreConcurrentAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "executions.jsonl")
	store, err := NewJSONLStore(path)
	if err != nil {
		t.Fatalf("创建存储失败: %v", err)
	}
	defer store.Close()

	const writers, each = 8, 25
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				record := sampleRecord("t-" + string(rune('a'+w)))
				record.ID = record.TriggerID + "-" + string(rune('0'+i%10)) + "-" + string(rune('0'+i/10))
				if err := store.Append(record); err != nil {
					t.Errorf("写入失败: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	records, err := store.Records()
	if err != nil {
		t.Fatalf("读回失败: %v", err)
	}
	if want := writers * each; len(records) != want {
		t.Fatalf("应有 %d 条记录，实际 %d（可能有记录被写进同一行）", want, len(records))
	}
	seen := make(map[string]bool, len(records))
	for _, record := range records {
		if seen[record.ID] {
			t.Fatalf("记录重复: %s", record.ID)
		}
		seen[record.ID] = true
	}
}

func TestJSONLStoreRejectsEmptyPath(t *testing.T) {
	if _, err := NewJSONLStore("   "); err == nil {
		t.Fatalf("空路径应报错")
	}
}

func TestJSONLStoreRejectsDirectoryPath(t *testing.T) {
	dir := t.TempDir()
	if _, err := NewJSONLStore(dir); err == nil {
		t.Fatalf("路径被目录占位时应报错")
	}
}

func TestJSONLStoreAppendAfterClose(t *testing.T) {
	store, err := NewJSONLStore(filepath.Join(t.TempDir(), "executions.jsonl"))
	if err != nil {
		t.Fatalf("创建存储失败: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("重复关闭应无副作用: %v", err)
	}
	if err := store.Append(sampleRecord("t-1")); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("关闭后写入应返回 ErrStoreClosed，实际 %v", err)
	}
}

func TestMemoryStore(t *testing.T) {
	store := NewMemoryStore(nil)
	if err := store.Append(sampleRecord("t-1")); err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	if records := store.Records(); len(records) != 1 || records[0].TriggerID != "t-1" {
		t.Fatalf("记录不符: %+v", records)
	}

	broken := NewMemoryStore(errors.New("磁盘满了"))
	if err := broken.Append(sampleRecord("t-2")); err == nil {
		t.Fatalf("应返回注入的错误")
	}
	if records := broken.Records(); len(records) != 0 {
		t.Errorf("写入失败时不应留下记录: %+v", records)
	}
}

// 内存存储不能把调用方的切片/指针直接存下来：调用方在写入后改值，
// 已落库的记录不应跟着变。
func TestMemoryStoreCopiesRecord(t *testing.T) {
	store := NewMemoryStore(nil)
	reason := "第一次失败"
	record := sampleRecord("t-1")
	record.Status = StatusFailed
	record.Reason = &reason
	record.Attempts = []string{"smtp-email"}
	if err := store.Append(record); err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	reason = "被调用方改掉了"
	record.Attempts[0] = "console-email"

	stored := store.Records()[0]
	if stored.Reason == nil || *stored.Reason != "第一次失败" {
		t.Errorf("原因被调用方改动影响: %+v", stored.Reason)
	}
	if len(stored.Attempts) != 1 || stored.Attempts[0] != "smtp-email" {
		t.Errorf("尝试序列被调用方改动影响: %+v", stored.Attempts)
	}
}
