package execution

import (
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// 记录器补齐标识与时间：调用点只负责描述「哪个渠道、成没成、为什么」。
func TestRecorderFillsIDAndTimestamp(t *testing.T) {
	store := NewMemoryStore(nil)
	recorder := NewRecorder(store, quietLogger())
	fixed := time.Date(2026, 11, 2, 2, 9, 12, 0, time.UTC)
	recorder.now = func() time.Time { return fixed }

	written, err := recorder.Write(Record{TriggerID: "t-1", Channel: "email", Status: StatusSent})
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	if len(written.ID) != 32 {
		t.Errorf("标识应为 32 位十六进制，实际 %q", written.ID)
	}
	if _, err := hex.DecodeString(written.ID); err != nil {
		t.Errorf("标识不是合法十六进制: %v", err)
	}
	if !written.CreatedAt.Equal(fixed) {
		t.Errorf("时间戳未补齐: %s", written.CreatedAt)
	}

	records := store.Records()
	if len(records) != 1 || records[0].ID != written.ID {
		t.Fatalf("落库记录与返回值不一致: %+v", records)
	}
}

// 每条记录的标识必须不同，否则执行明细的翻页次级键会撞车。
func TestRecorderGeneratesUniqueIDs(t *testing.T) {
	recorder := NewRecorder(NewMemoryStore(nil), quietLogger())
	seen := make(map[string]bool, 64)
	for i := 0; i < 64; i++ {
		record, err := recorder.Write(Record{TriggerID: "t-1", Channel: "sms", Status: StatusSent})
		if err != nil {
			t.Fatalf("写入失败: %v", err)
		}
		if seen[record.ID] {
			t.Fatalf("标识重复: %s", record.ID)
		}
		seen[record.ID] = true
	}
}

func TestRecorderKeepsProvidedIDAndTimestamp(t *testing.T) {
	recorder := NewRecorder(NewMemoryStore(nil), quietLogger())
	recorder.now = func() time.Time { return time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC) }

	given := NewTimestamp(time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC))
	record, err := recorder.Write(Record{
		ID:        "fixed-id",
		TriggerID: "t-1",
		Channel:   "push",
		Status:    StatusSent,
		CreatedAt: given,
	})
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	if record.ID != "fixed-id" {
		t.Errorf("已有标识不应被覆盖: %s", record.ID)
	}
	if !record.CreatedAt.Equal(given.Time) {
		t.Errorf("已有时间戳不应被覆盖: %s", record.CreatedAt)
	}
}

func TestRecorderRejectsInvalidRecord(t *testing.T) {
	store := NewMemoryStore(nil)
	recorder := NewRecorder(store, quietLogger())

	if _, err := recorder.Write(Record{TriggerID: "t-1", Channel: "email", Status: StatusFailed}); err == nil {
		t.Fatalf("失败但没原因应被拒绝")
	}
	if len(store.Records()) != 0 {
		t.Errorf("非法记录不应落库: %+v", store.Records())
	}
}

// 存储写不进去时返回错误并且不 panic：调用方据此记日志，但不会把投递判成失败。
func TestRecorderSurfacesStoreError(t *testing.T) {
	recorder := NewRecorder(NewMemoryStore(errors.New("磁盘只读")), quietLogger())
	if _, err := recorder.Write(Record{TriggerID: "t-1", Channel: "email", Status: StatusSent}); err == nil {
		t.Fatalf("存储报错应向上返回")
	}
}

// recorder 为 nil 时调用方不必判空：Write 原样返回，什么也不做。
func TestNilRecorderIsNoop(t *testing.T) {
	var recorder *Recorder
	record, err := recorder.Write(Record{TriggerID: "t-1", Channel: "email", Status: StatusSent})
	if err != nil {
		t.Fatalf("nil 记录器不应报错: %v", err)
	}
	if record.TriggerID != "t-1" {
		t.Errorf("记录应原样返回: %+v", record)
	}
	if NewRecorder(nil, nil) != nil {
		t.Errorf("nil 存储应返回 nil 记录器")
	}
}

// 实现标识写进记录前去空白：配置里的手误不该让查询按实现过滤时对不上。
func TestRecorderTrimsProvider(t *testing.T) {
	store := NewMemoryStore(nil)
	recorder := NewRecorder(store, quietLogger())
	if _, err := recorder.Write(Record{
		TriggerID: "t-1",
		Channel:   "email",
		Status:    StatusSent,
		Provider:  "  smtp-email\n",
	}); err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	if got := store.Records()[0].Provider; got != "smtp-email" {
		t.Errorf("实现标识未去空白: %q", got)
	}
}

// 落库的行文本要能直接被 API 侧的 datetime.fromisoformat 解析：
// 时间戳不能带 Z 后缀、不能是纳秒。
func TestRecorderLineIsReaderCompatible(t *testing.T) {
	path := t.TempDir() + "/executions.jsonl"
	store, err := NewJSONLStore(path)
	if err != nil {
		t.Fatalf("创建存储失败: %v", err)
	}
	defer store.Close()

	recorder := NewRecorder(store, quietLogger())
	if _, err := recorder.Write(Record{TriggerID: "t-1", Channel: "chat", Status: StatusSent}); err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取文件失败: %v", err)
	}
	const marker = `"created_at":"`
	idx := strings.Index(string(raw), marker)
	if idx < 0 {
		t.Fatalf("记录里没有 created_at：%s", raw)
	}
	// 2026-11-02T10:09:12.123456+00:00 —— 32 个字符，固定六位小数与显式偏移。
	stamp := string(raw)[idx+len(marker):]
	if len(stamp) < 32 {
		t.Fatalf("时间戳被截断：%q", stamp)
	}
	stamp = stamp[:32]
	if !strings.HasSuffix(stamp, "+00:00") {
		t.Errorf("时间戳应带显式偏移而不是 Z：%q", stamp)
	}
	if strings.Count(stamp, ".") != 1 || len(strings.SplitN(stamp, ".", 2)[1]) < 6 {
		t.Errorf("时间戳应保留六位小数：%q", stamp)
	}
}
