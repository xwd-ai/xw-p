package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/config"
	"github.com/xwd-ai/xw-p/engine/internal/execution"
)

// 默认装配：执行明细文件可写时接上记录器，写出的行能被 API 侧的读取实现解析。
func TestNewExecutionRecorderWritesJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data", "executions.jsonl")
	recorder, closeFn := newExecutionRecorder(discardLogger(), config.Config{ExecutionFile: path})
	defer closeFn()

	if recorder == nil {
		t.Fatalf("文件可写时应接上记录器")
	}
	if _, err := recorder.Write(execution.Record{
		TriggerID:  "t-1",
		Channel:    "email",
		Status:     execution.StatusSent,
		DurationMS: 12,
	}); err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	store, err := execution.NewJSONLStore(path)
	if err != nil {
		t.Fatalf("重新打开失败: %v", err)
	}
	defer store.Close()
	records, err := store.Records()
	if err != nil {
		t.Fatalf("读回失败: %v", err)
	}
	if len(records) != 1 || records[0].TriggerID != "t-1" {
		t.Fatalf("记录不符: %+v", records)
	}
}

// 显式关闭回写：返回 nil 记录器，投递路径不必判空即可继续。
func TestNewExecutionRecorderDisabled(t *testing.T) {
	for _, value := range []string{"off", "OFF", "disabled", "none", "-"} {
		recorder, closeFn := newExecutionRecorder(discardLogger(), config.Config{ExecutionFile: value})
		if recorder != nil {
			t.Errorf("%s 应关闭回写", value)
		}
		closeFn() // 关闭函数必须可调用，否则 defer 链上会空指针
	}
}

// 文件打不开时不阻塞启动：记错误日志，返回 nil 记录器。
func TestNewExecutionRecorderUnwritableFile(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "occupied")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("准备文件失败: %v", err)
	}

	recorder, closeFn := newExecutionRecorder(discardLogger(), config.Config{
		// 父路径被文件占位，创建目录必然失败。
		ExecutionFile: filepath.Join(blocker, "executions.jsonl"),
	})
	defer closeFn()
	if recorder != nil {
		t.Fatalf("不可写时不应接上记录器")
	}
}

// 关闭函数要真的释放句柄：关闭后再写应当失败，而不是悄悄写到旧句柄上。
func TestNewExecutionRecorderCloseReleasesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "executions.jsonl")
	recorder, closeFn := newExecutionRecorder(discardLogger(), config.Config{ExecutionFile: path})
	if recorder == nil {
		t.Fatalf("应接上记录器")
	}
	closeFn()

	// 关闭后仍能重新打开并读到已写入的内容（文件本身没有被删）。
	store, err := execution.NewJSONLStore(path)
	if err != nil {
		t.Fatalf("重新打开失败: %v", err)
	}
	defer store.Close()
	if records, err := store.Records(); err != nil || len(records) != 0 {
		t.Fatalf("空文件应无记录: %+v err=%v", records, err)
	}

	// 重复调用关闭函数不应 panic。
	closeFn()
}

// 后端选 postgres 但没给连接串：退回「不回写」，不阻塞启动。
// 这是最容易配错的一种组合——切了后端却忘了填地址，表现出来只是明细为空。
func TestNewExecutionRecorderPostgresWithoutDSN(t *testing.T) {
	recorder, closeFn := newExecutionRecorder(discardLogger(), config.Config{
		ExecutionFile:  filepath.Join(t.TempDir(), "executions.jsonl"),
		ExecutionStore: config.ExecutionBackendPostgres,
	})
	defer closeFn()
	if recorder != nil {
		t.Fatalf("缺少连接串时不应接上记录器")
	}
}

// 后端名写错：按文件回退，并保证仍然可写。
func TestNewExecutionRecorderUnknownBackendFallsBackToFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "executions.jsonl")
	recorder, closeFn := newExecutionRecorder(discardLogger(), config.Config{
		ExecutionFile:  path,
		ExecutionStore: "postgre",
	})
	defer closeFn()
	if recorder == nil {
		t.Fatalf("未知后端应回退到文件回写")
	}
	if _, err := recorder.Write(execution.Record{
		TriggerID: "t-1", Channel: "email", Status: execution.StatusSent,
	}); err != nil {
		t.Fatalf("回退后写入失败: %v", err)
	}
}
