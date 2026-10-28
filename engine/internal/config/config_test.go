package config

import (
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/worker"
)

// 并发度与退出等待的默认值：未配置或非法配置都要落到可用的默认值上，
// 不能让 0 个消费者或 0 等待这种配置悄悄生效。
func TestWorkersAndGraceDefaults(t *testing.T) {
	var zero Config
	if got := zero.WorkersOrDefault(); got != worker.DefaultPoolSize {
		t.Errorf("未配置并发度应取默认 %d，实际 %d", worker.DefaultPoolSize, got)
	}
	if got := zero.ShutdownGraceOrDefault(); got != worker.DefaultShutdownGrace {
		t.Errorf("未配置等待上限应取默认 %v，实际 %v", worker.DefaultShutdownGrace, got)
	}

	over := Config{Workers: worker.MaxPoolSize + 10, ShutdownGrace: 5 * time.Second}
	if got := over.WorkersOrDefault(); got != worker.MaxPoolSize {
		t.Errorf("超上限的并发度应收敛到 %d，实际 %d", worker.MaxPoolSize, got)
	}
	if got := over.ShutdownGraceOrDefault(); got != 5*time.Second {
		t.Errorf("显式配置的等待上限应生效，实际 %v", got)
	}
}

func TestLoadReadsWorkerEnv(t *testing.T) {
	t.Setenv("XW_WORKERS", "12")
	t.Setenv("XW_SHUTDOWN_GRACE", "7s")

	cfg := Load()
	if cfg.WorkersOrDefault() != 12 {
		t.Errorf("XW_WORKERS 应解析为 12，实际 %d", cfg.WorkersOrDefault())
	}
	if cfg.ShutdownGraceOrDefault() != 7*time.Second {
		t.Errorf("XW_SHUTDOWN_GRACE 应解析为 7s，实际 %v", cfg.ShutdownGraceOrDefault())
	}
}
