package config

import (
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
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

// 重试策略：默认值可直接启用，环境变量能改，显式关闭则回到「等租约回收」。
func TestRetryPolicyDefaults(t *testing.T) {
	t.Setenv("XW_RETRY_MAX_ATTEMPTS", "")
	policy := Load().RetryPolicyOrDefault()
	if !policy.Enabled() {
		t.Fatal("未配置时应启用默认重试策略")
	}
	if policy.MaxAttempts != queue.DefaultMaxAttempts {
		t.Errorf("默认最大次数应为 %d，实际 %d", queue.DefaultMaxAttempts, policy.MaxAttempts)
	}
	if policy.Base != queue.DefaultRetryBase || policy.Max != queue.DefaultRetryMax {
		t.Errorf("默认退避参数不符: %+v", policy)
	}
	if policy.Jitter != queue.DefaultRetryJitter {
		t.Errorf("默认抖动比例应为 %v，实际 %v", queue.DefaultRetryJitter, policy.Jitter)
	}

	t.Setenv("XW_RETRY_MAX_ATTEMPTS", "0")
	if Load().RetryPolicyOrDefault().Enabled() {
		t.Error("XW_RETRY_MAX_ATTEMPTS=0 应关闭重试")
	}
}

func TestLoadReadsRetryEnv(t *testing.T) {
	t.Setenv("XW_RETRY_MAX_ATTEMPTS", "7")
	t.Setenv("XW_RETRY_BASE_DELAY", "500ms")
	t.Setenv("XW_RETRY_MAX_DELAY", "30s")
	t.Setenv("XW_RETRY_JITTER", "0.15")
	t.Setenv("XW_RETRY_INTERVAL", "3s")

	cfg := Load()
	policy := cfg.RetryPolicyOrDefault()
	if policy.MaxAttempts != 7 {
		t.Errorf("XW_RETRY_MAX_ATTEMPTS 应解析为 7，实际 %d", policy.MaxAttempts)
	}
	if policy.Base != 500*time.Millisecond {
		t.Errorf("XW_RETRY_BASE_DELAY 应解析为 500ms，实际 %v", policy.Base)
	}
	if policy.Max != 30*time.Second {
		t.Errorf("XW_RETRY_MAX_DELAY 应解析为 30s，实际 %v", policy.Max)
	}
	if policy.Jitter != 0.15 {
		t.Errorf("XW_RETRY_JITTER 应解析为 0.15，实际 %v", policy.Jitter)
	}
	if cfg.RetryInterval != 3*time.Second {
		t.Errorf("XW_RETRY_INTERVAL 应解析为 3s，实际 %v", cfg.RetryInterval)
	}
}
