package config

import (
	"os"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
	"github.com/xwd-ai/xw-p/engine/internal/schedule"
	"github.com/xwd-ai/xw-p/engine/internal/webhook"
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

// 管理接口监听地址：未配置时落在本机 ENGINE_PORT，显式配置优先，
// off/空串关闭。运维接口不该默认对外暴露，所以默认只绑回环地址。
func TestAdminListenAddr(t *testing.T) {
	t.Setenv("XW_ADMIN_ADDR", "")
	if got := Load().AdminAddr; got != "" {
		t.Errorf("XW_ADMIN_ADDR 留空应关闭管理接口，实际 %q", got)
	}

	t.Setenv("XW_ADMIN_ADDR", "off")
	if got := Load().AdminAddr; got != "" {
		t.Errorf("XW_ADMIN_ADDR=off 应关闭管理接口，实际 %q", got)
	}

	t.Setenv("XW_ADMIN_ADDR", "0.0.0.0:9090")
	if got := Load().AdminAddr; got != "0.0.0.0:9090" {
		t.Errorf("显式地址应原样生效，实际 %q", got)
	}
}

func TestAdminListenAddrFallsBackToEnginePort(t *testing.T) {
	// 用 Unsetenv 区分「没有这个变量」与「变量为空」。
	t.Setenv("XW_ADMIN_ADDR", "placeholder")
	if err := os.Unsetenv("XW_ADMIN_ADDR"); err != nil {
		t.Fatalf("清除环境变量失败: %v", err)
	}
	t.Setenv("ENGINE_PORT", "9181")
	if got := Load().AdminAddr; got != "127.0.0.1:9181" {
		t.Errorf("未配置时应沿用 ENGINE_PORT 并只绑回环，实际 %q", got)
	}
}

// 延迟任务存储：默认走 Redis（重启不丢），显式 memory 才退回进程内，
// 前缀可单独配置用于环境隔离。
func TestDelayStoreConfig(t *testing.T) {
	t.Setenv("XW_DELAY_STORE", "")
	t.Setenv("XW_DELAY_PREFIX", "")
	cfg := Load()
	if cfg.DelayStore != "redis" {
		t.Errorf("默认延迟存储应为 redis，实际 %q", cfg.DelayStore)
	}
	if cfg.DelayPrefix != schedule.DefaultRedisPrefix {
		t.Errorf("默认前缀应为 %q，实际 %q", schedule.DefaultRedisPrefix, cfg.DelayPrefix)
	}

	t.Setenv("XW_DELAY_STORE", "memory")
	t.Setenv("XW_DELAY_PREFIX", "xw:delay:staging")
	cfg = Load()
	if cfg.DelayStore != "memory" || cfg.DelayPrefix != "xw:delay:staging" {
		t.Errorf("显式配置未生效：store=%q prefix=%q", cfg.DelayStore, cfg.DelayPrefix)
	}
}

// 渠道配置文件路径来自 XW_PROVIDERS_FILE，未配置时为空（走内置 console 实现）。
func TestLoadReadsProvidersFileEnv(t *testing.T) {
	t.Setenv("XW_PROVIDERS_FILE", "  /etc/xw/providers.json  ")
	if got := Load().ProvidersFile; got != "/etc/xw/providers.json" {
		t.Errorf("XW_PROVIDERS_FILE = %q, 期望去空白后的路径", got)
	}

	t.Setenv("XW_PROVIDERS_FILE", "")
	if got := Load().ProvidersFile; got != "" {
		t.Errorf("未配置时应为空，实际 %q", got)
	}
}

// 执行明细回写：默认落在与 API 共用的数据目录，写 off 系列值表示不回写。
func TestExecutionFileConfig(t *testing.T) {
	t.Setenv("XW_EXECUTION_FILE", "")
	if got := Load().ExecutionFile; got != defaultExecutionFile {
		t.Errorf("默认执行明细路径应为 %q，实际 %q", defaultExecutionFile, got)
	}

	t.Setenv("XW_EXECUTION_FILE", "  /tmp/xw/executions.jsonl  ")
	cfg := Load()
	if cfg.ExecutionFile != "/tmp/xw/executions.jsonl" {
		t.Errorf("XW_EXECUTION_FILE 应去空白，实际 %q", cfg.ExecutionFile)
	}
	if cfg.ExecutionDisabled() {
		t.Errorf("配置了路径时不应判定为关闭")
	}
}

func TestExecutionDisabled(t *testing.T) {
	cases := map[string]bool{
		"":                       true,
		"off":                    true,
		"OFF":                    true,
		" disabled ":             true,
		"none":                   true,
		"-":                      true,
		"api/data/execut.jsonl":  false,
		"/var/log/xw/exec.jsonl": false,
	}
	for value, want := range cases {
		if got := (Config{ExecutionFile: value}).ExecutionDisabled(); got != want {
			t.Errorf("ExecutionFile=%q 的关闭判定 = %v，期望 %v", value, got, want)
		}
	}
}

// 回调配置：地址去空白，超时/并发/队列有可用默认值，写 off 系列值表示关闭。
func TestLoadReadsCallbackEnv(t *testing.T) {
	t.Setenv("XW_CALLBACK_URL", "")
	t.Setenv("XW_CALLBACK_TIMEOUT", "")
	t.Setenv("XW_CALLBACK_WORKERS", "")
	t.Setenv("XW_CALLBACK_QUEUE", "")
	cfg := Load()
	if !cfg.CallbackDisabled() {
		t.Error("未配置地址时应视为关闭")
	}
	if cfg.CallbackTimeout != webhook.DefaultTimeout {
		t.Errorf("超时默认值应为 %v，实际 %v", webhook.DefaultTimeout, cfg.CallbackTimeout)
	}
	if cfg.CallbackWorkers != webhook.DefaultWorkers {
		t.Errorf("并发默认值应为 %d，实际 %d", webhook.DefaultWorkers, cfg.CallbackWorkers)
	}
	if cfg.CallbackQueue != webhook.DefaultQueueSize {
		t.Errorf("队列默认值应为 %d，实际 %d", webhook.DefaultQueueSize, cfg.CallbackQueue)
	}

	t.Setenv("XW_CALLBACK_URL", "  https://community.example.com/hooks/xw  ")
	t.Setenv("XW_CALLBACK_TIMEOUT", "3s")
	t.Setenv("XW_CALLBACK_WORKERS", "4")
	t.Setenv("XW_CALLBACK_QUEUE", "128")
	cfg = Load()
	if cfg.CallbackURL != "https://community.example.com/hooks/xw" {
		t.Errorf("XW_CALLBACK_URL 应去空白，实际 %q", cfg.CallbackURL)
	}
	if cfg.CallbackDisabled() {
		t.Error("配置了地址时不应判定为关闭")
	}
	if cfg.CallbackTimeout != 3*time.Second {
		t.Errorf("超时应解析为 3s，实际 %v", cfg.CallbackTimeout)
	}
	if cfg.CallbackWorkers != 4 || cfg.CallbackQueue != 128 {
		t.Errorf("并发与队列配置未生效: %d / %d", cfg.CallbackWorkers, cfg.CallbackQueue)
	}
}

func TestCallbackDisabled(t *testing.T) {
	cases := map[string]bool{
		"":                               true,
		"off":                            true,
		"OFF":                            true,
		" disabled ":                     true,
		"none":                           true,
		"-":                              true,
		"https://community.example/cb":   false,
		"http://127.0.0.1:8099/callback": false,
	}
	for value, want := range cases {
		if got := (Config{CallbackURL: value}).CallbackDisabled(); got != want {
			t.Errorf("CallbackURL=%q 的关闭判定 = %v，期望 %v", value, got, want)
		}
	}
}
