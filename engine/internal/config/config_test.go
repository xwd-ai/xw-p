package config

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/provider"
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

// 订阅者偏好文件：默认与 API 共用同一份数据目录，写 off 系列值表示不做判定。
func TestPreferencesFileConfig(t *testing.T) {
	t.Setenv("XW_PREFERENCES_FILE", "")
	if got := Load().PreferencesFile; got != defaultPreferencesFile {
		t.Errorf("默认偏好文件应为 %q，实际 %q", defaultPreferencesFile, got)
	}

	t.Setenv("XW_PREFERENCES_FILE", "  /tmp/xw/preferences.jsonl  ")
	cfg := Load()
	if cfg.PreferencesFile != "/tmp/xw/preferences.jsonl" {
		t.Errorf("XW_PREFERENCES_FILE 应去空白，实际 %q", cfg.PreferencesFile)
	}
	if cfg.PreferencesDisabled() {
		t.Error("配置了路径时不应判定为关闭")
	}
}

func TestPreferencesDisabled(t *testing.T) {
	cases := map[string]bool{
		"":                              true,
		"off":                           true,
		"OFF":                           true,
		" disabled ":                    true,
		"none":                          true,
		"-":                             true,
		"api/data/preferences.jsonl":    false,
		"/var/lib/xw/preferences.jsonl": false,
	}
	for value, want := range cases {
		if got := (Config{PreferencesFile: value}).PreferencesDisabled(); got != want {
			t.Errorf("PreferencesFile=%q 的关闭判定 = %v，期望 %v", value, got, want)
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
	if cfg.CallbackEvents != "" {
		t.Errorf("未配置事件类型时应为空（由装配方取默认集合），实际 %q", cfg.CallbackEvents)
	}

	t.Setenv("XW_CALLBACK_URL", "  https://community.example.com/hooks/xw  ")
	t.Setenv("XW_CALLBACK_EVENTS", "  trigger.received,delivery.failed  ")
	t.Setenv("XW_CALLBACK_TIMEOUT", "3s")
	t.Setenv("XW_CALLBACK_WORKERS", "4")
	t.Setenv("XW_CALLBACK_QUEUE", "128")
	t.Setenv("XW_CALLBACK_SECRET", "  s3cr3t-key-for-xw-notify-test  ")
	t.Setenv("XW_CALLBACK_RETRY_ATTEMPTS", "5")
	t.Setenv("XW_CALLBACK_RETRY_BASE", "250ms")
	t.Setenv("XW_CALLBACK_RETRY_MAX", "4s")
	cfg = Load()
	if cfg.CallbackURL != "https://community.example.com/hooks/xw" {
		t.Errorf("XW_CALLBACK_URL 应去空白，实际 %q", cfg.CallbackURL)
	}
	if cfg.CallbackEvents != "trigger.received,delivery.failed" {
		t.Errorf("XW_CALLBACK_EVENTS 应去空白，实际 %q", cfg.CallbackEvents)
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
	// 密钥去空白后才进签名器：配置里的换行很容易让长度判断失真。
	if cfg.CallbackSecret != "s3cr3t-key-for-xw-notify-test" || !cfg.CallbackSigned() {
		t.Errorf("签名密钥未生效: %q", cfg.CallbackSecret)
	}
	retry := cfg.CallbackRetry()
	if retry.MaxAttempts != 5 || retry.Base != 250*time.Millisecond || retry.Max != 4*time.Second {
		t.Errorf("重试曲线未生效: %+v", retry)
	}
	if retry.Jitter <= 0 || retry.Jitter > 1 {
		t.Errorf("抖动应由引擎统一给定: %v", retry.Jitter)
	}
}

// 未配置密钥时不签名；重试曲线缺省是「三次、500ms 起步、10s 封顶」。
func TestCallbackDefaults(t *testing.T) {
	t.Setenv("XW_CALLBACK_SECRET", "")
	t.Setenv("XW_CALLBACK_RETRY_ATTEMPTS", "")
	t.Setenv("XW_CALLBACK_RETRY_BASE", "")
	t.Setenv("XW_CALLBACK_RETRY_MAX", "")
	cfg := Load()

	if cfg.CallbackSigned() {
		t.Error("未配置密钥时不应签名")
	}
	retry := cfg.CallbackRetry()
	if retry.MaxAttempts != webhook.DefaultRetryAttempts {
		t.Errorf("重试次数默认值不符: %d", retry.MaxAttempts)
	}
	if retry.Base != webhook.DefaultRetryBase || retry.Max != webhook.DefaultRetryMax {
		t.Errorf("退避默认值不符: %+v", retry)
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

// 渠道探测：未配置时 30s / 3s，写 off 或 0 表示关闭周期探测，
// 写错的值回退默认而不是静默停摆。
func TestProviderProbeConfig(t *testing.T) {
	cases := []struct {
		interval string
		timeout  string
		want     time.Duration
		enabled  bool
		limit    time.Duration
	}{
		{interval: "", timeout: "", want: provider.DefaultProbeInterval, enabled: true, limit: provider.DefaultProbeTimeout},
		{interval: "5s", timeout: "1s", want: 5 * time.Second, enabled: true, limit: time.Second},
		{interval: "off", timeout: "500ms", want: 0, enabled: false, limit: 500 * time.Millisecond},
		{interval: "0", timeout: "", want: 0, enabled: false, limit: provider.DefaultProbeTimeout},
		{interval: "每周", timeout: "", want: provider.DefaultProbeInterval, enabled: true, limit: provider.DefaultProbeTimeout},
	}
	for _, tc := range cases {
		t.Setenv("XW_PROVIDER_PROBE_INTERVAL", tc.interval)
		t.Setenv("XW_PROVIDER_PROBE_TIMEOUT", tc.timeout)
		cfg := Load()
		if cfg.ProviderProbeInterval != tc.want {
			t.Errorf("interval=%q 解析为 %s，期望 %s", tc.interval, cfg.ProviderProbeInterval, tc.want)
		}
		if cfg.ProviderProbeEnabled() != tc.enabled {
			t.Errorf("interval=%q 的启用判定 = %v，期望 %v", tc.interval, cfg.ProviderProbeEnabled(), tc.enabled)
		}
		if got := cfg.ProviderProbeTimeoutOrDefault(); got != tc.limit {
			t.Errorf("timeout=%q 解析为 %s，期望 %s", tc.timeout, got, tc.limit)
		}
		// 关闭周期探测时仍要给出一个可用的默认间隔，便于日志与手动探测。
		if cfg.ProviderProbeIntervalOrDefault() <= 0 {
			t.Errorf("interval=%q 的兜底间隔应为正数", tc.interval)
		}
	}
}

// 日志格式与级别的归一化：写错的取值既不能拦住启动，也不能把日志静默关掉。
func TestLogSettingsNormalization(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{"", LogFormatJSON},
		{"json", LogFormatJSON},
		{"JSON", LogFormatJSON},
		{"text", "text"},
		{" Text ", "text"},
		{"yaml", LogFormatJSON},
	}
	for _, tc := range cases {
		if got := (Config{LogFormat: tc.raw}).LogFormatOrDefault(); got != tc.want {
			t.Errorf("LogFormat=%q 应归一化为 %q，实际 %q", tc.raw, tc.want, got)
		}
	}

	levels := map[string]slog.Level{
		"":         slog.LevelInfo,
		"info":     slog.LevelInfo,
		"DEBUG":    slog.LevelDebug,
		"warning":  slog.LevelWarn,
		"WARN":     slog.LevelWarn,
		"ERROR":    slog.LevelError,
		"critical": slog.LevelError,
		"nonsense": slog.LevelInfo,
	}
	for raw, want := range levels {
		if got := (Config{LogLevel: raw}).LogLevelValue(); got != want {
			t.Errorf("LogLevel=%q 应解析为 %v，实际 %v", raw, want, got)
		}
	}
}

// 未配置时默认是结构化日志：与 API 侧同一套默认值，两个进程的日志能被同一条
// 采集规则处理。
func TestLogDefaults(t *testing.T) {
	var zero Config
	if zero.LogFormatOrDefault() != LogFormatJSON {
		t.Error("未配置日志格式时应输出 JSON")
	}
	if zero.LogLevelValue() != slog.LevelInfo {
		t.Error("未配置日志级别时应输出 INFO 及以上")
	}
}

func TestExecutionBackendConfig(t *testing.T) {
	cases := map[string]struct {
		want       string
		recognized bool
	}{
		"":           {ExecutionBackendJSONL, true},
		"jsonl":      {ExecutionBackendJSONL, true},
		"file":       {ExecutionBackendJSONL, true},
		"postgres":   {ExecutionBackendPostgres, true},
		"PostgreSQL": {ExecutionBackendPostgres, true},
		"pg":         {ExecutionBackendPostgres, true},
		"postgre":    {ExecutionBackendJSONL, false},
	}
	for raw, want := range cases {
		got, ok := (Config{ExecutionStore: raw}).ExecutionBackend()
		if got != want.want || ok != want.recognized {
			t.Errorf("ExecutionStore=%q 解析为 (%q,%v)，期望 (%q,%v)",
				raw, got, ok, want.want, want.recognized)
		}
	}
}

func TestPostgresPoolConfig(t *testing.T) {
	t.Setenv("XW_DATABASE_URL", "  postgres://u@localhost:5432/xw  ")
	t.Setenv("XW_PG_MAX_OPEN", "12")
	t.Setenv("XW_PG_MAX_IDLE", "6")
	t.Setenv("XW_PG_CONN_LIFETIME", "10m")
	t.Setenv("XW_PG_WRITE_TIMEOUT", "2s")

	cfg := Load()
	if cfg.DatabaseURL != "postgres://u@localhost:5432/xw" {
		t.Errorf("XW_DATABASE_URL 应去空白，实际 %q", cfg.DatabaseURL)
	}
	if cfg.PGMaxOpenConns != 12 || cfg.PGMaxIdleConns != 6 {
		t.Errorf("连接池参数解析错误: open=%d idle=%d", cfg.PGMaxOpenConns, cfg.PGMaxIdleConns)
	}
	if cfg.PGConnMaxLifetime != 10*time.Minute {
		t.Errorf("连接存活时间解析错误: %s", cfg.PGConnMaxLifetime)
	}
	if cfg.PGWriteTimeout != 2*time.Second {
		t.Errorf("写入预算解析错误: %s", cfg.PGWriteTimeout)
	}
}

func TestPostgresPoolDefaults(t *testing.T) {
	for _, key := range []string{"XW_PG_MAX_OPEN", "XW_PG_MAX_IDLE", "XW_PG_CONN_LIFETIME", "XW_PG_WRITE_TIMEOUT", "XW_PG_CONNECT_TIMEOUT"} {
		t.Setenv(key, "")
	}
	cfg := Load()
	if cfg.PGMaxOpenConns != defaultPGMaxOpen || cfg.PGMaxIdleConns != defaultPGMaxIdle {
		t.Errorf("连接池默认值错误: open=%d idle=%d", cfg.PGMaxOpenConns, cfg.PGMaxIdleConns)
	}
	if cfg.PGWriteTimeout != defaultPGWriteTimeout {
		t.Errorf("写入预算默认值错误: %s", cfg.PGWriteTimeout)
	}
}

func TestProfilingConfig(t *testing.T) {
	t.Setenv("XW_PPROF", "on")
	t.Setenv("XW_PPROF_BLOCK_RATE", "10000")
	t.Setenv("XW_PPROF_MUTEX_FRACTION", "-3")

	cfg := Load()
	if !cfg.PprofEnabled {
		t.Fatalf("XW_PPROF=on 应视为开启")
	}
	if cfg.PprofBlockRate != 10000 {
		t.Errorf("阻塞采样阈值应为 10000，实际 %d", cfg.PprofBlockRate)
	}
	if cfg.PprofMutexFraction != 0 {
		t.Errorf("负数采样比例应按 0 处理，实际 %d", cfg.PprofMutexFraction)
	}
}

func TestProfilingActiveRequiresLoopback(t *testing.T) {
	cases := []struct {
		name        string
		pprof       string
		adminAddr   string
		allowRemote string
		want        bool
	}{
		{name: "回环 IPv4", pprof: "1", adminAddr: "127.0.0.1:9090", want: true},
		{name: "localhost", pprof: "1", adminAddr: "localhost:9090", want: true},
		{name: "回环 IPv6", pprof: "1", adminAddr: "[::1]:9090", want: true},
		{name: "监听所有网卡", pprof: "1", adminAddr: ":9090", want: false},
		{name: "显式所有网卡", pprof: "1", adminAddr: "0.0.0.0:9090", want: false},
		{name: "内网地址", pprof: "1", adminAddr: "10.0.0.5:9090", want: false},
		{name: "显式允许远程", pprof: "1", adminAddr: "10.0.0.5:9090", allowRemote: "1", want: true},
		{name: "管理接口关闭", pprof: "1", adminAddr: "off", allowRemote: "1", want: false},
		{name: "未开采样", adminAddr: "127.0.0.1:9090", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("XW_PPROF", tc.pprof)
			t.Setenv("XW_PPROF_ALLOW_REMOTE", tc.allowRemote)
			t.Setenv("XW_ADMIN_ADDR", tc.adminAddr)

			if got := Load().ProfilingActive(); got != tc.want {
				t.Fatalf("adminAddr=%q allowRemote=%q 应为 %v，实际 %v",
					tc.adminAddr, tc.allowRemote, tc.want, got)
			}
		})
	}
}
