// Package config 读取引擎运行时配置（全部由环境变量注入）。
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/backoff"
	"github.com/xwd-ai/xw-p/engine/internal/provider"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
	"github.com/xwd-ai/xw-p/engine/internal/schedule"
	"github.com/xwd-ai/xw-p/engine/internal/webhook"
	"github.com/xwd-ai/xw-p/engine/internal/worker"
)

// Config 是引擎运行时配置，全部由环境变量注入，未配置时使用默认值。
type Config struct {
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	QueueKey      string
	PollTimeout   int // 秒
	EnginePort    int
	// LogFormat 选择日志输出：json（默认，一行一条，便于采集端按字段过滤）或
	// text（本地阅读）。LogLevel 是最低输出级别，写错时回退 INFO——
	// 一个拼错的值不该让引擎起不来，但也不能悄悄关掉全部日志。
	LogFormat string
	LogLevel  string
	// DigestStore 选择聚合卡片存储：redis（跨实例共享，默认）或 memory（进程内）。
	DigestStore string
	// DigestPrefix 是聚合卡片在 Redis 里的键前缀，用于环境隔离。
	DigestPrefix string
	// DelayStore 选择延迟任务存储：redis（跨实例共享、重启不丢，默认）或
	// memory（进程内）。DelayPrefix 是其键前缀，DelayLease 是认领后的
	// 可见性超时（超过该时长未确认的任务会被回收重投）。
	DelayStore  string
	DelayPrefix string
	DelayLease  time.Duration
	// StepTimeout 是单步执行预算，WorkflowTimeout 是一条消息一次执行的总预算。
	// 为 0 表示沿用管线默认值（连字符串都写不出来时不该悄悄变成「不限时」）。
	StepTimeout     time.Duration
	WorkflowTimeout time.Duration
	// ClaimLease 是消息被取出后的在途租约：超过该时长仍未确认，
	// 视为领取者已失效，消息会被重新投递。
	ClaimLease time.Duration
	// ReclaimInterval 是在途消息回收的扫描间隔，0 表示只做启动时的首轮扫描。
	ReclaimInterval time.Duration
	// Workers 是并发消费者数量；ShutdownGrace 是退出时等待在途消息的上限。
	Workers       int
	ShutdownGrace time.Duration
	// Prefetch 是每个消费者每次领取的条数：1 表示逐条领取，
	// 大于 1 时改走批量领取，把多次脚本往返摊成一次。
	Prefetch int
	// 失败重试：处理失败的消息按指数退避（带抖动）重新投递，
	// 次数耗尽后转死信。MaxAttempts <= 0 表示关闭重试。
	RetryMaxAttempts int
	RetryBaseDelay   time.Duration
	RetryMaxDelay    time.Duration
	RetryJitter      float64
	// RetryInterval 是把到期重试写回主队列的扫描间隔。
	RetryInterval time.Duration
	// WorkflowsFile 是工作流定义快照文件（JSON 数组）的路径，
	// 用于给没有携带步骤定义的消息提供按名称回退查找。为空表示不加载。
	WorkflowsFile string
	// ProvidersFile 是渠道配置文件（JSON）的路径：声明每个渠道用哪种实现、
	// 优先级、开关与凭据。为空表示使用内置的 console 实现。
	ProvidersFile string
	// ProviderProbeInterval / ProviderProbeTimeout 是渠道健康探测的周期与
	// 单次超时。间隔 <=0 表示不做周期探测（运维接口仍可手动触发）。
	ProviderProbeInterval time.Duration
	ProviderProbeTimeout  time.Duration
	// ExecutionFile 是投递结果回写的执行明细文件路径。引擎与 API 共用同一份
	// 数据目录，默认指向 api/data/executions.jsonl；写 off/disabled/none/- 表示
	// 不回写（只保留日志与执行汇总）。
	ExecutionFile string
	// ExecutionStore 选择执行明细的落库后端：jsonl（默认，本地文件）或 postgres。
	ExecutionStore string
	// DatabaseURL 是 Postgres 连接串，与 API 共用同一个 XW_DATABASE_URL：
	// 两侧必须指向同一个库，引擎写进去的明细 API 才读得到。
	DatabaseURL string
	// 以下四项是连接池参数。默认值偏保守：执行明细是旁路写入，不值得为它
	// 抢占数据库的连接配额；MaxIdleConns 会被夹紧到不超过 MaxOpenConns。
	PGMaxOpenConns    int
	PGMaxIdleConns    int
	PGConnMaxLifetime time.Duration
	PGConnMaxIdleTime time.Duration
	// PGConnectTimeout 是启动时连通性检查与预编译语句的超时；
	// PGWriteTimeout 是单条明细的写入预算，超时只影响这一条记录。
	PGConnectTimeout time.Duration
	PGWriteTimeout   time.Duration

	// PreferencesFile 是订阅者偏好文件路径。引擎在投递前按「订阅者 + 工作流 +
	// 渠道」解析这份文件，与 API 共用同一份数据目录；写 off/disabled/none/- 表示
	// 完全不做偏好判定（所有渠道按接收处理）。
	PreferencesFile string

	// CallbackURL 是投递结果回调的目标地址，为空表示不回调。回调走独立的
	// 有界队列异步发送，不占用消费路径；地址里通常带接入方签发的令牌，
	// 因此日志只出现协议与主机名。
	CallbackURL string
	// CallbackTimeout 是单次回调的超时，CallbackWorkers 是发送并发数，
	// CallbackQueue 是排队上限（满了丢弃事件并计数，不阻塞投递）。
	CallbackTimeout time.Duration
	CallbackWorkers int
	CallbackQueue   int
	// CallbackSecret 是回调报文的签名密钥，为空表示不签名。签名覆盖时间戳
	// 与正文，接收方可用它判断报文是否被改过、是否被重放。
	CallbackSecret string
	// CallbackRetryAttempts / Base / Max 是回调事件的重试曲线：
	// 可重试失败（连不上、超时、5xx、429）按指数退避再试，
	// 次数含首次尝试，写 1 表示不重试。
	CallbackRetryAttempts int
	CallbackRetryBase     time.Duration
	CallbackRetryMax      time.Duration
	// CallbackEvents 是订阅的事件类型，逗号分隔（all 表示全部）；为空时
	// 只订阅汇总事件 delivery.result。
	CallbackEvents string
	// CallbackAuditFile 是回调审计文件路径：每一次发送尝试一行，按事件
	// 标识可回查。CallbackEventFile 留存事件正文，供人工重放；写 off 关闭。
	CallbackAuditFile string
	CallbackEventFile string

	// AdminAddr 是运维接口（死信查询/重放）的监听地址。
	// 空字符串表示不启动；未配置 XW_ADMIN_ADDR 时沿用 ENGINE_PORT。
	AdminAddr string
}

// Load 从环境变量读取配置。
func Load() Config {
	addr := os.Getenv("XW_REDIS_ADDR")
	if addr == "" {
		addr = envOr("REDIS_HOST", "localhost") + ":" + envOr("REDIS_PORT", "6379")
	}
	return Config{
		RedisAddr:     addr,
		RedisPassword: os.Getenv("REDIS_PASSWORD"),
		RedisDB:       envInt("REDIS_DB", 0),
		QueueKey:      envOr("XW_REDIS_QUEUE_KEY", "xw:queue:events"),
		PollTimeout:   envInt("XW_POLL_TIMEOUT", 2),
		EnginePort:    envInt("ENGINE_PORT", 8081),
		LogFormat:     envOr("XW_LOG_FORMAT", LogFormatJSON),
		LogLevel:      envOr("XW_LOG_LEVEL", "INFO"),
		DigestStore:   envOr("XW_DIGEST_STORE", "redis"),
		DigestPrefix:  envOr("XW_DIGEST_PREFIX", "xw:digest"),
		DelayStore:    envOr("XW_DELAY_STORE", "redis"),
		DelayPrefix:   envOr("XW_DELAY_PREFIX", schedule.DefaultRedisPrefix),
		DelayLease:    envDuration("XW_DELAY_LEASE", schedule.DefaultClaimLease),

		StepTimeout:     envDuration("XW_STEP_TIMEOUT", 0),
		WorkflowTimeout: envDuration("XW_WORKFLOW_TIMEOUT", 0),
		ClaimLease:      envDuration("XW_CLAIM_LEASE", queue.DefaultClaimLease),
		ReclaimInterval: envDuration("XW_RECLAIM_INTERVAL", worker.DefaultReclaimInterval),

		Workers:       envInt("XW_WORKERS", worker.DefaultPoolSize),
		ShutdownGrace: envDuration("XW_SHUTDOWN_GRACE", worker.DefaultShutdownGrace),
		Prefetch:      envInt("XW_PREFETCH", worker.DefaultPrefetch),

		RetryMaxAttempts: envInt("XW_RETRY_MAX_ATTEMPTS", queue.DefaultMaxAttempts),
		RetryBaseDelay:   envDuration("XW_RETRY_BASE_DELAY", queue.DefaultRetryBase),
		RetryMaxDelay:    envDuration("XW_RETRY_MAX_DELAY", queue.DefaultRetryMax),
		RetryJitter:      envFloat("XW_RETRY_JITTER", queue.DefaultRetryJitter),
		RetryInterval:    envDuration("XW_RETRY_INTERVAL", worker.DefaultRetryInterval),

		WorkflowsFile:         strings.TrimSpace(os.Getenv("XW_WORKFLOWS_FILE")),
		ProvidersFile:         strings.TrimSpace(os.Getenv("XW_PROVIDERS_FILE")),
		ProviderProbeInterval: probeInterval(),
		ProviderProbeTimeout:  envDuration("XW_PROVIDER_PROBE_TIMEOUT", provider.DefaultProbeTimeout),
		ExecutionFile:         strings.TrimSpace(envOr("XW_EXECUTION_FILE", defaultExecutionFile)),
		ExecutionStore:        strings.TrimSpace(envOr("XW_EXECUTION_STORE", ExecutionBackendJSONL)),
		DatabaseURL:           strings.TrimSpace(os.Getenv("XW_DATABASE_URL")),
		PGMaxOpenConns:        envInt("XW_PG_MAX_OPEN", defaultPGMaxOpen),
		PGMaxIdleConns:        envInt("XW_PG_MAX_IDLE", defaultPGMaxIdle),
		PGConnMaxLifetime:     envDuration("XW_PG_CONN_LIFETIME", defaultPGConnLifetime),
		PGConnMaxIdleTime:     envDuration("XW_PG_CONN_IDLE_TIME", defaultPGConnIdleTime),
		PGConnectTimeout:      envDuration("XW_PG_CONNECT_TIMEOUT", defaultPGConnectTimeout),
		PGWriteTimeout:        envDuration("XW_PG_WRITE_TIMEOUT", defaultPGWriteTimeout),
		PreferencesFile:       strings.TrimSpace(envOr("XW_PREFERENCES_FILE", defaultPreferencesFile)),
		CallbackURL:           strings.TrimSpace(os.Getenv("XW_CALLBACK_URL")),
		CallbackTimeout:       envDuration("XW_CALLBACK_TIMEOUT", webhook.DefaultTimeout),
		CallbackWorkers:       envInt("XW_CALLBACK_WORKERS", webhook.DefaultWorkers),
		CallbackQueue:         envInt("XW_CALLBACK_QUEUE", webhook.DefaultQueueSize),
		CallbackSecret:        strings.TrimSpace(os.Getenv("XW_CALLBACK_SECRET")),

		CallbackRetryAttempts: envInt("XW_CALLBACK_RETRY_ATTEMPTS", webhook.DefaultRetryAttempts),
		CallbackRetryBase:     envDuration("XW_CALLBACK_RETRY_BASE", webhook.DefaultRetryBase),
		CallbackRetryMax:      envDuration("XW_CALLBACK_RETRY_MAX", webhook.DefaultRetryMax),
		CallbackEvents:        strings.TrimSpace(os.Getenv("XW_CALLBACK_EVENTS")),
		CallbackAuditFile:     strings.TrimSpace(envOr("XW_CALLBACK_AUDIT_FILE", defaultCallbackAuditFile)),
		CallbackEventFile:     strings.TrimSpace(envOr("XW_CALLBACK_EVENT_FILE", defaultCallbackEventFile)),

		AdminAddr: adminListenAddr(),
	}
}

// LogFormatJSON 是结构化日志的格式名；除 text 外的取值一律按它处理。
const LogFormatJSON = "json"

// LogFormatOrDefault 归一化日志格式：只认 text，其余（含空值、写错）走 json。
func (c Config) LogFormatOrDefault() string {
	if strings.EqualFold(strings.TrimSpace(c.LogFormat), "text") {
		return "text"
	}
	return LogFormatJSON
}

// LogLevelValue 把级别名转成 slog 级别，无法识别时回退 INFO。
func (c Config) LogLevelValue() slog.Level {
	switch strings.ToUpper(strings.TrimSpace(c.LogLevel)) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR", "CRITICAL":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// CallbackRetry 返回回调的重试曲线。
func (c Config) CallbackRetry() backoff.Policy {
	return backoff.Policy{
		MaxAttempts: c.CallbackRetryAttempts,
		Base:        c.CallbackRetryBase,
		Max:         c.CallbackRetryMax,
		Jitter:      webhook.DefaultRetryJitter,
	}
}

// CallbackSigned 判断是否为回调报文签名。
func (c Config) CallbackSigned() bool { return c.CallbackSecret != "" }

// CallbackDisabled 判断是否关闭了投递结果回调。
func (c Config) CallbackDisabled() bool {
	switch strings.ToLower(strings.TrimSpace(c.CallbackURL)) {
	case "", "off", "disabled", "none", "-":
		return true
	}
	return false
}

// defaultCallbackAuditFile / defaultCallbackEventFile 是回调审计与事件正文的
// 默认路径，与执行明细放在同一份数据目录下。
const (
	defaultCallbackAuditFile = "api/data/callback_audit.jsonl"
	defaultCallbackEventFile = "api/data/callback_events.jsonl"
)

// CallbackAuditDisabled 判断是否关闭了回调审计。
func (c Config) CallbackAuditDisabled() bool {
	switch strings.ToLower(strings.TrimSpace(c.CallbackAuditFile)) {
	case "", "off", "disabled", "none", "-":
		return true
	}
	return false
}

// CallbackArchiveDisabled 判断是否关闭了事件正文留存（不留存即无法重放）。
func (c Config) CallbackArchiveDisabled() bool {
	switch strings.ToLower(strings.TrimSpace(c.CallbackEventFile)) {
	case "", "off", "disabled", "none", "-":
		return true
	}
	return false
}

// defaultExecutionFile 是执行明细回写的默认路径。
// 与收件箱（api/data/inbox.jsonl）同理：引擎写、API 读，两个进程共用一份
// 数据目录，因此默认指向 API 侧的执行明细文件。
const defaultExecutionFile = "api/data/executions.jsonl"

// 执行明细的两个后端。JSONL 是默认值：单机部署不需要数据库，零依赖即可跑通；
// Postgres 用在多实例场景，写入与查询都交给同一张 execution_details 表。
const (
	ExecutionBackendJSONL    = "jsonl"
	ExecutionBackendPostgres = "postgres"
)

// 连接池默认值。与 execution.PostgresConfig 的默认值保持同一组数字，
// 配置层不重复夹紧，最终取值由存储层统一处理。
const (
	defaultPGMaxOpen        = 8
	defaultPGMaxIdle        = 4
	defaultPGConnLifetime   = 30 * time.Minute
	defaultPGConnIdleTime   = 5 * time.Minute
	defaultPGConnectTimeout = 5 * time.Second
	defaultPGWriteTimeout   = 5 * time.Second
)

// ExecutionBackend 返回规范化后的执行明细后端，第二个返回值表示配置值是否被识别。
//
// 写错的值回退 jsonl 并由调用方记一条日志：拼错一个变量不该让引擎起不来，
// 但也不能悄悄写到另一个地方——「配了 postgres、实际写文件」是最难发现的一类
// 配置错误，因为两种情况都能跑。
func (c Config) ExecutionBackend() (string, bool) {
	switch strings.ToLower(strings.TrimSpace(c.ExecutionStore)) {
	case ExecutionBackendPostgres, "postgresql", "pg":
		return ExecutionBackendPostgres, true
	case "", ExecutionBackendJSONL, "file":
		return ExecutionBackendJSONL, true
	default:
		return ExecutionBackendJSONL, false
	}
}

// defaultPreferencesFile 是订阅者偏好文件的默认路径。
//
// 偏好由 API 写入、引擎读取，两个进程共用同一份数据目录，因此默认值与 API 侧
// 的 XW_PREFERENCES_FILE 保持一致。
const defaultPreferencesFile = "api/data/preferences.jsonl"

// PreferencesDisabled 判断是否关闭了投递前的偏好判定。
func (c Config) PreferencesDisabled() bool {
	switch strings.ToLower(strings.TrimSpace(c.PreferencesFile)) {
	case "", "off", "disabled", "none", "-":
		return true
	}
	return false
}

// ExecutionDisabled 判断是否关闭了执行明细回写。
func (c Config) ExecutionDisabled() bool {
	switch strings.ToLower(strings.TrimSpace(c.ExecutionFile)) {
	case "", "off", "disabled", "none", "-":
		return true
	}
	return false
}

// probeInterval 解析渠道探测间隔。
//
// off / disabled / 0 都表示关闭周期探测：健康状态仍由投递结果更新，运维接口
// 也还能手动探一次。写错的值回退默认而不是静默停摆——一个拼写错误让探测
// 再也不跑，比探测间隔变成 30 秒要难发现得多。
func probeInterval() time.Duration {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("XW_PROVIDER_PROBE_INTERVAL")))
	switch raw {
	case "off", "disabled", "none", "-":
		return 0
	}
	return envDuration("XW_PROVIDER_PROBE_INTERVAL", provider.DefaultProbeInterval)
}

// ProviderProbeEnabled 判断是否开启周期探测。
func (c Config) ProviderProbeEnabled() bool { return c.ProviderProbeInterval > 0 }

// ProviderProbeIntervalOrDefault 返回生效的探测间隔。
func (c Config) ProviderProbeIntervalOrDefault() time.Duration {
	if c.ProviderProbeInterval > 0 {
		return c.ProviderProbeInterval
	}
	return provider.DefaultProbeInterval
}

// ProviderProbeTimeoutOrDefault 返回生效的单次探测超时。
func (c Config) ProviderProbeTimeoutOrDefault() time.Duration {
	if c.ProviderProbeTimeout > 0 {
		return c.ProviderProbeTimeout
	}
	return provider.DefaultProbeTimeout
}

// adminListenAddr 解析运维接口的监听地址。
//
// XW_ADMIN_ADDR 显式配置优先：留空或写 off/disabled 表示关闭，
// 其余写法按原样监听（如 127.0.0.1:8081，只对本机开放）。
// 未配置时沿用 ENGINE_PORT，让运维接口有址可寻，又不必默认对外暴露。
func adminListenAddr() string {
	if raw, ok := os.LookupEnv("XW_ADMIN_ADDR"); ok {
		raw = strings.TrimSpace(raw)
		switch strings.ToLower(raw) {
		case "off", "disabled", "none", "-":
			return ""
		}
		return raw
	}
	return fmt.Sprintf("127.0.0.1:%d", envInt("ENGINE_PORT", 8081))
}

// WorkersOrDefault 返回生效的并发度：非法值回退默认，并收敛到上限以内。
func (c Config) WorkersOrDefault() int {
	if c.Workers <= 0 {
		return worker.DefaultPoolSize
	}
	return worker.NormalizePoolSize(c.Workers)
}

// ShutdownGraceOrDefault 返回退出等待上限。
func (c Config) ShutdownGraceOrDefault() time.Duration {
	if c.ShutdownGrace <= 0 {
		return worker.DefaultShutdownGrace
	}
	return c.ShutdownGrace
}

// PrefetchOrDefault 返回生效的预取条数：非法值回退逐条领取。
func (c Config) PrefetchOrDefault() int {
	return worker.NormalizePrefetch(c.Prefetch)
}

// ClaimLeaseOrDefault 返回在途租约；非法值回退默认，避免租约变成 0 导致
// 消息刚取出就被判定为失效。
func (c Config) ClaimLeaseOrDefault() time.Duration {
	if c.ClaimLease <= 0 {
		return queue.DefaultClaimLease
	}
	return c.ClaimLease
}

// DelayLeaseOrDefault 返回延迟任务的认领租约；非法值回退默认，
// 避免延迟任务刚认领就被判为过期、同一份通知被反复重投。
func (c Config) DelayLeaseOrDefault() time.Duration {
	if c.DelayLease <= 0 {
		return schedule.DefaultClaimLease
	}
	return c.DelayLease
}

// RetryPolicyOrDefault 返回生效的重试策略。
//
// MaxAttempts 为负按「关闭重试」处理；其余字段为 0 时补默认值——
// 配错一个延迟不该把重试退化成毫秒级忙循环。
func (c Config) RetryPolicyOrDefault() queue.RetryPolicy {
	policy := queue.RetryPolicy{
		MaxAttempts: c.RetryMaxAttempts,
		Base:        c.RetryBaseDelay,
		Max:         c.RetryMaxDelay,
		Jitter:      c.RetryJitter,
	}
	if policy.MaxAttempts < 0 {
		policy.MaxAttempts = 0
	}
	return policy.Normalize()
}

// PollTimeoutDuration 返回阻塞拉取的超时时间。
func (c Config) PollTimeoutDuration() time.Duration {
	if c.PollTimeout <= 0 {
		return time.Second
	}
	return time.Duration(c.PollTimeout) * time.Second
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// envFloat 读取浮点配置（如抖动比例），无法解析时回退默认值。
func envFloat(key string, fallback float64) float64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	if v, err := strconv.ParseFloat(raw, 64); err == nil {
		return v
	}
	return fallback
}

// envDuration 读取 Go duration 字符串（如 10s / 500ms）。
// 纯数字按秒处理，便于沿用旧配置习惯；无法解析时回退默认值。
func envDuration(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	if d, err := time.ParseDuration(raw); err == nil {
		return d
	}
	if n, err := strconv.Atoi(raw); err == nil {
		return time.Duration(n) * time.Second
	}
	return fallback
}
