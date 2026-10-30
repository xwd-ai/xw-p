// Package config 读取引擎运行时配置（全部由环境变量注入）。
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
	"github.com/xwd-ai/xw-p/engine/internal/schedule"
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

		RetryMaxAttempts: envInt("XW_RETRY_MAX_ATTEMPTS", queue.DefaultMaxAttempts),
		RetryBaseDelay:   envDuration("XW_RETRY_BASE_DELAY", queue.DefaultRetryBase),
		RetryMaxDelay:    envDuration("XW_RETRY_MAX_DELAY", queue.DefaultRetryMax),
		RetryJitter:      envFloat("XW_RETRY_JITTER", queue.DefaultRetryJitter),
		RetryInterval:    envDuration("XW_RETRY_INTERVAL", worker.DefaultRetryInterval),

		WorkflowsFile: strings.TrimSpace(os.Getenv("XW_WORKFLOWS_FILE")),
		AdminAddr:     adminListenAddr(),
	}
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
