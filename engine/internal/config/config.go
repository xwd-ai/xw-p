// Package config 读取引擎运行时配置（全部由环境变量注入）。
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config 是引擎运行时配置，默认值与旧版 application.yml 保持一致。
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
	// StepTimeout 是单步执行预算，WorkflowTimeout 是一条消息一次执行的总预算。
	// 为 0 表示沿用管线默认值（连字符串都写不出来时不该悄悄变成「不限时」）。
	StepTimeout     time.Duration
	WorkflowTimeout time.Duration
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

		StepTimeout:     envDuration("XW_STEP_TIMEOUT", 0),
		WorkflowTimeout: envDuration("XW_WORKFLOW_TIMEOUT", 0),
	}
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
