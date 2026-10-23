// Package config 读取引擎运行时配置（全部由环境变量注入）。
package config

import (
	"os"
	"strconv"
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
