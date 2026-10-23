// Command engine 是 xw-notify 的通知执行引擎入口。
//
// 职责：消费消息队列、执行工作流求值、digest 聚合与 provider 分发。
// Phase 0：从 Redis 队列（LPUSH/BRPOP）消费消息并 mock 发送。
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/cdgjhg/xw-p/engine/internal/config"
	"github.com/cdgjhg/xw-p/engine/internal/queue"
	"github.com/cdgjhg/xw-p/engine/internal/worker"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumer := queue.NewRedisConsumer(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, cfg.QueueKey)
	defer consumer.Close()

	pingCtx, cancel := context.WithTimeout(ctx, cfg.PollTimeoutDuration())
	err := consumer.Ping(pingCtx)
	cancel()
	if err != nil {
		logger.Warn("[engine] Redis 连接检查失败，稍后在消费时重试", "redis", cfg.RedisAddr, "error", err)
	}

	logger.Info("[engine] 启动",
		"redis", cfg.RedisAddr,
		"queue", cfg.QueueKey,
		"pollTimeout", cfg.PollTimeoutDuration(),
	)

	w := worker.New(consumer, cfg.PollTimeoutDuration(), logger)
	w.Run(ctx)
}
