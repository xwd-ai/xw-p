// Command engine 是 xw-notify 的通知执行引擎入口。
//
// 职责：消费消息队列、执行工作流求值、digest 聚合与 provider 分发。
// 当前阶段：从 Redis 队列（LPUSH/BRPOP）消费消息并 mock 发送，
// 同时运行延迟调度器，按时唤醒 delay 步骤之后的续跑步骤。
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/cdgjhg/xw-p/engine/internal/config"
	"github.com/cdgjhg/xw-p/engine/internal/pipeline"
	"github.com/cdgjhg/xw-p/engine/internal/queue"
	"github.com/cdgjhg/xw-p/engine/internal/schedule"
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

	// 延迟调度器：内存存储 + 管线续跑。delay 步骤到期后把剩余步骤跑完。
	scheduler := newDelayScheduler(logger)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		scheduler.Run(ctx)
	}()

	w := worker.New(consumer, cfg.PollTimeoutDuration(), logger)
	w.Run(ctx)

	// worker 退出（ctx 取消）后等待调度器收尾，避免任务被中断在日志中途。
	wg.Wait()
}

// newDelayScheduler 组装延迟调度器：到期任务 → 解码续跑任务 → 执行剩余步骤。
//
// in_app 渠道写入默认收件箱存储，其余渠道走 console mock，
// 与本进程的投递语义保持一致。
func newDelayScheduler(logger *slog.Logger) *schedule.Scheduler {
	pipe := pipeline.New(logger)
	handler := func(ctx context.Context, item schedule.Item) error {
		deferral, results, err := pipe.Resume(item.Payload)
		if err != nil {
			return err
		}
		sent := 0
		for _, r := range results {
			if r.Sent {
				sent++
			}
			if r.Error != "" {
				logger.Warn("[engine] 续跑步骤失败",
					"workflow", deferral.WorkflowName,
					"stepId", r.StepID,
					"channel", string(r.Channel),
					"error", r.Error,
				)
			}
		}
		logger.Info("[engine] 延迟任务已续跑",
			"workflow", deferral.WorkflowName,
			"delayStep", deferral.StepID,
			"subscriberId", deferral.Message.SubscriberID,
			"steps", len(results),
			"sent", sent,
		)
		return nil
	}
	return schedule.New(schedule.NewMemoryStore(), handler, logger)
}
