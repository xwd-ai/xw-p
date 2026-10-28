// Command engine 是 xw-notify 的通知执行引擎入口。
//
// 职责：消费消息队列、执行工作流求值、digest 聚合与 provider 分发。
// 当前阶段：从 Redis 队列可靠消费（取出即登记在途、处理成功才确认，
// 租约过期未确认的消息由回收流程重新入队），
// 同时运行延迟调度器，按时唤醒 delay 步骤之后的续跑步骤。
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/redis/go-redis/v9"

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

	consumer := queue.NewReliableConsumer(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, cfg.QueueKey, cfg.ClaimLeaseOrDefault())
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
		"claimLease", cfg.ClaimLeaseOrDefault(),
		"reclaimInterval", cfg.ReclaimInterval,
	)

	// 聚合卡片存储：多实例共享走 Redis，Redis 不可用时回退进程内存
	// （单实例语义正确，多实例会各开一张卡，日志里明示这件事）。
	pipe, closeDigest := newPipeline(logger, cfg)
	defer closeDigest()

	// 延迟调度器：到期任务 → 管线续跑。delay 步骤到期后把剩余步骤跑完。
	scheduler := newDelayScheduler(logger, pipe)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		scheduler.Run(ctx)
	}()

	// 在途消息回收：启动即扫描一轮，捡回上次崩溃时未确认的消息，
	// 之后周期扫描同集群其他实例异常退出留下的记录。
	if cfg.ReclaimInterval > 0 {
		wg.Add(1)
		reclaimer := worker.NewReclaimer(consumer, cfg.ReclaimInterval, logger)
		go func() {
			defer wg.Done()
			reclaimer.Run(ctx)
		}()
	} else {
		logger.Info("[engine] 在途消息回收已关闭", "reason", "XW_RECLAIM_INTERVAL<=0")
	}

	w := worker.New(consumer, cfg.PollTimeoutDuration(), logger)
	w.Run(ctx)

	// worker 退出（ctx 取消）后等待调度器收尾，避免任务被中断在日志中途。
	wg.Wait()
}

// newDelayScheduler 组装延迟调度器：到期任务 → 解码续跑任务 → 执行剩余步骤。
//
// in_app 渠道写入默认收件箱存储，其余渠道走 console mock，
// 与本进程的投递语义保持一致。
func newDelayScheduler(logger *slog.Logger, pipe *pipeline.Pipeline) *schedule.Scheduler {
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

// newPipeline 组装执行管线并选定聚合卡片存储，返回管线与资源释放函数。
//
// 默认使用 Redis 存储：多个引擎实例共享同一张聚合卡片，计数走 HINCRBY，
// 窗口靠键的 TTL 收敛。显式配置 XW_DIGEST_STORE=memory 或 Redis 不可用时
// 回退进程内存储——那意味着多实例会各开一张卡，只在单实例部署下成立。
func newPipeline(logger *slog.Logger, cfg config.Config) (*pipeline.Pipeline, func()) {
	pipe := pipeline.New(logger).WithTimeouts(timeoutPolicy(cfg))
	logger.Info("[engine] 执行超时预算",
		"step", pipe.Timeouts().Step,
		"workflow", pipe.Timeouts().Workflow,
	)

	if strings.EqualFold(strings.TrimSpace(cfg.DigestStore), "memory") {
		logger.Info("[engine] 聚合卡片存储：进程内存（单实例语义）")
		return pipe, func() {}
	}

	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	pingCtx, cancel := context.WithTimeout(context.Background(), cfg.PollTimeoutDuration())
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		logger.Warn("[engine] 聚合卡片存储连接 Redis 失败，本次回退进程内存；多实例下计数会各算各的",
			"redis", cfg.RedisAddr, "error", err)
		_ = client.Close()
		return pipe, func() {}
	}

	store := pipeline.NewRedisDigestStoreWithPrefix(client, cfg.DigestPrefix)
	pipe.WithDigestStore(store)
	logger.Info("[engine] 聚合卡片存储：Redis", "redis", cfg.RedisAddr, "prefix", store.Prefix())
	return pipe, func() { _ = client.Close() }
}

// timeoutPolicy 组装超时策略：环境变量未配置时沿用管线默认值。
func timeoutPolicy(cfg config.Config) pipeline.TimeoutPolicy {
	policy := pipeline.DefaultTimeoutPolicy()
	if cfg.StepTimeout > 0 {
		policy.Step = cfg.StepTimeout
	}
	if cfg.WorkflowTimeout > 0 {
		policy.Workflow = cfg.WorkflowTimeout
	}
	return policy
}
