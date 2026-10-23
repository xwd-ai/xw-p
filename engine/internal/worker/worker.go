// Package worker 实现通知消费与分发。
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

// Consumer 抽象消息源，便于测试用内存实现替换 Redis。
type Consumer interface {
	Next(ctx context.Context, timeout time.Duration) ([]byte, error)
}

// Worker 从消息源消费通知并执行 mock 分发。
// Phase 0：仅打印 mock 发送日志；真正的 provider 分发与落库在后续阶段接入。
type Worker struct {
	consumer Consumer
	timeout  time.Duration
	logger   *slog.Logger
}

// New 创建 worker。
func New(consumer Consumer, timeout time.Duration, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	return &Worker{consumer: consumer, timeout: timeout, logger: logger}
}

// Run 持续消费直到 ctx 取消。
func (w *Worker) Run(ctx context.Context) {
	w.logger.Info("[engine] worker 已启动，开始消费队列")
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("[engine] worker 已停止")
			return
		default:
		}

		raw, err := w.consumer.Next(ctx, w.timeout)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Warn("[engine] 消费消息失败，稍后重试", "error", err)
			// 退避，避免消息源不可用时忙循环空转、刷屏日志。
			// 后续 Phase 会替换为带抖动的指数退避。
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if raw == nil {
			continue // 超时，继续下一轮
		}
		if err := w.Handle(raw); err != nil {
			w.logger.Warn("[engine] 处理消息失败，跳过并继续", "error", err)
		}
	}
}

// Handle 解析并分发一条原始消息（供测试直接调用）。
func (w *Worker) Handle(raw []byte) error {
	msg, err := queue.Decode(raw)
	if err != nil {
		return err
	}
	w.Dispatch(msg)
	return nil
}

// Dispatch 执行 mock 投递。
func (w *Worker) Dispatch(msg queue.QueuedMessage) {
	w.logger.Info("[mock] 发送通知",
		"triggerId", msg.TriggerID,
		"workflow", msg.Name,
		"subscriberId", msg.SubscriberID,
		"channels", msg.EffectiveChannels(),
		"payload", msg.Payload,
	)
}
