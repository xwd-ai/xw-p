package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// DefaultRetryInterval 是两轮「到期重试写回队列」之间的间隔。
//
// 重试的等待时长最短从秒级起步，扫描比它密集没有意义；但也不能太粗，
// 否则 2 秒的退避会被拉长到扫描周期，退避曲线就失真了。
const DefaultRetryInterval = time.Second

// RetryQueue 是重试泵需要的最小能力。
type RetryQueue interface {
	PromoteDueRetries(ctx context.Context, batch int) ([]queue.Promoted, error)
}

// RetryPumper 周期性地把到期的重试消息写回主队列。
//
// 重试消息先按退避时间放进 Redis 的有序集合，到期后才回到主队列。这一步是
// 「等」与「跑」之间的连接点：没有它，重试记录只会静静躺在 zset 里，
// 消息看起来像丢了。
type RetryPumper struct {
	queue    RetryQueue
	interval time.Duration
	batch    int
	logger   *slog.Logger
}

// NewRetryPumper 创建重试泵；interval 非正时回退默认值。
func NewRetryPumper(q RetryQueue, interval time.Duration, logger *slog.Logger) *RetryPumper {
	if logger == nil {
		logger = slog.Default()
	}
	if interval <= 0 {
		interval = DefaultRetryInterval
	}
	return &RetryPumper{queue: q, interval: interval, batch: queue.DefaultRetryBatch, logger: logger}
}

// Run 按间隔循环，直到 ctx 取消。
func (r *RetryPumper) Run(ctx context.Context) {
	r.logger.Info("[engine] 重试调度已启动", "interval", r.interval, "batch", r.batch)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		r.pump(ctx)
		select {
		case <-ctx.Done():
			r.logger.Info("[engine] 重试调度已停止")
			return
		case <-ticker.C:
		}
	}
}

// Pump 执行一轮提升，返回本次写回主队列的消息（供测试与运维手动触发）。
func (r *RetryPumper) Pump(ctx context.Context) ([]queue.Promoted, error) {
	promoted, err := r.queue.PromoteDueRetries(ctx, r.batch)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			r.logger.Warn("[engine] 提升到期重试失败，下一轮重试", "error", err)
		}
		return nil, err
	}
	for _, item := range promoted {
		r.logger.Info("[engine] 到期重试已写回队列",
			"deliveryId", item.ID,
			"attempts", item.Attempts,
			"bytes", len(item.Raw),
		)
	}
	return promoted, nil
}

func (r *RetryPumper) pump(ctx context.Context) {
	_, _ = r.Pump(ctx)
}
