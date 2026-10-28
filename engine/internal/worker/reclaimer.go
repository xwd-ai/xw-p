package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

// ReclaimerConsumer 是回收流程需要的最小消息源能力。
type ReclaimerConsumer interface {
	ReclaimExpired(ctx context.Context, batch int) (queue.ReclaimReport, error)
}

// DefaultReclaimInterval 是两次回收扫描之间的间隔。
//
// 回收本身是一次小范围扫描（只取过期租约，受 batch 限制），高频执行代价很低；
// 但也没必要比租约还密——真正刚过期的记录要等下一个周期才会被捡起。
const DefaultReclaimInterval = 30 * time.Second

// Reclaimer 周期性地把租约过期未确认的消息写回队列。
//
// 覆盖两类场景：本进程上次崩溃时留在在途记录里的消息（启动后的首轮扫描即可
// 捡回），以及同集群里某个实例异常退出后留下的消息（后续周期扫描捡回）。
type Reclaimer struct {
	consumer ReclaimerConsumer
	interval time.Duration
	batch    int
	logger   *slog.Logger
}

// NewReclaimer 创建回收器；interval 非正时回退默认值。
func NewReclaimer(consumer ReclaimerConsumer, interval time.Duration, logger *slog.Logger) *Reclaimer {
	if logger == nil {
		logger = slog.Default()
	}
	if interval <= 0 {
		interval = DefaultReclaimInterval
	}
	return &Reclaimer{consumer: consumer, interval: interval, batch: queue.DefaultReclaimBatch, logger: logger}
}

// Run 先立即扫描一轮，然后按间隔重复，直到 ctx 取消。
//
// 把首轮扫描放在这里而不是调用方，是为了让「启动即回收」成为不依赖调用顺序的
// 行为：进程重启后不必等一个完整间隔，崩溃前未确认的消息马上回到队列。
func (r *Reclaimer) Run(ctx context.Context) {
	r.logger.Info("[engine] 在途消息回收已启动", "interval", r.interval, "batch", r.batch)
	r.sweep(ctx)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("[engine] 在途消息回收已停止")
			return
		case <-ticker.C:
			r.sweep(ctx)
		}
	}
}

// Sweep 执行一轮回收，返回扫描结果（供测试与运维手动触发）。
func (r *Reclaimer) Sweep(ctx context.Context) (queue.ReclaimReport, error) {
	report, err := r.consumer.ReclaimExpired(ctx, r.batch)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			r.logger.Warn("[engine] 回收在途消息失败，下一轮重试", "error", err)
		}
		return report, err
	}
	if report.Scanned == 0 {
		return report, nil
	}
	for _, item := range report.Requeued {
		r.logger.Warn("[engine] 在途消息租约过期，已重新入队",
			"deliveryId", item.ID,
			"leaseExpiredAt", item.LeaseExpiredAt.Format(time.RFC3339),
			"bytes", len(item.Raw),
		)
	}
	if report.Orphans > 0 {
		r.logger.Warn("[engine] 清理了没有消息体的残留在途记录", "count", report.Orphans)
	}
	return report, nil
}

// sweep 是 Run 内部使用的容错版本：错误只记录，不打断循环。
func (r *Reclaimer) sweep(ctx context.Context) {
	_, _ = r.Sweep(ctx)
}
