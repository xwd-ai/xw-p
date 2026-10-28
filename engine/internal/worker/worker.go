// Package worker 实现通知消费与分发。
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

// ackTimeout 是确认操作自身的超时。确认失败最多让消息被重新投递一次，
// 不值得用它拖住消费循环。
const ackTimeout = 5 * time.Second

// ErrInvalidMessage 表示消息体无法解析。这类消息重投多少次都不会变好，
// 直接丢弃并告警，避免占着在途记录反复回到队列。
var ErrInvalidMessage = errors.New("消息体无法解析")

// Consumer 抽象可靠消息源：领取一条消息，处理成功后确认。
type Consumer interface {
	Claim(ctx context.Context, timeout time.Duration) (*queue.Delivery, error)
	Ack(ctx context.Context, id string) error
}

// Worker 从消息源消费通知并执行分发。
// Phase 0：仅打印 mock 发送日志；真正的 provider 分发与落库在后续阶段接入。
type Worker struct {
	consumer Consumer
	timeout  time.Duration
	logger   *slog.Logger
	// handle 是单条消息的处理入口，默认指向 Handle；测试用它注入失败场景。
	handle func([]byte) error
}

// New 创建 worker。
func New(consumer Consumer, timeout time.Duration, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	w := &Worker{consumer: consumer, timeout: timeout, logger: logger}
	w.handle = w.Handle
	return w
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

		delivery, err := w.consumer.Claim(ctx, w.timeout)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Warn("[engine] 领取消息失败，稍后重试", "error", err)
			// 退避，避免消息源不可用时忙循环空转、刷屏日志。
			// 后续 Phase 会替换为带抖动的指数退避。
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if delivery == nil {
			continue // 等待超时，继续下一轮
		}
		w.process(delivery)
	}
}

// process 处理一条已领取的消息，并在成功（或确认无法处理）后确认。
func (w *Worker) process(delivery *queue.Delivery) {
	err := w.handle(delivery.Raw)
	if err != nil && !errors.Is(err, ErrInvalidMessage) {
		// 处理失败不确认：消息留在在途记录里，等回收流程重新投递，
		// 而不是像弹出即删除那样直接消失。
		w.logger.Warn("[engine] 处理消息失败，保留在途记录等待回收",
			"deliveryId", delivery.ID, "error", err)
		return
	}
	if err != nil {
		w.logger.Warn("[engine] 丢弃无法解析的消息", "deliveryId", delivery.ID, "error", err)
	}

	// 用独立上下文确认：关闭信号到达时，已经处理成功的消息不该被重新投递一次。
	ctx, cancel := context.WithTimeout(context.Background(), ackTimeout)
	defer cancel()
	if ackErr := w.consumer.Ack(ctx, delivery.ID); ackErr != nil {
		w.logger.Warn("[engine] 确认消息失败，稍后会被重新投递",
			"deliveryId", delivery.ID, "error", ackErr)
	}
}

// Handle 解析并分发一条原始消息（供测试直接调用）。
func (w *Worker) Handle(raw []byte) error {
	msg, err := queue.Decode(raw)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMessage, err)
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
