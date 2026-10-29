// Package worker 实现通知消费与分发。
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
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

// Executor 把一条已解码的队列消息交给执行管线。
//
// 返回 error 表示消息需要重新投递（暂时性故障）；「消息本身不可修复」
// 这类结论由执行方包装 queue.ErrUndeliverable 表达，消费侧会确认并丢弃它。
// 未注入 Executor 时 worker 走 Dispatch 的 mock 日志，仅用于无管线的场景。
type Executor interface {
	Execute(msg queue.QueuedMessage) error
}

// Retrier 决定一条失败消息是「稍后重试」还是「进死信」。
//
// 由 queue.Retrier 实现（绑定退避策略）。未注入时退化为旧路径：不确认消息，
// 留在在途记录里等租约过期回收——那样重投没有次数上限，也无法退避。
type Retrier interface {
	Retry(ctx context.Context, d *queue.Delivery, cause error) (queue.RetryOutcome, error)
}

// Worker 从消息源消费通知并执行分发。
type Worker struct {
	consumer Consumer
	timeout  time.Duration
	logger   *slog.Logger
	// executor 执行工作流的步骤定义；为空时退化为 Dispatch 的 mock 日志。
	executor Executor
	// retrier 处理失败消息的退避重试；为空时保留在途记录等租约回收。
	retrier Retrier
	// id 是消费者在池内的序号，仅用于日志区分并发消费者；单实例运行为 0。
	id int
	// handle 是单条消息的处理入口，默认指向 Handle；测试用它注入失败场景。
	handle func([]byte) error
	// onStart / onFinish 在开始与结束处理一条消息时回调，池子用它统计在途数量。
	onStart  func()
	onFinish func()
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

// WithExecutor 注入工作流执行器，返回 worker 自身便于链式装配。
// 传 nil 表示恢复 mock 分发。
func (w *Worker) WithExecutor(ex Executor) *Worker {
	w.executor = ex
	return w
}

// WithRetrier 注入失败重试处理器，返回 worker 自身便于链式装配。
// 传 nil 表示失败消息不做退避重试，留在在途记录里等租约回收。
func (w *Worker) WithRetrier(r Retrier) *Worker {
	w.retrier = r
	return w
}

// Run 持续消费直到 ctx 取消。
func (w *Worker) Run(ctx context.Context) {
	w.logger.Info("[engine] worker 已启动，开始消费队列", "worker", w.id)
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("[engine] worker 已停止", "worker", w.id)
			return
		default:
		}

		delivery, err := w.consumer.Claim(ctx, w.timeout)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Warn("[engine] 领取消息失败，稍后重试", "worker", w.id, "error", err)
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
//
// 这里刻意不接收 ctx：消息一旦领取就要处理完，关闭信号只影响「是否继续领取」，
// 不会把请求半路掐断留下写了一半的收件箱卡片。
func (w *Worker) process(delivery *queue.Delivery) {
	if w.onStart != nil {
		w.onStart()
	}
	if w.onFinish != nil {
		defer w.onFinish()
	}
	err := w.handle(delivery.Raw)
	if err != nil {
		// 「消息本身不可修复」（格式错误、工作流不存在、步骤校验不通过）
		// 重投多少次都一样，确认后丢弃并告警，不让它一直占着在途记录。
		if errors.Is(err, ErrInvalidMessage) || errors.Is(err, queue.ErrUndeliverable) {
			w.logger.Warn("[engine] 丢弃不可投递的消息",
				"worker", w.id, "deliveryId", delivery.ID, "error", err)
			w.ack(delivery)
			return
		}

		// 暂时性故障：交给重试处理器安排退避重试或转死信。
		// 它会把这次领取从在途记录里摘走，因此这里不能确认，也不必等租约回收。
		if w.retrier != nil {
			w.scheduleRetry(delivery, err)
			return
		}

		// 没有重试处理器时保留在途记录，等租约过期由回收流程重新投递。
		w.logger.Warn("[engine] 处理消息失败，保留在途记录等待回收",
			"worker", w.id, "deliveryId", delivery.ID, "error", err)
		return
	}

	w.ack(delivery)
}

// scheduleRetry 把失败消息交给重试处理器，并按结果记录日志。
func (w *Worker) scheduleRetry(delivery *queue.Delivery, cause error) {
	// 与确认一样用独立上下文：失败处理本身也是一次状态迁移，不该被关闭信号打断
	// 而留下「在途记录还在、重试也没排上」的中间状态。
	ctx, cancel := context.WithTimeout(context.Background(), ackTimeout)
	defer cancel()

	outcome, err := w.retrier.Retry(ctx, delivery, cause)
	if err != nil {
		// 重试登记失败：消息仍在在途记录里，租约到期后会被回收重投。
		w.logger.Warn("[engine] 登记重试失败，保留在途记录等待回收",
			"worker", w.id, "deliveryId", delivery.ID, "error", err)
		return
	}
	if outcome.Dead {
		w.logger.Error("[engine] 消息重试次数耗尽，已转入死信",
			"worker", w.id,
			"deliveryId", delivery.ID,
			"attempts", outcome.Attempt,
			"firstFailedAt", outcome.FirstFailedAt.Format(time.RFC3339),
			"error", cause,
		)
		return
	}
	w.logger.Warn("[engine] 处理失败，已安排重试",
		"worker", w.id,
		"deliveryId", delivery.ID,
		"attempt", outcome.Attempt,
		"delay", outcome.Delay,
		"error", cause,
	)
}

// ack 确认一条消息，失败只告警：确认不上最多让消息被重新投递一次。
func (w *Worker) ack(delivery *queue.Delivery) {
	// 用独立上下文确认：关闭信号到达时，已经处理成功的消息不该被重新投递一次。
	ctx, cancel := context.WithTimeout(context.Background(), ackTimeout)
	defer cancel()
	if err := w.consumer.Ack(ctx, delivery.ID); err != nil {
		w.logger.Warn("[engine] 确认消息失败，稍后会被重新投递",
			"worker", w.id, "deliveryId", delivery.ID, "error", err)
	}
}

// Handle 解析一条原始消息并交给执行器（供测试直接调用）。
func (w *Worker) Handle(raw []byte) error {
	msg, err := queue.Decode(raw)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	if w.executor != nil {
		return w.executor.Execute(msg)
	}
	w.Dispatch(msg)
	return nil
}

// Dispatch 在未注入执行器时打印投递意图，不产生任何真实投递。
func (w *Worker) Dispatch(msg queue.QueuedMessage) {
	w.logger.Info("[mock] 发送通知",
		"triggerId", msg.TriggerID,
		"workflow", msg.Name,
		"subscriberId", msg.SubscriberID,
		"channels", msg.EffectiveChannels(),
		"payload", msg.Payload,
	)
}
