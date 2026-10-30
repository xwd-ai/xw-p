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

// BatchConsumer 是支持批量领取、批量确认的消息源。
//
// 单条领取时每条消息都要一次脚本往返（领取一次、确认一次），消费者越多，
// 压给 Redis 的单线程执行队列越长。批量接口把 N 条消息的往返摊成一次，
// 语义与单条完全一致：取出即登记在途，确认才删除。
type BatchConsumer interface {
	Consumer
	ClaimBatch(ctx context.Context, max int) ([]*queue.Delivery, error)
	AckBatch(ctx context.Context, ids []string) error
}

// DefaultPrefetch 是未配置时每次领取的条数：1 表示逐条领取，与批量能力无关。
const DefaultPrefetch = 1

// MaxPrefetch 是每次领取的条数上限。
const MaxPrefetch = queue.MaxBatchClaim

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
	// prefetch 是每次向消息源领取的条数；<=1 表示逐条领取。
	prefetch int
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

// WithPrefetch 设置每次领取的条数，返回 worker 自身便于链式装配。
//
// 只有消息源实现了 BatchConsumer 且条数大于 1 时才走批量领取；
// 其余情况保持逐条领取，传 0 或负数同样回落到逐条。
func (w *Worker) WithPrefetch(n int) *Worker {
	w.prefetch = NormalizePrefetch(n)
	return w
}

// NormalizePrefetch 把预取条数收敛到 [1, MaxPrefetch]。
func NormalizePrefetch(n int) int {
	if n < DefaultPrefetch {
		return DefaultPrefetch
	}
	if n > MaxPrefetch {
		return MaxPrefetch
	}
	return n
}

// Run 持续消费直到 ctx 取消。
func (w *Worker) Run(ctx context.Context) {
	w.logger.Info("[engine] worker 已启动，开始消费队列", "worker", w.id)
	if bc, ok := w.consumer.(BatchConsumer); ok && w.prefetch > DefaultPrefetch {
		w.runBatched(ctx, bc)
		return
	}
	w.runSingle(ctx)
}

// runSingle 逐条领取：一条消息一次领取脚本、一次确认脚本。
func (w *Worker) runSingle(ctx context.Context) {
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
		w.process(delivery, nil)
	}
}

// runBatched 按批领取：一次脚本取回至多 prefetch 条，处理完再一次性确认。
//
// 队列空时退回逐条领取，借用它的阻塞等待，避免批量接口空转抢 CPU。
// 已在手里的这批消息一定处理完才退出：它们已经登记在途，半路撒手只会让
// 租约到期后再投一遍。
func (w *Worker) runBatched(ctx context.Context, bc BatchConsumer) {
	for {
		if ctx.Err() != nil {
			w.logger.Info("[engine] worker 已停止", "worker", w.id)
			return
		}

		batch, err := bc.ClaimBatch(ctx, w.prefetch)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Warn("[engine] 批量领取失败，稍后重试", "worker", w.id, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if len(batch) == 0 {
			delivery, err := w.consumer.Claim(ctx, w.timeout)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				w.logger.Warn("[engine] 领取消息失败，稍后重试", "worker", w.id, "error", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}
			if delivery == nil {
				continue
			}
			w.process(delivery, nil)
			continue
		}

		done := make([]string, 0, len(batch))
		collect := func(d *queue.Delivery) { done = append(done, d.ID) }
		for _, delivery := range batch {
			w.process(delivery, collect)
		}
		if len(done) > 0 {
			w.ackMany(bc, done)
		}
	}
}

// process 处理一条已领取的消息，并在成功（或确认无法处理）后确认。
//
// 这里刻意不接收 ctx：消息一旦领取就要处理完，关闭信号只影响「是否继续领取」，
// 不会把请求半路掐断留下写了一半的收件箱卡片。
//
// collect 非空时把「该确认的凭据」交给调用方（批量路径），由它凑齐后一次确认；
// 为空时立即确认（逐条路径）。
func (w *Worker) process(delivery *queue.Delivery, collect func(*queue.Delivery)) {
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
			w.confirm(delivery, collect)
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

	w.confirm(delivery, collect)
}

// confirm 决定这次确认是立刻发出去还是攒到批次结束。
func (w *Worker) confirm(delivery *queue.Delivery, collect func(*queue.Delivery)) {
	if collect != nil {
		collect(delivery)
		return
	}
	w.ack(delivery)
}

// ackMany 一次确认一批消息，失败只告警：确认不上最多让它们被重新投递一次。
func (w *Worker) ackMany(bc BatchConsumer, ids []string) {
	ctx, cancel := context.WithTimeout(context.Background(), ackTimeout)
	defer cancel()
	if err := bc.AckBatch(ctx, ids); err != nil {
		w.logger.Warn("[engine] 批量确认失败，稍后会被重新投递",
			"worker", w.id, "count", len(ids), "error", err)
	}
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
