package webhook

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultWorkers 是发送回调的并发数。
	//
	// 回调是旁路：消费路径只负责把事件塞进队列，真正的 HTTP 请求在这里
	// 发生。两个 goroutine 足够跑满常见接收方，再多只会在对方限流时把
	// 并发问题挪到网络层。
	DefaultWorkers = 2

	// DefaultQueueSize 是事件队列的长度。
	//
	// 队列满说明接收方持续慢于产出，此时阻塞消费是更坏的选择：宁可丢掉
	// 回执（有执行明细可查）也不能让通知投递停摆。
	DefaultQueueSize = 256

	// MaxWorkers / MaxQueueSize 是配置上限，防止一个数字写错就开出上千个
	// goroutine 或几百 MB 的缓冲。
	MaxWorkers   = 16
	MaxQueueSize = 8192
)

// Options 是分发器的可调参数。
type Options struct {
	// Workers 是发送并发数，非正数用 DefaultWorkers。
	Workers int
	// QueueSize 是缓冲队列长度，非正数用 DefaultQueueSize。
	QueueSize int
	// Timeout 是单次回调的超时，非正数用 DefaultTimeout。
	Timeout time.Duration
	// Logger 为空时用默认 logger。
	Logger *slog.Logger
}

// Normalized 把越界参数收敛到合法区间，供装配方在启动日志里打印实际生效的取值。
func (o Options) Normalized() Options {
	if o.Workers <= 0 {
		o.Workers = DefaultWorkers
	}
	if o.Workers > MaxWorkers {
		o.Workers = MaxWorkers
	}
	if o.QueueSize <= 0 {
		o.QueueSize = DefaultQueueSize
	}
	if o.QueueSize > MaxQueueSize {
		o.QueueSize = MaxQueueSize
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.Timeout > MaxTimeout {
		o.Timeout = MaxTimeout
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

// Stats 是分发器的运行计数。
type Stats struct {
	// Enqueued 是成功入队的事件数，Delivered / Failed 是发送结果。
	Enqueued  int64
	Delivered int64
	Failed    int64
	// Dropped 是因队列已满或已关闭而丢弃的事件数。
	Dropped int64
	// Pending 是当前排队等待发送的事件数。
	Pending int
}

// Dispatcher 把事件异步提交到回调地址。
//
// 消费路径调用 Notify 只做一次非阻塞入队，HTTP 请求在独立 goroutine 里
// 发生；因此接收方变慢不会拖长消息处理时间，也不会占住队列租约。
type Dispatcher struct {
	target  Target
	sender  Sender
	logger  *slog.Logger
	timeout time.Duration
	queue   chan Event

	mu     sync.RWMutex
	closed bool

	wg sync.WaitGroup

	enqueued  atomic.Int64
	delivered atomic.Int64
	failed    atomic.Int64
	dropped   atomic.Int64
}

// NewDispatcher 创建分发器并启动发送 goroutine。
//
// target 为空或 sender 为空时返回 nil：调用方按「未配置回调」处理，
// 不必在每次投递后判空两次。
func NewDispatcher(target Target, sender Sender, opts Options) *Dispatcher {
	if target.IsZero() || sender == nil {
		return nil
	}
	opts = opts.Normalized()

	d := &Dispatcher{
		target:  target,
		sender:  sender,
		logger:  opts.Logger,
		timeout: opts.Timeout,
		queue:   make(chan Event, opts.QueueSize),
	}
	d.wg.Add(opts.Workers)
	for i := 0; i < opts.Workers; i++ {
		go d.worker()
	}
	return d
}

// Target 返回回调地址（脱敏值由 Target.String 给出）。
func (d *Dispatcher) Target() Target { return d.target }

// Notify 尝试把事件放进队列，返回是否入队成功。
//
// 非阻塞是刻意的：这里运行在消费路径上，等队列腾位置意味着一条慢接收方
// 的回调能把整个消费池拖停。丢弃要能看见，所以计数并记 Warn。
func (d *Dispatcher) Notify(ev Event) bool {
	if d == nil {
		return false
	}
	if err := ev.Validate(); err != nil {
		d.logger.Warn("[webhook] 回调事件不合法，已丢弃",
			"triggerId", ev.TriggerID,
			"error", err,
		)
		return false
	}

	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		d.dropped.Add(1)
		return false
	}
	select {
	case d.queue <- ev:
		d.enqueued.Add(1)
		return true
	default:
		dropped := d.dropped.Add(1)
		d.logger.Warn("[webhook] 回调队列已满，本次结果不再回调",
			"triggerId", ev.TriggerID,
			"eventId", ev.ID,
			"dropped", dropped,
			"hint", "接收方持续慢于产出，可调大 XW_CALLBACK_QUEUE 或检查接收方",
		)
		return false
	}
}

// Close 停止收新事件，等队列里的事件发完。
//
// 最多等到 ctx 结束：退出流程不该被一个不响应的接收方拖住，未发完的
// 事件记为丢弃。
func (d *Dispatcher) Close(ctx context.Context) error {
	if d == nil {
		return nil
	}
	if ctx == nil {
		// 退出路径上偶尔会传空上下文；按「不限时」处理，好过 panic。
		ctx = context.Background()
	}

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	close(d.queue)
	d.mu.Unlock()

	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		pending := len(d.queue)
		d.dropped.Add(int64(pending))
		d.logger.Warn("[webhook] 退出等待超时，未发出的回调被丢弃",
			"pending", pending,
			"error", ctx.Err(),
		)
		return ctx.Err()
	}
}

// Stats 返回当前计数快照。
func (d *Dispatcher) Stats() Stats {
	if d == nil {
		return Stats{}
	}
	return Stats{
		Enqueued:  d.enqueued.Load(),
		Delivered: d.delivered.Load(),
		Failed:    d.failed.Load(),
		Dropped:   d.dropped.Load(),
		Pending:   len(d.queue),
	}
}

// worker 串行消费队列里的事件。
func (d *Dispatcher) worker() {
	defer d.wg.Done()
	for ev := range d.queue {
		d.send(ev)
	}
}

// send 发出单个事件并记账。
func (d *Dispatcher) send(ev Event) {
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()

	res := d.sender.Deliver(ctx, d.target, ev)
	if res.OK() {
		d.delivered.Add(1)
		d.logger.Debug("[webhook] 投递结果已回调",
			"triggerId", ev.TriggerID,
			"eventId", ev.ID,
			"status", ev.Status,
			"httpStatus", res.Status,
			"duration", res.Duration,
		)
		return
	}

	failed := d.failed.Add(1)
	d.logger.Warn("[webhook] 投递结果回调失败",
		"triggerId", ev.TriggerID,
		"eventId", ev.ID,
		"status", ev.Status,
		"httpStatus", res.Status,
		"retryable", res.Retryable,
		"failed", failed,
		"error", res.Err,
	)
}
