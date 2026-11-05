package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/backoff"
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

	// abortGrace 是宽限到期、打断退避之后留给 worker 收尾的时间。
	//
	// 收尾只是记一次失败并返回，不需要长等；卡住的 worker 多半在一次 HTTP
	// 请求里，进程退出会把它带走。
	abortGrace = 200 * time.Millisecond
)

// Options 是分发器的可调参数。
type Options struct {
	// Workers 是发送并发数，非正数用 DefaultWorkers。
	Workers int
	// QueueSize 是缓冲队列长度，非正数用 DefaultQueueSize。
	QueueSize int
	// Timeout 是单次回调的超时，非正数用 DefaultTimeout。
	Timeout time.Duration
	// Retry 是单条事件的重试曲线；MaxAttempts 为 0 时用缺省曲线
	// （三次尝试），写 1 表示不重试。
	Retry backoff.Policy
	// Selection 是订阅的事件类型；零值按 DefaultSelection 处理。
	Selection Selection
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
	o.Retry = normalizeRetryPolicy(o.Retry)
	o.Selection = o.Selection.Normalized()
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
	// Retried 是因可重试失败而再次发起请求的次数（不含首次尝试）。
	Retried int64
	// Dropped 是因队列已满或已关闭而丢弃的事件数。
	Dropped int64
	// Filtered 是按订阅集合筛掉、没有发出的数量。它不算丢弃：没订阅就是
	// 本来不该发，与「订阅了但队列满」是两件事。
	Filtered int64
	// Pending 是当前排队等待发送的事件数。
	Pending int
}

// Dispatcher 把事件异步提交到回调地址。
//
// 消费路径调用 Notify 只做一次非阻塞入队，HTTP 请求在独立 goroutine 里
// 发生；因此接收方变慢不会拖长消息处理时间，也不会占住队列租约。
type Dispatcher struct {
	target    Target
	sender    Sender
	logger    *slog.Logger
	timeout   time.Duration
	retry     backoff.Policy
	selection Selection
	queue     chan Event

	mu     sync.RWMutex
	closed bool
	// stop 在 Close 时关闭，用来打断正在等待的退避：退出流程不该被一条
	// 还在重试的事件按在退避上干等。
	stop     chan struct{}
	stopOnce sync.Once

	wg sync.WaitGroup

	enqueued  atomic.Int64
	delivered atomic.Int64
	failed    atomic.Int64
	retried   atomic.Int64
	dropped   atomic.Int64
	filtered  atomic.Int64
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
		target:    target,
		sender:    sender,
		logger:    opts.Logger,
		timeout:   opts.Timeout,
		retry:     opts.Retry,
		selection: opts.Selection,
		queue:     make(chan Event, opts.QueueSize),
		stop:      make(chan struct{}),
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
	// 先按订阅筛，再校验：没订阅的类型本来就不该发，为它记一条「事件不合法」
	// 的告警只会让日志里出现与自己无关的噪音。
	if !d.selection.Allows(ev.Event) {
		d.filtered.Add(1)
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
// 队列里的事件照常按重试曲线走完：这些回执对应的消息已经确认，丢掉就
// 再也没有第二次机会。最多等到 ctx 结束——退出流程不该被一个不响应的
// 接收方拖住。宽限到期时打断还在退避的等待，worker 立即收尾，未开始发送
// 的事件记为丢弃。
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
	}

	// 宽限到期：不再等退避。已经发出去的请求仍会跑完（受单次超时约束），
	// 只是不会再发起新的尝试。
	d.stopOnce.Do(func() { close(d.stop) })

	select {
	case <-done:
	case <-time.After(abortGrace):
		// worker 还卡在一次 HTTP 请求里；进程即将退出，不再等它。
	}

	pending := len(d.queue)
	d.dropped.Add(int64(pending))
	d.logger.Warn("[webhook] 退出等待超时，未发出的回调被丢弃",
		"pending", pending,
		"error", ctx.Err(),
	)
	return ctx.Err()
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
		Retried:   d.retried.Load(),
		Dropped:   d.dropped.Load(),
		Filtered:  d.filtered.Load(),
		Pending:   len(d.queue),
	}
}

// Selection 返回生效的事件订阅集合。
func (d *Dispatcher) Selection() Selection {
	if d == nil {
		return DefaultSelection()
	}
	return d.selection
}

// Retry 返回生效的重试曲线。
func (d *Dispatcher) Retry() backoff.Policy {
	if d == nil {
		return backoff.Policy{}
	}
	return d.retry
}

// worker 串行消费队列里的事件。
func (d *Dispatcher) worker() {
	defer d.wg.Done()
	for ev := range d.queue {
		d.send(ev)
	}
}

// send 发出单个事件并记账，可重试的失败按退避曲线再试。
//
// 判断依据是 Result.Retryable：连不上、超时、5xx、429 值得再试；报文或
// 鉴权问题（400/401/403/404/410/413）再试一次也是同一个结果，只会白占
// 接收方的处理能力。接收方给出 Retry-After 时按对方的时间等——它比自己
// 更清楚什么时候能接。
func (d *Dispatcher) send(ev Event) {
	attempts := d.retry.Attempts()
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
		res := d.sender.Deliver(ctx, d.target, ev)
		cancel()

		if res.OK() {
			d.delivered.Add(1)
			if attempt > 1 {
				d.logger.Info("[webhook] 投递结果重试后回调成功",
					"triggerId", ev.TriggerID,
					"eventId", ev.ID,
					"status", ev.Status,
					"attempt", attempt,
					"httpStatus", res.Status,
				)
				return
			}
			d.logger.Debug("[webhook] 投递结果已回调",
				"triggerId", ev.TriggerID,
				"eventId", ev.ID,
				"status", ev.Status,
				"httpStatus", res.Status,
				"duration", res.Duration,
			)
			return
		}

		if !res.Retryable || attempt >= attempts {
			d.failed.Add(1)
			reason := res.Err
			if res.Retryable && attempts > 1 {
				reason += fmt.Sprintf("（已尝试 %d 次）", attempt)
			}
			d.logger.Warn("[webhook] 投递结果回调失败",
				"triggerId", ev.TriggerID,
				"eventId", ev.ID,
				"status", ev.Status,
				"httpStatus", res.Status,
				"retryable", res.Retryable,
				"attempt", attempt,
				"failed", d.failed.Load(),
				"error", reason,
			)
			return
		}

		wait := d.retry.Delay(attempt)
		if res.RetryAfter > wait {
			wait = res.RetryAfter
			if wait > d.retry.Max {
				wait = d.retry.Max
			}
		}
		d.retried.Add(1)
		d.logger.Warn("[webhook] 回调失败，稍后重试",
			"triggerId", ev.TriggerID,
			"eventId", ev.ID,
			"attempt", attempt,
			"of", attempts,
			"wait", wait.Round(time.Millisecond),
			"httpStatus", res.Status,
			"error", res.Err,
		)

		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-d.stop:
			timer.Stop()
			d.failed.Add(1)
			d.logger.Warn("[webhook] 退出中放弃剩余重试",
				"triggerId", ev.TriggerID,
				"eventId", ev.ID,
				"attempt", attempt,
				"of", attempts,
			)
			return
		}
	}
}
