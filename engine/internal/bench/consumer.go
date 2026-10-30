package bench

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// ackTimeout 是确认操作自身的超时。确认失败最多让消息被重新投递一次，
// 不该拖住压测的收尾。
const ackTimeout = 5 * time.Second

// idleBackoff 是「领取到空队列」后的退让间隔。
const idleBackoff = time.Millisecond

// Claimer 是压测的读取端，与引擎消费侧的接口保持一致。
type Claimer interface {
	Claim(ctx context.Context, timeout time.Duration) (*queue.Delivery, error)
	Ack(ctx context.Context, id string) error
}

// Handler 处理一条消息体。返回 error 只用于计数，消息仍会被确认——
// 压测关心链路吞吐，让失败消息按租约周期反复重投会把统计搅成噪音。
type Handler func(ctx context.Context, raw []byte) error

// ConsumeStats 是读取端的统计。
type ConsumeStats struct {
	// Handled / Failed 是处理成功与失败的消息条数。
	Handled int
	Failed  int
	// Empty 是「领取时队列为空」的次数，用于区分「消费者闲」与「消费者卡住」。
	Empty int
	// ClaimErrors 是领取操作本身的失败次数（Redis 抖动等）。
	ClaimErrors int
	// Latency 是端到端耗时（入队打点 → 处理并确认完成）。
	Latency Samples
	// Errors 是失败原因样本。
	Errors []string
	// Started / Finished 是消费窗口的起止时刻。
	Started  time.Time
	Finished time.Time
}

// Consumer 是 N 个并发消费者的压测执行体。
//
// 与 worker.Pool 的区别：池子面向生产（领取、执行、失败重试都在里面），
// 压测需要的是「能被外部停下来、能把每条消息的端到端耗时记下来」，
// 所以这里单独实现一个薄循环，复用同一个 ReliableConsumer。
type Consumer struct {
	source      Claimer
	workers     int
	handler     Handler
	sentAt      func([]byte) (time.Time, bool)
	pollTimeout time.Duration
	now         func() time.Time

	cancel context.CancelFunc
	wg     sync.WaitGroup
	done   atomic.Int64
	// stopOnce 保证 Stop 只收尾一次：重复调用返回同一份统计，
	// 不会把各消费者样本重复合并进总量。
	stopOnce sync.Once

	mu    sync.Mutex
	stats ConsumeStats
	// local 是各消费者自己的延迟样本，收尾时统一合并，避免每条消息抢锁。
	local []*Samples
}

// NewConsumer 创建压测消费者。
func NewConsumer(source Claimer, workers int, handler Handler) *Consumer {
	if workers < 1 {
		workers = 1
	}
	return &Consumer{
		source:      source,
		workers:     workers,
		handler:     handler,
		pollTimeout: time.Second,
		now:         time.Now,
		local:       make([]*Samples, workers),
	}
}

// WithSentAt 指定如何从消息体读出端到端起点；不设置时只统计处理耗时。
func (c *Consumer) WithSentAt(fn func([]byte) (time.Time, bool)) *Consumer {
	c.sentAt = fn
	return c
}

// Start 启动消费者并立即返回；ctx 取消后各消费者退出。
func (c *Consumer) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.mu.Lock()
	c.stats.Started = c.now()
	c.mu.Unlock()

	for i := 0; i < c.workers; i++ {
		c.local[i] = &Samples{}
		c.wg.Add(1)
		go c.loop(runCtx, i)
	}
}

// Done 返回已收尾（完成处理并确认）的消息条数，供外部判断是否排空。
func (c *Consumer) Done() int64 { return c.done.Load() }

// Stop 停止消费者并等待在途消息处理完，返回合并后的统计。
// 重复调用返回同一份统计。
func (c *Consumer) Stop() ConsumeStats {
	c.stopOnce.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		c.wg.Wait()

		c.mu.Lock()
		defer c.mu.Unlock()
		for _, s := range c.local {
			if s != nil {
				c.stats.Latency.AddAll(s.vals)
			}
		}
		c.stats.Finished = c.now()
	})

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// Errors 返回已记录的错误原因样本数量，供外部（含测试）观察而不必拆锁。
func (c *Consumer) Errors() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.stats.Errors)
}

// ClaimErrors 返回领取/确认操作自身的失败次数。
func (c *Consumer) ClaimErrors() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats.ClaimErrors
}

func (c *Consumer) loop(ctx context.Context, worker int) {
	defer c.wg.Done()
	samples := c.local[worker]

	for ctx.Err() == nil {
		d, err := c.source.Claim(ctx, c.pollTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.mu.Lock()
			c.stats.ClaimErrors++
			recordErr(&c.stats.Errors, err)
			c.mu.Unlock()
			continue
		}
		if d == nil {
			c.mu.Lock()
			c.stats.Empty++
			c.mu.Unlock()
			// 领取超时说明队列暂时空了。退让一小步再试，避免消费端在
			// 「队列已排空但压测还未收尾」的窗口里空转抢 CPU。
			if err := sleepCtx(ctx, idleBackoff); err != nil {
				return
			}
			continue
		}

		start := c.now()
		sent, stamped := time.Time{}, false
		if c.sentAt != nil {
			sent, stamped = c.sentAt(d.Raw)
		}
		herr := c.handler(ctx, d.Raw)
		finished := c.now()

		// 用独立上下文确认：处理期间 ctx 可能刚到取消信号，
		// 但已经把消息处理完了，此时不确认会让它被重投一次。
		ackCtx, cancel := context.WithTimeout(context.Background(), ackTimeout)
		ackErr := c.source.Ack(ackCtx, d.ID)
		cancel()

		if stamped && !sent.IsZero() {
			samples.Add(finished.Sub(sent))
		} else {
			samples.Add(finished.Sub(start))
		}

		c.mu.Lock()
		switch {
		case ackErr != nil:
			c.stats.ClaimErrors++
			recordErr(&c.stats.Errors, ackErr)
		case herr != nil:
			c.stats.Failed++
			recordErr(&c.stats.Errors, herr)
		default:
			c.stats.Handled++
		}
		c.mu.Unlock()

		if ackErr == nil {
			c.done.Add(1)
		}
	}
}
