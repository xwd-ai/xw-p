package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// DefaultPoolSize 是未显式配置并发度时启动的消费者数量。
// 单消费者在慢渠道（SMTP、推送网关）上会把队列堵成串行，
// 默认给一个小并发度，实际值由 XW_WORKERS 决定。
const DefaultPoolSize = 4

// MaxPoolSize 是并发度上限。再多也只是把压力推给下游渠道与 Redis，
// 每个消费者都要占用一条阻塞连接，不该由配置无上限放大。
const MaxPoolSize = 256

// DefaultShutdownGrace 是退出时等待在途消息处理完成的最长时间。
// 超出后放弃等待：消息不会丢，只是没被确认，租约到期后由回收流程重新投递。
const DefaultShutdownGrace = 30 * time.Second

// Pool 是 N 个消费者的并发池。
//
// 每个消费者独立执行「领取 → 处理 → 确认」，互相之间不共享状态，
// 并发度靠多个 goroutine 同时向消息源领取实现。消息源（ReliableConsumer）
// 的领取与登记在 Redis 里是原子脚本，因此并发领取不会把同一条消息发给两个消费者。
//
// 退出语义：ctx 取消后各消费者不再领取新消息，正在处理的那条会继续跑完并确认；
// 超过 grace 仍未结束的，池子放弃等待，留下未确认的在途记录等待回收。
// 这是「至少一次」语义下的取舍——宁可重投一次，也不让进程卡在关闭流程里。
type Pool struct {
	consumer Consumer
	size     int
	timeout  time.Duration
	grace    time.Duration
	logger   *slog.Logger
	// handle 用于测试注入处理逻辑；为空时各消费者使用默认的 Handle。
	handle func([]byte) error
}

// NewPool 创建并发池。size 非法时退化为 1，超过上限时截断到上限。
func NewPool(consumer Consumer, size int, timeout time.Duration, logger *slog.Logger) *Pool {
	if logger == nil {
		logger = slog.Default()
	}
	return &Pool{
		consumer: consumer,
		size:     NormalizePoolSize(size),
		timeout:  timeout,
		grace:    DefaultShutdownGrace,
		logger:   logger,
	}
}

// WithShutdownGrace 设置退出时等待在途消息的上限。
func (p *Pool) WithShutdownGrace(grace time.Duration) *Pool {
	if grace > 0 {
		p.grace = grace
	}
	return p
}

// NormalizePoolSize 把并发度收敛到 [1, MaxPoolSize]。
func NormalizePoolSize(size int) int {
	if size < 1 {
		return 1
	}
	if size > MaxPoolSize {
		return MaxPoolSize
	}
	return size
}

// Size 返回实际生效的并发度。
func (p *Pool) Size() int { return p.size }

// Run 启动全部消费者并阻塞，直到 ctx 取消且池子收尾完成。
//
// 返回值是收尾时仍未处理完的消息数：正常情况下为 0；
// 非 0 说明有消费者卡在处理里超过 grace，这些消息会在租约到期后重新投递。
func (p *Pool) Run(ctx context.Context) int {
	p.logger.Info("[engine] 消费者池已启动", "workers", p.size, "shutdownGrace", p.grace)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		inflight int
	)
	for i := 0; i < p.size; i++ {
		w := New(p.consumer, p.timeout, p.logger)
		w.id = i + 1
		if p.handle != nil {
			w.handle = p.handle
		}
		w.onStart = func() {
			mu.Lock()
			inflight++
			mu.Unlock()
		}
		w.onFinish = func() {
			mu.Lock()
			inflight--
			mu.Unlock()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Run(ctx)
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info("[engine] 消费者池已停止")
		return 0
	case <-ctx.Done():
	}

	// 关闭信号已到：消费者停止领取，等手里那条处理完。
	select {
	case <-done:
		p.logger.Info("[engine] 消费者池已停止，在途消息处理完毕")
		return 0
	case <-time.After(p.grace):
	}

	mu.Lock()
	left := inflight
	mu.Unlock()
	p.logger.Warn("[engine] 关闭等待超时，放弃未完成的消息（租约到期后重新投递）",
		"grace", p.grace, "inflight", left)
	return left
}
