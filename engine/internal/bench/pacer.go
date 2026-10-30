package bench

import (
	"context"
	"sync"
	"time"
)

// DefaultPacer 是按目标速率分配时隙的节拍器。
//
// 做法是「预约制」：每个调用方在锁内领走下一个应发时刻，然后各自 sleep 到
// 那个时刻。相比集中式 ticker 发令牌，它不需要为每条消息创建定时器，也不会
// 因为某个生产者被调度延迟而让其他人白等——落后于计划时直接以当前时刻为界，
// 不做追赶式补偿（补偿会把限速变成脉冲，P99 立刻失真）。
type Pacer struct {
	mu    sync.Mutex
	gap   time.Duration
	next  time.Time
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
}

// NewPacer 按目标速率创建节拍器；rate <= 0 表示不限速。
func NewPacer(rate float64) *Pacer {
	p := &Pacer{now: time.Now, sleep: sleepCtx}
	if rate > 0 {
		p.gap = time.Duration(float64(time.Second) / rate)
	}
	return p
}

// Limited 表示是否真的限速。
func (p *Pacer) Limited() bool { return p != nil && p.gap > 0 }

// Wait 阻塞到本调用方的发送时隙；ctx 取消时返回其错误。
//
// 不限速时立刻返回，调用方不必分两条路径处理。
func (p *Pacer) Wait(ctx context.Context) error {
	if !p.Limited() {
		return ctx.Err()
	}
	p.mu.Lock()
	now := p.now()
	at := p.next
	if at.Before(now) {
		at = now
	}
	p.next = at.Add(p.gap)
	p.mu.Unlock()

	if d := at.Sub(now); d > 0 {
		return p.sleep(ctx, d)
	}
	return ctx.Err()
}

// sleepCtx 是可被取消的等待。
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
