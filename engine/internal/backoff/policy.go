// Package backoff 提供指数退避的等待时长计算。
//
// 队列层的消息重试与渠道层的投递重试用的是同一条曲线：等待时长从 base
// 起逐次翻倍，到 max 封顶，再按 jitter 向下抖动。抖动只向下取，因此上界
// 始终可预期；把曲线放在这里而不是各写一份，是为了让「第几次该等多久」
// 只有一处定义，调参时不会两边跑偏。
package backoff

import (
	"fmt"
	"math/rand"
	"time"
)

// 零值字段的兜底参数。
//
// 调用方通常显式给出 Base / Max / Jitter；这组值只在「只填了次数、没填
// 时长」时起作用，取的是偏保守的一组：2 秒起步、1 分钟封顶，连续几次
// 重试不会在几毫秒内打完，也不会把下游按在半分钟里反复捶。
const (
	DefaultBase   = 2 * time.Second
	DefaultMax    = time.Minute
	DefaultJitter = 0.3
)

// Policy 描述一条退避曲线。
//
// MaxAttempts 的含义是「最多尝试几次（含首次）」，<=0 表示不重试。它本身
// 不参与 Delay 的计算，由调用方据此决定循环次数。
type Policy struct {
	MaxAttempts int
	Base        time.Duration
	Max         time.Duration
	// Jitter 取 [0,1]，0 表示不抖动。
	Jitter float64
	// Rand 供测试注入固定随机源；为空时用全局随机数。
	Rand func() float64
}

// Attempts 返回实际要尝试的次数（至少一次）。
func (p Policy) Attempts() int {
	if p.MaxAttempts < 1 {
		return 1
	}
	return p.MaxAttempts
}

// Retries 返回首次之外的重试次数。
func (p Policy) Retries() int { return p.Attempts() - 1 }

// Normalize 补全非法参数，避免 0 值把等待时间压成「立即重试」的忙循环。
//
// 它不碰 MaxAttempts：不同调用方的「该重试几次」各有取舍，由调用方自己给。
func (p Policy) Normalize() Policy {
	if p.Base <= 0 {
		p.Base = DefaultBase
	}
	if p.Max <= 0 {
		p.Max = DefaultMax
	}
	if p.Max < p.Base {
		p.Max = p.Base
	}
	if p.Jitter < 0 {
		p.Jitter = 0
	}
	if p.Jitter > 1 {
		p.Jitter = 1
	}
	return p
}

// Delay 返回第 attempt 次重试前的等待时长（attempt 从 1 开始）。
//
// 指数增长：base、2*base、4*base……到 Max 封顶；随后按 Jitter 向下抖动，
// 让同一批同时失败的消息不会在同一毫秒一起回来。
func (p Policy) Delay(attempt int) time.Duration {
	p = p.Normalize()
	if attempt < 1 {
		attempt = 1
	}
	d := p.Base
	for i := 1; i < attempt && d < p.Max; i++ {
		if d > p.Max/2 {
			d = p.Max
			break
		}
		d *= 2
	}
	if d > p.Max {
		d = p.Max
	}
	if p.Jitter == 0 {
		return d
	}
	r := p.Rand
	if r == nil {
		r = rand.Float64
	}
	// 只向下抖动：等待时间始终不超过理论值，退避上界仍然可预期。
	return time.Duration(float64(d) * (1 - p.Jitter*r()))
}

// String 返回可写进日志的摘要，例如「3 次 500ms→5s 抖动 0.3」。
func (p Policy) String() string {
	if p.Attempts() <= 1 {
		return "关闭"
	}
	p = p.Normalize()
	out := fmt.Sprintf("%d 次 %s→%s", p.MaxAttempts, p.Base, p.Max)
	if p.Jitter > 0 {
		out += fmt.Sprintf(" 抖动 %.2f", p.Jitter)
	}
	return out
}
