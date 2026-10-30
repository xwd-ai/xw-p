// Package bench 压测队列链路：生产者按目标速率灌入消息，消费者领取并处理，
// 统计吞吐、端到端延迟分位与错误率。
//
// 压测工具只碰队列，不碰业务库：消息体与真实触发消息结构一致（额外的 bench
// 字段携带打点时间，解码时被忽略），因此量到的是「入队 → 领取 → 处理 → 确认」
// 这条真实链路，而不是孤立的函数调用。
package bench

import "time"

const (
	// DefaultTotal 是未指定总条数时灌入的消息量。
	DefaultTotal = 1000
	// DefaultConcurrency 是未指定时的生产者并发数。
	DefaultConcurrency = 8
	// MaxConcurrency 是生产者并发上限，避免误配把本机压垮。
	MaxConcurrency = 1024
	// DefaultWorkers 是未指定时的消费者数量。
	DefaultWorkers = 8
	// DefaultDrainTimeout 是「灌完之后等待消息全部被确认」的上限。
	DefaultDrainTimeout = 60 * time.Second
	// MaxRate 是目标速率上限（条/秒）。再高也只是把 sleep 变成忙等，
	// 触发端到端的排队延迟，测出来的数字没有参考价值。
	MaxRate = 100000
)

// Plan 描述一次压测的负载形状。
type Plan struct {
	// Total 是总条数。
	Total int
	// Concurrency 是同时写入队列的生产者数量。
	Concurrency int
	// Rate 是目标速率（条/秒）；0 表示不限制，能写多快写多快。
	Rate float64
	// Workers 是消费侧并发消费者数量。
	Workers int
	// Work 是每条消息的模拟处理耗时，用于给消费侧一个可解释的耗时基线。
	Work time.Duration
	// DrainTimeout 是灌完之后等待全部确认的上限。
	DrainTimeout time.Duration
}

// Normalize 把非法字段收敛到可用区间，返回可直接执行的负载形状。
//
// 收敛而不是报错：压测工具被脚本调用时，一个越界的并发数不该让整轮
// 测量白跑；真正越界的部分在报告里以「生效值」呈现。
func (p Plan) Normalize() Plan {
	if p.Total <= 0 {
		p.Total = DefaultTotal
	}
	if p.Concurrency <= 0 {
		p.Concurrency = DefaultConcurrency
	}
	if p.Concurrency > MaxConcurrency {
		p.Concurrency = MaxConcurrency
	}
	if p.Workers <= 0 {
		p.Workers = DefaultWorkers
	}
	if p.Workers > MaxConcurrency {
		p.Workers = MaxConcurrency
	}
	if p.Rate < 0 {
		p.Rate = 0
	}
	if p.Rate > MaxRate {
		p.Rate = MaxRate
	}
	if p.Work <= 0 {
		p.Work = 0
	}
	if p.DrainTimeout <= 0 {
		p.DrainTimeout = DefaultDrainTimeout
	}
	return p
}

// RateLimited 表示这一轮是否限速。
func (p Plan) RateLimited() bool { return p.Rate > 0 }
