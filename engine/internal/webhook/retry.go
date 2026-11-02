package webhook

import (
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/backoff"
)

// 回调重试的缺省参数。
//
// 回调是旁路：重试的等待时间会占住发送 goroutine，而队列里的下一条事件
// 还在等。因此曲线比队列重试更短——三次尝试、500ms 起步、10s 封顶，
// 一条事件的失败路径最多让一个 worker 停一秒多。
const (
	DefaultRetryAttempts = 3
	MaxRetryAttempts     = 10
	DefaultRetryBase     = 500 * time.Millisecond
	DefaultRetryMax      = 10 * time.Second
	DefaultRetryJitter   = 0.25
)

// defaultRetryPolicy 返回缺省曲线。
func defaultRetryPolicy() backoff.Policy {
	return backoff.Policy{
		MaxAttempts: DefaultRetryAttempts,
		Base:        DefaultRetryBase,
		Max:         DefaultRetryMax,
		Jitter:      DefaultRetryJitter,
	}
}

// normalizeRetryPolicy 收敛越界取值。
//
// MaxAttempts 为 0 表示「没配」，补上缺省曲线；显式写 1 表示不重试，
// 这个区别必须保留，否则「我只想发一次」的配置会被悄悄改成三次。
func normalizeRetryPolicy(p backoff.Policy) backoff.Policy {
	if p.MaxAttempts == 0 {
		p = defaultRetryPolicy()
	}
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	if p.MaxAttempts > MaxRetryAttempts {
		p.MaxAttempts = MaxRetryAttempts
	}
	return p.Normalize()
}
