package xwnotify

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// 重试在通知系统里不是「多试几次更稳」，而是一个会制造重复通知的动作：
// 触发接口一旦入队成功，重发就是第二条站内信。所以这里把两件事分开判断，
// 两个条件同时成立才会重发：
//
//  1. 这次失败值得重发（IsRetryable）：传输层没拿到结论，或服务端暂时不行
//     （429 与 5xx）。参数错误、鉴权失败、资源不存在、响应形状不对都不在其中；
//  2. 这次请求重发安全（replaySafe）：GET / HEAD / OPTIONS / PUT / DELETE
//     天然幂等，POST 必须带 Idempotency-Key——服务端按键去重，重复提交返回
//     同一份回执。没有幂等键的 POST 一律不重发，即使打开了重试策略。
//
// 等待时长是指数退避加向下抖动：第 n 次失败后等 BaseDelay * 2^(n-1)，按
// Jitter 比例随机下压，封顶 MaxDelay。向下抖动保证等待不超过上限，也不会
// 「第一次就比基准等得更久」。服务端给了 Retry-After 时以它为准，但封顶在
// MaxRetryAfter，避免一个异常响应把调用方挂住。

const (
	// DefaultRetryBaseDelay 是第一次退避的基准时长。
	DefaultRetryBaseDelay = 200 * time.Millisecond
	// DefaultRetryMaxDelay 是退避的封顶时长。
	DefaultRetryMaxDelay = 5 * time.Second
	// MaxRetryAfter 是从 Retry-After 采纳的最大等待时长。
	MaxRetryAfter = time.Minute
)

// RetryPolicy 描述重试次数与退避曲线。
//
// 零值字段按默认值处理：BaseDelay 取 DefaultRetryBaseDelay、MaxDelay 取
// DefaultRetryMaxDelay，因此 RetryPolicy{MaxAttempts: 3} 就是一份可用策略。
// 需要显式关闭重试时不要用它，而是不传 WithRetry。
type RetryPolicy struct {
	// MaxAttempts 是总尝试次数（含首次）。小于 1 会被拒绝。
	MaxAttempts int
	// BaseDelay 是第一次退避的基准，0 表示用默认值。
	BaseDelay time.Duration
	// MaxDelay 是退避封顶，0 表示用默认值。
	MaxDelay time.Duration
	// Jitter 是向下抖动比例（0–1），0 表示不抖动。
	Jitter float64
	// IgnoreRetryAfter 为真时不采纳服务端给出的 Retry-After。
	IgnoreRetryAfter bool
}

// DefaultRetryPolicy 是三试、200ms 起步、5s 封顶、抖动三成的策略。
//
// 次数之所以小：通知是「越晚越没有意义」的投递，等上几分钟再补发一条
// 「有人赞了你」没有价值，不如把失败如实报给调用方。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   DefaultRetryBaseDelay,
		MaxDelay:    DefaultRetryMaxDelay,
		Jitter:      0.3,
	}
}

// Enabled 报告策略是否会重发（尝试次数大于 1）。
func (p RetryPolicy) Enabled() bool { return p.MaxAttempts > 1 }

func (p RetryPolicy) validate() error {
	if p.MaxAttempts < 1 {
		return errors.New("xw-notify: RetryPolicy.MaxAttempts 至少为 1（1 表示不重试）")
	}
	if p.BaseDelay < 0 {
		return errors.New("xw-notify: RetryPolicy.BaseDelay 不能为负")
	}
	if p.MaxDelay < 0 {
		return errors.New("xw-notify: RetryPolicy.MaxDelay 不能为负")
	}
	if p.BaseDelay > 0 && p.MaxDelay > 0 && p.MaxDelay < p.BaseDelay {
		return errors.New("xw-notify: RetryPolicy.MaxDelay 不应小于 BaseDelay，否则退避曲线没有意义")
	}
	if p.Jitter < 0 || p.Jitter > 1 {
		return errors.New("xw-notify: RetryPolicy.Jitter 需要落在 0 到 1 之间")
	}
	return nil
}

// delay 返回第 attempt 次失败之后等多久（attempt 从 1 开始）。
func (p RetryPolicy) delay(attempt int, retryAfter time.Duration) time.Duration {
	base := p.BaseDelay
	if base <= 0 {
		base = DefaultRetryBaseDelay
	}
	ceiling := p.MaxDelay
	if ceiling <= 0 {
		ceiling = DefaultRetryMaxDelay
	}

	backoff := base
	for i := 1; i < attempt && backoff < ceiling; i++ {
		backoff *= 2
		if backoff > ceiling {
			backoff = ceiling
		}
	}
	if backoff > ceiling {
		backoff = ceiling
	}
	if p.Jitter > 0 {
		backoff = time.Duration(float64(backoff) * (1 - p.Jitter*rand.Float64()))
	}
	if !p.IgnoreRetryAfter && retryAfter > backoff {
		// 服务端明确说了要等多久，它知道自己的负载；上界仍是 MaxRetryAfter。
		backoff = retryAfter
	}
	if backoff < 0 {
		return 0
	}
	return backoff
}

// replaySafe 报告这个请求重发一次会不会产生额外副作用。
func replaySafe(method string, headers map[string]string) bool {
	switch strings.ToUpper(method) {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPut, http.MethodDelete:
		return true
	}
	for name, value := range headers {
		if strings.EqualFold(name, "Idempotency-Key") && strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

// parseRetryAfter 解析 Retry-After 头的两种写法：整数秒，或 HTTP 日期。
// 解析不出、为负、或超过 MaxRetryAfter 都返回 0，表示「没有可用提示」。
func parseRetryAfter(value string, now time.Time) time.Duration {
	text := strings.TrimSpace(value)
	if text == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(text); err == nil {
		if seconds <= 0 {
			return 0
		}
		wait := time.Duration(seconds) * time.Second
		if wait > MaxRetryAfter {
			return MaxRetryAfter
		}
		return wait
	}
	moment, err := http.ParseTime(text)
	if err != nil {
		return 0
	}
	wait := moment.Sub(now)
	if wait <= 0 {
		return 0
	}
	if wait > MaxRetryAfter {
		return MaxRetryAfter
	}
	return wait
}

// retryAfterOf 取出错误携带的服务端等待建议，非 APIError 返回 0。
func retryAfterOf(err error) time.Duration {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.RetryAfter
	}
	return 0
}

// sleepContext 等待 d，期间可被 ctx 打断。
func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
