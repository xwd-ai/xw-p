package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultTimeout 是单次回调的缺省超时。
	//
	// 接收方是接入方自己的服务，正常在百毫秒级返回 2xx；超过五秒基本是
	// 对方出了问题，继续等只会占住发送 goroutine。
	DefaultTimeout = 5 * time.Second

	// MaxTimeout 是允许配置的最长超时。
	MaxTimeout = time.Minute

	// userAgent 是请求头里的客户端标识，便于接收方在自己的访问日志里
	// 区分这批请求来自通知引擎而不是别的系统。
	userAgent = "xw-notify-webhook/1.0"

	// errorBodyLimit 是失败响应体读入错误信息的字节上限。
	//
	// 接收方的错误页可能有几十 KB 的 HTML，全读进来只会把日志撑爆；
	// 前 512 字节足够看出是鉴权失败还是路径写错。
	errorBodyLimit = 512

	// MaxRetryAfter 是接收方 Retry-After 的上限。
	//
	// 对方要求「一小时后再来」时，回调队列里的事件早已过期，继续等着只是
	// 占住发送 goroutine；超过上限按上限处理，日志里仍记录对方给的原值。
	MaxRetryAfter = time.Minute
)

// statusHints 把接收方的状态码翻成一句能指导下一步动作的话。
//
// Retryable 是给后续重试策略用的判断：4xx 里除了 429 都是「这条报文
// 不该再发第二次」，5xx 与 429 是「对方暂时不行」。
var statusHints = map[int]struct {
	reason    string
	retryable bool
}{
	400: {"报文格式被拒绝，检查事件字段与接收方的解析规则", false},
	401: {"鉴权失败，检查接收方地址里的令牌是否已轮换", false},
	403: {"接收方拒绝该来源，检查回调地址的访问策略", false},
	404: {"回调地址不存在，检查路径是否写错或端点已被撤销", false},
	410: {"回调端点已下线，需要接入方重新签发地址", false},
	413: {"事件报文超过接收方的体积上限", false},
	429: {"接收方限流，稍后可再试", true},
}

// Result 是一次回调尝试的结果。
type Result struct {
	// Status 是 HTTP 状态码；请求根本没发出去时为 0。
	Status int
	// Duration 是本次请求耗时。
	Duration time.Duration
	// RetryAfter 是接收方要求的等待时长（来自 429/503 的 Retry-After），
	// 没有该头部时为 0。
	RetryAfter time.Duration
	// Err 是失败原因，成功时为空。
	Err string
	// Retryable 表示这次失败是否值得再试（连不上、超时、5xx、429）。
	Retryable bool
}

// OK 判断本次回调是否成功。
func (r Result) OK() bool { return r.Err == "" }

// Sender 把事件提交到一个回调地址。抽出接口是为了让分发器在测试里
// 换掉真实网络，也让后续加上签名、重试时不必改动分发逻辑。
type Sender interface {
	Deliver(ctx context.Context, target Target, ev Event) Result
}

// Client 是回调的 HTTP 实现。
type Client struct {
	http    *http.Client
	timeout time.Duration
	signer  *Signer
	// clock 供测试固定签名时刻；为空时用 time.Now。
	clock func() time.Time
}

// NewClient 按超时构造客户端；超时非正数或超过上限时收敛到合法区间。
func NewClient(timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	if timeout > MaxTimeout {
		timeout = MaxTimeout
	}
	return &Client{
		http:    &http.Client{Timeout: timeout},
		timeout: timeout,
		clock:   time.Now,
	}
}

// Timeout 返回生效的超时。
func (c *Client) Timeout() time.Duration { return c.timeout }

// WithHTTPClient 替换底层 HTTP 客户端（测试里注入传输层）。
func (c *Client) WithHTTPClient(client *http.Client) *Client {
	if client != nil {
		c.http = client
	}
	return c
}

// WithSigner 让回调带上签名头部；传 nil 表示不签名。
func (c *Client) WithSigner(signer *Signer) *Client {
	c.signer = signer
	return c
}

// Signed 表示当前是否会给报文签名。
func (c *Client) Signed() bool { return c != nil && c.signer != nil }

// WithClock 替换签名时刻的来源（测试里固定时间戳）。
func (c *Client) WithClock(clock func() time.Time) *Client {
	if clock != nil {
		c.clock = clock
	}
	return c
}

// Deliver 把事件以 JSON 提交给回调地址。
//
// 事件缺字段时不发请求，直接返回失败：接收方收到一条无法归属的通知，
// 只会让两边都多一轮排查。
func (c *Client) Deliver(ctx context.Context, target Target, ev Event) Result {
	if target.IsZero() {
		return Result{Err: "回调地址为空"}
	}
	if err := ev.Validate(); err != nil {
		return Result{Err: err.Error()}
	}

	body, err := json.Marshal(ev)
	if err != nil {
		return Result{Err: fmt.Sprintf("事件序列化失败：%v", err)}
	}

	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.URL(), bytes.NewReader(body))
	if err != nil {
		return Result{Err: fmt.Sprintf("构造请求失败：%v", err), Duration: time.Since(start)}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)
	// 事件名与事件标识各给一个头部：接收方可以在读完报文之前先做路由与去重。
	req.Header.Set("X-XW-Event", ev.Event)
	req.Header.Set("X-XW-Event-Id", ev.ID)
	// 签名覆盖时间戳与正文，重试时正文不变、时间戳更新，接收方仍能按
	// X-XW-Event-Id 去重。
	if c.signer != nil {
		signedAt := c.clock().UTC().Truncate(time.Second)
		timestampValue, signatureValue := c.signer.Headers(signedAt, body)
		req.Header.Set(TimestampHeader, timestampValue)
		req.Header.Set(SignatureHeader, signatureValue)
	}

	resp, err := c.http.Do(req)
	duration := time.Since(start)
	if err != nil {
		return Result{
			Duration:  duration,
			Err:       fmt.Sprintf("回调请求失败（%s）：%v", target.String(), cancelHint(err)),
			Retryable: true,
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// 成功响应体通常为空；读掉一小段让连接可以复用，不必解析内容。
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, errorBodyLimit))
		return Result{Status: resp.StatusCode, Duration: duration}
	}

	snippet := readSnippet(resp.Body)
	reason := fmt.Sprintf("回调被拒绝（%d %s）", resp.StatusCode, http.StatusText(resp.StatusCode))
	retryable := resp.StatusCode >= 500
	if hint, ok := statusHints[resp.StatusCode]; ok {
		reason += "：" + hint.reason
		retryable = hint.retryable
	} else if resp.StatusCode >= 500 {
		reason += "：接收方暂时不可用"
	}
	retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
	if retryAfter > 0 {
		reason += fmt.Sprintf("；对方建议 %s 后再试", retryAfter)
	}
	if snippet != "" {
		reason += "；响应：" + snippet
	}
	return Result{
		Status:     resp.StatusCode,
		Duration:   duration,
		RetryAfter: retryAfter,
		Err:        reason,
		Retryable:  retryable,
	}
}

// parseRetryAfter 解析 Retry-After 头部。
//
// 两种写法都要认：整数秒（限流接口常见）与 HTTP 日期（网关常见）。解析
// 不出来就当作没给，交给退避曲线决定等待时间；超过 MaxRetryAfter 的取值
// 收敛到上限，日志里保留对方给的原值。
func parseRetryAfter(raw string) time.Duration {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0
	}
	var wait time.Duration
	if seconds, err := strconv.Atoi(trimmed); err == nil {
		if seconds <= 0 {
			return 0
		}
		wait = time.Duration(seconds) * time.Second
	} else if at, err := http.ParseTime(trimmed); err == nil {
		wait = time.Until(at)
		if wait <= 0 {
			return 0
		}
	} else {
		return 0
	}
	if wait > MaxRetryAfter {
		return MaxRetryAfter
	}
	return wait
}

// cancelHint 把网络错误翻成一句更贴近现场的话。
//
// 先剥掉 net/url 的包装再取原因：*url.Error 的 Error() 会带出完整地址，
// 而回调地址里通常有令牌，原样写进日志就等于把它泄露了。
func cancelHint(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = urlErr.Err
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "超时，接收方未在预算内响应"
	case errors.Is(err, context.Canceled):
		return "请求被取消"
	}
	return err.Error()
}

// readSnippet 读取响应体开头的一小段并压成单行，供错误信息引用。
func readSnippet(body io.Reader) string {
	raw, err := io.ReadAll(io.LimitReader(body, errorBodyLimit))
	if err != nil {
		return ""
	}
	return strings.Join(strings.Fields(string(raw)), " ")
}
