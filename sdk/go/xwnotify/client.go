// Package xwnotify 是 xw-notify 触发接口的 Go 客户端。
//
// 只依赖标准库，覆盖单条触发与批量触发两种入口：
//
//	client, err := xwnotify.NewClient("http://127.0.0.1:8000")
//	if err != nil {
//		return err
//	}
//	ack, err := client.Trigger(ctx, xwnotify.NewEvent("post_commented", "creator-1001").
//		WithPayload(map[string]any{"post_id": "P-2026-1001", "actor": "小林"}))
//
// 客户端不做任何隐式重试：一次 Trigger 就是一次请求。重试要由调用方按
// 业务语义决定（点赞可以重发，评论重发会让创作者收到两条），错误类型里的
// Retryable 只回答「这次失败本身值不值得再发」，不代替这个判断。
package xwnotify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// DefaultBaseURL 与服务端 README 里的启动命令一致。
	DefaultBaseURL = "http://127.0.0.1:8000"
	// DefaultEnvironment 是显式发送的默认环境；客户端不会把它留给服务端默认值。
	DefaultEnvironment = "development"
	// DefaultTimeout 覆盖连接、发送与读取三个阶段。
	DefaultTimeout = 10 * time.Second
	// maxResponseBytes 限制读入内存的响应体大小，避免异常服务端把客户端撑爆。
	// 触发回执只有几百字节，4 MiB 已远超实际需要。
	maxResponseBytes = 4 << 20
)

// Client 是触发接口的客户端。零值不可用，请用 NewClient 构造。
//
// 并发安全：构造完成后只读，可以被多个 goroutine 同时使用。
type Client struct {
	baseURL     string
	environment string
	apiKey      string
	userAgent   string
	httpClient  *http.Client
}

// Option 在构造期修改客户端配置。返回错误是为了让非法取值在启动时暴露，
// 而不是等第一次请求才以一句难懂的报文失败。
type Option func(*Client) error

// WithAPIKey 设置该环境的 API Key。仅当服务端为该环境配置了 Key 时才需要；
// 空串等价于不发送该请求头。
func WithAPIKey(key string) Option {
	return func(c *Client) error {
		c.apiKey = strings.TrimSpace(key)
		return nil
	}
}

// WithEnvironment 设置隔离环境（development / production）。
//
// 环境标识始终显式发送，不依赖服务端默认值：运维切了环境却以为在看另一套
// 数据，是这类客户端最常见的事故，而显式发送让「请求打到了哪套环境」在
// 抓包里就能确认。
func WithEnvironment(environment string) Option {
	return func(c *Client) error {
		text := strings.TrimSpace(environment)
		if text == "" {
			return errors.New("xw-notify: environment 不能为空")
		}
		c.environment = text
		return nil
	}
}

// WithHTTPClient 注入自定义的 HTTP 客户端（代理、连接池、TLS 配置等）。
//
// 注意：默认客户端不跟随重定向（见 newDefaultHTTPClient）。注入的客户端如果
// 跟随重定向，X-Api-Key 会被带到新地址去——本 SDK 没有理由跨地址搬运凭据，
// 需要重定向时请自行确认目标可信，或保留默认行为。
func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) error {
		if httpClient == nil {
			return errors.New("xw-notify: httpClient 不能为 nil")
		}
		c.httpClient = httpClient
		return nil
	}
}

// WithUserAgent 覆盖默认的 User-Agent，服务端日志里据此区分调用方。
func WithUserAgent(userAgent string) Option {
	return func(c *Client) error {
		text := strings.TrimSpace(userAgent)
		if text == "" {
			return errors.New("xw-notify: userAgent 不能为空")
		}
		c.userAgent = text
		return nil
	}
}

// NewClient 构造客户端。
//
// baseURL 只写到主机与端口（可带路径前缀），不要带 /v1——接口路径由 SDK 拼接。
func NewClient(baseURL string, opts ...Option) (*Client, error) {
	text := strings.TrimSpace(baseURL)
	if text == "" {
		return nil, errors.New("xw-notify: baseURL 不能为空")
	}
	parsed, err := url.Parse(text)
	if err != nil {
		return nil, fmt.Errorf("xw-notify: baseURL 不是合法地址：%w", err)
	}
	// 少写协议头时 http.NewRequest 会给出 "unsupported protocol scheme"，
	// 那句话看不出是哪里错，这里提前拦下并说明。
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("xw-notify: baseURL 需要以 http:// 或 https:// 开头：%s", baseURL)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("xw-notify: baseURL 缺少主机名：%s", baseURL)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("xw-notify: baseURL 不应带查询串或片段：%s", baseURL)
	}

	client := &Client{
		baseURL:     strings.TrimRight(text, "/"),
		environment: DefaultEnvironment,
		userAgent:   "xw-notify-go/0.1",
		httpClient:  newDefaultHTTPClient(),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(client); err != nil {
			return nil, err
		}
	}
	return client, nil
}

// BaseURL 返回规范化后的根地址（去掉结尾的 /）。
func (c *Client) BaseURL() string { return c.baseURL }

// Environment 返回当前环境标识。
func (c *Client) Environment() string { return c.environment }

// newDefaultHTTPClient 造一个不跟随重定向的客户端。
//
// 跟随意味着把 X-Api-Key 按服务端（或被改写过的响应）给出的地址再发一次，
// API 客户端没有理由这么做。3xx 在 do 里按错误返回，并带上目标地址供排查。
func newDefaultHTTPClient() *http.Client {
	return &http.Client{
		Timeout: DefaultTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// do 发一次请求并返回 2xx 的响应体。
//
// 状态码判定集中在这里：传输层把 4xx/5xx 也当普通响应返回，2xx 之外一律
// 折成错误，调用方（资源层）只处理正常路径。
func (c *Client) do(ctx context.Context, method, path string, body any, headers map[string]string) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, &ValidationError{Field: "body", Message: "无法序列化为 JSON：" + err.Error()}
		}
		reader = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return nil, &ValidationError{Field: "path", Message: "无法构造请求：" + err.Error()}
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Environment", c.environment)
	req.Header.Set("User-Agent", c.userAgent)
	if body != nil {
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	if c.apiKey != "" {
		req.Header.Set("X-Api-Key", c.apiKey)
	}
	for name, value := range headers {
		if value != "" {
			req.Header.Set(name, value)
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, &TransportError{Method: method, Path: path, Err: err}
	}
	defer func() { _ = resp.Body.Close() }()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		// 已经拿到响应头、读响应体时断流，仍属于传输失败：请求可能已被处理，
		// 调用方需要它和「连不上」一样可被识别。
		return nil, &TransportError{Method: method, Path: path, Err: err}
	}

	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		location := resp.Header.Get("Location")
		if location == "" {
			location = "(未给出 Location)"
		}
		return nil, &APIError{
			StatusCode: resp.StatusCode,
			Method:     method,
			Path:       path,
			Detail:     "服务端要求重定向到 " + location + "；客户端不跟随重定向，请确认 baseURL 是否写对",
			Body:       payload,
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &APIError{
			StatusCode: resp.StatusCode,
			Method:     method,
			Path:       path,
			Detail:     errorDetail(payload),
			Body:       payload,
		}
	}
	if len(bytes.TrimSpace(payload)) == 0 {
		return nil, &ProtocolError{Method: method, Path: path, Reason: "响应体为空，无法读取回执", Body: payload}
	}
	return payload, nil
}

// decode 把成功响应解析进 out，失败时把原始报文留在错误里。
func decode(method, path string, payload []byte, out any) error {
	if err := json.Unmarshal(payload, out); err != nil {
		return &ProtocolError{
			Method: method,
			Path:   path,
			Reason: "响应不是合法 JSON：" + err.Error(),
			Body:   payload,
		}
	}
	return nil
}
