package xwnotify

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// retryServer 按状态码脚本回应，并统计请求次数。
func retryServer(t *testing.T, statuses []int, body func(status int) string, header map[string]string) (*httptest.Server, *int32) {
	t.Helper()
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(atomic.AddInt32(&hits, 1)) - 1
		status := statuses[len(statuses)-1]
		if n < len(statuses) {
			status = statuses[n]
		}
		for name, value := range header {
			w.Header().Set(name, value)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body(status)))
	}))
	t.Cleanup(server.Close)
	return server, &hits
}

const ackBody = `{"acknowledged":true,"triggerId":"trg-1","transactionId":"txn-1","acknowledgmentId":"trg-1"}`

// noSleep 替换客户端的等待函数，记录每次等待时长而不真的等。
func noSleep(c *Client, waits *[]time.Duration) {
	c.sleep = func(ctx context.Context, d time.Duration) error {
		if waits != nil {
			*waits = append(*waits, d)
		}
		return nil
	}
}

func TestRetryPolicyValidation(t *testing.T) {
	cases := map[string]RetryPolicy{
		"次数为零":   {MaxAttempts: 0},
		"基准为负":   {MaxAttempts: 2, BaseDelay: -1},
		"封顶小于基准": {MaxAttempts: 2, BaseDelay: 2 * time.Second, MaxDelay: time.Second},
		"抖动越界":   {MaxAttempts: 2, Jitter: 1.5},
		"抖动为负":   {MaxAttempts: 2, Jitter: -0.1},
	}
	for name, policy := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewClient("http://127.0.0.1:8000", WithRetry(policy)); err == nil {
				t.Fatalf("非法策略应当在构造期被拒绝：%+v", policy)
			}
		})
	}
}

func TestRetryPolicyEnabled(t *testing.T) {
	if (RetryPolicy{}).Enabled() {
		t.Fatal("零值策略不应重试")
	}
	if !DefaultRetryPolicy().Enabled() {
		t.Fatal("默认策略应当重试")
	}
	if (RetryPolicy{MaxAttempts: 1}).Enabled() {
		t.Fatal("MaxAttempts=1 表示不重试")
	}
}

func TestRetryDelayDoublesAndCaps(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 5, BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second}
	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		time.Second,
		time.Second,
	}
	for index, expected := range want {
		got := policy.delay(index+1, 0)
		if got != expected {
			t.Fatalf("第 %d 次退避应为 %s，实际 %s", index+1, expected, got)
		}
	}
}

func TestRetryDelayJitterStaysWithinLowerBound(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: 10 * time.Second, Jitter: 0.3}
	for i := 0; i < 200; i++ {
		got := policy.delay(1, 0)
		if got < 700*time.Millisecond || got > time.Second {
			t.Fatalf("抖动后的等待应在 700ms–1s 之间，实际 %s", got)
		}
	}
}

func TestRetryDelayHonoursRetryAfter(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 3, BaseDelay: 100 * time.Millisecond, MaxDelay: 5 * time.Second}
	if got := policy.delay(1, 2*time.Second); got != 2*time.Second {
		t.Fatalf("Retry-After 应压过退避曲线，实际 %s", got)
	}
	if got := policy.delay(1, 10*time.Millisecond); got != 100*time.Millisecond {
		t.Fatalf("退避更长时应保留退避，实际 %s", got)
	}
	ignoring := policy
	ignoring.IgnoreRetryAfter = true
	if got := ignoring.delay(1, 2*time.Second); got != 100*time.Millisecond {
		t.Fatalf("显式忽略 Retry-After 时不应等待 2s，实际 %s", got)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	cases := map[string]struct {
		value string
		want  time.Duration
	}{
		"空":       {"", 0},
		"空白":      {"   ", 0},
		"整数秒":     {"3", 3 * time.Second},
		"零":       {"0", 0},
		"负数":      {"-2", 0},
		"非数字":     {"soon", 0},
		"超上限":     {"600", MaxRetryAfter},
		"HTTP 日期": {"Wed, 04 Nov 2026 12:00:30 GMT", 30 * time.Second},
		"日期已过去":   {"Wed, 04 Nov 2026 11:59:00 GMT", 0},
		"日期超上限":   {"Wed, 04 Nov 2026 13:30:00 GMT", MaxRetryAfter},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := parseRetryAfter(tc.value, now); got != tc.want {
				t.Fatalf("解析 %q 期望 %s，实际 %s", tc.value, tc.want, got)
			}
		})
	}
}

func TestReplaySafe(t *testing.T) {
	cases := []struct {
		method  string
		headers map[string]string
		want    bool
	}{
		{http.MethodGet, nil, true},
		{http.MethodPut, nil, true},
		{http.MethodDelete, nil, true},
		{http.MethodPost, nil, false},
		{http.MethodPost, map[string]string{"Content-Type": "application/json"}, false},
		{http.MethodPost, map[string]string{"Idempotency-Key": "comment-C-8842"}, true},
		{http.MethodPost, map[string]string{"idempotency-key": "   "}, false},
	}
	for _, tc := range cases {
		if got := replaySafe(tc.method, tc.headers); got != tc.want {
			t.Fatalf("%s %v 期望 %v，实际 %v", tc.method, tc.headers, tc.want, got)
		}
	}
}

func TestClientRetriesPostWithIdempotencyKey(t *testing.T) {
	server, hits := retryServer(t, []int{503, 200}, func(int) string { return ackBody }, nil)
	client, err := NewClient(server.URL, WithRetry(RetryPolicy{MaxAttempts: 2}))
	if err != nil {
		t.Fatalf("构造客户端失败：%v", err)
	}
	var waits []time.Duration
	noSleep(client, &waits)

	ack, err := client.Trigger(context.Background(),
		NewEvent("post_commented", "creator-1001"),
		WithIdempotencyKey("comment-C-8842"),
	)
	if err != nil {
		t.Fatalf("带幂等键的重发应当成功：%v", err)
	}
	if ack.TriggerID != "trg-1" {
		t.Fatalf("回执标识不对：%s", ack.TriggerID)
	}
	if got := atomic.LoadInt32(hits); got != 2 {
		t.Fatalf("应当请求两次，实际 %d 次", got)
	}
	if len(waits) != 1 || waits[0] != 200*time.Millisecond {
		t.Fatalf("第一次退避应为 200ms，实际 %v", waits)
	}
}

func TestClientDoesNotRetryPostWithoutIdempotencyKey(t *testing.T) {
	server, hits := retryServer(t, []int{503, 200}, func(int) string { return ackBody }, nil)
	client, err := NewClient(server.URL, WithRetry(RetryPolicy{MaxAttempts: 3}))
	if err != nil {
		t.Fatalf("构造客户端失败：%v", err)
	}
	var waits []time.Duration
	noSleep(client, &waits)

	_, err = client.Trigger(context.Background(), NewEvent("post_liked", "creator-1001"))
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("应当返回 503，实际 %v", err)
	}
	if got := atomic.LoadInt32(hits); got != 1 {
		t.Fatalf("没有幂等键不应重发，实际请求 %d 次", got)
	}
	if len(waits) != 0 {
		t.Fatalf("不该发生等待：%v", waits)
	}
}

func TestClientDoesNotRetryClientErrors(t *testing.T) {
	server, hits := retryServer(t, []int{422, 200}, func(int) string {
		return `{"detail":[{"loc":["body","to","subscriberId"],"msg":"Field required"}]}`
	}, nil)
	client, err := NewClient(server.URL, WithRetry(DefaultRetryPolicy()))
	if err != nil {
		t.Fatalf("构造客户端失败：%v", err)
	}
	noSleep(client, nil)

	_, err = client.Trigger(context.Background(),
		NewEvent("post_liked", "creator-1001"),
		WithIdempotencyKey("post-liked-P-1"),
	)
	if !IsValidation(err) {
		t.Fatalf("应当报校验失败，实际 %v", err)
	}
	if got := atomic.LoadInt32(hits); got != 1 {
		t.Fatalf("422 不应重发，实际请求 %d 次", got)
	}
}

func TestClientHonoursRetryAfterHeader(t *testing.T) {
	server, hits := retryServer(t, []int{429, 200}, func(int) string { return ackBody }, map[string]string{
		"Retry-After": "2",
	})
	client, err := NewClient(server.URL, WithRetry(RetryPolicy{MaxAttempts: 2, BaseDelay: 10 * time.Millisecond}))
	if err != nil {
		t.Fatalf("构造客户端失败：%v", err)
	}
	var waits []time.Duration
	noSleep(client, &waits)

	if _, err := client.Trigger(context.Background(),
		NewEvent("post_liked", "creator-1001"),
		WithIdempotencyKey("post-liked-P-1"),
	); err != nil {
		t.Fatalf("应当重发成功：%v", err)
	}
	if len(waits) != 1 || waits[0] != 2*time.Second {
		t.Fatalf("应按 Retry-After 等 2s，实际 %v", waits)
	}
	if got := atomic.LoadInt32(hits); got != 2 {
		t.Fatalf("应当请求两次，实际 %d 次", got)
	}
}

func TestClientRecordsRetryAfterOnError(t *testing.T) {
	server, _ := retryServer(t, []int{429}, func(int) string { return `{"detail":"触发太频繁"}` }, map[string]string{
		"Retry-After": "7",
	})
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("构造客户端失败：%v", err)
	}
	_, err = client.Trigger(context.Background(), NewEvent("post_liked", "creator-1001"))
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("应当是 APIError，实际 %v", err)
	}
	if apiErr.RetryAfter != 7*time.Second {
		t.Fatalf("RetryAfter 应为 7s，实际 %s", apiErr.RetryAfter)
	}
	if got := StatusCode(err); got != http.StatusTooManyRequests {
		t.Fatalf("状态码应为 429，实际 %d", got)
	}
}

func TestClientRetriesTransportError(t *testing.T) {
	server, hits := retryServer(t, []int{200}, func(int) string { return ackBody }, nil)
	client, err := NewClient(server.URL, WithRetry(RetryPolicy{MaxAttempts: 3}))
	if err != nil {
		t.Fatalf("构造客户端失败：%v", err)
	}
	noSleep(client, nil)

	// 第一次在传输层失败（连接被重置），第二次交给真实 RoundTripper。
	var failed int32
	base := client.httpClient.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	client.httpClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if atomic.CompareAndSwapInt32(&failed, 0, 1) {
			return nil, errors.New("connection reset by peer")
		}
		return base.RoundTrip(r)
	})

	if _, err := client.Trigger(context.Background(),
		NewEvent("post_liked", "creator-1001"),
		WithIdempotencyKey("post-liked-P-1"),
	); err != nil {
		t.Fatalf("传输错误后应当重发并成功：%v", err)
	}
	if got := atomic.LoadInt32(hits); got != 1 {
		t.Fatalf("服务端应当只收到一次，实际 %d 次", got)
	}
	if atomic.LoadInt32(&failed) != 1 {
		t.Fatal("第一次应当在传输层失败")
	}
}

func TestClientStopsRetryingWhenContextCancelledDuringBackoff(t *testing.T) {
	server, hits := retryServer(t, []int{503}, func(int) string { return ackBody }, nil)
	client, err := NewClient(server.URL, WithRetry(RetryPolicy{MaxAttempts: 5}))
	if err != nil {
		t.Fatalf("构造客户端失败：%v", err)
	}
	client.sleep = func(ctx context.Context, d time.Duration) error {
		return context.Canceled
	}

	_, err = client.Trigger(context.Background(),
		NewEvent("post_liked", "creator-1001"),
		WithIdempotencyKey("post-liked-P-1"),
	)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("取消后应返回最后一次失败，实际 %v", err)
	}
	if got := atomic.LoadInt32(hits); got != 1 {
		t.Fatalf("取消后不应再发请求，实际 %d 次", got)
	}
}

func TestClientWithoutRetryPolicyDoesNotRetry(t *testing.T) {
	server, hits := retryServer(t, []int{503, 200}, func(int) string { return ackBody }, nil)
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("构造客户端失败：%v", err)
	}
	if _, err := client.Trigger(context.Background(),
		NewEvent("post_liked", "creator-1001"),
		WithIdempotencyKey("post-liked-P-1"),
	); err == nil {
		t.Fatal("没有策略时应当直接失败")
	}
	if got := atomic.LoadInt32(hits); got != 1 {
		t.Fatalf("默认只发一次，实际 %d 次", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
