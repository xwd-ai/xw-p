package xwnotify

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewClientRejectsBadBaseURL(t *testing.T) {
	cases := map[string]string{
		"空串":     "",
		"只有空白":   "   ",
		"缺少协议头":  "127.0.0.1:8000",
		"不支持的协议": "ftp://127.0.0.1:8000",
		"缺少主机名":  "http:///v1",
		"带查询串":   "http://127.0.0.1:8000?env=dev",
		"带片段":    "http://127.0.0.1:8000#top",
	}
	for name, baseURL := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewClient(baseURL); err == nil {
				t.Fatalf("NewClient(%q) 应当报错", baseURL)
			}
		})
	}
}

func TestNewClientDefaults(t *testing.T) {
	client, err := NewClient("http://127.0.0.1:8000/")
	if err != nil {
		t.Fatalf("NewClient 报错：%v", err)
	}
	if client.BaseURL() != "http://127.0.0.1:8000" {
		t.Fatalf("结尾的 / 未去掉：%q", client.BaseURL())
	}
	if client.Environment() != DefaultEnvironment {
		t.Fatalf("默认环境应当是 %q，实际 %q", DefaultEnvironment, client.Environment())
	}
}

func TestOptionsApply(t *testing.T) {
	client, err := NewClient("http://127.0.0.1:8000",
		WithAPIKey("  key-1  "),
		WithEnvironment(" production "),
		WithUserAgent("custom-agent/9"),
	)
	if err != nil {
		t.Fatalf("NewClient 报错：%v", err)
	}
	if client.apiKey != "key-1" {
		t.Fatalf("API Key 未去空白：%q", client.apiKey)
	}
	if client.Environment() != "production" {
		t.Fatalf("环境未生效：%q", client.Environment())
	}
	if client.userAgent != "custom-agent/9" {
		t.Fatalf("User-Agent 未生效：%q", client.userAgent)
	}
}

func TestOptionsRejectInvalidValues(t *testing.T) {
	cases := map[string]Option{
		"空环境":          WithEnvironment("  "),
		"空 User-Agent": WithUserAgent(""),
		"空 HTTP 客户端":   WithHTTPClient(nil),
	}
	for name, opt := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewClient("http://127.0.0.1:8000", opt); err == nil {
				t.Fatal("应当报错")
			}
		})
	}
}

func TestTransportErrorIsRetryable(t *testing.T) {
	canceled := &TransportError{Method: "POST", Path: triggerPath, Err: context.Canceled}
	if canceled.Retryable() {
		t.Fatal("主动取消不应被建议重试")
	}
	if !errors.Is(canceled, context.Canceled) {
		t.Fatal("TransportError 应当能穿透到 context.Canceled")
	}
	refused := &TransportError{Method: "POST", Path: triggerPath, Err: errors.New("connection refused")}
	if !refused.Retryable() || !IsRetryable(refused) {
		t.Fatal("连接失败应被判定为可重试")
	}
}

func TestRetryableClassification(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{&APIError{StatusCode: http.StatusTooManyRequests}, true},
		{&APIError{StatusCode: http.StatusInternalServerError}, true},
		{&APIError{StatusCode: http.StatusBadGateway}, true},
		{&APIError{StatusCode: http.StatusBadRequest}, false},
		{&APIError{StatusCode: http.StatusNotFound}, false},
		{&ValidationError{Field: "name", Message: "不能为空"}, false},
		{&ProtocolError{Reason: "坏报文"}, false},
		{errors.New("未识别的错误"), false},
	}
	for _, c := range cases {
		if got := IsRetryable(c.err); got != c.want {
			t.Errorf("IsRetryable(%v) = %v，期望 %v", c.err, got, c.want)
		}
	}
}

func TestStatusHelpers(t *testing.T) {
	notFound := &APIError{StatusCode: http.StatusNotFound}
	if !IsNotFound(notFound) || StatusCode(notFound) != http.StatusNotFound {
		t.Fatal("404 判定不正确")
	}
	if IsConflict(notFound) || IsUnauthorized(notFound) {
		t.Fatal("状态码判定串了")
	}
	if !IsValidation(&ValidationError{}) {
		t.Fatal("本地校验错误应被 IsValidation 识别")
	}
	if !IsValidation(&APIError{StatusCode: http.StatusUnprocessableEntity}) {
		t.Fatal("422 应被 IsValidation 识别")
	}
	if !IsLocal(&ValidationError{}) || IsLocal(notFound) {
		t.Fatal("IsLocal 判定不正确")
	}
}

func TestRedirectNotFollowed(t *testing.T) {
	var hits int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		_, _ = w.Write([]byte(`{"acknowledged":true,"triggerId":"t-1"}`))
	}))
	defer target.Close()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+triggerPath, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	client, err := NewClient(origin.URL, WithAPIKey("secret-key"))
	if err != nil {
		t.Fatalf("NewClient 报错：%v", err)
	}
	_, err = client.Trigger(context.Background(), NewEvent("post_liked", "creator-1001"))
	if err == nil {
		t.Fatal("3xx 应当报错")
	}
	if !strings.Contains(err.Error(), "不跟随重定向") || !strings.Contains(err.Error(), target.URL) {
		t.Fatalf("错误文案应说明不跟随重定向并带上目标地址：%v", err)
	}
	if got := atomic.LoadInt32(&hits); got != 0 {
		t.Fatalf("重定向目标被请求了 %d 次，API Key 可能已泄露", got)
	}
	if StatusCode(err) != http.StatusTemporaryRedirect {
		t.Fatalf("应当是 307，实际 %d", StatusCode(err))
	}
}

func TestErrorDetailString(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"detail":"工作流模板不存在或未启用：post_liked（环境 development）"}`))
	}))
	defer server.Close()

	client, _ := NewClient(server.URL)
	_, err := client.Trigger(context.Background(), NewEvent("post_liked", "creator-1001"))
	if !IsNotFound(err) {
		t.Fatalf("应当是 404：%v", err)
	}
	if !strings.Contains(err.Error(), "post_liked") {
		t.Fatalf("错误文案应带上服务端 detail：%v", err)
	}
}

func TestErrorDetailValidationArray(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"detail":[` +
			`{"type":"missing","loc":["body","to","subscriberId"],"msg":"Field required"},` +
			`{"type":"string_too_long","loc":["body","name"],"msg":"String should have at most 255 characters"}]}`))
	}))
	defer server.Close()

	client, _ := NewClient(server.URL)
	_, err := client.Trigger(context.Background(), NewEvent("post_liked", "creator-1001"))
	if !IsValidation(err) {
		t.Fatalf("应当是校验失败：%v", err)
	}
	text := err.Error()
	if !strings.Contains(text, "to.subscriberId: Field required") {
		t.Fatalf("字段路径未拼出：%s", text)
	}
	if !strings.Contains(text, "name: String should have at most 255 characters") {
		t.Fatalf("第二条原因丢失：%s", text)
	}
	if strings.Contains(text, "body.") {
		t.Fatalf("不该把 body 前缀带进文案：%s", text)
	}
}

func TestErrorDetailHTMLPreview(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("<html>\n  <body>502 Bad Gateway</body>\n</html>"))
	}))
	defer server.Close()

	client, _ := NewClient(server.URL)
	_, err := client.Trigger(context.Background(), NewEvent("post_liked", "creator-1001"))
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("应当是 APIError：%v", err)
	}
	if apiErr.StatusCode != http.StatusBadGateway {
		t.Fatalf("状态码应为 502，实际 %d", apiErr.StatusCode)
	}
	if !strings.Contains(apiErr.Detail, "502 Bad Gateway") {
		t.Fatalf("HTML 报文应给出预览：%q", apiErr.Detail)
	}
	if strings.Contains(apiErr.Detail, "\n") {
		t.Fatalf("预览应当压成一行：%q", apiErr.Detail)
	}
	if !IsRetryable(err) {
		t.Fatal("502 应可重试")
	}
}

func TestSuccessBodyNotJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
	}))
	defer server.Close()

	client, _ := NewClient(server.URL)
	_, err := client.Trigger(context.Background(), NewEvent("post_liked", "creator-1001"))
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("应当是 ProtocolError：%v", err)
	}
	if !strings.Contains(protocolErr.Reason, "不是合法 JSON") {
		t.Fatalf("原因说明不对：%s", protocolErr.Reason)
	}
}

func TestSuccessBodyEmpty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, _ := NewClient(server.URL)
	_, err := client.Trigger(context.Background(), NewEvent("post_liked", "creator-1001"))
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("应当是 ProtocolError：%v", err)
	}
	if !strings.Contains(protocolErr.Reason, "为空") {
		t.Fatalf("原因说明不对：%s", protocolErr.Reason)
	}
}

func TestTimeoutIsTransportError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte(`{"acknowledged":true,"triggerId":"t-1"}`))
	}))
	defer server.Close()

	client, _ := NewClient(server.URL, WithHTTPClient(&http.Client{Timeout: 30 * time.Millisecond}))
	_, err := client.Trigger(context.Background(), NewEvent("post_liked", "creator-1001"))
	var transportErr *TransportError
	if !errors.As(err, &transportErr) {
		t.Fatalf("超时应当是 TransportError：%v", err)
	}
	if !IsRetryable(err) {
		t.Fatal("超时应被判定为可重试（换一次尝试可能就过了）")
	}
}

func TestCanceledContext(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		time.Sleep(200 * time.Millisecond)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()
	client, _ := NewClient(server.URL)
	_, err := client.Trigger(ctx, NewEvent("post_liked", "creator-1001"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("应当能识别出取消：%v", err)
	}
	if IsRetryable(err) {
		t.Fatal("主动取消不应建议重试")
	}
}

func TestResponseBodyLimitDoesNotHang(t *testing.T) {
	// 大响应体应当被截断后正常返回解析失败，而不是无限读入内存。
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chunk := strings.Repeat("x", 4096)
		for i := 0; i < 8; i++ {
			if _, err := io.WriteString(w, chunk); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	client, _ := NewClient(server.URL)
	_, err := client.Trigger(context.Background(), NewEvent("post_liked", "creator-1001"))
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("应当是 ProtocolError：%v", err)
	}
}
