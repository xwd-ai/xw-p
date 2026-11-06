package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// sampleEvent 构造一条可直接发送的事件。
func sampleEvent() Event {
	return Event{
		ID:         "evt-1",
		Event:      EventDeliveryResult,
		OccurredAt: occurredAtNow(),
		TriggerID:  "trig-1",
		Workflow:   "post_liked",
		Status:     StatusSent,
		Summary:    &Summary{Total: 1, Sent: 1},
		Deliveries: []Delivery{{StepID: "notify", Channel: "in_app", Status: StatusSent}},
	}
}

// 正常回调：POST + JSON，带事件名与事件标识两个头部。
func TestClientDeliversEvent(t *testing.T) {
	var got struct {
		method  string
		ctype   string
		agent   string
		event   string
		eventID string
		body    Event
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.method = r.Method
		got.ctype = r.Header.Get("Content-Type")
		got.agent = r.Header.Get("User-Agent")
		got.event = r.Header.Get("X-XW-Event")
		got.eventID = r.Header.Get("X-XW-Event-Id")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &got.body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	target := mustTarget(t, srv.URL+"/hooks/xw")
	res := NewClient(time.Second).Deliver(context.Background(), target, sampleEvent())
	if !res.OK() {
		t.Fatalf("回调应成功: %+v", res)
	}
	if res.Status != http.StatusNoContent {
		t.Errorf("状态码记录不符: %d", res.Status)
	}
	if got.method != http.MethodPost {
		t.Errorf("应使用 POST，实际 %s", got.method)
	}
	if got.ctype != "application/json" {
		t.Errorf("Content-Type 不符: %q", got.ctype)
	}
	if got.agent != userAgent {
		t.Errorf("User-Agent 不符: %q", got.agent)
	}
	if got.event != EventDeliveryResult || got.eventID != "evt-1" {
		t.Errorf("事件头部不符: event=%q id=%q", got.event, got.eventID)
	}
	if got.body.TriggerID != "trig-1" || len(got.body.Deliveries) != 1 {
		t.Errorf("报文内容不符: %+v", got.body)
	}
}

// 非 2xx 一律判失败，原因里带状态码与响应片段；只有 429 与 5xx 标为可重试。
func TestClientMapsFailureStatus(t *testing.T) {
	cases := []struct {
		status    int
		retryable bool
		contains  string
	}{
		{http.StatusBadRequest, false, "报文格式被拒绝"},
		{http.StatusUnauthorized, false, "鉴权失败"},
		{http.StatusForbidden, false, "拒绝该来源"},
		{http.StatusNotFound, false, "回调地址不存在"},
		{http.StatusGone, false, "端点已下线"},
		{http.StatusRequestEntityTooLarge, false, "体积上限"},
		{http.StatusTooManyRequests, true, "限流"},
		{http.StatusInternalServerError, true, "接收方暂时不可用"},
		{http.StatusBadGateway, true, "接收方暂时不可用"},
		{http.StatusTeapot, false, "回调被拒绝（418"},
	}

	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte("  上游说明\n第二行  "))
		}))
		target := mustTarget(t, srv.URL+"/cb")
		res := NewClient(time.Second).Deliver(context.Background(), target, sampleEvent())
		srv.Close()

		if res.OK() {
			t.Errorf("%d 应判失败", tc.status)
			continue
		}
		if res.Status != tc.status {
			t.Errorf("%d：状态码记录不符 %d", tc.status, res.Status)
		}
		if !strings.Contains(res.Err, tc.contains) {
			t.Errorf("%d：原因应包含 %q，实际 %q", tc.status, tc.contains, res.Err)
		}
		if res.Retryable != tc.retryable {
			t.Errorf("%d：retryable 应为 %v，实际 %v", tc.status, tc.retryable, res.Retryable)
		}
		// 响应体压成单行，日志里不会因为换行被拆成多条。
		if !strings.Contains(res.Err, "上游说明 第二行") {
			t.Errorf("%d：响应片段应压成单行，实际 %q", tc.status, res.Err)
		}
	}
}

// 连不上接收方：标记为可重试，原因里只出现脱敏后的地址。
func TestClientConnectionFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	target := mustTarget(t, srv.URL+"/hooks/secret-token")
	srv.Close()

	res := NewClient(time.Second).Deliver(context.Background(), target, sampleEvent())
	if res.OK() {
		t.Fatal("应判失败")
	}
	if !res.Retryable {
		t.Error("连接失败应标为可重试")
	}
	if strings.Contains(res.Err, "secret-token") {
		t.Errorf("失败原因不应带出地址里的令牌: %q", res.Err)
	}
	if !strings.Contains(res.Err, "回调请求失败") {
		t.Errorf("原因应说明是请求没发出去: %q", res.Err)
	}
}

// 接收方不响应时按超时失败，不无限等待。
func TestClientTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)

	target := mustTarget(t, srv.URL+"/cb")
	start := time.Now()
	res := NewClient(30*time.Millisecond).Deliver(context.Background(), target, sampleEvent())
	if res.OK() {
		t.Fatal("应判超时失败")
	}
	if !strings.Contains(res.Err, "超时") {
		t.Errorf("原因应指明超时: %q", res.Err)
	}
	if !res.Retryable {
		t.Error("超时应标为可重试")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("超时预算没有生效，耗时 %v", elapsed)
	}
}

func TestCancelHintRecognizesNetworkTimeout(t *testing.T) {
	err := &url.Error{Op: "Post", URL: "https://example.com/callback", Err: os.ErrDeadlineExceeded}
	if got := cancelHint(err); !strings.Contains(got, "超时") {
		t.Errorf("网络超时原因应指明超时: %q", got)
	}
}

// 事件本身不合法时不发请求：接收方收到无法归属的通知只会多一轮排查。
func TestClientRejectsInvalidEvent(t *testing.T) {
	hit := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit++
	}))
	defer srv.Close()

	ev := sampleEvent()
	ev.TriggerID = ""
	res := NewClient(time.Second).Deliver(context.Background(), mustTarget(t, srv.URL+"/cb"), ev)
	if res.OK() {
		t.Fatal("缺 trigger_id 的事件不应发出")
	}
	if hit != 0 {
		t.Errorf("不应产生请求，实际 %d 次", hit)
	}
}

// 超时配置会被收敛到合法区间。
func TestNewClientNormalizesTimeout(t *testing.T) {
	if got := NewClient(0).Timeout(); got != DefaultTimeout {
		t.Errorf("零超时应回退默认值，实际 %v", got)
	}
	if got := NewClient(-time.Second).Timeout(); got != DefaultTimeout {
		t.Errorf("负超时应回退默认值，实际 %v", got)
	}
	if got := NewClient(time.Hour).Timeout(); got != MaxTimeout {
		t.Errorf("超长超时应收敛到上限，实际 %v", got)
	}
}

func mustTarget(t *testing.T, raw string) Target {
	t.Helper()
	target, err := ParseTarget(raw)
	if err != nil {
		t.Fatalf("构造回调地址失败（%s）：%v", raw, err)
	}
	return target
}
