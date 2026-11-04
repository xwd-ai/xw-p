package xwnotify

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

// captureBody 读出请求体并还原成通用结构，便于按字段断言。
func captureBody(t *testing.T, r *http.Request, out any) {
	t.Helper()
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("读请求体失败：%v", err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		t.Fatalf("请求体不是合法 JSON（%s）：%v", raw, err)
	}
}

func TestTriggerSendsEventAndReturnsAck(t *testing.T) {
	var seen struct {
		method string
		path   string
		header http.Header
		body   map[string]any
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.method = r.Method
		seen.path = r.URL.Path
		seen.header = r.Header.Clone()
		captureBody(t, r, &seen.body)
		_, _ = w.Write([]byte(`{"acknowledged":true,"triggerId":"trig-1","transactionId":"txn-1","acknowledgmentId":"trig-1"}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithAPIKey("key-1"), WithEnvironment("production"))
	if err != nil {
		t.Fatalf("NewClient 报错：%v", err)
	}
	event := NewEvent("post_commented", "creator-1001").
		WithEmail("creator@example.com").
		WithDeviceToken("fcm-token-9f2c1d").
		WithChannels("in_app", "push", "email").
		WithPayload(map[string]any{
			"post_id":    "P-2026-1001",
			"post_title": "我的第一支 Vlog",
			"actor":      "小林",
			"comment":    "拍得真好，求教程！",
		})

	ack, err := client.Trigger(context.Background(), event, WithIdempotencyKey("post-commented-P-2026-1001"))
	if err != nil {
		t.Fatalf("Trigger 报错：%v", err)
	}
	if ack.TriggerID != "trig-1" || ack.TransactionID != "txn-1" || ack.AcknowledgmentID != "trig-1" || !ack.Acknowledged {
		t.Fatalf("回执解析不对：%+v", ack)
	}

	if seen.method != http.MethodPost || seen.path != triggerPath {
		t.Fatalf("请求应当是 POST %s，实际 %s %s", triggerPath, seen.method, seen.path)
	}
	if got := seen.header.Get("X-Environment"); got != "production" {
		t.Fatalf("X-Environment 应为 production，实际 %q", got)
	}
	if got := seen.header.Get("X-Api-Key"); got != "key-1" {
		t.Fatalf("X-Api-Key 应为 key-1，实际 %q", got)
	}
	if got := seen.header.Get("Idempotency-Key"); got != "post-commented-P-2026-1001" {
		t.Fatalf("Idempotency-Key 未发送：%q", got)
	}
	if got := seen.header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("Content-Type 不对：%q", got)
	}
	if got := seen.header.Get("User-Agent"); !strings.HasPrefix(got, "xw-notify-go/") {
		t.Fatalf("User-Agent 不对：%q", got)
	}

	want := map[string]any{
		"name": "post_commented",
		"to": map[string]any{
			"subscriberId": "creator-1001",
			"email":        "creator@example.com",
			"deviceToken":  "fcm-token-9f2c1d",
			"channels":     []any{"in_app", "push", "email"},
		},
		"payload": map[string]any{
			"post_id":    "P-2026-1001",
			"post_title": "我的第一支 Vlog",
			"actor":      "小林",
			"comment":    "拍得真好，求教程！",
		},
	}
	if !reflect.DeepEqual(seen.body, want) {
		t.Fatalf("请求体不符合预期：\n实际 %#v\n期望 %#v", seen.body, want)
	}
}

func TestTriggerOmitsEmptyOptionalFields(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captureBody(t, r, &body)
		if got := r.Header.Get("X-Api-Key"); got != "" {
			t.Errorf("未配置 API Key 时不应发送该请求头，实际 %q", got)
		}
		if got := r.Header.Get("Idempotency-Key"); got != "" {
			t.Errorf("未指定幂等键时不应发送该请求头，实际 %q", got)
		}
		_, _ = w.Write([]byte(`{"acknowledged":true,"triggerId":"trig-1","transactionId":"txn-1","acknowledgmentId":"trig-1"}`))
	}))
	defer server.Close()

	client, _ := NewClient(server.URL)
	if _, err := client.Trigger(context.Background(), NewEvent("post_liked", "creator-1001")); err != nil {
		t.Fatalf("Trigger 报错：%v", err)
	}
	want := map[string]any{
		"name": "post_liked",
		"to":   map[string]any{"subscriberId": "creator-1001"},
	}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("空字段不应出现在请求体里：%#v", body)
	}
}

func TestTriggerValidationSkipsRequest(t *testing.T) {
	var hits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = w.Write([]byte(`{"acknowledged":true,"triggerId":"trig-1"}`))
	}))
	defer server.Close()

	client, _ := NewClient(server.URL)
	cases := map[string]Event{
		"缺少名称":        NewEvent("  ", "creator-1001"),
		"缺少订阅者标识":     NewEvent("post_liked", "   "),
		"名称超过 255 字符": NewEvent(strings.Repeat("赞", 256), "creator-1001"),
	}
	for name, event := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := client.Trigger(context.Background(), event)
			if !IsValidation(err) || !IsLocal(err) {
				t.Fatalf("应当是本地校验错误：%v", err)
			}
		})
	}
	if hits != 0 {
		t.Fatalf("校验失败不应发出请求，实际发出 %d 次", hits)
	}
}

func TestTriggerParsesUnacknowledgedAsProtocolError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"acknowledged":false}`))
	}))
	defer server.Close()

	client, _ := NewClient(server.URL)
	_, err := client.Trigger(context.Background(), NewEvent("post_liked", "creator-1001"))
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("应当是 ProtocolError：%v", err)
	}
}

func TestTriggerParsesMissingTriggerIDAsProtocolError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"acknowledged":true,"transactionId":"txn-1"}`))
	}))
	defer server.Close()

	client, _ := NewClient(server.URL)
	_, err := client.Trigger(context.Background(), NewEvent("post_liked", "creator-1001"))
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) || !strings.Contains(protocolErr.Reason, "triggerId") {
		t.Fatalf("缺少 triggerId 应当报 ProtocolError：%v", err)
	}
}

func TestWithChannelsCopiesSlice(t *testing.T) {
	channels := []string{"in_app", "push"}
	event := NewEvent("post_liked", "creator-1001").WithChannels(channels...)
	channels[0] = "sms"
	if event.To.Channels[0] != "in_app" {
		t.Fatal("事件应持有渠道列表的副本")
	}
	event = event.WithChannels()
	if event.To.Channels != nil {
		t.Fatal("不传渠道应回到「由工作流决定」")
	}
}

func TestTriggerBatchKeepsOrderAndPartials(t *testing.T) {
	var (
		path   string
		header http.Header
		body   struct {
			Events []map[string]any `json:"events"`
		}
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		header = r.Header.Clone()
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("批量请求体解析失败：%v", err)
		}
		// 中间那条被拒：结果仍按请求顺序返回，下标对齐。
		_, _ = w.Write([]byte(`{"acknowledged":true,"accepted":2,"rejected":1,"results":[` +
			`{"index":0,"name":"post_liked","subscriberId":"creator-1001","status":"accepted","triggerId":"t-0","transactionId":"x-0","acknowledgmentId":"t-0"},` +
			`{"index":1,"name":"post_mentioned","subscriberId":"creator-1002","status":"rejected","error":"工作流模板不存在或未启用"},` +
			`{"index":2,"name":"post_commented","subscriberId":"creator-1003","status":"accepted","triggerId":"t-2","transactionId":"x-2","acknowledgmentId":"t-2"}]}`))
	}))
	defer server.Close()

	client, _ := NewClient(server.URL)
	events := []Event{
		NewEvent("post_liked", "creator-1001").WithPayload(map[string]any{"post_id": "P-1"}),
		NewEvent("post_mentioned", "creator-1002"),
		NewEvent("post_commented", "creator-1003"),
	}
	ack, err := client.TriggerBatch(context.Background(), events, WithIdempotencyKey("burst-1"))
	if err != nil {
		t.Fatalf("TriggerBatch 报错：%v", err)
	}
	if path != batchPath {
		t.Fatalf("应当请求 %s，实际 %s", batchPath, path)
	}
	if got := header.Get("Idempotency-Key"); got != "burst-1" {
		t.Fatalf("整批幂等键未发送：%q", got)
	}
	if len(body.Events) != 3 {
		t.Fatalf("批量请求应带 3 条事件，实际 %d 条", len(body.Events))
	}
	if body.Events[0]["name"] != "post_liked" || body.Events[2]["name"] != "post_commented" {
		t.Fatalf("事件顺序被改动：%#v", body.Events)
	}
	if ack.Accepted != 2 || ack.Rejected != 1 {
		t.Fatalf("计数不对：%+v", ack)
	}
	if !ack.Results[0].Accepted() || ack.Results[1].Accepted() || !ack.Results[2].Accepted() {
		t.Fatalf("逐条状态不对：%+v", ack.Results)
	}
	if ack.Results[1].Error == "" {
		t.Fatal("被拒条目应带原因")
	}
}

func TestTriggerBatchLocalValidation(t *testing.T) {
	var hits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = w.Write([]byte(`{"acknowledged":true,"accepted":0,"rejected":0,"results":[]}`))
	}))
	defer server.Close()

	client, _ := NewClient(server.URL)
	if _, err := client.TriggerBatch(context.Background(), nil); !IsLocal(err) {
		t.Fatalf("空批次应当本地报错：%v", err)
	}

	tooMany := make([]Event, MaxBatchEvents+1)
	for i := range tooMany {
		tooMany[i] = NewEvent("post_liked", "creator-1001")
	}
	if _, err := client.TriggerBatch(context.Background(), tooMany); !IsLocal(err) {
		t.Fatalf("超过上限应当本地报错：%v", err)
	}

	bad := []Event{
		NewEvent("post_liked", "creator-1001"),
		NewEvent("", "creator-1001"),
	}
	_, err := client.TriggerBatch(context.Background(), bad)
	if !IsLocal(err) {
		t.Fatalf("坏事件应当本地报错：%v", err)
	}
	if !strings.Contains(err.Error(), "第 1 条事件") {
		t.Fatalf("错误文案应指出是第几条：%v", err)
	}
	if hits != 0 {
		t.Fatalf("校验失败不应发出请求，实际 %d 次", hits)
	}
}

func TestTriggerBatchRejectsMisalignedResults(t *testing.T) {
	cases := map[string]string{
		"条数不足": `{"acknowledged":true,"accepted":1,"rejected":0,"results":[` +
			`{"index":0,"name":"post_liked","subscriberId":"c","status":"accepted","triggerId":"t-0"}]}`,
		"下标错位": `{"acknowledged":true,"accepted":2,"rejected":0,"results":[` +
			`{"index":0,"name":"post_liked","subscriberId":"c","status":"accepted","triggerId":"t-0"},` +
			`{"index":5,"name":"post_liked","subscriberId":"c","status":"accepted","triggerId":"t-1"}]}`,
	}
	for name, response := range cases {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(response))
			}))
			defer server.Close()

			client, _ := NewClient(server.URL)
			events := []Event{NewEvent("post_liked", "creator-1001"), NewEvent("post_liked", "creator-1002")}
			_, err := client.TriggerBatch(context.Background(), events)
			var protocolErr *ProtocolError
			if !errors.As(err, &protocolErr) {
				t.Fatalf("应当报 ProtocolError：%v", err)
			}
		})
	}
}

func TestTriggerSendsToBaseURLWithPrefix(t *testing.T) {
	var path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		_, _ = w.Write([]byte(`{"acknowledged":true,"triggerId":"trig-1"}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL + "/notify")
	if err != nil {
		t.Fatalf("NewClient 报错：%v", err)
	}
	if _, err := client.Trigger(context.Background(), NewEvent("post_liked", "creator-1001")); err != nil {
		t.Fatalf("Trigger 报错：%v", err)
	}
	if path != "/notify"+triggerPath {
		t.Fatalf("网关前缀丢失，实际路径 %s", path)
	}
}
