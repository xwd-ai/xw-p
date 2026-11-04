package xwnotify_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/xwd-ai/xw-p/sdk/go/xwnotify"
)

// ExampleClient_Trigger 触发一条评论通知，并带上幂等键。
//
// 幂等键由业务侧生成：同一篇帖子的同一条评论重发时，服务端返回首次回执，
// 不会让创作者收到两条。
func ExampleClient_Trigger() {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"acknowledged":true,"triggerId":"trig-1","transactionId":"txn-1","acknowledgmentId":"trig-1"}`))
	}))
	defer server.Close()

	client, err := xwnotify.NewClient(server.URL)
	if err != nil {
		panic(err)
	}

	ack, err := client.Trigger(
		context.Background(),
		xwnotify.NewEvent("post_commented", "creator-1001").
			WithEmail("creator@example.com").
			WithPayload(map[string]any{"post_id": "P-2026-1001", "actor": "小林", "comment": "求教程"}),
		xwnotify.WithIdempotencyKey("post-commented-P-2026-1001"),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(ack.TriggerID, ack.Acknowledged)
	// Output: trig-1 true
}

// ExampleClient_TriggerBatch 一次提交同一条热帖下的多条互动。
func ExampleClient_TriggerBatch() {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"acknowledged":true,"accepted":2,"rejected":0,"results":[` +
			`{"index":0,"name":"post_liked","subscriberId":"creator-1001","status":"accepted","triggerId":"t-0"},` +
			`{"index":1,"name":"post_mentioned","subscriberId":"creator-1001","status":"accepted","triggerId":"t-1"}]}`))
	}))
	defer server.Close()

	client, err := xwnotify.NewClient(server.URL)
	if err != nil {
		panic(err)
	}

	events := []xwnotify.Event{
		xwnotify.NewEvent("post_liked", "creator-1001").WithPayload(map[string]any{"post_id": "P-2026-1001", "actor": "小林"}),
		xwnotify.NewEvent("post_mentioned", "creator-1001").WithPayload(map[string]any{"post_id": "P-2026-1001", "actor": "阿澈"}),
	}
	ack, err := client.TriggerBatch(context.Background(), events)
	if err != nil {
		panic(err)
	}

	// 逐条结果与请求等长且按下标对齐。
	fmt.Println(ack.Accepted, ack.Rejected, ack.Results[0].TriggerID, ack.Results[1].TriggerID)
	// Output: 2 0 t-0 t-1
}
