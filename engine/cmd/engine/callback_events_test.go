package main

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
	"github.com/xwd-ai/xw-p/engine/internal/runner"
	"github.com/xwd-ai/xw-p/engine/internal/webhook"
)

// sinkSnapshot 在读锁内取一份事件副本，便于按事件名归类断言。
func sinkSnapshot(sink *eventSink) []webhook.Event {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return append([]webhook.Event(nil), sink.events...)
}

func eventsNamed(events []webhook.Event, name string) []webhook.Event {
	var out []webhook.Event
	for _, ev := range events {
		if ev.Event == name {
			out = append(out, ev)
		}
	}
	return out
}

// 订阅全部事件时，一次带聚合步骤的执行会真的通过 HTTP 发出四类报文：
// 受理、逐渠道成功、聚合卡片状态、汇总。这里用真实的收发链路（httptest
// 服务端 + webhook.Client，接收端逐条验签），只把队列与 Redis 换成进程内对象。
func TestCallbackEventTypesEndToEnd(t *testing.T) {
	sink := &eventSink{secret: callbackSecret}
	srv := httptest.NewServer(sink)
	defer srv.Close()

	target, err := webhook.ParseTarget(srv.URL + "/hooks/xw")
	if err != nil {
		t.Fatalf("回调地址解析失败: %v", err)
	}
	signer, err := webhook.NewSigner(callbackSecret)
	if err != nil {
		t.Fatalf("构造签名器失败: %v", err)
	}
	dispatcher := webhook.NewDispatcher(target, webhook.NewClient(2*time.Second).WithSigner(signer), webhook.Options{
		Workers:   2,
		QueueSize: 32,
		Timeout:   2 * time.Second,
		Selection: webhook.AllEvents(),
		Logger:    discardLogger(),
	})
	if dispatcher == nil {
		t.Fatal("应装配回调分发器")
	}

	inbox := pipeline.NewMemoryInboxStore()
	registry := pipeline.NewProviderRegistry()
	registry.Register(pipeline.NewInAppProvider(discardLogger(), inbox))
	pipe := pipeline.NewWithRegistry(discardLogger(), registry)
	exec := runner.New(pipe, nil, discardLogger()).WithNotifier(dispatcher)

	// 同一篇帖子的两次点赞：聚合计数从 1 累加到 2。
	for _, trigger := range []string{"t-events-1", "t-events-2"} {
		msg := queue.QueuedMessage{
			TriggerID:     trigger,
			TransactionID: "tx-" + trigger,
			Name:          "post_liked",
			SubscriberID:  "creator_1",
			Channels:      []string{"in_app"},
			Payload: map[string]any{
				"post_id":    "P-2026-1001",
				"post_title": "我的第一支 Vlog",
				"actor":      "小林",
			},
			Workflow: &queue.WorkflowSnapshot{
				Name:    "post_liked",
				Version: 2,
				Steps: []queue.StepSpec{
					{
						ID:   "aggregate",
						Type: string(domain.ChannelDigest),
						Config: map[string]any{
							"key":      "post_id",
							"window":   "10m",
							"title":    "你的帖子有新互动",
							"template": "{{ digest.count }} 人赞了你的帖子",
						},
					},
					{
						ID:     "notify",
						Type:   string(domain.ChannelInApp),
						Config: map[string]any{"template": "{{ payload.actor }} 赞了你的帖子"},
					},
				},
			},
		}
		if _, err := exec.Run(msg); err != nil {
			t.Fatalf("%s 执行失败: %v", trigger, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := dispatcher.Close(ctx); err != nil {
		t.Fatalf("关闭分发器失败: %v", err)
	}
	if problems := sink.signatureProblems(); len(problems) != 0 {
		t.Fatalf("回调签名校验未通过：%v", problems)
	}

	events := sinkSnapshot(sink)
	// 每次执行：受理 1 + 两个渠道（聚合卡片、站内信）2 + 聚合状态 1 + 汇总 1。
	if len(events) != 10 {
		t.Fatalf("应收到 10 条报文，实际 %d", len(events))
	}

	received := eventsNamed(events, webhook.EventTriggerReceived)
	if len(received) != 2 {
		t.Fatalf("受理事件 %d 条", len(received))
	}
	if received[1].Trigger == nil || received[1].Trigger.Version != 2 || received[1].Trigger.Steps != 2 {
		t.Errorf("受理事件内容 = %+v", received[1].Trigger)
	}
	if len(received[1].Trigger.Channels) != 1 || received[1].Trigger.Channels[0] != "in_app" {
		t.Errorf("受理事件渠道 = %v", received[1].Trigger.Channels)
	}

	sent := eventsNamed(events, webhook.EventDeliverySent)
	if len(sent) != 4 {
		t.Fatalf("渠道成功事件 %d 条，期望 4", len(sent))
	}
	steps := map[string]bool{}
	for _, ev := range sent {
		if ev.Delivery == nil {
			t.Fatalf("渠道事件缺少明细: %+v", ev)
		}
		steps[ev.Delivery.StepID] = true
	}
	if !steps["aggregate"] || !steps["notify"] {
		t.Errorf("渠道事件应覆盖两个步骤，实际 %v", steps)
	}

	digests := eventsNamed(events, webhook.EventDigestUpdated)
	if len(digests) != 2 {
		t.Fatalf("聚合事件 %d 条", len(digests))
	}
	if digests[0].Digest == nil || digests[0].Digest.Key != "P-2026-1001" {
		t.Errorf("第一条聚合事件 = %+v", digests[0].Digest)
	}
	if digests[0].Digest.Count != 1 || !digests[0].Digest.Opened {
		t.Errorf("首次互动应开卡计数 1: %+v", digests[0].Digest)
	}
	if digests[1].Digest.Count != 2 || digests[1].Digest.Opened {
		t.Errorf("第二次互动应累加到 2: %+v", digests[1].Digest)
	}
	if digests[1].Digest.ExpiresAt == nil {
		t.Error("聚合事件应带窗口到期时刻")
	}

	results := eventsNamed(events, webhook.EventDeliveryResult)
	if len(results) != 2 {
		t.Fatalf("汇总事件 %d 条", len(results))
	}
	for _, ev := range results {
		if ev.Summary == nil || ev.Summary.Total != 2 || ev.Summary.Sent != 2 {
			t.Errorf("汇总事件 = %+v", ev.Summary)
		}
		if ev.Status != webhook.StatusSent {
			t.Errorf("汇总状态 = %q", ev.Status)
		}
	}

	ids := map[string]bool{}
	for _, ev := range events {
		if ev.ID == "" || ids[ev.ID] {
			t.Errorf("事件标识应非空且唯一：%q", ev.ID)
		}
		ids[ev.ID] = true
	}
}

// 只订阅汇总事件时，同一次执行只发一条报文：升级引擎不改变已有接入方的流量。
func TestCallbackEventSelectionFiltersTraffic(t *testing.T) {
	sink := &eventSink{}
	srv := httptest.NewServer(sink)
	defer srv.Close()

	target, err := webhook.ParseTarget(srv.URL + "/callback")
	if err != nil {
		t.Fatalf("回调地址解析失败: %v", err)
	}
	dispatcher := webhook.NewDispatcher(target, webhook.NewClient(2*time.Second), webhook.Options{
		Workers:   1,
		QueueSize: 8,
		Timeout:   2 * time.Second,
		Logger:    discardLogger(),
	})

	inbox := pipeline.NewMemoryInboxStore()
	registry := pipeline.NewProviderRegistry()
	registry.Register(pipeline.NewInAppProvider(discardLogger(), inbox))
	exec := runner.New(pipeline.NewWithRegistry(discardLogger(), registry), nil, discardLogger()).
		WithNotifier(dispatcher)

	msg := queue.QueuedMessage{
		TriggerID:    "t-filter-1",
		Name:         "post_commented",
		SubscriberID: "creator_1",
		Payload:      map[string]any{"post_id": "P-2026-1002", "actor": "momo", "comment": "写得好"},
		Workflow: &queue.WorkflowSnapshot{
			Name: "post_commented",
			Steps: []queue.StepSpec{{
				ID:     "notify",
				Type:   string(domain.ChannelInApp),
				Config: map[string]any{"template": "{{ payload.actor }} 评论了你的帖子"},
			}},
		},
	}
	if _, err := exec.Run(msg); err != nil {
		t.Fatalf("执行失败: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := dispatcher.Close(ctx); err != nil {
		t.Fatalf("关闭分发器失败: %v", err)
	}

	events := sinkSnapshot(sink)
	if len(events) != 1 {
		t.Fatalf("默认订阅只应发一条汇总事件，实际 %d 条", len(events))
	}
	if events[0].Event != webhook.EventDeliveryResult {
		t.Errorf("收到的事件 = %q", events[0].Event)
	}
	if stats := dispatcher.Stats(); stats.Filtered != 2 {
		t.Errorf("受理与渠道事件应被筛掉，实际 filtered=%d", stats.Filtered)
	}
}
