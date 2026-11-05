package runner

import (
	"strings"
	"sync"
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
	"github.com/xwd-ai/xw-p/engine/internal/webhook"
)

// recordingNotifier 收下事件，便于断言回调内容。
type recordingNotifier struct {
	mu     sync.Mutex
	events []webhook.Event
	accept bool
}

func (n *recordingNotifier) Notify(ev webhook.Event) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.accept {
		return false
	}
	n.events = append(n.events, ev)
	return true
}

func (n *recordingNotifier) captured() []webhook.Event {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]webhook.Event(nil), n.events...)
}

func eventsByName(events []webhook.Event, name string) []webhook.Event {
	var out []webhook.Event
	for _, ev := range events {
		if ev.Event == name {
			out = append(out, ev)
		}
	}
	return out
}

// 正常执行结束后，受理与结果事件都交给回调队列，内容与步骤结果一致。
func TestRunnerNotifiesDeliveryResults(t *testing.T) {
	notifier := &recordingNotifier{accept: true}
	inbox := pipeline.NewMemoryInboxStore()
	registry := pipeline.NewProviderRegistry()
	registry.Register(pipeline.NewInAppProvider(testLogger(), inbox))
	r := New(pipeline.NewWithRegistry(testLogger(), registry), nil, testLogger()).
		WithNotifier(notifier)

	msg := likedMessage("t-200", inAppStep("notify", "{{ payload.actor }} 赞了你的帖子"))
	rep, err := r.Run(msg)
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	// 一条受理 + 一条渠道成功 + 一条汇总。
	if rep.Notified != 3 {
		t.Fatalf("应入队 3 条事件（受理 / 渠道成功 / 汇总），实际 %d", rep.Notified)
	}

	events := notifier.captured()
	if len(events) != 3 {
		t.Fatalf("应产生 3 条事件，实际 %d", len(events))
	}

	received := eventsByName(events, webhook.EventTriggerReceived)
	if len(received) != 1 {
		t.Fatalf("应有一条受理事件，实际 %+v", received)
	}
	if received[0].Trigger == nil || received[0].Trigger.Steps != 1 {
		t.Errorf("受理事件应带步骤数: %+v", received[0].Trigger)
	}
	if len(received[0].Trigger.Channels) != 1 || received[0].Trigger.Channels[0] != "in_app" {
		t.Errorf("受理事件应带声明渠道: %+v", received[0].Trigger.Channels)
	}
	if received[0].Trigger.Version != 3 || !received[0].Trigger.FromSnapshot {
		t.Errorf("受理事件应带版本与来源: %+v", received[0].Trigger)
	}

	sent := eventsByName(events, webhook.EventDeliverySent)
	if len(sent) != 1 {
		t.Fatalf("应有一条渠道成功事件，实际 %+v", sent)
	}
	if sent[0].Delivery == nil || sent[0].Delivery.Channel != "in_app" || sent[0].Delivery.StepID != "notify" {
		t.Errorf("渠道事件应指向站内信步骤: %+v", sent[0].Delivery)
	}

	results := eventsByName(events, webhook.EventDeliveryResult)
	if len(results) != 1 {
		t.Fatalf("应有一条汇总事件，实际 %+v", results)
	}
	ev := results[0]
	if ev.TriggerID != "t-200" {
		t.Errorf("事件归属不符: %+v", ev)
	}
	if ev.Workflow != "post_liked" || ev.SubscriberID != "creator_1" || ev.TransactionID != "tx-t-200" {
		t.Errorf("事件缺少工作流与订阅者信息: %+v", ev)
	}
	if ev.Status != webhook.StatusSent {
		t.Errorf("全部成功应为 sent，实际 %q", ev.Status)
	}
	if len(ev.Deliveries) != 1 {
		t.Fatalf("应有一条投递明细，实际 %+v", ev.Deliveries)
	}
	if ev.Deliveries[0].Channel != "in_app" || ev.Deliveries[0].StepID != "notify" {
		t.Errorf("明细应指向站内信步骤: %+v", ev.Deliveries[0])
	}
}

// 渠道失败时回调带原因，整体状态为 failed。
func TestRunnerNotifiesFailureReason(t *testing.T) {
	notifier := &recordingNotifier{accept: true}
	r, _, _ := writebackRig(t, func(reg *pipeline.ProviderRegistry) {
		register(t, reg, stubProvider{
			id:  "console-email",
			ch:  domain.ChannelEmail,
			res: pipeline.SendResult{Sent: false, Reason: "SMTP 连接失败"},
		}, "console-email", 100)
	})
	r.WithNotifier(notifier)

	msg := emailMessage("t-201", queue.StepSpec{
		ID: "mail", Type: string(domain.ChannelEmail), Config: map[string]any{"template": "有新互动"},
	})
	rep, err := r.Run(msg)
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if rep.Notified != 3 {
		t.Fatalf("应入队 3 条事件（受理 / 渠道失败 / 汇总），实际 %d", rep.Notified)
	}

	events := notifier.captured()
	failed := eventsByName(events, webhook.EventDeliveryFailed)
	if len(failed) != 1 {
		t.Fatalf("应有一条渠道失败事件，实际 %+v", failed)
	}
	if failed[0].Status != webhook.StatusFailed {
		t.Errorf("失败事件的状态应为 failed，实际 %q", failed[0].Status)
	}
	if failed[0].Delivery == nil || !strings.Contains(failed[0].Delivery.Reason, "SMTP") {
		t.Errorf("失败事件应带原因: %+v", failed[0].Delivery)
	}

	results := eventsByName(events, webhook.EventDeliveryResult)
	if len(results) != 1 || results[0].Status != webhook.StatusFailed {
		t.Fatalf("汇总应为 failed，实际 %+v", results)
	}
}

// 没有投递动作（全部被过滤 / 短路）时不产生结果事件：「没发」不是「发失败」。
// 受理事件仍然会发——触发确实被受理并执行了，只是没有渠道动作。
func TestRunnerSkipsCallbackWhenNothingDelivered(t *testing.T) {
	notifier := &recordingNotifier{accept: true}
	inbox := pipeline.NewMemoryInboxStore()
	registry := pipeline.NewProviderRegistry()
	registry.Register(pipeline.NewInAppProvider(testLogger(), inbox))
	r := New(pipeline.NewWithRegistry(testLogger(), registry), nil, testLogger()).
		WithNotifier(notifier)

	msg := likedMessage("t-202", queue.StepSpec{
		ID:   "filtered",
		Type: string(domain.ChannelInApp),
		Config: map[string]any{
			"template": "不该发出",
			"if":       map[string]any{"field": "payload.actor", "operator": "eq", "value": "不存在的人"},
		},
	})
	rep, err := r.Run(msg)
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if rep.Notified != 1 {
		t.Errorf("只有受理事件，实际入队 %d", rep.Notified)
	}
	for _, ev := range notifier.captured() {
		if ev.Event != webhook.EventTriggerReceived {
			t.Errorf("不应产生受理之外的事件: %+v", ev)
		}
	}
}

// 未接回调、或队列已满（投递方返回 false）时，报告如实标记未回调。
func TestRunnerCallbackUnavailable(t *testing.T) {
	inbox := pipeline.NewMemoryInboxStore()
	registry := pipeline.NewProviderRegistry()
	registry.Register(pipeline.NewInAppProvider(testLogger(), inbox))
	msg := likedMessage("t-203", inAppStep("notify", "{{ payload.actor }} 赞了你的帖子"))

	plain := New(pipeline.NewWithRegistry(testLogger(), registry), nil, testLogger())
	if rep, err := plain.Run(msg); err != nil {
		t.Fatalf("执行失败: %v", err)
	} else if rep.Notified != 0 {
		t.Error("未接回调时不应标记已回调")
	}

	full := New(pipeline.NewWithRegistry(testLogger(), registry), nil, testLogger()).
		WithNotifier(&recordingNotifier{accept: false})
	if rep, err := full.Run(msg); err != nil {
		t.Fatalf("执行失败: %v", err)
	} else if rep.Notified != 0 {
		t.Error("投递方拒收时不应标记已回调")
	}
}

// 折算入口在没接通知方时直接返回，避免调用点多一层判空。
func TestNotifyResultsWithoutNotifier(t *testing.T) {
	msg := likedMessage("t-204", inAppStep("notify", "你有新的互动"))
	results := []pipeline.StepResult{{StepID: "notify", Channel: domain.ChannelInApp, Sent: true}}
	if got := NotifyResults(nil, msg, results); got != 0 {
		t.Errorf("没有通知方时不应报告已投递，实际 %d", got)
	}
}

// 聚合步骤除卡片投递外，还多一条 digest.updated 事件，带当前计数与窗口。
func TestRunnerNotifiesDigestUpdate(t *testing.T) {
	notifier := &recordingNotifier{accept: true}
	inbox := pipeline.NewMemoryInboxStore()
	registry := pipeline.NewProviderRegistry()
	registry.Register(pipeline.NewInAppProvider(testLogger(), inbox))
	r := New(pipeline.NewWithRegistry(testLogger(), registry), nil, testLogger()).
		WithNotifier(notifier)

	msg := likedMessage("t-205", queue.StepSpec{
		ID:   "aggregate",
		Type: string(domain.ChannelDigest),
		Config: map[string]any{
			"key":      "post_id",
			"window":   "10m",
			"title":    "你的帖子有新互动",
			"template": "{{ digest.count }} 人赞了你的帖子",
		},
	})

	rep, err := r.Run(msg)
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}

	digests := eventsByName(notifier.captured(), webhook.EventDigestUpdated)
	if len(digests) != 1 {
		t.Fatalf("应有一条聚合事件，实际 %d（notified=%d）", len(digests), rep.Notified)
	}
	d := digests[0].Digest
	if d == nil {
		t.Fatalf("聚合事件应带 digest 段: %+v", digests[0])
	}
	if d.Key != "P-2026-1001" || d.Count != 1 || !d.Opened {
		t.Errorf("首次互动应开卡计数 1: %+v", d)
	}
	if d.ExpiresAt == nil {
		t.Error("聚合事件应带窗口到期时刻")
	}
	if err := digests[0].Validate(); err != nil {
		t.Errorf("聚合事件应通过校验: %v", err)
	}
	if digests[0].Workflow != "post_liked" || digests[0].SubscriberID != "creator_1" {
		t.Errorf("聚合事件缺少归属信息: %+v", digests[0])
	}
}

// 同一篇帖子的第二次互动命中同一张卡：计数累加，事件里不再是新开卡。
func TestRunnerNotifiesDigestAccumulation(t *testing.T) {
	notifier := &recordingNotifier{accept: true}
	inbox := pipeline.NewMemoryInboxStore()
	registry := pipeline.NewProviderRegistry()
	registry.Register(pipeline.NewInAppProvider(testLogger(), inbox))
	r := New(pipeline.NewWithRegistry(testLogger(), registry), nil, testLogger()).
		WithNotifier(notifier)

	step := queue.StepSpec{
		ID:   "aggregate",
		Type: string(domain.ChannelDigest),
		Config: map[string]any{
			"key":      "post_id",
			"window":   "10m",
			"title":    "你的帖子有新互动",
			"template": "{{ digest.count }} 人赞了你的帖子",
		},
	}
	for _, trigger := range []string{"t-206", "t-207"} {
		if _, err := r.Run(likedMessage(trigger, step)); err != nil {
			t.Fatalf("%s 执行失败: %v", trigger, err)
		}
	}

	var counts []int
	for _, ev := range notifier.captured() {
		if ev.Event == webhook.EventDigestUpdated {
			counts = append(counts, ev.Digest.Count)
		}
	}
	if len(counts) != 2 || counts[0] != 1 || counts[1] != 2 {
		t.Fatalf("两次互动应依次给出 1、2 条，实际 %v", counts)
	}

	second := eventsByName(notifier.captured(), webhook.EventDigestUpdated)[1]
	if second.Digest.Opened {
		t.Error("第二次互动不是新开卡")
	}
}
