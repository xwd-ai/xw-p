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

// 正常执行结束后，结果事件被交给回调队列，内容与步骤结果一致。
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
	if !rep.Notified {
		t.Fatal("应把结果交给回调")
	}

	events := notifier.captured()
	if len(events) != 1 {
		t.Fatalf("应产生 1 条事件，实际 %d", len(events))
	}
	ev := events[0]
	if ev.Event != webhook.EventDeliveryResult || ev.TriggerID != "t-200" {
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
	if !rep.Notified {
		t.Fatal("应把结果交给回调")
	}

	events := notifier.captured()
	if len(events) != 1 {
		t.Fatalf("应产生 1 条事件，实际 %d", len(events))
	}
	ev := events[0]
	if ev.Status != webhook.StatusFailed {
		t.Errorf("全部失败应为 failed，实际 %q", ev.Status)
	}
	if len(ev.Deliveries) != 1 || !strings.Contains(ev.Deliveries[0].Reason, "SMTP") {
		t.Errorf("回调应带失败原因: %+v", ev.Deliveries)
	}
}

// 没有投递动作（全部被过滤 / 短路）时不回调：「没发」不是「发失败」。
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
	if rep.Notified {
		t.Error("没有投递动作时不应回调")
	}
	if len(notifier.captured()) != 0 {
		t.Error("不应产生事件")
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
	} else if rep.Notified {
		t.Error("未接回调时不应标记已回调")
	}

	full := New(pipeline.NewWithRegistry(testLogger(), registry), nil, testLogger()).
		WithNotifier(&recordingNotifier{accept: false})
	if rep, err := full.Run(msg); err != nil {
		t.Fatalf("执行失败: %v", err)
	} else if rep.Notified {
		t.Error("投递方拒收时不应标记已回调")
	}
}

// 折算入口在没接通知方时直接返回，避免调用点多一层判空。
func TestNotifyResultsWithoutNotifier(t *testing.T) {
	msg := likedMessage("t-204", inAppStep("notify", "你有新的互动"))
	results := []pipeline.StepResult{{StepID: "notify", Channel: domain.ChannelInApp, Sent: true}}
	if NotifyResults(nil, msg, results) {
		t.Error("没有通知方时不应报告已投递")
	}
}
