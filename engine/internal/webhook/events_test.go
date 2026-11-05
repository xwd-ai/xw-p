package webhook

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

func testWorkflow() domain.Workflow {
	return domain.Workflow{
		ID:      "wf-1",
		Name:    "post_liked",
		Version: 4,
		Steps: []domain.Step{
			{ID: "aggregate", Type: domain.ChannelDigest},
			{ID: "notify", Type: domain.ChannelInApp},
			{ID: "mail", Type: domain.ChannelEmail},
			// 同一渠道配两个步骤只报一次渠道。
			{ID: "notify_again", Type: domain.ChannelInApp},
		},
	}
}

// 受理事件给出步骤数、声明的渠道、版本与来源；渠道去重且按步骤顺序。
func TestNewTriggerEvent(t *testing.T) {
	ev, ok := NewTriggerEvent(testMessage(), testWorkflow(), true)
	if !ok {
		t.Fatal("应产生受理事件")
	}
	if ev.Event != EventTriggerReceived {
		t.Errorf("事件名 = %q", ev.Event)
	}
	if ev.Trigger == nil {
		t.Fatal("应带 trigger 段")
	}
	if ev.Trigger.Steps != 4 {
		t.Errorf("步骤数 = %d", ev.Trigger.Steps)
	}
	want := []string{"in_app", "email"}
	if len(ev.Trigger.Channels) != len(want) {
		t.Fatalf("声明渠道 = %v", ev.Trigger.Channels)
	}
	for i, ch := range want {
		if ev.Trigger.Channels[i] != ch {
			t.Errorf("第 %d 个渠道 = %q，期望 %q", i, ev.Trigger.Channels[i], ch)
		}
	}
	if ev.Trigger.Version != 4 || !ev.Trigger.FromSnapshot {
		t.Errorf("版本/来源 = %+v", ev.Trigger)
	}
	if ev.Workflow != "post_commented" || ev.TriggerID != "trig-1" {
		t.Errorf("归属字段 = %+v", ev)
	}
	if err := ev.Validate(); err != nil {
		t.Errorf("受理事件应通过校验: %v", err)
	}
}

// 消息没带工作流名时用定义表里的名字兜底；两边都没有则不产生事件。
func TestNewTriggerEventWorkflowFallback(t *testing.T) {
	msg := testMessage()
	msg.Name = ""
	ev, ok := NewTriggerEvent(msg, testWorkflow(), false)
	if !ok {
		t.Fatal("定义表有名字时仍应产生受理事件")
	}
	if ev.Workflow != "post_liked" {
		t.Errorf("工作流名 = %q", ev.Workflow)
	}

	empty := queue.QueuedMessage{TriggerID: "trig-2"}
	if _, ok := NewTriggerEvent(empty, domain.Workflow{}, false); ok {
		t.Error("没有工作流名时不应产生受理事件")
	}
}

// 聚合步骤按配置里的卡片渠道计入：只看步骤类型会把「卡片进收件箱」漏掉。
func TestNewTriggerEventIncludesDigestChannel(t *testing.T) {
	wf := domain.Workflow{
		Name: "post_liked",
		Steps: []domain.Step{
			{ID: "aggregate", Type: domain.ChannelDigest, Config: map[string]any{"key": "post_id", "channel": "email"}},
			{ID: "notify", Type: domain.ChannelInApp},
		},
	}
	ev, ok := NewTriggerEvent(testMessage(), wf, false)
	if !ok {
		t.Fatal("应产生受理事件")
	}
	want := []string{"email", "in_app"}
	if len(ev.Trigger.Channels) != len(want) {
		t.Fatalf("声明渠道 = %v", ev.Trigger.Channels)
	}
	for i, ch := range want {
		if ev.Trigger.Channels[i] != ch {
			t.Errorf("第 %d 个渠道 = %q，期望 %q", i, ev.Trigger.Channels[i], ch)
		}
	}

	// 默认渠道是站内信：没写 channel 的聚合步骤也应计入。
	wf.Steps[0].Config = map[string]any{"key": "post_id"}
	ev, _ = NewTriggerEvent(testMessage(), wf, false)
	if len(ev.Trigger.Channels) != 1 || ev.Trigger.Channels[0] != "in_app" {
		t.Errorf("默认聚合渠道应并入站内信: %v", ev.Trigger.Channels)
	}
}

// 逐渠道事件：成功与失败各一条，跳过没有投递动作的步骤。
func TestNewChannelEventsSplitStatus(t *testing.T) {
	results := []pipeline.StepResult{
		{StepID: "notify", StepType: domain.StepTypeInApp, Channel: domain.ChannelInApp, Sent: true, UsedProvider: "inapp"},
		{StepID: "mail", StepType: domain.StepTypeEmail, Channel: domain.ChannelEmail, Error: "550 拒收"},
		{StepID: "filtered", Channel: domain.ChannelPush, Skipped: true, SkipReason: "preference"},
		{StepID: "later", Channel: domain.ChannelEmail, Deferred: true},
		{StepID: "slow", Channel: domain.ChannelSMS, TimedOut: true},
	}
	events := NewChannelEvents(testMessage(), results)
	if len(events) != 2 {
		t.Fatalf("应只有两条渠道事件，实际 %d：%+v", len(events), events)
	}

	if events[0].Event != EventDeliverySent || events[0].Status != StatusSent {
		t.Errorf("第一条应为成功事件: %+v", events[0])
	}
	if events[0].Delivery == nil || events[0].Delivery.StepType != string(domain.StepTypeInApp) {
		t.Errorf("事件应带步骤类型: %+v", events[0].Delivery)
	}
	if events[1].Event != EventDeliveryFailed || events[1].Status != StatusFailed {
		t.Errorf("第二条应为失败事件: %+v", events[1])
	}
	if !strings.Contains(events[1].Delivery.Reason, "550") {
		t.Errorf("失败事件应带原因: %+v", events[1].Delivery)
	}
	for _, ev := range events {
		if err := ev.Validate(); err != nil {
			t.Errorf("渠道事件应通过校验: %v", err)
		}
	}
}

// 渠道事件持有明细的副本：调用方事后改步骤结果不影响已入队的事件。
func TestNewChannelEventsCopiesDelivery(t *testing.T) {
	attempts := []string{"smtp-email", "console-email"}
	results := []pipeline.StepResult{{
		StepID:           "mail",
		Channel:          domain.ChannelEmail,
		Error:            "连接失败",
		ProviderAttempts: attempts,
	}}
	events := NewChannelEvents(testMessage(), results)
	if len(events) != 1 || events[0].Delivery == nil {
		t.Fatalf("应产生一条事件: %+v", events)
	}
	attempts[0] = "被改掉"
	if events[0].Delivery.Attempts[0] != "smtp-email" {
		t.Errorf("事件应持有副本，实际 %v", events[0].Delivery.Attempts)
	}
	results[0].Error = "被改掉"
	if events[0].Delivery.Reason != "连接失败" {
		t.Errorf("事件应持有副本，实际 %q", events[0].Delivery.Reason)
	}
}

// 聚合事件只在卡片真的落下来时才有；重放也要报，因为它决定计数是否变化。
func TestNewDigestEvents(t *testing.T) {
	expires := time.Now().Add(10 * time.Minute)
	results := []pipeline.StepResult{
		{
			StepID: "aggregate", StepType: domain.StepTypeDigest, Channel: domain.ChannelInApp,
			Sent: true, AggregateKey: "P1001", AggregateCount: 3, AggregateOpened: false,
			AggregateExpiresAt: expires,
		},
		{
			StepID: "replay", StepType: domain.StepTypeDigest, Channel: domain.ChannelInApp,
			AggregateKey: "P1002", AggregateCount: 5, AggregateReplayed: true,
			AggregateExpiresAt: expires,
		},
		// 聚合键缺失（步骤失败）时没有卡片可言。
		{StepID: "broken", StepType: domain.StepTypeDigest, Channel: domain.ChannelInApp, Error: "缺少聚合键"},
		// 非聚合步骤不产生这类事件。
		{StepID: "notify", StepType: domain.StepTypeInApp, Channel: domain.ChannelInApp, Sent: true},
	}
	events := NewDigestEvents(testMessage(), results)
	if len(events) != 2 {
		t.Fatalf("应只有两条聚合事件，实际 %d：%+v", len(events), events)
	}
	if events[0].Digest.Key != "P1001" || events[0].Digest.Count != 3 || events[0].Digest.Opened {
		t.Errorf("聚合事件内容 = %+v", events[0].Digest)
	}
	if events[0].Digest.ExpiresAt == nil || events[0].Digest.ExpiresAt.IsZero() {
		t.Error("应带窗口到期时刻")
	}
	if !events[1].Digest.Replayed {
		t.Errorf("重放标记 = %+v", events[1].Digest)
	}
	for _, ev := range events {
		if err := ev.Validate(); err != nil {
			t.Errorf("聚合事件应通过校验: %v", err)
		}
	}
}

// 报文形态：公共信封 + 类型专属段落，未用到的段落不出现。
func TestEventJSONShapeByType(t *testing.T) {
	trigger, ok := NewTriggerEvent(testMessage(), testWorkflow(), false)
	if !ok {
		t.Fatal("应产生受理事件")
	}
	raw, err := json.Marshal(trigger)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	payload := string(raw)
	for _, want := range []string{
		`"event":"trigger.received"`,
		`"trigger":{"steps":4,"channels":["in_app","email"],"version":4}`,
	} {
		if !strings.Contains(payload, want) {
			t.Errorf("报文缺少 %s：%s", want, payload)
		}
	}
	for _, unwanted := range []string{`"delivery"`, `"deliveries"`, `"summary"`, `"digest"`, `"status"`} {
		if strings.Contains(payload, unwanted) {
			t.Errorf("受理事件不应带 %s：%s", unwanted, payload)
		}
	}

	failed := NewChannelEvents(testMessage(), []pipeline.StepResult{{
		StepID: "mail", Channel: domain.ChannelEmail, Error: "550 拒收", Duration: 12 * time.Millisecond,
	}})
	raw, err = json.Marshal(failed[0])
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	payload = string(raw)
	for _, want := range []string{
		`"event":"delivery.failed"`,
		`"status":"failed"`,
		`"delivery":{"step_id":"mail","channel":"email","status":"failed","reason":"550 拒收","duration_ms":12}`,
	} {
		if !strings.Contains(payload, want) {
			t.Errorf("报文缺少 %s：%s", want, payload)
		}
	}
}

// 校验按类型分工：缺正文、状态与事件名不一致、聚合字段自相矛盾都要拦住。
func TestEventValidateByType(t *testing.T) {
	expires := occurredAtNow()

	base := func() Event {
		return Event{
			ID:            "e1",
			OccurredAt:    occurredAtNow(),
			TriggerID:     "trig-1",
			Workflow:      "post_liked",
			TransactionID: "tx-1",
		}
	}

	triggered := base()
	triggered.Event = EventTriggerReceived
	triggered.Trigger = &Trigger{Steps: 2, Channels: []string{"in_app"}}
	if err := triggered.Validate(); err != nil {
		t.Fatalf("合法受理事件不应报错: %v", err)
	}

	sent := base()
	sent.Event = EventDeliverySent
	sent.Status = StatusSent
	sent.Delivery = &Delivery{StepID: "notify", Channel: "in_app", Status: StatusSent}
	if err := sent.Validate(); err != nil {
		t.Fatalf("合法渠道事件不应报错: %v", err)
	}

	digested := base()
	digested.Event = EventDigestUpdated
	digested.Digest = &Digest{StepID: "aggregate", Key: "P1001", Count: 2, ExpiresAt: &expires}
	if err := digested.Validate(); err != nil {
		t.Fatalf("合法聚合事件不应报错: %v", err)
	}

	cases := map[string]Event{
		"受理缺正文":   func() Event { e := triggered; e.Trigger = nil; return e }(),
		"受理缺工作流":  func() Event { e := triggered; e.Workflow = ""; return e }(),
		"渠道事件缺明细": func() Event { e := sent; e.Delivery = nil; return e }(),
		"渠道事件状态不符": func() Event {
			e := sent
			e.Event = EventDeliveryFailed
			return e
		}(),
		"渠道事件明细状态不符": func() Event {
			e := sent
			e.Delivery = &Delivery{Channel: "in_app", Status: StatusFailed}
			return e
		}(),
		"渠道事件缺渠道": func() Event {
			e := sent
			e.Delivery = &Delivery{Status: StatusSent}
			return e
		}(),
		"聚合缺正文": func() Event { e := digested; e.Digest = nil; return e }(),
		"聚合缺键": func() Event {
			e := digested
			d := *digested.Digest
			d.Key = " "
			e.Digest = &d
			return e
		}(),
		"聚合计数为零": func() Event {
			e := digested
			d := *digested.Digest
			d.Count = 0
			e.Digest = &d
			return e
		}(),
		"聚合既是新卡又是重放": func() Event {
			e := digested
			d := *digested.Digest
			d.Opened = true
			d.Replayed = true
			e.Digest = &d
			return e
		}(),
		"聚合缺到期时刻": func() Event {
			e := digested
			d := *digested.Digest
			d.ExpiresAt = nil
			e.Digest = &d
			return e
		}(),
	}
	for name, ev := range cases {
		if err := ev.Validate(); err == nil {
			t.Errorf("%s：应判为不合法", name)
		} else if !strings.Contains(err.Error(), "事件不合法") {
			t.Errorf("%s：错误信息应可直接读：%v", name, err)
		}
	}
}
