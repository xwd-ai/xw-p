package webhook

import (
	"context"
	"strings"
	"testing"
	"time"
)

// 未配置时只订阅汇总事件：升级引擎不会让已有接入方的回调量翻几倍。
func TestSelectionDefault(t *testing.T) {
	for _, raw := range []string{"", "   "} {
		sel, err := ParseSelection(raw)
		if err != nil {
			t.Fatalf("%q 应解析成功: %v", raw, err)
		}
		if !sel.Allows(EventDeliveryResult) {
			t.Errorf("%q 应订阅汇总事件", raw)
		}
		for _, name := range []string{EventTriggerReceived, EventDeliverySent, EventDeliveryFailed, EventDigestUpdated} {
			if sel.Allows(name) {
				t.Errorf("%q 不应订阅 %s", raw, name)
			}
		}
		if sel.String() != EventDeliveryResult {
			t.Errorf("默认集合的可读形态 = %q", sel.String())
		}
	}

	// 结构体零值（Options 字面量没写这个字段）同样按默认处理。
	var zero Selection
	if !zero.Allows(EventDeliveryResult) || zero.Allows(EventTriggerReceived) {
		t.Error("零值应按默认集合处理")
	}
}

// 全量与列表两种写法；大小写与空格不影响解析，重复项去重。
func TestSelectionParse(t *testing.T) {
	for _, raw := range []string{"all", "ALL", "All", " * "} {
		sel, err := ParseSelection(raw)
		if err != nil {
			t.Fatalf("%q 应解析成功: %v", raw, err)
		}
		if !sel.All() || sel.String() != "all" {
			t.Errorf("%q 应订阅全部", raw)
		}
		for _, name := range EventTypes() {
			if !sel.Allows(name) {
				t.Errorf("%q 应包含 %s", raw, name)
			}
		}
	}

	sel, err := ParseSelection(" Delivery.Failed , trigger.received ,delivery.failed")
	if err != nil {
		t.Fatalf("列表应解析成功: %v", err)
	}
	if !sel.Allows(EventDeliveryFailed) || !sel.Allows(EventTriggerReceived) {
		t.Errorf("订阅集合 = %v", sel.Names())
	}
	if sel.Allows(EventDeliveryResult) {
		t.Error("未列出的类型不应订阅")
	}
	// 输出顺序固定：按事件全集的顺序，与配置里的书写顺序无关。
	if got := sel.String(); got != "trigger.received,delivery.failed" {
		t.Errorf("可读形态 = %q", got)
	}
	if sel.All() {
		t.Error("列表写法不应视为全部")
	}
}

// 拼错类型名要报错并列出可选值，而不是悄悄退化成「收不到」。
func TestSelectionParseInvalid(t *testing.T) {
	for _, raw := range []string{"delivery.maybe", "trigger", "delivery.result,,oops", ",,,"} {
		if _, err := ParseSelection(raw); err == nil {
			t.Errorf("%q 应判为不合法", raw)
		} else if raw != ",,," && !strings.Contains(err.Error(), "可选") {
			t.Errorf("%q 的错误信息应列出可选值: %v", raw, err)
		}
	}
}

// 分发器按订阅集合筛选：未订阅的事件不入队、计入 Filtered，也不算丢弃。
func TestDispatcherFiltersUnsubscribedEvents(t *testing.T) {
	sender := &fakeSender{}
	opts := quietOptions()
	opts.Selection, _ = ParseSelection(EventDeliveryFailed)

	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), sender, opts)
	if d == nil {
		t.Fatal("应创建分发器")
	}
	defer d.Close(context.Background())

	if got := d.Selection().String(); got != EventDeliveryFailed {
		t.Errorf("生效的订阅集合 = %q", got)
	}

	sent := sampleEvent() // delivery.result，未订阅
	if d.Notify(sent) {
		t.Error("未订阅的事件不应入队")
	}

	ev := Event{
		ID:         "evt-2",
		Event:      EventDeliveryFailed,
		OccurredAt: occurredAtNow(),
		TriggerID:  "trig-2",
		Workflow:   "post_commented",
		Status:     StatusFailed,
		Delivery:   &Delivery{StepID: "mail", Channel: "email", Status: StatusFailed, Reason: "550"},
	}
	if !d.Notify(ev) {
		t.Fatal("已订阅的事件应入队")
	}

	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}
	stats := d.Stats()
	if stats.Filtered != 1 {
		t.Errorf("过滤计数 = %d", stats.Filtered)
	}
	if stats.Dropped != 0 {
		t.Errorf("过滤不算丢弃，实际 %d", stats.Dropped)
	}
	if stats.Enqueued != 1 || stats.Delivered != 1 {
		t.Errorf("入队/送达计数 = %+v", stats)
	}
	if sender.calls() != 1 {
		t.Errorf("只应发出一条请求，实际 %d", sender.calls())
	}
}

// 不属于任何订阅集合的事件类型在入队前就被挡住，不会静默发出。
func TestDispatcherRejectsUnknownEventType(t *testing.T) {
	sender := &fakeSender{}
	opts := quietOptions()
	opts.Selection = AllEvents()
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), sender, opts)
	if d == nil {
		t.Fatal("应创建分发器")
	}
	defer d.Close(context.Background())

	ev := sampleEvent()
	ev.Event = "delivery.result.v2"
	if d.Notify(ev) {
		t.Error("未知类型不应入队")
	}
	if stats := d.Stats(); stats.Filtered != 0 || stats.Enqueued != 0 {
		t.Errorf("未知类型不属于「未订阅」，计数应为零: %+v", stats)
	}
}

// 订阅全部时四种类型都能入队，顺序与入队顺序一致。
func TestDispatcherAcceptsAllEventTypes(t *testing.T) {
	sender := &fakeSender{}
	opts := quietOptions()
	opts.Selection = AllEvents()
	opts.QueueSize = 8
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), sender, opts)
	if d == nil {
		t.Fatal("应创建分发器")
	}

	trigger, ok := NewTriggerEvent(testMessage(), testWorkflow(), true)
	if !ok {
		t.Fatal("应产生受理事件")
	}

	if !d.Notify(trigger) {
		t.Fatal("受理事件应入队")
	}
	if !d.Notify(sampleEvent()) {
		t.Fatal("汇总事件应入队")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := d.Close(ctx); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}
	if stats := d.Stats(); stats.Enqueued != 2 || stats.Delivered != 2 {
		t.Errorf("计数 = %+v", stats)
	}
}
