package webhook

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/execution"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

func occurredAtNow() execution.Timestamp { return execution.NewTimestamp(time.Now()) }

func occurredAtZero() execution.Timestamp { return execution.Timestamp{} }

func testMessage() queue.QueuedMessage {
	return queue.QueuedMessage{
		TriggerID:     "trig-1",
		TransactionID: "tx-1",
		Name:          "post_commented",
		SubscriberID:  "creator_1",
	}
}

// 两个渠道一成一败：整体是 partial，明细各自带状态与原因。
func TestNewEventBuildsDeliveries(t *testing.T) {
	results := []pipeline.StepResult{
		{
			StepID:           "notify",
			Channel:          domain.ChannelInApp,
			Sent:             true,
			UsedProvider:     "console-in-app",
			ProviderAttempts: []string{"console-in-app"},
			Duration:         12 * time.Millisecond,
		},
		{
			StepID:           "mail",
			Channel:          domain.ChannelEmail,
			Error:            "SMTP 收件人 creator@example.com 失败（550）：no such user",
			UsedProvider:     "smtp-email",
			ProviderAttempts: []string{"smtp-email", "smtp-email", "console-email"},
			ProviderFallback: true,
			Duration:         128 * time.Millisecond,
		},
	}

	ev, ok := NewEvent(testMessage(), results)
	if !ok {
		t.Fatalf("应产生事件")
	}
	if ev.Event != EventDeliveryResult {
		t.Errorf("事件名不符: %q", ev.Event)
	}
	if ev.Status != StatusPartial {
		t.Errorf("一成一败应为 partial，实际 %q", ev.Status)
	}
	if ev.Summary == nil || *ev.Summary != (Summary{Total: 2, Sent: 1, Failed: 1}) {
		t.Errorf("汇总不符: %+v", ev.Summary)
	}
	if ev.TriggerID != "trig-1" || ev.TransactionID != "tx-1" || ev.Workflow != "post_commented" || ev.SubscriberID != "creator_1" {
		t.Errorf("事件缺少归属信息: %+v", ev)
	}
	if ev.ID == "" || len(ev.ID) != 32 {
		t.Errorf("事件标识应为 32 位十六进制，实际 %q", ev.ID)
	}
	if ev.OccurredAt.IsZero() {
		t.Error("事件应带发生时间")
	}

	first, second := ev.Deliveries[0], ev.Deliveries[1]
	if first.Channel != "in_app" || first.Status != StatusSent || first.Reason != "" {
		t.Errorf("站内信明细不符: %+v", first)
	}
	if first.DurationMS != 12 {
		t.Errorf("耗时换算成毫秒应为 12，实际 %d", first.DurationMS)
	}
	if second.Channel != "email" || second.Status != StatusFailed {
		t.Errorf("邮件明细不符: %+v", second)
	}
	if !strings.Contains(second.Reason, "550") {
		t.Errorf("失败原因应原样带出: %q", second.Reason)
	}
	if !second.Fallback || len(second.Attempts) != 3 {
		t.Errorf("回退信息不符: %+v", second)
	}

	if err := ev.Validate(); err != nil {
		t.Errorf("事件应通过校验: %v", err)
	}
}

// 全成功与全失败分别对应 sent / failed。
func TestNewEventOverallStatus(t *testing.T) {
	sent := []pipeline.StepResult{
		{StepID: "a", Channel: domain.ChannelInApp, Sent: true},
		{StepID: "b", Channel: domain.ChannelPush, Sent: true},
	}
	ev, ok := NewEvent(testMessage(), sent)
	if !ok || ev.Status != StatusSent {
		t.Fatalf("全部成功应为 sent，实际 %+v", ev)
	}

	failed := []pipeline.StepResult{
		{StepID: "a", Channel: domain.ChannelInApp, Error: "收件箱不可写"},
	}
	ev, ok = NewEvent(testMessage(), failed)
	if !ok || ev.Status != StatusFailed {
		t.Fatalf("全部失败应为 failed，实际 %+v", ev)
	}
}

// 没有投递动作的步骤不进明细：被过滤、条件不满足、上游短路、登记延迟、
// 既非成功也无原因，以及没有渠道的结果。
func TestNewEventSkipsStepsWithoutDelivery(t *testing.T) {
	results := []pipeline.StepResult{
		{StepID: "filtered", Channel: domain.ChannelInApp, Skipped: true, SkipReason: "filter"},
		{StepID: "branch", Channel: domain.ChannelInApp, Skipped: true, SkipReason: "condition"},
		{StepID: "never", Channel: domain.ChannelEmail, ShortCircuited: true},
		{StepID: "slow", Channel: domain.ChannelEmail, TimedOut: true},
		{StepID: "later", Channel: domain.ChannelEmail, Deferred: true},
		{StepID: "nochannel", Sent: true},
		{StepID: "nothing", Channel: domain.ChannelSMS},
	}

	if _, ok := NewEvent(testMessage(), results); ok {
		t.Fatal("没有任何投递动作时不应产生事件")
	}

	results = append(results, pipeline.StepResult{
		StepID:  "notify",
		Channel: domain.ChannelInApp,
		Sent:    true,
	})
	ev, ok := NewEvent(testMessage(), results)
	if !ok {
		t.Fatal("应只因那一步投递产生事件")
	}
	if len(ev.Deliveries) != 1 || ev.Deliveries[0].StepID != "notify" {
		t.Fatalf("明细应只含真正投递的步骤: %+v", ev.Deliveries)
	}
}

// 事件里的尝试列表是副本：调用方事后再改步骤结果，不应影响已入队的事件。
func TestNewEventCopiesAttempts(t *testing.T) {
	attempts := []string{"smtp-email"}
	results := []pipeline.StepResult{{
		StepID:           "mail",
		Channel:          domain.ChannelEmail,
		Error:            "连接失败",
		ProviderAttempts: attempts,
	}}

	ev, ok := NewEvent(testMessage(), results)
	if !ok {
		t.Fatal("应产生事件")
	}
	attempts[0] = "被改掉"
	if ev.Deliveries[0].Attempts[0] != "smtp-email" {
		t.Errorf("事件应持有尝试列表的副本，实际 %v", ev.Deliveries[0].Attempts)
	}
}

// 报文格式：时间戳六位小数 + 00:00 偏移，字段名 snake_case。
func TestEventJSONShape(t *testing.T) {
	ev, ok := NewEvent(testMessage(), []pipeline.StepResult{{
		StepID:  "notify",
		Channel: domain.ChannelInApp,
		Sent:    true,
	}})
	if !ok {
		t.Fatal("应产生事件")
	}

	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	payload := string(raw)
	for _, want := range []string{
		`"event":"delivery.result"`,
		`"trigger_id":"trig-1"`,
		`"transaction_id":"tx-1"`,
		`"subscriber_id":"creator_1"`,
		`"step_id":"notify"`,
		`"channel":"in_app"`,
		`"duration_ms":0`,
	} {
		if !strings.Contains(payload, want) {
			t.Errorf("报文缺少 %s：%s", want, payload)
		}
	}
	// 时间戳必须与执行明细同格式：写入方与读取方跨进程，格式不能各写一套。
	stamp := regexp.MustCompile(`"occurred_at":"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+00:00"`)
	if !stamp.MatchString(payload) {
		t.Errorf("时间戳格式不符：%s", payload)
	}
}

// 对外报文缺字段时在入队前就挡住。
func TestEventValidate(t *testing.T) {
	good := Event{
		ID:         "e1",
		Event:      EventDeliveryResult,
		OccurredAt: occurredAtNow(),
		TriggerID:  "trig-1",
		Status:     StatusSent,
		Summary:    &Summary{Total: 1, Sent: 1},
		Deliveries: []Delivery{{Channel: "in_app", Status: StatusSent}},
	}
	if err := good.Validate(); err != nil {
		t.Fatalf("合法事件不应报错: %v", err)
	}

	cases := map[string]func(Event) Event{
		"缺 id":         func(e Event) Event { e.ID = ""; return e },
		"缺事件名":         func(e Event) Event { e.Event = ""; return e },
		"未知事件名":        func(e Event) Event { e.Event = "delivery.maybe"; return e },
		"缺 trigger_id": func(e Event) Event { e.TriggerID = " "; return e },
		"未知状态":         func(e Event) Event { e.Status = "done"; return e },
		"没有明细":         func(e Event) Event { e.Deliveries = nil; return e },
		"缺汇总":          func(e Event) Event { e.Summary = nil; return e },
		"汇总与明细不一致":     func(e Event) Event { e.Summary = &Summary{Total: 2, Sent: 2}; return e },
		"缺时间":          func(e Event) Event { e.OccurredAt = occurredAtZero(); return e },
	}
	for name, mutate := range cases {
		if err := mutate(good).Validate(); err == nil {
			t.Errorf("%s：应判为不合法", name)
		} else if !strings.Contains(err.Error(), "事件不合法") {
			t.Errorf("%s：错误信息应可直接读：%v", name, err)
		}
	}
}
