package queue

import (
	"reflect"
	"testing"
)

// 验证与 Python 侧 QueuedMessage 的 snake_case JSON 契约一致。

func TestDecodeSnakeCasePayload(t *testing.T) {
	raw := []byte(`{
		"trigger_id": "t-001",
		"transaction_id": "x-001",
		"name": "post_commented",
		"subscriber_id": "creator_1",
		"email": "hi@example.com",
		"phone": null,
		"channels": ["in_app", "email"],
		"payload": {"post_id": "P10293"}
	}`)
	m, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode 失败: %v", err)
	}
	if m.TriggerID != "t-001" || m.TransactionID != "x-001" || m.Name != "post_commented" {
		t.Errorf("基本字段解析错误: %+v", m)
	}
	if m.SubscriberID != "creator_1" || m.Email != "hi@example.com" {
		t.Errorf("订阅者字段解析错误: %+v", m)
	}
	if !reflect.DeepEqual(m.Channels, []string{"in_app", "email"}) {
		t.Errorf("channels = %v", m.Channels)
	}
	if m.Payload["post_id"] != "P10293" {
		t.Errorf("payload 解析错误: %v", m.Payload)
	}
}

func TestDecodeToleratesUnknownFields(t *testing.T) {
	raw := []byte(`{"trigger_id":"t-002","transaction_id":"x-002","name":"post_commented","subscriber_id":"creator_2","future_field":{"anything":true}}`)
	m, err := Decode(raw)
	if err != nil {
		t.Fatalf("未知字段应被忽略: %v", err)
	}
	if m.TriggerID != "t-002" {
		t.Errorf("trigger_id = %s", m.TriggerID)
	}
	if m.Channels != nil {
		t.Errorf("缺失 channels 应为 nil，实际 %v", m.Channels)
	}
}

func TestEffectiveChannelsFallback(t *testing.T) {
	m := QueuedMessage{}
	if got := m.EffectiveChannels(); !reflect.DeepEqual(got, []string{"in_app"}) {
		t.Errorf("空渠道应回退 in_app，实际 %v", got)
	}
	m.Channels = []string{"sms"}
	if got := m.EffectiveChannels(); !reflect.DeepEqual(got, []string{"sms"}) {
		t.Errorf("已有渠道不应被覆盖，实际 %v", got)
	}
}

func TestEncodeRoundTrip(t *testing.T) {
	orig := QueuedMessage{TriggerID: "t-9", Name: "post_liked", SubscriberID: "u9"}
	data, err := orig.Encode()
	if err != nil {
		t.Fatalf("Encode 失败: %v", err)
	}
	back, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode 失败: %v", err)
	}
	if back.TriggerID != orig.TriggerID || back.Name != orig.Name {
		t.Errorf("往返不一致: %+v", back)
	}
}

// 消息自带的步骤快照：字段名与 Python 侧 WorkflowSnapshot 对齐，
// 解码后能直接还原成领域模型供管线执行。
func TestDecodeWorkflowSnapshot(t *testing.T) {
	raw := []byte(`{
		"trigger_id": "t-101",
		"transaction_id": "x-101",
		"name": "post_liked",
		"subscriber_id": "creator_1",
		"payload": {"post_id": "P-2026-1001"},
		"workflow": {
			"name": "post_liked",
			"version": 4,
			"steps": [
				{"id": "step-1", "type": "digest", "name": "点赞聚合",
				 "config": {"key": "post_id", "window": "10m", "depends_on": []}},
				{"id": "step-2", "type": "in_app", "name": "站内信",
				 "config": {"template": "{{ payload.actor }} 赞了你的帖子"}}
			]
		}
	}`)
	m, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode 失败: %v", err)
	}
	if m.Workflow == nil {
		t.Fatal("workflow 快照未解析")
	}
	if m.Workflow.Name != "post_liked" || m.Workflow.Version != 4 {
		t.Errorf("快照元信息错误: %+v", m.Workflow)
	}
	if len(m.Workflow.Steps) != 2 || m.Workflow.Steps[0].Type != "digest" {
		t.Fatalf("步骤解析错误: %+v", m.Workflow.Steps)
	}

	wf, ok := m.InlineWorkflow()
	if !ok {
		t.Fatal("带步骤的消息应返回内联工作流")
	}
	if len(wf.Steps) != 2 || wf.Steps[1].Type != "in_app" {
		t.Errorf("领域模型转换错误: %+v", wf.Steps)
	}
	if wf.Steps[1].Config["template"] != "{{ payload.actor }} 赞了你的帖子" {
		t.Errorf("步骤配置丢失: %+v", wf.Steps[1].Config)
	}
	if !wf.Active {
		t.Errorf("快照还原的工作流应视为启用（入队前已校验）")
	}
}

// 没有快照的老消息应能正常解码，只是拿不到内联步骤。
func TestInlineWorkflowAbsentForLegacyMessage(t *testing.T) {
	m, err := Decode([]byte(`{"trigger_id":"t-102","name":"post_commented","subscriber_id":"creator_1"}`))
	if err != nil {
		t.Fatalf("缺少 workflow 不应导致解码失败: %v", err)
	}
	if _, ok := m.InlineWorkflow(); ok {
		t.Errorf("没有快照时不应返回内联工作流")
	}

	empty := QueuedMessage{Workflow: &WorkflowSnapshot{Name: "post_liked"}}
	if _, ok := empty.InlineWorkflow(); ok {
		t.Errorf("空步骤快照不应视为可执行")
	}
}
