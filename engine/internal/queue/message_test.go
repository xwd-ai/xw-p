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
		"name": "order_confirmed",
		"subscriber_id": "user_1",
		"email": "hi@example.com",
		"phone": null,
		"channels": ["in_app", "email"],
		"payload": {"order_id": "A10293"}
	}`)
	m, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode 失败: %v", err)
	}
	if m.TriggerID != "t-001" || m.TransactionID != "x-001" || m.Name != "order_confirmed" {
		t.Errorf("基本字段解析错误: %+v", m)
	}
	if m.SubscriberID != "user_1" || m.Email != "hi@example.com" {
		t.Errorf("订阅者字段解析错误: %+v", m)
	}
	if !reflect.DeepEqual(m.Channels, []string{"in_app", "email"}) {
		t.Errorf("channels = %v", m.Channels)
	}
	if m.Payload["order_id"] != "A10293" {
		t.Errorf("payload 解析错误: %v", m.Payload)
	}
}

func TestDecodeToleratesUnknownFields(t *testing.T) {
	raw := []byte(`{"trigger_id":"t-002","transaction_id":"x-002","name":"order_confirmed","subscriber_id":"user_2","future_field":{"anything":true}}`)
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
	orig := QueuedMessage{TriggerID: "t-9", Name: "order_shipped", SubscriberID: "u9"}
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
