// Package queue 定义队列消息契约与 Redis 消费实现。
//
// 与 Python API 侧（api/app/bus.py）配对：
// Python 用 LPUSH 入队，本包从右侧取出，消息体为 snake_case JSON。
// 消费走「取出 → 登记在途 → 确认后删除」的可靠语义，见 ReliableConsumer。
package queue

import (
	"encoding/json"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// QueuedMessage 是队列中的通知消息，与 Python API 侧 QueuedMessage 的
// JSON 结构一一对应（snake_case 字段），确保跨语言消息契约一致。
type QueuedMessage struct {
	TriggerID     string         `json:"trigger_id"`
	TransactionID string         `json:"transaction_id"`
	Name          string         `json:"name"`
	SubscriberID  string         `json:"subscriber_id"`
	Email         string         `json:"email,omitempty"`
	Phone         string         `json:"phone,omitempty"`
	DeviceToken   string         `json:"device_token,omitempty"`
	Channels      []string       `json:"channels,omitempty"`
	Payload       map[string]any `json:"payload,omitempty"`
	Overrides     map[string]any `json:"overrides,omitempty"`
}

// Decode 反序列化队列消息；未知字段被忽略（向前兼容）。
func Decode(data []byte) (QueuedMessage, error) {
	var m QueuedMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return QueuedMessage{}, err
	}
	return m, nil
}

// Encode 序列化为 JSON（调试与测试用）。
func (m QueuedMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// EffectiveChannels 返回实际投递渠道；未指定时回退到 in_app。
func (m QueuedMessage) EffectiveChannels() []string {
	if len(m.Channels) == 0 {
		return []string{string(domain.ChannelInApp)}
	}
	return m.Channels
}
