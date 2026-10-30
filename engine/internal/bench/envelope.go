package bench

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// benchField 是打点字段名。引擎解码队列消息时忽略未知字段，
// 因此压测消息与真实触发消息可以共用同一条消费路径。
const benchField = "bench"

// Stamp 是随消息下发的打点信息。
type Stamp struct {
	// SentAt 是入队时刻的纳秒时间戳，端到端延迟以它为起点。
	SentAt int64 `json:"sent_at"`
	// Seq 是消息序号，便于对账「灌了多少、处理了多少」。
	Seq int `json:"seq"`
}

// Envelope 是压测消息体：字段与真实触发消息一致，额外带一个 bench 打点对象。
type Envelope struct {
	TriggerID     string                  `json:"trigger_id"`
	TransactionID string                  `json:"transaction_id"`
	Name          string                  `json:"name"`
	SubscriberID  string                  `json:"subscriber_id"`
	DeviceToken   string                  `json:"device_token,omitempty"`
	Channels      []string                `json:"channels,omitempty"`
	Payload       map[string]any          `json:"payload,omitempty"`
	Workflow      *queue.WorkflowSnapshot `json:"workflow,omitempty"`
	Stamp         Stamp                   `json:"bench"`
}

// Encode 序列化消息体。
func (e Envelope) Encode() ([]byte, error) { return json.Marshal(e) }

// BuildEnvelope 生成第 seq 条压测消息。
//
// payload 沿用社区互动场景：同一篇帖子的点赞会被聚合，因此 post_id 在
// 一个小集合内循环，让 digest 的聚合分支也进入压测路径，而不是每条消息
// 都开新卡片——那会把聚合的开销测成零。
func BuildEnvelope(seq int, now time.Time, workflow *queue.WorkflowSnapshot) Envelope {
	post := fmt.Sprintf("post-%d", seq%64)
	return Envelope{
		TriggerID:     fmt.Sprintf("bench-%08d", seq),
		TransactionID: fmt.Sprintf("bench-tx-%08d", seq),
		Name:          workloadName,
		SubscriberID:  fmt.Sprintf("creator-%d", seq%128),
		DeviceToken:   fmt.Sprintf("device-%d", seq%128),
		Channels:      []string{"in_app"},
		Payload: map[string]any{
			"post_id":    post,
			"post_title": "关于通知聚合的一次尝试",
			"actor":      fmt.Sprintf("reader-%d", seq%512),
			"actor_id":   fmt.Sprintf("reader-%d", seq%512),
		},
		Workflow: workflow,
		Stamp:    Stamp{SentAt: now.UnixNano(), Seq: seq},
	}
}

// StampOf 从消息体里读出入队时刻；读不到时返回 false（消息不是压测灌入的）。
func StampOf(raw []byte) (time.Time, bool) {
	var probe struct {
		Stamp *Stamp `json:"bench"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil || probe.Stamp == nil {
		return time.Time{}, false
	}
	if probe.Stamp.SentAt <= 0 {
		return time.Time{}, false
	}
	return time.Unix(0, probe.Stamp.SentAt), true
}
