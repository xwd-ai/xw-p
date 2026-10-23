package domain

import "time"

// Step 是工作流中的一个步骤。顺序由父 Workflow 的 Steps 列表天然决定；
// Type 引用 Channel；Config 承载该步骤的私有参数（模板、过滤条件、延迟时长等）。
type Step struct {
	ID     string         `json:"id"`
	Type   Channel        `json:"type"`
	Name   string         `json:"name,omitempty"`
	Config map[string]any `json:"config,omitempty"`
}

// IsDelivery 判断该步骤是否为投递步骤（跳过 delay/digest 编排步骤）。
func (s Step) IsDelivery() bool {
	return s.Type.IsDelivery()
}

// Workflow 是工作流/通知模板，定义从触发到投递的步骤序列。
// 字段与 Python 侧 Workflow 及 workflows 表结构保持一致。
type Workflow struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	Steps       []Step      `json:"steps"`
	Version     int         `json:"version"`
	Environment Environment `json:"environment"`
	Active      bool        `json:"active"`
	CreatedAt   time.Time   `json:"created_at"`
}

// DeliverySteps 返回真正执行投递的步骤（跳过 delay/digest）。
func (w Workflow) DeliverySteps() []Step {
	var out []Step
	for _, s := range w.Steps {
		if s.IsDelivery() {
			out = append(out, s)
		}
	}
	return out
}

// Subscriber 是通知的接收方，可绑定多种渠道的联系方式。
type Subscriber struct {
	ID          string      `json:"id"`
	Email       string      `json:"email,omitempty"`
	Phone       string      `json:"phone,omitempty"`
	DeviceToken string      `json:"device_token,omitempty"`
	Channels    []Channel   `json:"channels,omitempty"`
	Environment Environment `json:"environment"`
	CreatedAt   time.Time   `json:"created_at"`
}

// Message 是一次触发投递产生的执行实例，关联工作流与订阅者；
// Status 走消息状态机（created → queued → processing → sent/failed）。
type Message struct {
	ID           string         `json:"id"`
	WorkflowID   string         `json:"workflow_id"`
	SubscriberID string         `json:"subscriber_id"`
	Channel      Channel        `json:"channel"`
	Payload      map[string]any `json:"payload,omitempty"`
	Status       MessageStatus  `json:"status"`
	Error        string         `json:"error,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
}
