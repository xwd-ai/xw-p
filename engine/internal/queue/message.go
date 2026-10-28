// Package queue 定义队列消息契约与 Redis 消费实现。
//
// 与 Python API 侧（api/app/bus.py）配对：
// Python 用 LPUSH 入队，本包从右侧取出，消息体为 snake_case JSON。
// 消费走「取出 → 登记在途 → 确认后删除」的可靠语义，见 ReliableConsumer。
package queue

import (
	"encoding/json"
	"errors"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// ErrUndeliverable 标记一条「格式正确但无法执行」的消息：例如引用的工作流
// 已被删除、步骤校验不通过。重投多少次结果都一样，消费方应确认并丢弃它，
// 而不是让它在队列与在途记录之间来回打转。
var ErrUndeliverable = errors.New("队列消息不可投递")

// QueuedMessage 是队列中的通知消息，与 Python API 侧 QueuedMessage 的
// JSON 结构一一对应（snake_case 字段），确保跨语言消息契约一致。
//
// Workflow 携带触发时刻的工作流步骤快照：引擎消费时按快照执行，
// 不再回查工作流存储。这样已经在队列里的消息不会因为模板被后续编辑
// 而半路换了步骤序列，也省掉一次跨进程查询。
type QueuedMessage struct {
	TriggerID     string            `json:"trigger_id"`
	TransactionID string            `json:"transaction_id"`
	Name          string            `json:"name"`
	SubscriberID  string            `json:"subscriber_id"`
	Email         string            `json:"email,omitempty"`
	Phone         string            `json:"phone,omitempty"`
	DeviceToken   string            `json:"device_token,omitempty"`
	Channels      []string          `json:"channels,omitempty"`
	Payload       map[string]any    `json:"payload,omitempty"`
	Overrides     map[string]any    `json:"overrides,omitempty"`
	Workflow      *WorkflowSnapshot `json:"workflow,omitempty"`
}

// WorkflowSnapshot 是随消息下发的步骤定义快照。
//
// 字段与 Python 侧 api/app/schemas.py 的 WorkflowSnapshot 对齐：
// steps 里每一项形如 {"id": "...", "type": "in_app", "name": "...", "config": {...}}，
// 与工作流 CRUD 接口返回的步骤结构一致。
type WorkflowSnapshot struct {
	Name    string     `json:"name"`
	Version int        `json:"version,omitempty"`
	Steps   []StepSpec `json:"steps"`
}

// StepSpec 是步骤定义的传输形态，与 domain.Step 的 JSON 结构一致。
// 单独声明而不是直接复用 domain.Step，是为了让「队列契约」与「领域模型」
// 各自演进：队列里只传执行需要的字段，领域侧增加字段不会立刻影响解码。
type StepSpec struct {
	ID     string         `json:"id"`
	Type   string         `json:"type"`
	Name   string         `json:"name,omitempty"`
	Config map[string]any `json:"config,omitempty"`
}

// HasSteps 判断快照里是否真的带了步骤定义。
func (s *WorkflowSnapshot) HasSteps() bool {
	return s != nil && len(s.Steps) > 0
}

// Workflow 把快照还原为领域模型。仅当 HasSteps 为真时调用才有意义。
//
// 快照描述的是「一条消息要执行的步骤」，不涉及模板的启用状态与环境归属，
// 因此 Active 固定为 true：能进到队列的消息，在触发时已经通过了启用检查。
func (s WorkflowSnapshot) Workflow() domain.Workflow {
	steps := make([]domain.Step, 0, len(s.Steps))
	for _, spec := range s.Steps {
		steps = append(steps, domain.Step{
			ID:     spec.ID,
			Type:   domain.Channel(spec.Type),
			Name:   spec.Name,
			Config: spec.Config,
		})
	}
	return domain.Workflow{
		Name:    s.Name,
		Version: s.Version,
		Steps:   steps,
		Active:  true,
	}
}

// InlineWorkflow 返回消息自带的步骤定义；未携带或步骤为空时返回 false。
func (m QueuedMessage) InlineWorkflow() (domain.Workflow, bool) {
	if !m.Workflow.HasSteps() {
		return domain.Workflow{}, false
	}
	return m.Workflow.Workflow(), true
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
