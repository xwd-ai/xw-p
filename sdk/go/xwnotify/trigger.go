package xwnotify

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

const (
	// MaxBatchEvents 与 API 侧单次批量上限一致。超限的请求一定会被 422 拒，
	// 与其发出去换一张错误单，不如在本地指出是哪一条超了。
	MaxBatchEvents = 100

	triggerPath = "/v1/events/trigger"
	batchPath   = "/v1/events/trigger/batch"
)

// Target 是触发的目标订阅者。
//
// 地址字段按渠道填：有 email 才可能投邮件，有 deviceToken 才可能投推送。
// 缺地址不会让触发失败——工作流侧会跳过投不了的渠道，并把原因写进执行明细。
type Target struct {
	SubscriberID string   `json:"subscriberId"`
	Email        string   `json:"email,omitempty"`
	Phone        string   `json:"phone,omitempty"`
	DeviceToken  string   `json:"deviceToken,omitempty"`
	Channels     []string `json:"channels,omitempty"`
}

// Event 是一条互动事件，对应请求体。
//
// Payload 里放渲染变量（post_id、actor、comment 之类），键名由工作流模板决定，
// 客户端不改写任何一个键——post_id 与 postId 同时出现是调用方的业务数据。
type Event struct {
	Name      string         `json:"name"`
	To        Target         `json:"to"`
	Payload   map[string]any `json:"payload,omitempty"`
	Overrides map[string]any `json:"overrides,omitempty"`
}

// NewEvent 构造一条事件，名称与订阅者标识会去掉首尾空白。
//
// 只填了必填项，其余用链式方法补：
//
//	xwnotify.NewEvent("post_liked", "creator-1001").
//		WithPayload(map[string]any{"post_id": "P-2026-1001", "actor": "小林"}).
//		WithChannels("in_app", "push")
func NewEvent(name, subscriberID string) Event {
	return Event{
		Name: strings.TrimSpace(name),
		To:   Target{SubscriberID: strings.TrimSpace(subscriberID)},
	}
}

// WithPayload 替换渲染变量。传 nil 表示不带变量。
func (e Event) WithPayload(payload map[string]any) Event {
	e.Payload = payload
	return e
}

// WithOverrides 替换模板覆盖项。
func (e Event) WithOverrides(overrides map[string]any) Event {
	e.Overrides = overrides
	return e
}

// WithChannels 指定分发渠道，留空表示由工作流的步骤决定。
func (e Event) WithChannels(channels ...string) Event {
	if len(channels) == 0 {
		e.To.Channels = nil
		return e
	}
	e.To.Channels = append([]string(nil), channels...)
	return e
}

// WithEmail 设置收件邮箱。
func (e Event) WithEmail(email string) Event {
	e.To.Email = strings.TrimSpace(email)
	return e
}

// WithPhone 设置收短信号码（E.164 或服务端能规整的写法）。
func (e Event) WithPhone(phone string) Event {
	e.To.Phone = strings.TrimSpace(phone)
	return e
}

// WithDeviceToken 设置移动推送的设备令牌。
func (e Event) WithDeviceToken(token string) Event {
	e.To.DeviceToken = strings.TrimSpace(token)
	return e
}

// Validate 检查必填项，规则与服务端一致：名称 1–255 个字符，订阅者标识非空。
func (e Event) Validate() error {
	name := strings.TrimSpace(e.Name)
	if name == "" {
		return &ValidationError{Field: "name", Message: "不能为空（例如 post_commented）"}
	}
	// 服务端按字符数限制，这里同样按 rune 计，避免中文名称被误判超长。
	if len([]rune(name)) > 255 {
		return &ValidationError{Field: "name", Message: "不能超过 255 个字符"}
	}
	if strings.TrimSpace(e.To.SubscriberID) == "" {
		return &ValidationError{Field: "to.subscriberId", Message: "不能为空"}
	}
	return nil
}

// Ack 是单条触发的回执。
type Ack struct {
	Acknowledged     bool   `json:"acknowledged"`
	TriggerID        string `json:"triggerId"`
	TransactionID    string `json:"transactionId"`
	AcknowledgmentID string `json:"acknowledgmentId"`
}

// BatchItem 是批量触发中单条事件的处理结果。
//
// 被拒条目 Status 为 "rejected"，原因在 Error；受理条目带三个标识。
type BatchItem struct {
	Index            int    `json:"index"`
	Name             string `json:"name"`
	SubscriberID     string `json:"subscriberId"`
	Status           string `json:"status"`
	TriggerID        string `json:"triggerId,omitempty"`
	TransactionID    string `json:"transactionId,omitempty"`
	AcknowledgmentID string `json:"acknowledgmentId,omitempty"`
	Error            string `json:"error,omitempty"`
}

// Accepted 报告该条是否已入队。
func (i BatchItem) Accepted() bool { return i.Status == "accepted" }

// BatchAck 是批量触发的回执。Results 与请求顺序一致、等长，按下标对齐。
type BatchAck struct {
	Acknowledged bool        `json:"acknowledged"`
	Accepted     int         `json:"accepted"`
	Rejected     int         `json:"rejected"`
	Results      []BatchItem `json:"results"`
}

// TriggerOption 是单条与批量触发共用的请求选项。
type TriggerOption func(*triggerConfig)

type triggerConfig struct {
	idempotencyKey string
}

// WithIdempotencyKey 设置幂等键。
//
// 单条触发时它决定「同一个键只入队一次」；批量触发时作用于整批，不是逐条
// 各来一个键。键由调用方生成且有业务含义（例如 post-commented-P-2026-1001），
// 服务端只做字符串匹配。
func WithIdempotencyKey(key string) TriggerOption {
	return func(cfg *triggerConfig) { cfg.idempotencyKey = strings.TrimSpace(key) }
}

func newTriggerConfig(opts []TriggerOption) triggerConfig {
	var cfg triggerConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// Trigger 触发一条互动事件，返回回执。
//
// Channels 留空表示由工作流步骤决定投哪些渠道；指定了列表则只投其中的渠道。
// 工作流不存在或已停用返回 404，可用 IsNotFound 判定。
func (c *Client) Trigger(ctx context.Context, event Event, opts ...TriggerOption) (*Ack, error) {
	if err := event.Validate(); err != nil {
		return nil, err
	}
	cfg := newTriggerConfig(opts)
	payload, err := c.do(ctx, http.MethodPost, triggerPath, event, idempotencyHeaders(cfg))
	if err != nil {
		return nil, err
	}
	return parseAck(triggerPath, payload)
}

// TriggerBatch 一次提交多条事件。
//
// 某条事件的工作流不存在或未启用只会让该条记为 rejected，同批其余照常入队，
// 因此务必按下标对齐 Results，而不是依次取「成功的那几条」。
//
// 空批次、超过 MaxBatchEvents、或任一条缺必填项都会在本地报错，请求不会发出。
func (c *Client) TriggerBatch(ctx context.Context, events []Event, opts ...TriggerOption) (*BatchAck, error) {
	if len(events) == 0 {
		return nil, &ValidationError{Field: "events", Message: "至少需要一条事件"}
	}
	if len(events) > MaxBatchEvents {
		return nil, &ValidationError{
			Field:   "events",
			Message: fmt.Sprintf("单次最多 %d 条，本次 %d 条，请分批提交", MaxBatchEvents, len(events)),
		}
	}
	for index, event := range events {
		if err := event.Validate(); err != nil {
			// 包一层下标：一次提交上百条时，「第几条错了」比错误本身更关键。
			return nil, fmt.Errorf("xw-notify: 第 %d 条事件：%w", index, err)
		}
	}

	cfg := newTriggerConfig(opts)
	body := struct {
		Events []Event `json:"events"`
	}{Events: events}
	payload, err := c.do(ctx, http.MethodPost, batchPath, body, idempotencyHeaders(cfg))
	if err != nil {
		return nil, err
	}
	return parseBatchAck(batchPath, payload, len(events))
}

func idempotencyHeaders(cfg triggerConfig) map[string]string {
	if cfg.idempotencyKey == "" {
		return nil
	}
	return map[string]string{"Idempotency-Key": cfg.idempotencyKey}
}

// parseAck 解析单条回执，并守住两个契约：acknowledged 必须为真、触发标识非空。
// 否则调用方会拿到一个「成功但没有标识」的结果，后续查活动与执行明细都无从下手。
func parseAck(path string, payload []byte) (*Ack, error) {
	var ack Ack
	if err := decode(http.MethodPost, path, payload, &ack); err != nil {
		return nil, err
	}
	if !ack.Acknowledged {
		return nil, &ProtocolError{Method: http.MethodPost, Path: path, Reason: "服务端未受理该事件", Body: payload}
	}
	if strings.TrimSpace(ack.TriggerID) == "" {
		return nil, &ProtocolError{Method: http.MethodPost, Path: path, Reason: "回执缺少 triggerId", Body: payload}
	}
	return &ack, nil
}

// parseBatchAck 解析批量回执。结果必须与请求等长且按下标对齐，否则调用方
// 对齐时会静默错位——一条评论的回执被当成点赞的，比直接报错更糟。
func parseBatchAck(path string, payload []byte, want int) (*BatchAck, error) {
	var ack BatchAck
	if err := decode(http.MethodPost, path, payload, &ack); err != nil {
		return nil, err
	}
	if len(ack.Results) != want {
		return nil, &ProtocolError{
			Method: http.MethodPost,
			Path:   path,
			Reason: fmt.Sprintf("逐条结果与请求不等长（请求 %d 条，返回 %d 条）", want, len(ack.Results)),
			Body:   payload,
		}
	}
	for index, item := range ack.Results {
		if item.Index != index {
			return nil, &ProtocolError{
				Method: http.MethodPost,
				Path:   path,
				Reason: fmt.Sprintf("第 %d 条结果的下标为 %d，与请求顺序不一致", index, item.Index),
				Body:   payload,
			}
		}
	}
	return &ack, nil
}
