package webhook

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/execution"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

const (
	// EventTriggerReceived 表示一次触发已被引擎受理、即将执行。
	//
	// 时间点是引擎从队列取到消息的时刻，不是 API 接收请求的时刻：两者相隔
	// 通常只有毫秒级，但接入方如果要按它算端到端延迟，应当知道这个起点。
	EventTriggerReceived = "trigger.received"

	// EventDeliverySent / EventDeliveryFailed 是单个渠道的投递结果。
	//
	// 与 EventDeliveryResult 的分工：汇总事件回答「这次触发的整体结果」，
	// 逐渠道事件回答「某一个渠道成没成」，接入方可以只订阅后者做按渠道的
	// 告警，也可以只订阅前者做一次性的状态回调。
	EventDeliverySent   = "delivery.sent"
	EventDeliveryFailed = "delivery.failed"

	// EventDigestUpdated 表示聚合卡片被创建或被改写。
	//
	// 同一篇帖子的连续互动不会每次发一条通知，而是把计数累加进同一张卡片；
	// 这个事件给出的是卡片当前的样子（聚合键、累计条数、是否新开卡、窗口
	// 到期时刻），接收方据此可以自己渲染「N 人赞了你」。
	EventDigestUpdated = "digest.updated"

	// EventDeliveryResult 是「这条触发的投递结果」汇总事件。
	//
	// 事件名是接入方路由的依据，写死在这里而不是拼进 URL：同一个接收端点
	// 同时收多种类型，用头部与报文字段区分比用不同路径更省事。
	EventDeliveryResult = "delivery.result"

	// StatusSent / StatusFailed / StatusPartial 是整体结果。
	//
	// 一次触发会落到多个渠道，只看单个渠道的成败不够：三条里两条成功，
	// 整体既不是成功也不是失败。分开报，接入方按自己的容忍度决定要不要
	// 重试或告警。
	StatusSent    = "sent"
	StatusFailed  = "failed"
	StatusPartial = "partial"
)

// eventTypes 是全部事件类型，按「受理 → 单渠道 → 聚合 → 汇总」排列。
//
// 顺序即订阅配置里的书写顺序，也是 Selection.String 的输出顺序：
// 同一份配置两次打印必须一样，否则日志对比没有意义。
var eventTypes = []string{
	EventTriggerReceived,
	EventDeliverySent,
	EventDeliveryFailed,
	EventDigestUpdated,
	EventDeliveryResult,
}

// EventTypes 返回全部事件类型。
func EventTypes() []string { return append([]string(nil), eventTypes...) }

// IsKnownEvent 判断事件类型是否在全集内。
func IsKnownEvent(name string) bool {
	for _, known := range eventTypes {
		if known == name {
			return true
		}
	}
	return false
}

// Delivery 是单个渠道的投递结果。
//
// 字段与执行明细一一对应，接入方拿到回调后不必再查一次 GET /v1/executions
// 才知道是哪个实现、试了几次。
type Delivery struct {
	StepID     string   `json:"step_id,omitempty"`
	StepType   string   `json:"step_type,omitempty"`
	Channel    string   `json:"channel"`
	Status     string   `json:"status"`
	Reason     string   `json:"reason,omitempty"`
	Provider   string   `json:"provider,omitempty"`
	Attempts   []string `json:"attempts,omitempty"`
	Fallback   bool     `json:"fallback,omitempty"`
	DurationMS int64    `json:"duration_ms"`
}

// Summary 是各渠道结果的计数，方便接收方先看总量再看明细。
type Summary struct {
	Total  int `json:"total"`
	Sent   int `json:"sent"`
	Failed int `json:"failed"`
}

// Trigger 是「触发已受理」事件的正文。
//
// 它的用处是让接入方在投递发生之前就知道这次会动到哪些渠道：工作流的渠道
// 集合随订阅者档案变化（没有邮箱就没有邮件），而 Channels 是工作流声明的
// 那部分，实际是否投递还要看偏好与地址。
type Trigger struct {
	// Steps 是本次执行的步骤数。
	Steps int `json:"steps"`
	// Channels 是工作流声明的投递渠道（去重、按步骤出现顺序）。
	Channels []string `json:"channels,omitempty"`
	// Version 是步骤定义的版本，快照未带版本时为 0。
	Version int `json:"version,omitempty"`
	// FromSnapshot 表示步骤取自消息自带的快照（而不是启动时加载的定义表）。
	FromSnapshot bool `json:"from_snapshot,omitempty"`
}

// Digest 是聚合卡片的状态。
type Digest struct {
	StepID string `json:"step_id"`
	// Key 是聚合键的取值，内容社区里通常是帖子标识。
	Key string `json:"key"`
	// Count 是卡片当前累计的互动条数。
	Count int `json:"count"`
	// Opened 表示本次开了新卡（窗口内第一条互动）。
	Opened bool `json:"opened,omitempty"`
	// Replayed 表示这条事件在窗口内已计过数：计数没变，也没有重复投递。
	Replayed bool `json:"replayed,omitempty"`
	// ExpiresAt 是当前窗口的到期时刻，到期后再来的互动会开新卡。
	ExpiresAt *execution.Timestamp `json:"expires_at,omitempty"`
}

// Event 是一次执行的回调报文。
//
// 报文是「公共信封 + 类型专属正文」的形态：ID / Event / OccurredAt / 归属字段
// 每种类型都有，正文按事件类型放在 delivery、digest、trigger、deliveries
// 各自的段里。接收方先看 event 再取对应段落，不必为每种类型写一套解析。
//
// ID 由引擎生成，接收方按它去重：回调没有「恰好一次」的保证，接收方超时
// 未响应或进程重启都会让同一条事件再来一次。
type Event struct {
	ID            string              `json:"id"`
	Event         string              `json:"event"`
	OccurredAt    execution.Timestamp `json:"occurred_at"`
	TriggerID     string              `json:"trigger_id"`
	TransactionID string              `json:"transaction_id,omitempty"`
	Workflow      string              `json:"workflow"`
	SubscriberID  string              `json:"subscriber_id,omitempty"`
	Status        string              `json:"status,omitempty"`
	Summary       *Summary            `json:"summary,omitempty"`
	Deliveries    []Delivery          `json:"deliveries,omitempty"`
	Delivery      *Delivery           `json:"delivery,omitempty"`
	Trigger       *Trigger            `json:"trigger,omitempty"`
	Digest        *Digest             `json:"digest,omitempty"`
}

// ErrInvalidEvent 表示事件缺必填字段或自相矛盾。
var ErrInvalidEvent = errors.New("webhook: 事件不合法")

// Validate 检查事件能否发出去。
//
// 回调是对外的报文，字段缺了接收方只能收到一条无法归属的通知；因此在入队
// 之前就挡住，而不是等接收方回 400。校验按事件类型分别做：公共字段之外，
// 每种类型只认自己那一段正文，防止「声称是聚合事件却只有投递明细」这类
// 自相矛盾的报文被发出去。
func (e Event) Validate() error {
	var problems []string
	if strings.TrimSpace(e.ID) == "" {
		problems = append(problems, "缺少 id")
	}
	if strings.TrimSpace(e.Event) == "" {
		problems = append(problems, "缺少 event")
	} else if !IsKnownEvent(e.Event) {
		problems = append(problems, fmt.Sprintf("未知事件类型 %q（可选：%s）",
			e.Event, strings.Join(eventTypes, " / ")))
	}
	if strings.TrimSpace(e.TriggerID) == "" {
		problems = append(problems, "缺少 trigger_id")
	}
	if e.OccurredAt.IsZero() {
		problems = append(problems, "缺少 occurred_at")
	}

	problems = append(problems, e.validateBody()...)
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("%w：%s", ErrInvalidEvent, strings.Join(problems, "；"))
}

// validateBody 校验类型专属正文。
func (e Event) validateBody() []string {
	switch e.Event {
	case EventTriggerReceived:
		var problems []string
		if strings.TrimSpace(e.Workflow) == "" {
			problems = append(problems, "缺少 workflow")
		}
		if e.Trigger == nil {
			problems = append(problems, "缺少 trigger 段")
		}
		return problems

	case EventDeliverySent, EventDeliveryFailed:
		want := StatusSent
		if e.Event == EventDeliveryFailed {
			want = StatusFailed
		}
		var problems []string
		if e.Status != want {
			problems = append(problems, fmt.Sprintf("事件 %s 的 status 应为 %s，实际 %q", e.Event, want, e.Status))
		}
		switch {
		case e.Delivery == nil:
			problems = append(problems, "缺少 delivery 段")
		case strings.TrimSpace(e.Delivery.Channel) == "":
			problems = append(problems, "delivery 缺少 channel")
		case e.Delivery.Status != want:
			problems = append(problems, fmt.Sprintf("事件 %s 的 delivery.status 应为 %s，实际 %q",
				e.Event, want, e.Delivery.Status))
		}
		return problems

	case EventDigestUpdated:
		var problems []string
		if strings.TrimSpace(e.Workflow) == "" {
			problems = append(problems, "缺少 workflow")
		}
		switch {
		case e.Digest == nil:
			problems = append(problems, "缺少 digest 段")
		default:
			if strings.TrimSpace(e.Digest.StepID) == "" {
				problems = append(problems, "digest 缺少 step_id")
			}
			if strings.TrimSpace(e.Digest.Key) == "" {
				problems = append(problems, "digest 缺少聚合键")
			}
			if e.Digest.Count < 1 {
				problems = append(problems, "digest 的累计条数应大于 0")
			}
			if e.Digest.Opened && e.Digest.Replayed {
				problems = append(problems, "digest 不能同时是新开卡与重放")
			}
			if e.Digest.ExpiresAt == nil {
				problems = append(problems, "digest 缺少窗口到期时刻")
			}
		}
		return problems

	case EventDeliveryResult:
		var problems []string
		switch e.Status {
		case StatusSent, StatusFailed, StatusPartial:
		default:
			problems = append(problems, fmt.Sprintf("status %q 不是 sent / failed / partial", e.Status))
		}
		if len(e.Deliveries) == 0 {
			problems = append(problems, "没有投递明细")
		}
		if e.Summary == nil {
			problems = append(problems, "缺少 summary")
		} else if total := e.Summary.Sent + e.Summary.Failed; total != len(e.Deliveries) {
			problems = append(problems, fmt.Sprintf("summary 计数 %d 与明细条数 %d 不一致",
				total, len(e.Deliveries)))
		}
		return problems
	}
	return nil
}

// NewEvent 把一次执行的步骤结果折算成汇总事件。
//
// 只有真正走到分发阶段的步骤才进投递明细：被过滤、条件不满足、上游失败
// 短路、仅登记了延迟的步骤都没有投递动作。一条都没有时不产生事件——
// 「没发」不是「发失败」，拿它去回调只会让接收方以为出了故障。
func NewEvent(msg queue.QueuedMessage, results []pipeline.StepResult) (Event, bool) {
	deliveries := make([]Delivery, 0, len(results))
	sent, failed := 0, 0

	for _, res := range results {
		item, ok := buildDelivery(res)
		if !ok {
			continue
		}
		deliveries = append(deliveries, item)
		if item.Status == StatusSent {
			sent++
		} else {
			failed++
		}
	}
	if len(deliveries) == 0 {
		return Event{}, false
	}

	status := StatusPartial
	switch {
	case failed == 0:
		status = StatusSent
	case sent == 0:
		status = StatusFailed
	}

	ev := newEnvelope(EventDeliveryResult, msg)
	ev.Status = status
	ev.Summary = &Summary{Total: len(deliveries), Sent: sent, Failed: failed}
	ev.Deliveries = deliveries
	return ev, true
}

// newEnvelope 填充每种事件都有的归属字段。
func newEnvelope(event string, msg queue.QueuedMessage) Event {
	return Event{
		ID:            newEventID(),
		Event:         event,
		OccurredAt:    execution.NewTimestamp(time.Now()),
		TriggerID:     msg.TriggerID,
		TransactionID: msg.TransactionID,
		Workflow:      msg.Name,
		SubscriberID:  msg.SubscriberID,
	}
}

// buildDelivery 把单个步骤结果折算成一条投递明细。
//
// 第二个返回值为 false 表示这个步骤没有投递动作，不进回调。
func buildDelivery(res pipeline.StepResult) (Delivery, bool) {
	channel := strings.TrimSpace(string(res.Channel))
	if channel == "" {
		return Delivery{}, false
	}
	if res.Skipped || res.ShortCircuited || res.Deferred || res.TimedOut {
		return Delivery{}, false
	}

	item := Delivery{
		StepID:     res.StepID,
		StepType:   string(res.StepType),
		Channel:    channel,
		Provider:   res.UsedProvider,
		Attempts:   append([]string(nil), res.ProviderAttempts...),
		Fallback:   res.ProviderFallback,
		DurationMS: res.Duration.Milliseconds(),
	}

	switch {
	case res.Sent:
		item.Status = StatusSent
	case res.Error != "":
		item.Status = StatusFailed
		item.Reason = res.Error
	default:
		// 既没有成功也没有失败原因：聚合卡片在窗口内命中、渠道按设计不
		// 重复投递等情形。这类步骤没有「这次投递的结果」。
		return Delivery{}, false
	}
	return item, true
}

// newEventID 生成 32 位十六进制标识，与执行明细的记录标识同口径。
func newEventID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand 失败在实践中不可达；退化成纳秒时间戳，进程内仍然唯一。
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(buf[:])
}
