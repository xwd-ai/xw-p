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
	// EventDeliveryResult 是「这条触发的投递结果」事件名。
	//
	// 事件名是接入方路由的依据，写死在这里而不是拼进 URL：同一个接收端点
	// 以后还要收别的类型（3.5 后续会有投递失败重试、聚合卡片更新），
	// 用头部与报文字段区分比用不同路径更省事。
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

// Delivery 是单个渠道的投递结果。
//
// 字段与执行明细一一对应，接入方拿到回调后不必再查一次 GET /v1/executions
// 才知道是哪个实现、试了几次。
type Delivery struct {
	StepID     string   `json:"step_id,omitempty"`
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

// Event 是一次执行的回调报文。
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
	Status        string              `json:"status"`
	Summary       Summary             `json:"summary"`
	Deliveries    []Delivery          `json:"deliveries"`
}

// ErrInvalidEvent 表示事件缺必填字段或自相矛盾。
var ErrInvalidEvent = errors.New("webhook: 事件不合法")

// Validate 检查事件能否发出去。
//
// 回调是对外的报文，字段缺了接收方只能收到一条无法归属的通知；
// 因此在入队之前就挡住，而不是等接收方回 400。
func (e Event) Validate() error {
	var problems []string
	if strings.TrimSpace(e.ID) == "" {
		problems = append(problems, "缺少 id")
	}
	if e.Event == "" {
		problems = append(problems, "缺少 event")
	}
	if strings.TrimSpace(e.TriggerID) == "" {
		problems = append(problems, "缺少 trigger_id")
	}
	switch e.Status {
	case StatusSent, StatusFailed, StatusPartial:
	default:
		problems = append(problems, fmt.Sprintf("status %q 不是 sent / failed / partial", e.Status))
	}
	if len(e.Deliveries) == 0 {
		problems = append(problems, "没有投递明细")
	}
	if e.OccurredAt.IsZero() {
		problems = append(problems, "缺少 occurred_at")
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("%w：%s", ErrInvalidEvent, strings.Join(problems, "；"))
}

// NewEvent 把一次执行的步骤结果折算成回调事件。
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

	return Event{
		ID:            newEventID(),
		Event:         EventDeliveryResult,
		OccurredAt:    execution.NewTimestamp(time.Now()),
		TriggerID:     msg.TriggerID,
		TransactionID: msg.TransactionID,
		Workflow:      msg.Name,
		SubscriberID:  msg.SubscriberID,
		Status:        status,
		Summary:       Summary{Total: len(deliveries), Sent: sent, Failed: failed},
		Deliveries:    deliveries,
	}, true
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
