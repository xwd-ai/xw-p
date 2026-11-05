// Package execution 把一次投递的结果回写成执行明细。
//
// 执行明细与活动流互补：活动流一条触发一条记录，回答「有没有触发」；执行明细
// 按「触发 × 渠道」记账，回答「这个渠道最后发出去没有、失败原因是什么、用了哪个
// 实现」。分发阶段拿到的是每个步骤的结果，本包负责把它落成一行可查询的记录。
//
// 状态只有三种：“sent“ / “failed“ / “skipped“。“skipped“ 表示按订阅者
// 偏好没有发（没有取实现、没有发请求），原因写命中的层级——「没发」与「发失败」
// 是两件事，混在一个状态里会让失败率与排查方向都失真。
//
// 记录格式与 API 侧 api/app/execution_log.py 的行格式保持一致（snake_case），
// 引擎写、API 读，因此字段名与取值都要按那边的约定来：
//
//	{"id":"…","trigger_id":"…","channel":"email","status":"failed",
//	 "reason":"SMTP 收件人 … 失败（550）：no such user","duration_ms":128,
//	 "created_at":"2026-11-02T02:09:12.123456+00:00", …}
//
// 比 API 侧多出来的是几个排查用的字段：step_id / provider / attempts / fallback。
// API 的读取实现按字典取字段，多出来的键不影响解析；Postgres 后端只是不落这几列。
package execution

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Status 是执行明细的投递结果。
type Status string

const (
	// StatusSent 表示该渠道的投递已完成。
	StatusSent Status = "sent"
	// StatusFailed 表示该渠道的投递失败，原因写在 Record.Reason。
	StatusFailed Status = "failed"
	// StatusSkipped 表示该渠道按订阅者偏好跳过：没有取实现、没有发请求，
	// 原因写在 Record.Reason（命中的层级与目标）。
	StatusSkipped Status = "skipped"
)

// Valid 判断取值是否是已知的投递结果。
func (s Status) Valid() bool {
	return s == StatusSent || s == StatusFailed || s == StatusSkipped
}

// timestampLayout 是记录时间戳的序列化格式。
//
// 固定六位小数并以 +00:00 表示 UTC，而不是 Go 默认的 RFC3339Nano：后者会输出
// 九位小数与 Z 后缀，Python 侧的 datetime.fromisoformat 在旧版本上不认 Z，
// 跨进程读写的格式必须按对方的解析能力来定。
const timestampLayout = "2006-01-02T15:04:05.000000-07:00"

// Timestamp 是按 timestampLayout 序列化的时间戳。
//
// 直接给 time.Time 加 `json` 标签会走默认的 RFC3339Nano，因此用一个包装类型
// 接管序列化；填充取值与读取记录都用它。
type Timestamp struct {
	time.Time
}

// NewTimestamp 用给定时刻构造时间戳。
func NewTimestamp(t time.Time) Timestamp {
	return Timestamp{Time: t}
}

// MarshalJSON 按 timestampLayout 输出 UTC 时间。
func (t Timestamp) MarshalJSON() ([]byte, error) {
	if t.Time.IsZero() {
		return []byte("null"), nil
	}
	return json.Marshal(t.UTC().Format(timestampLayout))
}

// UnmarshalJSON 解析 ISO 8601 时间戳（带时区偏移，允许 Z 后缀）。
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		t.Time = time.Time{}
		return nil
	}
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return fmt.Errorf("时间戳 %q 不是 ISO 8601 格式: %w", raw, err)
	}
	t.Time = parsed
	return nil
}

// Record 是一条执行明细。
type Record struct {
	// ID 是记录自身的标识，作为翻页的次级排序键。
	ID string `json:"id"`
	// TriggerID 定位这次触发。
	TriggerID string `json:"trigger_id"`
	// Tenant / Environment 是隔离维度，API 读取时据此收窄。
	Tenant      string `json:"tenant,omitempty"`
	Environment string `json:"environment,omitempty"`
	// Workflow / SubscriberID / StepID 说明这条记录属于哪次执行的哪个步骤。
	Workflow     string `json:"workflow,omitempty"`
	SubscriberID string `json:"subscriber_id,omitempty"`
	StepID       string `json:"step_id,omitempty"`
	// Channel 是投递渠道（in_app / email / sms / push / chat）。
	Channel string `json:"channel"`
	// Status 是投递结果。
	Status Status `json:"status"`
	// Reason 是失败原因；成功时为 null。
	Reason *string `json:"reason"`
	// Provider 是实际完成投递的实现标识（发生回退时是备选的标识）。
	Provider string `json:"provider,omitempty"`
	// Attempts 按尝试顺序记录参与投递的实现。
	Attempts []string `json:"attempts,omitempty"`
	// Fallback 表示默认实现失败后由备选完成投递。
	Fallback bool `json:"fallback,omitempty"`
	// DurationMS 是投递耗时（毫秒）。
	DurationMS int64 `json:"duration_ms"`
	// CreatedAt 是记录写入时刻。
	CreatedAt Timestamp `json:"created_at"`
}

// ErrInvalidRecord 表示记录缺必填字段或取值自相矛盾。
var ErrInvalidRecord = errors.New("execution: 记录不合法")

// Validate 检查记录是否可以落库：触发标识与渠道必填，失败的记录必须带原因
// （没有原因的失败无法排查，与「只写 status」没有区别）。
func (r Record) Validate() error {
	var problems []string
	if strings.TrimSpace(r.TriggerID) == "" {
		problems = append(problems, "缺少 trigger_id")
	}
	if strings.TrimSpace(r.Channel) == "" {
		problems = append(problems, "缺少 channel")
	}
	if !r.Status.Valid() {
		problems = append(problems, fmt.Sprintf("status %q 不是 sent / failed / skipped", string(r.Status)))
	}
	// failed 与 skipped 都必须带原因：前者用来排查「为什么没发出去」，后者用来
	// 回答「哪一层偏好拦下的」。只有状态没有原因，两种记录都等于没写。
	if r.Status != StatusSent && (r.Reason == nil || strings.TrimSpace(*r.Reason) == "") {
		problems = append(problems, "状态 "+string(r.Status)+" 的记录缺少 reason")
	}
	if r.DurationMS < 0 {
		problems = append(problems, fmt.Sprintf("duration_ms 不能为负：%d", r.DurationMS))
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("%w：%s", ErrInvalidRecord, strings.Join(problems, "；"))
}

// clone 复制记录，避免调用方在写入后修改切片字段影响到已落库的快照。
func (r Record) clone() Record {
	if r.Attempts != nil {
		r.Attempts = append([]string(nil), r.Attempts...)
	}
	if r.Reason != nil {
		reason := *r.Reason
		r.Reason = &reason
	}
	return r
}
