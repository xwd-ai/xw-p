// Package preference 在投递前按订阅者偏好决定某个渠道发不发。
//
// 偏好记录由 API 进程写入数据目录下的 preferences.jsonl（一行一条），引擎只读。
// 一条记录由「订阅者 + 层级 + 目标」唯一确定：
//
//	global    目标为空    这个订阅者要不要收任何通知（总开关）
//	workflow  工作流名    这类互动要不要通知我
//	channel   渠道        这类通知要不要用这个渠道发
//
// 三层是一条覆盖链而不是三张独立的表：解析时从最具体的一层往上找，命中第一条
// 即停。渠道关了但工作流开着，这条通知只是不走这个渠道；渠道没配但工作流关了，
// 整条通知都跳过。三层都没有记录时按接收处理——偏好用于收窄投递范围，默认关
// 闭会让刚注册的订阅者收不到任何东西。
//
// 只有开关没有地址：地址属于订阅者档案（邮箱、手机号、设备 token），偏好只表达
// 意愿。两者分开才能做到「先停掉这个渠道，地址先留着」，恢复时不必重新收集。
//
// 判定只做否决。缺收件地址、渠道没配实现这类问题仍由投递路径报失败，不因为
// 偏好允许就被绕过。
package preference

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// Scope 是偏好的层级。
type Scope string

const (
	// ScopeGlobal 是订阅者的总开关，目标为空。
	ScopeGlobal Scope = "global"
	// ScopeWorkflow 针对某个工作流。
	ScopeWorkflow Scope = "workflow"
	// ScopeChannel 针对某个投递渠道。
	ScopeChannel Scope = "channel"
)

// Valid 判断层级取值是否已知。
func (s Scope) Valid() bool {
	switch s {
	case ScopeGlobal, ScopeWorkflow, ScopeChannel:
		return true
	}
	return false
}

// label 是层级在原因文案里的名字。
func (s Scope) label() string {
	switch s {
	case ScopeGlobal:
		return "全局"
	case ScopeWorkflow:
		return "工作流"
	case ScopeChannel:
		return "渠道"
	}
	return string(s)
}

// 长度上限与 API 侧一致：同一个标识在写入方被判合法、在读取方被判非法，
// 会让两进程对「这条记录是否存在」得出不同结论。
const (
	// MaxSubscriberIDLength 是订阅者标识的长度上限。
	MaxSubscriberIDLength = 128
	// MaxWorkflowLength 是工作流名的长度上限。
	MaxWorkflowLength = 64
)

// isDeliveryChannel 判断某个取值能否作为偏好的渠道目标。
//
// delay / digest 是编排步骤而不是投递渠道，挂在偏好上等于写了一个永远匹配
// 不到的条件；未知取值同样按无效处理。
func isDeliveryChannel(value string) bool {
	ch, ok := domain.ParseChannel(value)
	return ok && ch.IsDelivery()
}

// Record 是一条偏好记录。
type Record struct {
	SubscriberID string    `json:"subscriber_id"`
	Scope        Scope     `json:"scope"`
	Target       string    `json:"target"`
	Enabled      bool      `json:"enabled"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// recordKey 是记录的主键：同一订阅者同一层级同一目标只有一条。
type recordKey struct {
	subscriber string
	scope      Scope
	target     string
}

func (r Record) key() recordKey {
	return recordKey{subscriber: r.SubscriberID, scope: r.Scope, target: r.Target}
}

// rawRecord 是落盘行的形态。Enabled 用指针接，缺字段时按「接收」处理——
// 手写的记录常常只写 subscriber_id / scope / target。
type rawRecord struct {
	SubscriberID string `json:"subscriber_id"`
	Scope        string `json:"scope"`
	Target       string `json:"target"`
	Enabled      *bool  `json:"enabled"`
	UpdatedAt    string `json:"updated_at"`
}

// ParseRecord 解析一行偏好记录；记录不成立时返回 error。
func ParseRecord(line []byte) (Record, error) {
	var raw rawRecord
	if err := json.Unmarshal(line, &raw); err != nil {
		return Record{}, fmt.Errorf("不是合法的 JSON: %w", err)
	}
	return raw.normalize()
}

func (r rawRecord) normalize() (Record, error) {
	subscriber := strings.TrimSpace(r.SubscriberID)
	if subscriber == "" {
		return Record{}, fmt.Errorf("缺少 subscriber_id")
	}
	if len(subscriber) > MaxSubscriberIDLength {
		return Record{}, fmt.Errorf("subscriber_id 超过 %d 个字符", MaxSubscriberIDLength)
	}
	scope := Scope(strings.TrimSpace(r.Scope))
	if !scope.Valid() {
		return Record{}, fmt.Errorf("未知层级 %q", r.Scope)
	}
	target, err := NormalizeTarget(scope, r.Target)
	if err != nil {
		return Record{}, err
	}
	enabled := true
	if r.Enabled != nil {
		enabled = *r.Enabled
	}
	record := Record{
		SubscriberID: subscriber,
		Scope:        scope,
		Target:       target,
		Enabled:      enabled,
	}
	if raw := strings.TrimSpace(r.UpdatedAt); raw != "" {
		if stamp, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			record.UpdatedAt = stamp
		}
	}
	return record, nil
}

// NormalizeTarget 收敛某一层级的 target，规则与 API 侧一致。
//
// 全局不接受目标（总开关带目标后语义会与工作流层重叠），工作流要一个不含空白
// 的名字，渠道要是可投递渠道。
func NormalizeTarget(scope Scope, raw string) (string, error) {
	target := strings.TrimSpace(raw)
	switch scope {
	case ScopeGlobal:
		if target != "" {
			return "", fmt.Errorf("全局偏好不接受 target")
		}
		return "", nil
	case ScopeWorkflow:
		if target == "" {
			return "", fmt.Errorf("工作流偏好缺少 target")
		}
		if len(target) > MaxWorkflowLength {
			return "", fmt.Errorf("工作流名超过 %d 个字符", MaxWorkflowLength)
		}
		if strings.ContainsFunc(target, unicode.IsSpace) {
			return "", fmt.Errorf("工作流名不能包含空白字符")
		}
		return target, nil
	case ScopeChannel:
		if target == "" {
			return "", fmt.Errorf("渠道偏好缺少 target")
		}
		if !isDeliveryChannel(target) {
			return "", fmt.Errorf("%q 不是可投递渠道", target)
		}
		return target, nil
	}
	return "", fmt.Errorf("未知层级 %q", string(scope))
}

// Set 是某一时刻的偏好快照。
//
// 构造后只读，因此多个消费者可以并发解析同一份快照：一次投递要问三层，
// 逐层读文件会在同一个人身上做三次读取，还可能读到两个不同时刻的视图。
type Set struct {
	records map[recordKey]Record
}

// NewSet 由记录构造快照；同一主键重复出现时后一条覆盖前一条
// （与存储的覆盖写语义一致）。
func NewSet(records ...Record) *Set {
	set := &Set{records: make(map[recordKey]Record, len(records))}
	for _, record := range records {
		set.records[record.key()] = record
	}
	return set
}

// Len 返回快照里的记录条数。
func (s *Set) Len() int {
	if s == nil {
		return 0
	}
	return len(s.records)
}

// Lookup 按主键读取一条记录。
func (s *Set) Lookup(subscriberID string, scope Scope, target string) (Record, bool) {
	if s == nil {
		return Record{}, false
	}
	record, ok := s.records[recordKey{
		subscriber: strings.TrimSpace(subscriberID),
		scope:      scope,
		target:     strings.TrimSpace(target),
	}]
	return record, ok
}

// Decision 是一次解析的结论：发不发、是哪一层决定的、为什么。
type Decision struct {
	// Allowed 为 false 表示这条通知不该走这个渠道。
	Allowed bool
	// Scope / Target 是命中的那一层（未命中时为空）。
	Scope  Scope
	Target string
	// Matched 表示至少命中了一条记录；false 时是默认接收。
	Matched bool
	// Reason 是一句话说明，可直接写进日志与执行明细。
	Reason string
}

// SkipMessage 是写进执行明细的跳过原因；放行时为空。
func (d Decision) SkipMessage() string {
	if d.Allowed {
		return ""
	}
	return "偏好跳过：" + d.Reason
}

// Resolve 按「渠道 → 工作流 → 全局」解析，命中第一条即返回。
//
// 渠道关掉只影响这一个渠道，工作流关掉影响它下面所有渠道，因此渠道先于
// 工作流、工作流先于全局。channel 与 workflow 传空表示这一层不参与判定
// （例如消息没带工作流名时只判渠道层与全局层）。
func (s *Set) Resolve(subscriberID, workflow, channel string) Decision {
	type candidate struct {
		scope  Scope
		target string
	}
	candidates := make([]candidate, 0, 3)
	if target := strings.TrimSpace(channel); isDeliveryChannel(target) {
		candidates = append(candidates, candidate{scope: ScopeChannel, target: target})
	}
	if target := strings.TrimSpace(workflow); isWorkflowName(target) {
		candidates = append(candidates, candidate{scope: ScopeWorkflow, target: target})
	}
	candidates = append(candidates, candidate{scope: ScopeGlobal})

	for _, c := range candidates {
		record, ok := s.Lookup(subscriberID, c.scope, c.target)
		if !ok {
			continue
		}
		return Decision{
			Allowed: record.Enabled,
			Scope:   c.scope,
			Target:  c.target,
			Matched: true,
			Reason:  describe(c.scope, c.target, record.Enabled),
		}
	}
	return Decision{Allowed: true, Reason: "未设置偏好，按默认接收"}
}

// isWorkflowName 判断工作流名是否可用作查询键（空名与空格名查不到记录）。
func isWorkflowName(value string) bool {
	if value == "" || len(value) > MaxWorkflowLength {
		return false
	}
	return !strings.ContainsFunc(value, unicode.IsSpace)
}

// describe 生成命中记录的一句话说明。
func describe(scope Scope, target string, enabled bool) string {
	state := "已开启"
	if !enabled {
		state = "已关闭"
	}
	if scope == ScopeGlobal {
		return "全局总开关" + state
	}
	return scope.label() + " " + target + " " + state
}
