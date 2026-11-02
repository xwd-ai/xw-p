package provider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/backoff"
	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// RetryPolicy 是渠道层的退避策略：最多尝试几次、每次失败后等多久。
//
// 曲线与队列重试共用 backoff.Policy，这里只换了一组更适合网络调用的参数：
// 起步更短、封顶更低。区别在于它作用于「一次投递里的同一个实现」，用完
// 次数后回退链继续试下一个实现，而不是把整条消息重新投递。
type RetryPolicy = backoff.Policy

const (
	// DefaultRetryBase 是渠道重试的首次等待：比队列侧短，投递失败多半是
	// 一次抖动，等一下再试的代价远小于把消息打回队列。
	DefaultRetryBase = 500 * time.Millisecond
	// DefaultRetryMax 是渠道重试的等待上限。
	DefaultRetryMax = 5 * time.Second
	// DefaultRetryJitter 是渠道重试的抖动比例。
	DefaultRetryJitter = 0.3
	// MaxRetryAttempts 是单个实现最多尝试的次数（含首次）。
	//
	// 上限放在 10：再往上单条消息在这一步就能耗掉整条工作流的预算，
	// 与其把下游按在地上反复捶，不如尽早回退或失败。
	MaxRetryAttempts = 10
	// maxRetryBase 与 maxRetryMax 是等待时长的合理上界，防止配置把
	// 单步预算全花在 sleep 上。
	maxRetryBase = time.Minute
	maxRetryMax  = 5 * time.Minute
)

// DefaultRetryPolicy 返回默认策略：不重试，只保留一组等待参数。
//
// 默认关闭是有意为之：渠道层之上还有队列层的失败重试，两层同时打开会让
// 一次故障的请求量按乘积放大。需要重试的渠道在配置文件里显式写
// max_attempts。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		Base:   DefaultRetryBase,
		Max:    DefaultRetryMax,
		Jitter: DefaultRetryJitter,
	}
}

// RetryTable 按渠道保存退避策略。
//
// 零值可用：查不到渠道、也没有默认项时返回「不重试」，与配置里没写
// retries 段的行为一致。
type RetryTable struct {
	defaults  RetryPolicy
	byChannel map[domain.Channel]RetryPolicy
}

// DefaultRetryTable 返回不做任何重试的空表。
func DefaultRetryTable() RetryTable { return RetryTable{defaults: DefaultRetryPolicy()} }

// NewRetryTable 校验并构造渠道级重试表。
func NewRetryTable(defaults RetryPolicy, overrides map[domain.Channel]RetryPolicy) (RetryTable, error) {
	base := withRetryDefaults(defaults)
	var issues []RetryIssue
	issues = append(issues, validateRetry("default", base)...)

	var byChannel map[domain.Channel]RetryPolicy
	if len(overrides) > 0 {
		byChannel = make(map[domain.Channel]RetryPolicy, len(overrides))
	}
	for ch, policy := range overrides {
		name := string(ch)
		if ch == "" {
			issues = append(issues, RetryIssue{Section: name, Field: "channel", Problem: "渠道不能为空"})
			continue
		}
		if !ch.IsDelivery() {
			issues = append(issues, RetryIssue{
				Section: name, Field: "channel",
				Problem: fmt.Sprintf("%q 是编排步骤而非投递渠道", ch),
			})
			continue
		}
		filled := withRetryDefaults(policy)
		issues = append(issues, validateRetry(name, filled)...)
		byChannel[ch] = filled.Normalize()
	}
	if len(issues) > 0 {
		sortIssues(issues)
		return RetryTable{}, &RetryError{Issues: issues}
	}
	return RetryTable{defaults: base.Normalize(), byChannel: byChannel}, nil
}

// Lookup 返回某渠道生效的策略：渠道有专属配置时用它，否则用默认项。
func (t RetryTable) Lookup(ch domain.Channel) RetryPolicy {
	if policy, ok := t.byChannel[ch]; ok {
		return policy
	}
	return t.defaults
}

// Defaults 返回默认策略。
func (t RetryTable) Defaults() RetryPolicy { return t.defaults }

// Channels 返回做了单独配置的渠道，顺序与 domain.AllChannels 一致。
func (t RetryTable) Channels() []domain.Channel {
	if len(t.byChannel) == 0 {
		return nil
	}
	out := make([]domain.Channel, 0, len(t.byChannel))
	for _, ch := range domain.AllChannels() {
		if _, ok := t.byChannel[ch]; ok {
			out = append(out, ch)
		}
	}
	return out
}

// Enabled 表示是否有任何渠道会重试（含默认项）。
func (t RetryTable) Enabled() bool {
	if t.defaults.Retries() > 0 {
		return true
	}
	for _, policy := range t.byChannel {
		if policy.Retries() > 0 {
			return true
		}
	}
	return false
}

// String 返回可写进启动日志的摘要，例如「default=关闭 email=3 次 500ms→5s」。
func (t RetryTable) String() string {
	parts := []string{"default=" + t.defaults.String()}
	for _, ch := range t.Channels() {
		parts = append(parts, string(ch)+"="+t.byChannel[ch].String())
	}
	return strings.Join(parts, " ")
}

// withRetryDefaults 补齐没写的等待参数：只填了次数的条目沿用渠道层的默认值。
//
// 它只看「写没写」补零值，不纠正越界配置——纠正放到校验通过之后再做，
// 否则「上限小于基础值」这类配错会被悄悄改正常，配置文件看起来仍然有效。
func withRetryDefaults(p RetryPolicy) RetryPolicy {
	if p.Base <= 0 {
		p.Base = DefaultRetryBase
	}
	if p.Max <= 0 {
		p.Max = DefaultRetryMax
	}
	return p
}

// RetryIssue 是一条重试配置问题。
type RetryIssue struct {
	// Section 是配置段名：default 或渠道名。
	Section string
	// Field 是出问题的字段，如 max_attempts / base_delay。
	Field string
	// Problem 是问题描述。
	Problem string
}

// Error 实现 error，输出形如「email.max_attempts：不能超过 10（当前 12）」。
func (i RetryIssue) Error() string {
	if i.Field == "" || i.Field == i.Section {
		return fmt.Sprintf("%s：%s", i.Section, i.Problem)
	}
	return fmt.Sprintf("%s.%s：%s", i.Section, i.Field, i.Problem)
}

// RetryError 汇总一次校验发现的全部重试配置问题。
type RetryError struct {
	// Source 是配置来源（文件路径），用于定位。
	Source string
	Issues []RetryIssue
}

// Error 实现 error：来源 + 前几条问题 + 剩余条数。
func (e *RetryError) Error() string {
	head := "渠道重试配置校验失败"
	if e.Source != "" {
		head += "（" + e.Source + "）"
	}
	const limit = 6
	var b strings.Builder
	b.WriteString(head)
	for i, issue := range e.Issues {
		if i == limit {
			fmt.Fprintf(&b, "；另有 %d 条问题", len(e.Issues)-limit)
			break
		}
		b.WriteString("；")
		b.WriteString(issue.Error())
	}
	return b.String()
}

// validateRetry 校验一条策略，问题一次列全。
func validateRetry(section string, p RetryPolicy) []RetryIssue {
	var issues []RetryIssue
	add := func(field, problem string) {
		issues = append(issues, RetryIssue{Section: section, Field: field, Problem: problem})
	}
	if p.MaxAttempts < 0 {
		add("max_attempts", fmt.Sprintf("不能为负数（当前 %d）", p.MaxAttempts))
	}
	if p.MaxAttempts > MaxRetryAttempts {
		add("max_attempts", fmt.Sprintf("不能超过 %d（当前 %d）", MaxRetryAttempts, p.MaxAttempts))
	}
	if p.Base <= 0 {
		add("base_delay", "必须为正的时长")
	}
	if p.Base > maxRetryBase {
		add("base_delay", fmt.Sprintf("不能超过 %s（当前 %s）", maxRetryBase, p.Base))
	}
	if p.Max <= 0 {
		add("max_delay", "必须为正的时长")
	}
	if p.Max > maxRetryMax {
		add("max_delay", fmt.Sprintf("不能超过 %s（当前 %s）", maxRetryMax, p.Max))
	}
	if p.Max < p.Base {
		add("max_delay", fmt.Sprintf("不能小于 base_delay（%s < %s）", p.Max, p.Base))
	}
	if p.Jitter < 0 || p.Jitter > 1 {
		add("jitter", fmt.Sprintf("取值为 0 到 1（当前 %g）", p.Jitter))
	}
	return issues
}

func sortIssues(issues []RetryIssue) {
	sort.SliceStable(issues, func(i, j int) bool {
		if issues[i].Section != issues[j].Section {
			return issues[i].Section < issues[j].Section
		}
		return issues[i].Field < issues[j].Field
	})
}

// retryEntry 是配置文件里一段重试配置，字段全为指针：只有写了的字段才覆盖
// 上一层（default 段或内置默认），否则「只改等待时长」会把次数也一起清掉。
type retryEntry struct {
	MaxAttempts *int           `json:"max_attempts"`
	BaseDelay   *durationValue `json:"base_delay"`
	MaxDelay    *durationValue `json:"max_delay"`
	Jitter      *float64       `json:"jitter"`
}

func (e retryEntry) apply(base RetryPolicy) RetryPolicy {
	if e.MaxAttempts != nil {
		base.MaxAttempts = *e.MaxAttempts
	}
	if e.BaseDelay != nil {
		base.Base = time.Duration(*e.BaseDelay)
	}
	if e.MaxDelay != nil {
		base.Max = time.Duration(*e.MaxDelay)
	}
	if e.Jitter != nil {
		base.Jitter = *e.Jitter
	}
	return base
}

// durationValue 接受两种写法：带单位的字符串（"500ms"）或毫秒数（250）。
//
// 允许数字是因为 JSON 里写 "500ms" 不如 500 顺手；但单位必须写清楚，
// 所以数字固定按毫秒解释，不做「猜是秒还是毫秒」。
type durationValue time.Duration

func (d *durationValue) UnmarshalJSON(data []byte) error {
	text := strings.TrimSpace(string(data))
	if text == "" || text == "null" {
		return nil
	}
	if text[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		parsed, err := time.ParseDuration(strings.TrimSpace(s))
		if err != nil {
			return fmt.Errorf("时长格式不正确（示例：250ms、2s）：%w", err)
		}
		*d = durationValue(parsed)
		return nil
	}
	ms, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return fmt.Errorf("时长应为字符串或毫秒数：%s", text)
	}
	*d = durationValue(time.Duration(ms * float64(time.Millisecond)))
	return nil
}

// parseRetryTable 解析 retries 段：{"default": {...}, "email": {...}}。
func parseRetryTable(raw json.RawMessage) (RetryTable, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return DefaultRetryTable(), nil
	}

	var sections map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &sections); err != nil {
		return RetryTable{}, fmt.Errorf("解析渠道重试配置失败：%w", err)
	}

	names := make([]string, 0, len(sections))
	for name := range sections {
		names = append(names, name)
	}
	sort.Strings(names)

	entries := make(map[string]retryEntry, len(sections))
	for _, name := range names {
		dec := json.NewDecoder(bytes.NewReader(sections[name]))
		// 未知字段直接报错：把 jitter 写成 jitter_ratio 时静默忽略只会让
		// 配置看起来生效、实际仍用默认值。
		dec.DisallowUnknownFields()
		var entry retryEntry
		if err := dec.Decode(&entry); err != nil {
			return RetryTable{}, fmt.Errorf("解析渠道重试配置失败（%s）：%w", name, err)
		}
		entries[name] = entry
	}

	defaults := withRetryDefaults(DefaultRetryPolicy())
	if entry, ok := entries["default"]; ok {
		defaults = withRetryDefaults(entry.apply(DefaultRetryPolicy()))
		delete(entries, "default")
	}
	var issues []RetryIssue
	issues = append(issues, validateRetry("default", defaults)...)

	overrides := make(map[domain.Channel]RetryPolicy, len(entries))
	for _, name := range names {
		entry, ok := entries[name]
		if !ok {
			continue
		}
		ch, parsed := domain.ParseChannel(name)
		if !parsed {
			issues = append(issues, RetryIssue{
				Section: name, Field: "channel",
				Problem: fmt.Sprintf("未知渠道 %q，取值为 %s", name, channelList(domain.DeliveryChannels())),
			})
			continue
		}
		if !ch.IsDelivery() {
			issues = append(issues, RetryIssue{
				Section: name, Field: "channel",
				Problem: fmt.Sprintf("%q 是编排步骤而非投递渠道", ch),
			})
			continue
		}
		policy := withRetryDefaults(entry.apply(defaults))
		issues = append(issues, validateRetry(name, policy)...)
		overrides[ch] = policy.Normalize()
	}
	if len(issues) > 0 {
		sortIssues(issues)
		return RetryTable{}, &RetryError{Issues: issues}
	}

	table := RetryTable{defaults: defaults.Normalize(), byChannel: overrides}
	if len(overrides) == 0 {
		table.byChannel = nil
	}
	return table, nil
}
