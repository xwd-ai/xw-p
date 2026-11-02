package provider

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// Kind 是渠道实现的种类标识：同一个渠道可以有多种接入方式（邮件既能走
// SMTP 也能走 console mock），配置里用 kind 指明要用哪一种。
type Kind string

const (
	// KindConsole 打印结构化日志的 mock 实现，用于本地跑通链路。
	KindConsole Kind = "console"
	// KindInApp 写入站内信收件箱的实现。
	KindInApp Kind = "inapp"
	// KindSMTP 通过 SMTP 提交邮件。
	KindSMTP Kind = "smtp"
	// KindTwilio 通过 Twilio 风格的短信接口下发。
	KindTwilio Kind = "twilio"
	// KindFCM 通过 FCM 下发移动推送。
	KindFCM Kind = "fcm"
	// KindAPNs 通过 APNs 下发移动推送。
	KindAPNs Kind = "apns"
	// KindPushMock 推送的 mock 实现：按 FCM / APNs 的报文规则校验后只留痕，不联网。
	KindPushMock Kind = "push-mock"
	// KindSlack 通过 Slack 群机器人 webhook 推消息。
	KindSlack Kind = "slack"
	// KindTeams 通过 Teams 群机器人 webhook 推消息。
	KindTeams Kind = "teams"
)

// kindSpec 描述一种实现种类：能用于哪些渠道、需要哪些凭据、还接受哪些可选凭据。
//
// 凭据按「缺了就跑不起来」与「可选项」分开，未列出的键一律视为拼写错误——
// 配置里把 password 写成 passwd 时，最坏的结果不是启动失败，而是运行到投递
// 那一刻才报认证失败，所以宁可在装配阶段就指出来。
type kindSpec struct {
	channels []domain.Channel
	required []string
	optional []string
}

var kindSpecs = map[Kind]kindSpec{
	KindConsole: {
		channels: domain.DeliveryChannels(),
	},
	KindInApp: {
		channels: []domain.Channel{domain.ChannelInApp},
	},
	KindSMTP: {
		channels: []domain.Channel{domain.ChannelEmail},
		required: []string{"host", "from"},
		optional: []string{"port", "username", "password", "tls", "reply_to"},
	},
	KindTwilio: {
		channels: []domain.Channel{domain.ChannelSMS},
		required: []string{"account_sid", "auth_token", "from"},
		optional: []string{"endpoint"},
	},
	KindFCM: {
		channels: []domain.Channel{domain.ChannelPush},
		required: []string{"project_id", "access_token"},
		optional: []string{"endpoint"},
	},
	KindAPNs: {
		channels: []domain.Channel{domain.ChannelPush},
		required: []string{"key_id", "team_id", "private_key"},
		optional: []string{"bundle_id", "sandbox", "endpoint"},
	},
	KindPushMock: {
		channels: []domain.Channel{domain.ChannelPush},
		optional: []string{"platform", "project_id", "topic", "fail_with", "capacity"},
	},
	KindSlack: {
		channels: []domain.Channel{domain.ChannelChat},
		required: []string{"webhook_url"},
		optional: []string{"channel", "username", "icon_emoji", "timeout"},
	},
	KindTeams: {
		channels: []domain.Channel{domain.ChannelChat},
		required: []string{"webhook_url"},
	},
}

// Kinds 返回全部实现种类，按名称排序，便于在错误提示与文档里稳定列出。
func Kinds() []Kind {
	out := make([]Kind, 0, len(kindSpecs))
	for kind := range kindSpecs {
		out = append(out, kind)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// KindChannels 返回某种实现支持的渠道；未知种类返回 false。
func KindChannels(kind Kind) ([]domain.Channel, bool) {
	spec, ok := kindSpecs[kind]
	if !ok {
		return nil, false
	}
	out := make([]domain.Channel, len(spec.channels))
	copy(out, spec.channels)
	return out, true
}

// Credentials 是一条渠道实现的凭据集合（SMTP 口令、短信账号、推送密钥等）。
//
// 值原样保存在进程内存里，只在真正投递时交给实现；日志与错误信息一律走
// Redacted，不把密钥写进日志文件。
type Credentials map[string]string

// Get 返回去空白后的凭据值，缺失返回空串。
func (c Credentials) Get(key string) string {
	return strings.TrimSpace(c[key])
}

// Has 判断凭据是否已设置（空视为未设置）。
func (c Credentials) Has(key string) bool {
	return c.Get(key) != ""
}

// Keys 返回已设置的凭据名，按名称排序。
func (c Credentials) Keys() []string {
	out := make([]string, 0, len(c))
	for key, value := range c {
		if strings.TrimSpace(value) != "" {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

// Missing 返回给定凭据中未设置的那些（保持传入顺序）。
func (c Credentials) Missing(keys ...string) []string {
	var out []string
	for _, key := range keys {
		if !c.Has(key) {
			out = append(out, key)
		}
	}
	return out
}

// Redacted 返回脱敏副本：已设置的值统一替换为 ***，未设置保持空串。
//
// 用于日志、管理接口与错误信息——那里只需要知道「配了哪些」，不需要知道
// 「配的是什么」。
func (c Credentials) Redacted() map[string]string {
	out := make(map[string]string, len(c))
	for key, value := range c {
		if strings.TrimSpace(value) == "" {
			out[key] = ""
			continue
		}
		out[key] = "***"
	}
	return out
}

// Clone 返回深拷贝，调用方可以安全修改而不会影响配置里的原值。
func (c Credentials) Clone() Credentials {
	if c == nil {
		return nil
	}
	out := make(Credentials, len(c))
	for key, value := range c {
		out[key] = value
	}
	return out
}

// Config 是一条渠道配置：用哪个标识、服务哪个渠道、用哪种实现、取用顺序，
// 以及该实现需要的凭据。
//
// Priority 与 Enabled 用指针表达「未配置」：优先级 0 是合法的高优先级，
// 不能和「没写」混为一谈；开关同理，未写时默认启用。
type Config struct {
	ID          string         `json:"id"`
	Channel     domain.Channel `json:"channel"`
	Kind        Kind           `json:"kind"`
	Priority    *int           `json:"priority,omitempty"`
	Enabled     *bool          `json:"enabled,omitempty"`
	Credentials Credentials    `json:"credentials,omitempty"`
}

// EffectivePriority 返回生效的优先级，未配置时用 DefaultPriority。
func (c Config) EffectivePriority() int {
	if c.Priority == nil {
		return DefaultPriority
	}
	return *c.Priority
}

// IsEnabled 返回是否启用，未配置时默认启用。
func (c Config) IsEnabled() bool {
	if c.Enabled == nil {
		return true
	}
	return *c.Enabled
}

// String 返回不含凭据值的描述，可直接写日志。
func (c Config) String() string {
	state := "on"
	if !c.IsEnabled() {
		state = "off"
	}
	out := fmt.Sprintf("%s channel=%s kind=%s priority=%d %s",
		c.ID, c.Channel, c.Kind, c.EffectivePriority(), state)
	if keys := c.Credentials.Keys(); len(keys) > 0 {
		out += " credentials=[" + strings.Join(keys, ",") + "]"
	}
	return out
}

// normalized 清理空白字段并复制凭据，保证 Set 持有的是不可变快照。
func (c Config) normalized() Config {
	c.ID = strings.TrimSpace(c.ID)
	c.Channel = domain.Channel(strings.TrimSpace(string(c.Channel)))
	c.Kind = Kind(strings.TrimSpace(string(c.Kind)))
	creds := make(Credentials, len(c.Credentials))
	for key, value := range c.Credentials {
		creds[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	if len(creds) == 0 {
		creds = nil
	}
	c.Credentials = creds
	return c
}

// ConfigIssue 是一条配置问题，定位到具体的配置项与字段。
type ConfigIssue struct {
	// Index 是配置项在文件中的序号，从 1 开始。
	Index int
	// ProviderID 是该配置项的标识（标识本身缺失时为空）。
	ProviderID string
	// Field 是出问题的字段，如 id / channel / kind / priority / credentials.host。
	Field string
	// Problem 是问题描述。
	Problem string
}

// Error 实现 error，输出形如「第 1 项（email-smtp）credentials.host：必填」。
func (i ConfigIssue) Error() string {
	if i.ProviderID != "" {
		return fmt.Sprintf("第 %d 项（%s）%s：%s", i.Index, i.ProviderID, i.Field, i.Problem)
	}
	return fmt.Sprintf("第 %d 项 %s：%s", i.Index, i.Field, i.Problem)
}

// ConfigError 汇总一次校验发现的全部问题。
//
// 一次列全而不是遇到第一条就返回：改配置的人手上只有一次机会，逐条试错
// 的代价比多写几行汇总高得多。
type ConfigError struct {
	// Source 是配置来源（文件路径或 "内联配置"），用于定位。
	Source string
	// Issues 是发现的问题，按配置项顺序排列。
	Issues []ConfigIssue
}

// Error 实现 error：来源 + 前几条问题 + 剩余条数。
func (e *ConfigError) Error() string {
	head := "渠道配置校验失败"
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

// Set 是一组已通过校验的渠道配置，按渠道建立绑定索引。
//
// 同渠道的多条配置构成一条取用序列：优先级小的在前，同优先级保持配置顺序。
// Set 构造后不再变化，可以被多个投递链路并发读取。
type Set struct {
	source    string
	configs   []Config
	byChannel map[domain.Channel][]Config
	retries   RetryTable
}

// NewSet 校验并归一化一组配置（不带渠道级重试）。
func NewSet(configs []Config) (*Set, error) {
	return newSet("", configs, DefaultRetryTable())
}

func newSet(source string, configs []Config, retries RetryTable) (*Set, error) {
	normalized := make([]Config, len(configs))
	for i, cfg := range configs {
		normalized[i] = cfg.normalized()
	}

	issues := validateConfigs(normalized)
	if len(issues) > 0 {
		return nil, &ConfigError{Source: source, Issues: issues}
	}

	byChannel := make(map[domain.Channel][]Config)
	for _, cfg := range normalized {
		byChannel[cfg.Channel] = append(byChannel[cfg.Channel], cfg)
	}
	for ch := range byChannel {
		list := byChannel[ch]
		sort.SliceStable(list, func(i, j int) bool {
			return list[i].EffectivePriority() < list[j].EffectivePriority()
		})
	}
	return &Set{source: source, configs: normalized, byChannel: byChannel, retries: retries}, nil
}

// validateConfigs 逐条校验并汇总问题；校验不改变输入。
func validateConfigs(configs []Config) []ConfigIssue {
	var issues []ConfigIssue
	seen := make(map[string]int, len(configs))
	for i, cfg := range configs {
		index := i + 1
		issues = append(issues, validateConfig(index, cfg)...)

		if cfg.ID == "" {
			continue
		}
		if first, ok := seen[cfg.ID]; ok {
			issues = append(issues, ConfigIssue{
				Index: index, ProviderID: cfg.ID, Field: "id",
				Problem: fmt.Sprintf("标识重复（与第 %d 项相同）", first),
			})
			continue
		}
		seen[cfg.ID] = index
	}
	return issues
}

// validateConfig 校验单条配置。
func validateConfig(index int, cfg Config) []ConfigIssue {
	var issues []ConfigIssue
	add := func(field, problem string) {
		issues = append(issues, ConfigIssue{Index: index, ProviderID: cfg.ID, Field: field, Problem: problem})
	}

	if cfg.ID == "" {
		add("id", "必填，用于启停、替换与日志溯源")
	}

	channelOK := false
	if cfg.Channel == "" {
		add("channel", "必填，取值为 "+channelList(domain.DeliveryChannels()))
	} else if parsed, ok := domain.ParseChannel(string(cfg.Channel)); !ok {
		add("channel", fmt.Sprintf("未知渠道 %q，取值为 %s", cfg.Channel, channelList(domain.DeliveryChannels())))
	} else if !parsed.IsDelivery() {
		add("channel", fmt.Sprintf("%q 是编排步骤而非投递渠道", parsed))
	} else {
		channelOK = true
	}

	spec, kindOK := kindSpecs[cfg.Kind]
	switch {
	case cfg.Kind == "":
		add("kind", "必填，取值为 "+kindList())
	case !kindOK:
		add("kind", fmt.Sprintf("未知实现种类 %q，取值为 %s", cfg.Kind, kindList()))
	default:
		if channelOK && !containsChannel(spec.channels, cfg.Channel) {
			add("kind", fmt.Sprintf("%q 不能用于渠道 %q，可用的渠道为 %s",
				cfg.Kind, cfg.Channel, channelList(spec.channels)))
		}
	}

	if cfg.Priority != nil && *cfg.Priority < 0 {
		add("priority", fmt.Sprintf("不能为负数（当前 %d）", *cfg.Priority))
	}

	if kindOK {
		required := spec.required
		if missing := cfg.Credentials.Missing(required...); len(missing) > 0 {
			add("credentials", "缺少必填凭据 "+strings.Join(missing, "、"))
		}
		allowed := make(map[string]bool, len(spec.required)+len(spec.optional))
		for _, key := range spec.required {
			allowed[key] = true
		}
		for _, key := range spec.optional {
			allowed[key] = true
		}
		for _, key := range cfg.Credentials.Keys() {
			if allowed[key] {
				continue
			}
			if guess, ok := nearKey(key, allowed); ok {
				add("credentials."+key, fmt.Sprintf("未识别的凭据项，是否应写 %q", guess))
				continue
			}
			add("credentials."+key, "未识别的凭据项")
		}
	}
	return issues
}

// nearKey 在允许的键里找一个只差大小写的近似项，用于提示拼写错误。
func nearKey(key string, allowed map[string]bool) (string, bool) {
	for candidate := range allowed {
		if strings.EqualFold(candidate, key) {
			return candidate, true
		}
	}
	return "", false
}

func containsChannel(list []domain.Channel, target domain.Channel) bool {
	for _, ch := range list {
		if ch == target {
			return true
		}
	}
	return false
}

func channelList(list []domain.Channel) string {
	names := make([]string, len(list))
	for i, ch := range list {
		names[i] = string(ch)
	}
	return strings.Join(names, " / ")
}

func kindList() string {
	kinds := Kinds()
	names := make([]string, len(kinds))
	for i, kind := range kinds {
		names[i] = string(kind)
	}
	return strings.Join(names, " / ")
}

// fileShape 是配置文件的完整对象形态：{"providers": [...], "retries": {...}}。
type fileShape struct {
	Providers []Config `json:"providers"`
	// Retries 是按渠道配置的投递重试策略，缺省表示不重试。
	Retries json.RawMessage `json:"retries,omitempty"`
}

// ParseSet 解析配置内容，接受两种写法：顶层数组，或 {"providers": [...]}。
//
// 未知字段一律报错：把 credential 写成 credentials 之外的名字时，静默忽略
// 只会让配置看起来生效、实际全部缺失。
func ParseSet(raw []byte) (*Set, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, errors.New("渠道配置为空")
	}

	var configs []Config
	retries := DefaultRetryTable()
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.DisallowUnknownFields()
	if trimmed[0] == '[' {
		if err := dec.Decode(&configs); err != nil {
			return nil, fmt.Errorf("解析渠道配置失败：%w", err)
		}
	} else {
		var shape fileShape
		if err := dec.Decode(&shape); err != nil {
			return nil, fmt.Errorf("解析渠道配置失败：%w", err)
		}
		if shape.Providers == nil {
			return nil, errors.New("渠道配置缺少 providers 字段")
		}
		configs = shape.Providers
		table, err := parseRetryTable(shape.Retries)
		if err != nil {
			return nil, err
		}
		retries = table
	}
	// 顶层数组后面还跟着内容（两个 JSON 文档拼在一起）同样视为格式错误。
	if dec.More() {
		return nil, errors.New("渠道配置存在多余内容，一个文件只接受一个 JSON 文档")
	}
	return newSet("", configs, retries)
}

// LoadSet 从文件读取并校验配置；来源路径会写进错误信息。
func LoadSet(path string) (*Set, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取渠道配置失败：%w", err)
	}
	set, err := ParseSet(raw)
	if err != nil {
		var cfgErr *ConfigError
		if errors.As(err, &cfgErr) {
			cfgErr.Source = path
		}
		var retryErr *RetryError
		if errors.As(err, &retryErr) {
			retryErr.Source = path
		}
		return nil, err
	}
	set.source = path
	return set, nil
}

// Source 返回配置来源（文件路径），内联配置为空串。
func (s *Set) Source() string {
	if s == nil {
		return ""
	}
	return s.source
}

// Len 返回配置条数（含停用的）。
func (s *Set) Len() int {
	if s == nil {
		return 0
	}
	return len(s.configs)
}

// All 返回全部配置的快照，按文件中的顺序排列。
func (s *Set) All() []Config {
	if s == nil {
		return nil
	}
	out := make([]Config, len(s.configs))
	for i, cfg := range s.configs {
		out[i] = cfg.clone()
	}
	return out
}

// Channel 返回某渠道的取用序列：优先级小的在前，同优先级保持配置顺序。
func (s *Set) Channel(ch domain.Channel) []Config {
	if s == nil {
		return nil
	}
	list := s.byChannel[ch]
	out := make([]Config, len(list))
	for i, cfg := range list {
		out[i] = cfg.clone()
	}
	return out
}

// Primary 返回某渠道的默认实现：序列中第一个启用的配置。
func (s *Set) Primary(ch domain.Channel) (Config, bool) {
	for _, cfg := range s.byChannel[ch] {
		if cfg.IsEnabled() {
			return cfg.clone(), true
		}
	}
	return Config{}, false
}

// Channels 返回配置涉及的渠道，顺序与 domain.AllChannels 一致。
func (s *Set) Channels() []domain.Channel {
	if s == nil {
		return nil
	}
	var out []domain.Channel
	for _, ch := range domain.AllChannels() {
		if _, ok := s.byChannel[ch]; ok {
			out = append(out, ch)
		}
	}
	return out
}

// Retries 返回渠道级重试策略表（配置文件未写 retries 段时为不重试）。
func (s *Set) Retries() RetryTable {
	if s == nil {
		return DefaultRetryTable()
	}
	return s.retries
}

// Unbound 返回 given 中没有启用配置的渠道，保持传入顺序。
//
// 装配时用它确认「工作流会投递到的渠道都有人接」——缺一个渠道不会让进程
// 起不来，但会在运行期退化成分发阶段的 mock 打印，提前指出来更省事。
func (s *Set) Unbound(given []domain.Channel) []domain.Channel {
	var out []domain.Channel
	for _, ch := range given {
		if _, ok := s.Primary(ch); !ok {
			out = append(out, ch)
		}
	}
	return out
}

// String 返回脱敏摘要，可直接写日志。
func (s *Set) String() string {
	if s == nil {
		return "Set{empty}"
	}
	parts := make([]string, 0, len(s.byChannel))
	for _, ch := range s.Channels() {
		ids := make([]string, 0, len(s.byChannel[ch]))
		for _, cfg := range s.byChannel[ch] {
			id := cfg.ID
			if !cfg.IsEnabled() {
				id += "(off)"
			}
			ids = append(ids, id)
		}
		parts = append(parts, fmt.Sprintf("%s:[%s]", ch, strings.Join(ids, " ")))
	}
	out := fmt.Sprintf("Set{providers=%d bindings={%s}}", len(s.configs), strings.Join(parts, " "))
	if s.retries.Enabled() {
		out += fmt.Sprintf(" retries={%s}", s.retries.String())
	}
	return out
}

// clone 复制配置，避免调用方修改返回值里的凭据影响 Set。
func (c Config) clone() Config {
	c.Credentials = c.Credentials.Clone()
	return c
}

// Factory 按一条已校验的配置构造渠道实现。
//
// 实现种类与具体实现解耦在这里：配置说「email 用 smtp」，由 Factory 决定
// 构造哪个 Sender、怎么把凭据喂进去。尚未接入的种类返回 ErrUnsupportedKind，
// 调用方据此跳过并告警，而不是让整个进程起不来。
type Factory interface {
	Build(cfg Config) (Sender, error)
}

// FactoryFunc 让函数直接实现 Factory。
type FactoryFunc func(Config) (Sender, error)

// Build 实现 Factory。
func (f FactoryFunc) Build(cfg Config) (Sender, error) {
	return f(cfg)
}

// ErrUnsupportedKind 表示该实现种类尚未接入。
var ErrUnsupportedKind = errors.New("provider: 该实现种类尚未接入")

// Skipped 是一条因实现尚未接入而跳过的配置。
type Skipped struct {
	ID      string
	Kind    Kind
	Channel domain.Channel
	Reason  string
}

// BindReport 是配置装配到注册表的结果。
type BindReport struct {
	// Bound 是成功登记的条目，顺序与配置顺序一致。
	Bound []Entry
	// Skipped 是因实现种类尚未接入而跳过的配置。
	Skipped []Skipped
	// Missing 是注册表里仍然没有启用实现的投递渠道。
	Missing []domain.Channel
}

// Bind 按配置构造实现并登记进注册表，返回装配结果。
//
// 先全部构造、再统一登记：任何一条构造失败都不会留下半套注册表。
// 尚未接入的种类（配置先写好、实现后续补）跳过并记入 Skipped，不视为失败。
func (s *Set) Bind(reg *Registry, factory Factory) (BindReport, error) {
	var report BindReport
	if s == nil {
		return report, errors.New("provider: 渠道配置为空")
	}
	if reg == nil {
		return report, errors.New("provider: 注册表不能为空")
	}
	if factory == nil {
		return report, errors.New("provider: 未提供实现工厂")
	}

	entries := make([]Entry, 0, len(s.configs))
	for _, cfg := range s.configs {
		sender, err := factory.Build(cfg.clone())
		if err != nil {
			if errors.Is(err, ErrUnsupportedKind) {
				report.Skipped = append(report.Skipped, Skipped{
					ID: cfg.ID, Kind: cfg.Kind, Channel: cfg.Channel, Reason: err.Error(),
				})
				continue
			}
			return report, fmt.Errorf("构造渠道实现失败（%s，%s/%s）：%w", cfg.ID, cfg.Channel, cfg.Kind, err)
		}
		priority := cfg.EffectivePriority()
		entries = append(entries, Entry{
			ID:       cfg.ID,
			Channel:  cfg.Channel,
			Priority: priority,
			Enabled:  cfg.IsEnabled(),
			Sender:   sender,
		})
	}

	for _, entry := range entries {
		if err := reg.RegisterEntry(entry); err != nil {
			return report, fmt.Errorf("登记渠道实现失败（%s）：%w", entry.ID, err)
		}
		report.Bound = append(report.Bound, entry)
	}
	// 以注册表为准统计缺口：调用方可能已经预置了一些实现。
	for _, ch := range domain.DeliveryChannels() {
		if !reg.Has(ch) {
			report.Missing = append(report.Missing, ch)
		}
	}
	return report, nil
}
