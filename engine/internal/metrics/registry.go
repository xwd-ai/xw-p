// Package metrics 采集投递侧的成功、失败与延迟，供运维接口读取。
//
// 指标的来源是「一次真实的投递动作」：某个渠道的一次分发走了哪条实现、成功
// 还是失败、花了多久、中间重试了几次、有没有回退到备选。做聚合卡片时窗口内
// 命中不重复外发、被过滤条件拦下、上游失败短路的步骤都没有投递动作，不进这里
// 统计——把「没发」和「发失败」混在一个失败数里，告警就没法用了。
//
// 这一层只管内存里的计数，不负责抓取与持久化：进程重启归零是刻意的，运维接口
// 暴露的就是「本实例自启动以来」的口径，跨实例汇总交给抓取端按实例聚合。
package metrics

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// Status 是一次投递的结果。
type Status string

const (
	// StatusSent 表示投递成功。
	StatusSent Status = "sent"
	// StatusFailed 表示投递失败（含超时）。
	StatusFailed Status = "failed"
)

// UnknownLabel 是没有渠道或实现标识时的占位标签。
//
// 正常路径不会走到这里（分发阶段一定填了渠道），真出现说明上游漏填了字段；
// 归到显式标签下比静默丢弃更容易被发现。
const UnknownLabel = "unknown"

// Sample 是一次投递动作的观测。
type Sample struct {
	// Channel 是分发渠道（in_app / email / sms / push / chat）。
	Channel string
	// Provider 是实际完成投递的实现标识；回退时是最终成功（或最后尝试）的那个。
	Provider string
	// Status 是投递结果，空值按失败计。
	Status Status
	// Duration 是分发阶段耗时，含回退链上每一次失败的尝试。
	Duration time.Duration
	// Retries 是首次之外多打的尝试次数（同一实现重试 + 回退到备选）。
	Retries int
	// Fallback 表示默认实现失败后由备选完成。
	Fallback bool
	// TimedOut 表示这次投递因超时被放弃。
	TimedOut bool
}

// Registry 是并发安全的指标收集器。
//
// 只有一把锁：临界区里是几次自增和一次桶定位，不做分配、不做 I/O。压缩到
// 这个粒度是刻意的——指标采集挂在投递路径上，任何额外开销都会摊到每条消息。
type Registry struct {
	mu        sync.Mutex
	startedAt time.Time
	now       func() time.Time
	bounds    []float64
	total     counters
	channels  map[string]*counters
	providers map[string]*counters
}

// Option 是 Registry 的构造选项。
type Option func(*Registry)

// WithBuckets 自定义延迟桶上界（毫秒）。非法值被丢弃，丢弃后为空则用默认桶。
func WithBuckets(bounds []float64) Option {
	return func(r *Registry) {
		r.bounds = normalizeBounds(bounds)
	}
}

// WithClock 注入时钟，用于测试启动时间与运行时长。
func WithClock(now func() time.Time) Option {
	return func(r *Registry) {
		if now != nil {
			r.now = now
		}
	}
}

// New 创建收集器。
func New(opts ...Option) *Registry {
	r := &Registry{
		now:       time.Now,
		bounds:    normalizeBounds(DefaultBucketBoundsMS),
		channels:  make(map[string]*counters),
		providers: make(map[string]*counters),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	r.total = *newCounters(r.bounds)
	r.startedAt = r.now()
	return r
}

// Record 记录一次投递。Registry 为 nil 时是空操作，调用方不必判空。
func (r *Registry) Record(s Sample) {
	if r == nil {
		return
	}

	channel := strings.TrimSpace(s.Channel)
	if channel == "" {
		channel = UnknownLabel
	}
	provider := strings.TrimSpace(s.Provider)
	if provider == "" {
		provider = UnknownLabel
	}
	// 负耗时只可能来自时钟回拨或测试注入，按 0 记账而不是让分位数被拉低。
	ms := float64(s.Duration) / float64(time.Millisecond)
	if ms < 0 {
		ms = 0
	}
	retries := s.Retries
	if retries < 0 {
		retries = 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.total.observe(s, ms, retries)
	r.entry(r.channels, channel).observe(s, ms, retries)
	r.entry(r.providers, provider).observe(s, ms, retries)
}

// entry 取出或新建某个标签的计数；调用方必须持锁。
func (r *Registry) entry(table map[string]*counters, label string) *counters {
	if c, ok := table[label]; ok {
		return c
	}
	c := newCounters(r.bounds)
	table[label] = c
	return c
}

// Snapshot 返回当前计数的一份拷贝。
//
// 返回的是值而不是内部指针：调用方（HTTP 处理器）序列化期间投递仍在继续，
// 拿指针等于把一把锁的持有时间交给网络。
func (r *Registry) Snapshot() Snapshot {
	if r == nil {
		return Snapshot{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.now()
	out := Snapshot{
		StartedAt:     r.startedAt,
		UptimeSeconds: round3(now.Sub(r.startedAt).Seconds()),
		Total:         r.total.snapshot("", ""),
		Channels:      make([]Stat, 0, len(r.channels)),
		Providers:     make([]Stat, 0, len(r.providers)),
	}
	for label, c := range r.channels {
		out.Channels = append(out.Channels, c.snapshot(label, ""))
	}
	for label, c := range r.providers {
		out.Providers = append(out.Providers, c.snapshot("", label))
	}
	sortStats(out.Channels)
	sortStats(out.Providers)
	return out
}

// Reset 清空计数并重新计时。
//
// 用途是「换完渠道配置，从这一刻重新看」：累计口径没法回答「改动之后有没有
// 变好」，而重启进程代价太大。
func (r *Registry) Reset() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.startedAt = r.now()
	r.total = *newCounters(r.bounds)
	r.channels = make(map[string]*counters)
	r.providers = make(map[string]*counters)
}

// counters 是一个标签下的累计量。
type counters struct {
	sent     int64
	failed   int64
	retried  int64
	fallback int64
	timeouts int64
	hist     histogram
}

func newCounters(bounds []float64) *counters {
	return &counters{hist: newHistogram(bounds)}
}

func (c *counters) observe(s Sample, ms float64, retries int) {
	if s.Status == StatusSent {
		c.sent++
	} else {
		c.failed++
	}
	c.retried += int64(retries)
	if s.Fallback {
		c.fallback++
	}
	if s.TimedOut {
		c.timeouts++
	}
	c.hist.observe(ms)
}

func (c *counters) snapshot(channel, provider string) Stat {
	attempted := c.sent + c.failed
	out := Stat{
		Channel:   channel,
		Provider:  provider,
		Sent:      c.sent,
		Failed:    c.failed,
		Attempted: attempted,
		Retried:   c.retried,
		Fallback:  c.fallback,
		Timeouts:  c.timeouts,
		Latency:   c.hist.snapshot(),
	}
	if attempted > 0 {
		out.ErrorRate = round4(float64(c.failed) / float64(attempted))
	}
	return out
}

// Snapshot 是指标的一次快照。
type Snapshot struct {
	// StartedAt 是本次统计的起点（进程启动或上一次 Reset）。
	StartedAt time.Time `json:"startedAt"`
	// UptimeSeconds 是统计窗口的秒数。
	UptimeSeconds float64 `json:"uptimeSeconds"`
	// Total 是全部渠道的合计。
	Total Stat `json:"total"`
	// Channels / Providers 是按渠道、按实现拆开的同一批计数。
	Channels  []Stat `json:"channels"`
	Providers []Stat `json:"providers"`
}

// Stat 是一个渠道（或一个实现）的投递统计。
type Stat struct {
	// Channel / Provider 二者只填一个：渠道维度与实现维度分开看。
	Channel  string `json:"channel,omitempty"`
	Provider string `json:"provider,omitempty"`
	// Sent / Failed 是成功与失败的投递次数，Attempted 是两者之和。
	Sent      int64 `json:"sent"`
	Failed    int64 `json:"failed"`
	Attempted int64 `json:"attempted"`
	// ErrorRate 是 failed / attempted，没有投递时为 0。
	ErrorRate float64 `json:"errorRate"`
	// Retried 是重试次数，Fallback 是回退到备选的次数，Timeouts 是超时次数。
	Retried  int64 `json:"retried"`
	Fallback int64 `json:"fallback"`
	Timeouts int64 `json:"timeouts"`
	// Latency 是分发阶段的耗时分布（含失败的尝试）。
	Latency Latency `json:"latency"`
}

// sortStats 按「投递量大的在前，同量按名称」排序，让接口输出稳定可读。
func sortStats(stats []Stat) {
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].Attempted != stats[j].Attempted {
			return stats[i].Attempted > stats[j].Attempted
		}
		return label(stats[i]) < label(stats[j])
	})
}

func label(s Stat) string {
	if s.Channel != "" {
		return s.Channel
	}
	return s.Provider
}

// round4 把比例收敛到 4 位小数：0.3333333 这类尾数对读数没有帮助。
func round4(v float64) float64 {
	return float64(int64(v*10000+0.5)) / 10000
}
