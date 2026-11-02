package provider

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// State 是一条渠道实现的健康状态。
//
// 状态只有四种，含义按「还能不能投」而不是按指标划分：unknown 是还没有任何
// 观测，up 是最近一次观测成功，degraded 是最近一次失败但还没到判死的次数，
// down 是连续失败到阈值、基本可以认为这条通道当前不通。
type State string

const (
	// StateUnknown 表示还没有投递结果或探测结果可供判断。
	StateUnknown State = "unknown"
	// StateUp 表示最近一次观测成功。
	StateUp State = "up"
	// StateDegraded 表示最近一次观测失败，但连续失败次数还没到阈值。
	StateDegraded State = "degraded"
	// StateDown 表示连续失败次数达到阈值。
	StateDown State = "down"
)

const (
	// DefaultDownAfter 是连续失败多少次后判为 down。
	//
	// 取 3 而不是 1：单次失败常见于对端抖动或超时，一次就判死会让主备在
	// 网络毛刺上频繁切换；连续三次仍然失败，才值得让运维看到「这条通道
	// 现在不通」。
	DefaultDownAfter = 3
	// DefaultProbeInterval 是主动探测的默认间隔。
	DefaultProbeInterval = 30 * time.Second
	// DefaultProbeTimeout 是单次探测的默认超时。
	DefaultProbeTimeout = 3 * time.Second
)

// ErrNotProbeable 表示该实现没有实现 Prober，只能靠投递结果判断健康。
var ErrNotProbeable = errors.New("provider: 该实现不支持主动探测")

// Prober 是 Sender 的可选扩展：主动检查实现依赖的外部条件是否还可达。
//
// 探测只回答「能不能连上」这类问题，不做真实投递——每 30 秒往群里发一条
// 测试消息、或者给用户发一条短信，都不是运维检查该干的事。返回 nil 表示
// 当前可用；错误信息会被写进健康快照，因此不能带凭据。
type Prober interface {
	Probe(ctx context.Context) error
}

// Status 是一条渠道实现的健康快照，也是运维接口的返回形态。
//
// 字段分三组：注册表属性（标识/渠道/优先级/开关）、状态（状态机与计数）、
// 时间戳（最近一次观测、成功、失败、探测）。时间戳用指针是为了序列化时
// 省略「从未发生」的字段——零值时间在 JSON 里会变成 0001-01-01。
type Status struct {
	ID       string         `json:"id"`
	Channel  domain.Channel `json:"channel"`
	Priority int            `json:"priority"`
	Enabled  bool           `json:"enabled"`
	State    State          `json:"state"`

	// ConsecutiveFailures 是当前连续失败次数，成功一次即清零。
	ConsecutiveFailures int `json:"consecutive_failures"`
	// Successes / Failures 是进程启动以来的累计观测次数。
	Successes int `json:"successes"`
	Failures  int `json:"failures"`
	// LastError 是最近一次失败的原始原因（成功时为空）。
	LastError string `json:"last_error,omitempty"`

	LastObservedAt *time.Time `json:"last_observed_at,omitempty"`
	LastSuccessAt  *time.Time `json:"last_success_at,omitempty"`
	LastFailureAt  *time.Time `json:"last_failure_at,omitempty"`

	// ProbeSupported 表示该实现能否被主动探测；不支持时只能靠投递结果
	// 推断健康，运维看到的 unknown 是「还没有消息经过它」而不是「坏了」。
	ProbeSupported bool       `json:"probe_supported"`
	LastProbeAt    *time.Time `json:"last_probe_at,omitempty"`
	ProbeLatencyMS int64      `json:"probe_latency_ms,omitempty"`
	ProbeError     string     `json:"probe_error,omitempty"`
}

// Summary 是快照的汇总计数。
//
// Up/Degraded/Down/Unknown 只统计启用中的条目：停用的实现不在取用序列里，
// 把它算进「不可用」会让汇总看起来比实际糟糕。停用条数单独给出。
type Summary struct {
	Total    int `json:"total"`
	Enabled  int `json:"enabled"`
	Disabled int `json:"disabled"`
	Up       int `json:"up"`
	Degraded int `json:"degraded"`
	Down     int `json:"down"`
	Unknown  int `json:"unknown"`
}

// healthRecord 是一个实现的观测记录，只在 HealthTracker 内部使用。
type healthRecord struct {
	state        State
	consecutive  int
	successes    int
	failures     int
	lastError    string
	lastObserved time.Time
	lastSuccess  time.Time
	lastFailure  time.Time
	probeAt      time.Time
	probeLatency time.Duration
	probeError   string
}

// HealthTracker 汇总各实现的健康观测，供运维接口读取。
//
// 它只记录「发生过什么」，不参与取用决策：谁先发、失败了换谁，那是回退链
// 的事。把选路逻辑分散到两个地方，出问题时很难说清是哪一层做的判断；健康
// 状态在这里的作用是让运维看得见、并能据此手动停用某个实现。
//
// 观测来源有两处：投递结果（被动）与主动探测（Prober）。两者走同一套状态
// 转移，因此「消息发不出去」和「探测连不上」会把同一条实现推到 degraded/down。
type HealthTracker struct {
	mu       sync.Mutex
	registry *Registry
	records  map[string]*healthRecord
	// downAfter 是判为 down 的连续失败次数。
	downAfter int
	now       func() time.Time
}

// NewHealthTracker 在注册表上构造健康记录器。
//
// 注册表用于补齐渠道、优先级、开关这些属性和枚举从未观测过的实现；传 nil
// 时快照为空，观测也会被忽略。
func NewHealthTracker(registry *Registry) *HealthTracker {
	return &HealthTracker{
		registry:  registry,
		records:   make(map[string]*healthRecord),
		downAfter: DefaultDownAfter,
		now:       time.Now,
	}
}

// WithDownAfter 调整判为 down 的连续失败次数，返回自身便于链式装配。
// 非正数回退默认值；应在开始观测前调用。
func (h *HealthTracker) WithDownAfter(n int) *HealthTracker {
	if h == nil {
		return h
	}
	if n <= 0 {
		n = DefaultDownAfter
	}
	h.downAfter = n
	return h
}

// WithClock 注入时钟（测试用），返回自身。传 nil 恢复 time.Now。
func (h *HealthTracker) WithClock(now func() time.Time) *HealthTracker {
	if h == nil {
		return h
	}
	if now == nil {
		now = time.Now
	}
	h.now = now
	return h
}

// DownAfter 返回当前生效的判死阈值。
func (h *HealthTracker) DownAfter() int {
	if h == nil || h.downAfter <= 0 {
		return DefaultDownAfter
	}
	return h.downAfter
}

// Record 记录一次投递结果，返回是否命中已登记的条目。
//
// 未登记的标识直接忽略：它可能是刚刚被摘除的实现，为一条已经不在取用序列
// 里的记录维护状态没有意义，还会让快照里出现无法解释的条目。
func (h *HealthTracker) Record(id string, ok bool, reason string) bool {
	if h == nil || h.registry == nil {
		return false
	}
	if _, found := h.registry.LookupByID(id); !found {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.observeLocked(id, ok, reason, h.now())
	return true
}

// RecordProbe 记录一次主动探测结果（err 为 nil 表示可达），返回是否命中。
func (h *HealthTracker) RecordProbe(id string, err error, latency time.Duration) bool {
	if h == nil || h.registry == nil {
		return false
	}
	if _, found := h.registry.LookupByID(id); !found {
		return false
	}
	now := h.now()
	if latency < 0 {
		latency = 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.recordLocked(id)
	r.probeAt = now
	r.probeLatency = latency
	if err == nil {
		r.probeError = ""
	} else {
		r.probeError = strings.TrimSpace(err.Error())
	}
	h.observeAtLocked(r, err == nil, errorText(err), now)
	return true
}

// Reset 清除一个实现的观测记录（回到 unknown），返回是否命中。
//
// 手动重新启用某个实现时顺带重置：它可能正是因为连续失败被停用的，
// 保留旧计数会让它一恢复就是 down，直到下一次探测才翻过来。
func (h *HealthTracker) Reset(id string) bool {
	if h == nil || h.registry == nil {
		return false
	}
	if _, found := h.registry.LookupByID(id); !found {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.records, strings.TrimSpace(id))
	return true
}

// Status 返回单个实现的健康快照。
func (h *HealthTracker) Status(id string) (Status, bool) {
	if h == nil || h.registry == nil {
		return Status{}, false
	}
	entry, found := h.registry.LookupByID(id)
	if !found {
		return Status{}, false
	}
	return h.statusFor(entry), true
}

// Snapshot 返回全部登记实现的健康快照。
//
// 顺序按渠道（与 domain.AllChannels 一致）再按优先级，同一优先级保持登记
// 顺序——运维接口的输出稳定，才方便前后两次对比。
func (h *HealthTracker) Snapshot() []Status {
	if h == nil || h.registry == nil {
		return nil
	}
	entries := h.registry.Entries()
	order := make(map[domain.Channel]int, len(domain.AllChannels()))
	for i, ch := range domain.AllChannels() {
		order[ch] = i
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if order[entries[i].Channel] != order[entries[j].Channel] {
			return order[entries[i].Channel] < order[entries[j].Channel]
		}
		return entries[i].Priority < entries[j].Priority
	})

	out := make([]Status, 0, len(entries))
	for _, entry := range entries {
		out = append(out, h.statusFor(entry))
	}
	return out
}

// Summary 返回快照的汇总计数。
func (h *HealthTracker) Summary() Summary {
	statuses := h.Snapshot()
	sum := Summary{Total: len(statuses)}
	for _, s := range statuses {
		if !s.Enabled {
			sum.Disabled++
			continue
		}
		sum.Enabled++
		switch s.State {
		case StateUp:
			sum.Up++
		case StateDegraded:
			sum.Degraded++
		case StateDown:
			sum.Down++
		default:
			sum.Unknown++
		}
	}
	return sum
}

// statusFor 组装一条快照，读锁由调用方（registry 内部）保证。
func (h *HealthTracker) statusFor(entry Entry) Status {
	st := Status{
		ID:       entry.ID,
		Channel:  entry.Channel,
		Priority: entry.Priority,
		Enabled:  entry.Enabled,
		State:    StateUnknown,
	}
	if _, ok := entry.Sender.(Prober); ok {
		st.ProbeSupported = true
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.records[entry.ID]
	if !ok {
		return st
	}
	st.State = r.state
	st.ConsecutiveFailures = r.consecutive
	st.Successes = r.successes
	st.Failures = r.failures
	st.LastError = r.lastError
	st.LastObservedAt = timePtr(r.lastObserved)
	st.LastSuccessAt = timePtr(r.lastSuccess)
	st.LastFailureAt = timePtr(r.lastFailure)
	st.LastProbeAt = timePtr(r.probeAt)
	st.ProbeLatencyMS = r.probeLatency.Milliseconds()
	st.ProbeError = r.probeError
	if st.State == "" {
		st.State = StateUnknown
	}
	return st
}

// observeLocked 更新状态机，调用方必须持有写锁。
func (h *HealthTracker) observeLocked(id string, ok bool, reason string, now time.Time) {
	h.observeAtLocked(h.recordLocked(id), ok, strings.TrimSpace(reason), now)
}

// observeAtLocked 是状态转移的唯一实现，被动观测与主动探测共用。
func (h *HealthTracker) observeAtLocked(r *healthRecord, ok bool, reason string, now time.Time) {
	r.lastObserved = now
	if ok {
		r.state = StateUp
		r.consecutive = 0
		r.successes++
		r.lastSuccess = now
		r.lastError = ""
		return
	}
	r.consecutive++
	r.failures++
	r.lastFailure = now
	r.lastError = reason
	if r.consecutive >= h.DownAfter() {
		r.state = StateDown
		return
	}
	r.state = StateDegraded
}

// recordLocked 取出或新建一条记录，调用方必须持有写锁。
func (h *HealthTracker) recordLocked(id string) *healthRecord {
	r, ok := h.records[id]
	if !ok {
		r = &healthRecord{state: StateUnknown}
		h.records[id] = r
	}
	return r
}

// errorText 把探测错误转成可落盘的文本，nil 返回空串。
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// timePtr 把时间转成指针，零值返回 nil（JSON 里省略该字段）。
func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// Checker 周期性主动探测注册表里的实现，把结果写进 HealthTracker。
//
// 只探测启用中的条目：停用的实现不在取用序列里，探通了也没有意义，探不通
// 只会让运维看到一个不该关心的红灯。没有实现 Prober 的条目跳过，其健康
// 状态仍然由投递结果更新。
type Checker struct {
	registry *Registry
	health   *HealthTracker
	logger   *slog.Logger
	interval time.Duration
	timeout  time.Duration
}

// NewChecker 在注册表与健康记录器上构造探测器。
func NewChecker(registry *Registry, health *HealthTracker, logger *slog.Logger) *Checker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Checker{
		registry: registry,
		health:   health,
		logger:   logger,
		interval: DefaultProbeInterval,
		timeout:  DefaultProbeTimeout,
	}
}

// WithInterval 设置探测间隔，返回自身。非正数表示不做周期探测（Run 只跑一轮）。
func (c *Checker) WithInterval(d time.Duration) *Checker {
	if c == nil {
		return c
	}
	c.interval = d
	return c
}

// WithTimeout 设置单次探测的超时，返回自身。非正数回退默认值。
func (c *Checker) WithTimeout(d time.Duration) *Checker {
	if c == nil {
		return c
	}
	if d <= 0 {
		d = DefaultProbeTimeout
	}
	c.timeout = d
	return c
}

// Interval 返回当前探测间隔。
func (c *Checker) Interval() time.Duration {
	if c == nil {
		return DefaultProbeInterval
	}
	return c.interval
}

// Timeout 返回当前单次探测超时。
func (c *Checker) Timeout() time.Duration {
	if c == nil || c.timeout <= 0 {
		return DefaultProbeTimeout
	}
	return c.timeout
}

// Run 启动即探一轮，随后按间隔重复，ctx 取消时退出。
//
// 启动先探一轮是有意的：刚起的进程里所有实现都是 unknown，等一个间隔才有
// 第一条健康数据，运维打开接口看到的会是一片未知。
func (c *Checker) Run(ctx context.Context) {
	if c == nil || c.registry == nil {
		return
	}
	c.runRound(ctx)
	if c.Interval() <= 0 {
		return
	}
	ticker := time.NewTicker(c.Interval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.runRound(ctx)
		}
	}
}

// CheckOnce 探测一轮全部启用的实现，返回被探测条目的快照。
//
// 逐个探测而不是并发：实现数量是个位数，串行省掉了并发控制，也让每轮
// 的总耗时（条数 × 超时）可预期；探测本身有独立超时，不会拖住整轮。
func (c *Checker) CheckOnce(ctx context.Context) []Status {
	if c == nil || c.registry == nil {
		return nil
	}
	entries := c.registry.Entries()
	out := make([]Status, 0, len(entries))
	for _, entry := range entries {
		if !entry.Enabled {
			continue
		}
		if _, ok := entry.Sender.(Prober); !ok {
			continue
		}
		st, err := c.Probe(ctx, entry.ID)
		if err != nil {
			c.logger.Warn("[provider] 渠道探测失败",
				"provider", entry.ID,
				"channel", string(entry.Channel),
				"error", err,
			)
		}
		out = append(out, st)
	}
	return out
}

// Probe 立即探测一条实现并更新健康状态。
//
// 返回值里的快照即使探测失败也会带上新的状态，运维接口可以直接透出。
// 未登记的标识返回 ErrNotFound，不支持探测的实现返回 ErrNotProbeable。
func (c *Checker) Probe(ctx context.Context, id string) (Status, error) {
	if c == nil || c.registry == nil {
		return Status{}, fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	entry, ok := c.registry.LookupByID(id)
	if !ok {
		return Status{}, fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	prober, ok := entry.Sender.(Prober)
	if !ok {
		st, _ := c.health.Status(entry.ID)
		return st, fmt.Errorf("%w: %q", ErrNotProbeable, entry.ID)
	}

	if ctx == nil {
		ctx = context.Background()
	}
	probeCtx, cancel := context.WithTimeout(ctx, c.Timeout())
	defer cancel()

	start := c.now()
	err := prober.Probe(probeCtx)
	latency := c.now().Sub(start)
	c.health.RecordProbe(entry.ID, err, latency)
	st, _ := c.health.Status(entry.ID)
	if err != nil {
		return st, err
	}
	return st, nil
}

// runRound 执行一轮探测；ctx 已取消时立即返回，不再发起新的探测。
func (c *Checker) runRound(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	statuses := c.CheckOnce(ctx)
	if len(statuses) == 0 {
		return
	}
	sum := c.health.Summary()
	c.logger.Debug("[provider] 渠道健康探测完成",
		"probed", len(statuses),
		"up", sum.Up,
		"degraded", sum.Degraded,
		"down", sum.Down,
	)
}

// now 返回当前时刻，未注入时用 time.Now。
func (c *Checker) now() time.Time {
	return time.Now()
}
