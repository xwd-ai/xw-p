package pipeline

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// 摘要聚合（digest）步骤：把一连串同类互动收敛成一张卡片。
//
// 内容社区里最典型的场景是「一篇帖子被点赞 50 次」：如果每次点赞都发一条
// 站内信，创作者的收件箱会被瞬间刷爆。digest 步骤的作用就是把它们合并——
//
//	事件1（P1001 被赞）→ 开卡：「你的帖子收到了 1 条互动」
//	事件2（P1001 被赞）→ 命中窗口内的同一张卡：计数 1 → 2，卡片原地改写
//	事件3（P1001 被赞）→ 计数 2 → 3，仍然只有一条收件箱条目
//	窗口过期后再来的互动 → 重新开卡（新条目），旧卡片留在收件箱里作为历史
//
// 三个要素：
//
//   - 聚合键（config.key，默认 payload.post_id）：同一篇帖子的互动才合并；
//   - 固定窗口（config.window，默认 10 分钟）：窗口不只按时间切，还按卡片身份；
//   - 卡片身份（工作流 + 订阅者 + 步骤 + 聚合键）：四项相同才算同一张卡。
//
// 窗口是「固定窗口」而非滑动窗口：每次互动不刷新到期时刻，否则热门帖子
// 会一直续期、卡片永远不结算。

const (
	// defaultDigestKeyField 是默认聚合键字段：帖子 ID。
	defaultDigestKeyField = "payload.post_id"
	// defaultDigestActorField 是默认互动者字段：用于卡片上展示「谁赞了你」。
	defaultDigestActorField = "payload.actor"
	// defaultDigestWindow 是默认聚合窗口长度。
	defaultDigestWindow = 10 * time.Minute
	// maxDigestWindow 是窗口上限（24 小时），拦住把天当成秒之类的误配置。
	maxDigestWindow = 24 * time.Hour
	// maxDigestActors 是卡片上保留的互动者数量上限（只保留前几位用于展示）。
	maxDigestActors = 5
	// maxDigestTrackedEvents 是单张卡片记录的事件 ID 数量上限。
	// 去重集合只服务于「重放不重复计数」，超出上限后不再记录新事件
	// （内存实现），避免热门帖子把进程内存吃满。
	maxDigestTrackedEvents = 4096
)

// 卡片默认文案：工作流可通过 config.title / config.template 覆盖。
const (
	defaultDigestTitle = "互动汇总"
	defaultDigestBody  = "你的帖子 {{ digest.key }} 累计 {{ digest.count }} 条互动"
)

// DigestSpec 是一个 digest 步骤解析后的配置。
type DigestSpec struct {
	// KeyField 是聚合键的变量表达式，如 payload.post_id。
	KeyField string
	// ActorField 是互动者变量表达式；为空表示不记录互动者。
	ActorField string
	// Window 是聚合窗口长度，恒为正且不超过 maxDigestWindow。
	Window time.Duration
	// Channel 是卡片投递渠道，默认 in_app。
	Channel domain.Channel
	// Title 是卡片标题模板（可用变量）。
	Title string
	// Template 是卡片正文模板（可用 payload.* 与 digest.* 变量）。
	Template string
}

// ParseDigestSpec 解析 digest 步骤的 config。
//
// 支持的键：
//
//	{"key": "post_id", "window": "10m", "channel": "in_app",
//	 "title": "你的帖子有新互动", "template": "{{ digest.count }} 人赞了你"}
//
// key 可写裸字段名（自动补 payload. 前缀）或完整表达式；window 可为
// Go duration 字符串（"90s" / "10m"）或秒数（数字）。
func ParseDigestSpec(step domain.Step) (DigestSpec, error) {
	spec := DigestSpec{
		KeyField:   defaultDigestKeyField,
		ActorField: defaultDigestActorField,
		Window:     defaultDigestWindow,
		Channel:    domain.ChannelInApp,
		Title:      defaultDigestTitle,
		Template:   defaultDigestBody,
	}
	cfg := step.Config

	if raw, ok := cfg["key"]; ok {
		text, ok := raw.(string)
		if !ok {
			return DigestSpec{}, fmt.Errorf("digest 步骤的 key 必须是字符串，实际为 %T", raw)
		}
		field, err := normalizeDigestField(text)
		if err != nil {
			return DigestSpec{}, err
		}
		spec.KeyField = field
	}

	if raw, ok := cfg["actor"]; ok {
		text, ok := raw.(string)
		if !ok {
			return DigestSpec{}, fmt.Errorf("digest 步骤的 actor 必须是字符串，实际为 %T", raw)
		}
		if strings.TrimSpace(text) == "" {
			spec.ActorField = "" // 显式置空：不记录互动者
		} else {
			field, err := normalizeDigestField(text)
			if err != nil {
				return DigestSpec{}, err
			}
			spec.ActorField = field
		}
	}

	if raw, ok := cfg["window"]; ok {
		window, err := parseDigestWindow(raw)
		if err != nil {
			return DigestSpec{}, err
		}
		spec.Window = window
	}

	if raw, ok := cfg["channel"]; ok {
		text, ok := raw.(string)
		if !ok {
			return DigestSpec{}, fmt.Errorf("digest 步骤的 channel 必须是字符串，实际为 %T", raw)
		}
		ch, ok := domain.ParseChannel(strings.TrimSpace(text))
		if !ok || !ch.IsDelivery() {
			return DigestSpec{}, fmt.Errorf("digest 步骤的 channel 只支持投递渠道（in_app/email/sms/push/chat），实际为 %q", text)
		}
		spec.Channel = ch
	}

	if raw, ok := cfg["title"]; ok {
		text, ok := raw.(string)
		if !ok {
			return DigestSpec{}, fmt.Errorf("digest 步骤的 title 必须是字符串，实际为 %T", raw)
		}
		if strings.TrimSpace(text) != "" {
			spec.Title = text
		}
	}

	if raw, ok := cfg["template"]; ok {
		text, ok := raw.(string)
		if !ok {
			return DigestSpec{}, fmt.Errorf("digest 步骤的 template 必须是字符串，实际为 %T", raw)
		}
		if strings.TrimSpace(text) != "" {
			spec.Template = text
		}
	}

	return spec, nil
}

// normalizeDigestField 把裸字段名规范为变量表达式：无前缀时补 payload.。
func normalizeDigestField(text string) (string, error) {
	field := strings.TrimSpace(text)
	if field == "" {
		return "", errors.New("digest 步骤的聚合键字段不能为空")
	}
	if !strings.Contains(field, ".") && !strings.HasPrefix(field, "$") {
		return "payload." + field, nil
	}
	return field, nil
}

// parseDigestWindow 解析窗口长度：duration 字符串或秒数。
func parseDigestWindow(raw any) (time.Duration, error) {
	var window time.Duration
	switch v := raw.(type) {
	case string:
		text := strings.TrimSpace(v)
		// 纯数字字符串按秒处理，兼容 {"window": "600"} 这种写法。
		if secs, err := strconv.ParseFloat(text, 64); err == nil {
			window = time.Duration(secs * float64(time.Second))
		} else {
			d, err := time.ParseDuration(text)
			if err != nil {
				return 0, fmt.Errorf("digest 步骤的 window 无法解析：%q", text)
			}
			window = d
		}
	default:
		secs, err := durationCount(raw)
		if err != nil {
			return 0, fmt.Errorf("digest 步骤的 window 非法：%w", err)
		}
		window = time.Duration(secs * float64(time.Second))
	}
	if window <= 0 {
		return 0, fmt.Errorf("digest 步骤的 window 必须为正，实际为 %s", window)
	}
	if window > maxDigestWindow {
		return 0, fmt.Errorf("digest 步骤的 window 超出上限 24 小时，实际为 %s", window)
	}
	return window, nil
}

// DigestScope 标识一张聚合卡片：工作流 + 订阅者 + 步骤 + 聚合键取值。
//
// 带上工作流与步骤是刻意的：「被赞」与「被评论」的计数语义不同，
// 不应该合并进同一张卡；同一工作流里两个独立的 digest 步骤也应各自成卡。
type DigestScope struct {
	Workflow   string
	Subscriber string
	StepID     string
	Key        string
}

// ID 返回卡片的作用域标识（用于存储索引）。
func (s DigestScope) ID() string {
	return strings.Join([]string{s.Workflow, s.Subscriber, s.StepID, s.Key}, "|")
}

// DigestCard 是一张聚合卡片的当前状态。
type DigestCard struct {
	// Scope 是卡片身份。
	Scope DigestScope
	// EntryID 是卡片在收件箱里的条目 ID，窗口内保持不变（这就是「原地更新」）。
	EntryID string
	// Count 是窗口内累计的互动条数。
	Count int
	// Actors 是窗口内出现过的互动者（去重、按首次出现顺序、有上限）。
	Actors []string
	// Channel 是卡片投递渠道。
	Channel domain.Channel
	// OpenedAt 是开卡时刻。
	OpenedAt time.Time
	// LastAt 是最近一次互动的时刻。
	LastAt time.Time
	// ExpiresAt 是窗口到期时刻；到期后的互动会开新卡。
	ExpiresAt time.Time
}

// clone 返回卡片副本，避免调用方拿到内部切片后与存储互相影响。
func (c DigestCard) clone() DigestCard {
	out := c
	if len(c.Actors) > 0 {
		out.Actors = append([]string(nil), c.Actors...)
	}
	return out
}

// DigestEvent 是一次待累计的互动事件。
type DigestEvent struct {
	// Scope 是卡片身份（工作流 + 订阅者 + 步骤 + 聚合键）。
	Scope DigestScope
	// EventID 是事件的幂等键（引擎用 trigger_id）。
	// 同一张卡片内重复出现的事件 ID 视为重放，不再累加计数；为空表示不去重。
	EventID string
	// Actor 是互动者（用于卡片文案展示，可空）。
	Actor string
	// Channel 是卡片投递渠道。
	Channel domain.Channel
	// Now 是本次互动的时刻。
	Now time.Time
	// Window 是聚合窗口长度。
	Window time.Duration
}

// validate 校验事件的基本约束。
func (e DigestEvent) validate() error {
	if e.Window <= 0 {
		return fmt.Errorf("聚合窗口必须为正，实际为 %s", e.Window)
	}
	if strings.TrimSpace(e.Scope.Key) == "" {
		return errors.New("聚合键为空，无法聚合")
	}
	return nil
}

// DigestOutcome 描述一次累计的性质。
type DigestOutcome struct {
	// Opened 表示本次开了新卡（窗口内首次互动，或上一个窗口已过期）。
	Opened bool
	// Replayed 表示本次事件在窗口内已累计过（队列重投、进程重启续跑等），
	// 计数没有增加，返回的是卡片当前状态。
	Replayed bool
}

// DigestStore 是聚合卡片存储接口。
//
// 两个实现：
//   - MemoryDigestStore：进程内，单引擎实例可用，测试与本地 demo 的默认值；
//   - RedisDigestStore：跨实例共享，用 SET NX 开卡 + HINCRBY 计数保证并发正确。
//
// 接口刻意保持「一次调用完成累计并返回快照」的语义，计数与开卡判定由存储层
// 用原子操作一次做完，避免调用方分两步读改写而留下竞态窗口。
type DigestStore interface {
	// Apply 在窗口内累计一次互动事件，并按 EventID 做重放去重。
	//
	// 重放判定与累加必须是同一次原子操作：先查去重集合再累加的两步写法，
	// 在并发重投下两个副本会同时通过检查，计数照样翻倍。
	Apply(event DigestEvent) (DigestCard, DigestOutcome, error)
	// Accumulate 是不带事件 ID 的简写：不做去重，每调一次计数 +1。
	Accumulate(scope DigestScope, actor string, channel domain.Channel, now time.Time, window time.Duration) (DigestCard, bool, error)
	// Snapshot 读取卡片当前状态（不改变计数）；不存在或已过期时 ok=false。
	Snapshot(scope DigestScope, now time.Time) (DigestCard, bool)
}

// MemoryDigestStore 是进程内聚合卡片存储，线程安全。
type MemoryDigestStore struct {
	mu    sync.Mutex
	cards map[string]*DigestCard
	// events 按卡片条目 ID 记录已累计的事件 ID，用于重放去重。
	// 卡片换新（窗口过期）时旧的集合一并丢弃，不去维护跨窗口的历史。
	events map[string]map[string]struct{}
}

// NewMemoryDigestStore 创建空的内存卡片存储。
func NewMemoryDigestStore() *MemoryDigestStore {
	return &MemoryDigestStore{
		cards:  make(map[string]*DigestCard),
		events: make(map[string]map[string]struct{}),
	}
}

// Apply 实现 DigestStore：命中未过期卡片则累加，否则开新卡。
//
// 整个「重放判定 + 累加」在同一把锁内完成，同一篇帖子的并发互动不会重复开卡，
// 也不会因为队列重投把同一次点赞记两遍。
func (s *MemoryDigestStore) Apply(event DigestEvent) (DigestCard, DigestOutcome, error) {
	if err := event.validate(); err != nil {
		return DigestCard{}, DigestOutcome{}, err
	}

	id := event.Scope.ID()
	s.mu.Lock()
	defer s.mu.Unlock()

	// 命中条件：卡片存在且窗口未过期。过期即重新开卡（固定窗口语义）。
	if card, ok := s.cards[id]; ok && event.Now.Before(card.ExpiresAt) {
		if s.alreadyApplied(card.EntryID, event.EventID) {
			return card.clone(), DigestOutcome{Replayed: true}, nil
		}
		card.Count++
		card.LastAt = event.Now
		if event.Channel != "" {
			card.Channel = event.Channel
		}
		card.Actors = appendActor(card.Actors, event.Actor, maxDigestActors)
		s.trackEvent(card.EntryID, event.EventID)
		return card.clone(), DigestOutcome{}, nil
	}

	// 窗口过期：丢弃旧卡的去重集合，避免按条目 ID 记录的事件集合无限增长。
	if old, ok := s.cards[id]; ok {
		delete(s.events, old.EntryID)
	}

	opened := &DigestCard{
		Scope:     event.Scope,
		EntryID:   digestEntryID(event.Scope, event.Now),
		Count:     1,
		Channel:   event.Channel,
		OpenedAt:  event.Now,
		LastAt:    event.Now,
		ExpiresAt: event.Now.Add(event.Window),
	}
	opened.Actors = appendActor(nil, event.Actor, maxDigestActors)
	s.cards[id] = opened
	s.trackEvent(opened.EntryID, event.EventID)
	return opened.clone(), DigestOutcome{Opened: true}, nil
}

// Accumulate 实现 DigestStore：不带事件 ID 的累计（不去重）。
func (s *MemoryDigestStore) Accumulate(scope DigestScope, actor string, channel domain.Channel, now time.Time, window time.Duration) (DigestCard, bool, error) {
	card, outcome, err := s.Apply(DigestEvent{
		Scope:   scope,
		Actor:   actor,
		Channel: channel,
		Now:     now,
		Window:  window,
	})
	return card, outcome.Opened, err
}

// alreadyApplied 判断事件是否已计入该卡片；调用方需持有锁。
func (s *MemoryDigestStore) alreadyApplied(entryID, eventID string) bool {
	if strings.TrimSpace(eventID) == "" {
		return false
	}
	_, ok := s.events[entryID][eventID]
	return ok
}

// trackEvent 记录事件 ID；调用方需持有锁。
func (s *MemoryDigestStore) trackEvent(entryID, eventID string) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" || entryID == "" {
		return
	}
	seen, ok := s.events[entryID]
	if !ok {
		seen = make(map[string]struct{})
		s.events[entryID] = seen
	}
	if len(seen) >= maxDigestTrackedEvents {
		// 超上限后不再记录：重放保护降级为尽力而为，计数正确性不受影响。
		return
	}
	seen[eventID] = struct{}{}
}

// Snapshot 实现 DigestStore：返回未过期卡片的副本。
func (s *MemoryDigestStore) Snapshot(scope DigestScope, now time.Time) (DigestCard, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	card, ok := s.cards[scope.ID()]
	if !ok || !now.Before(card.ExpiresAt) {
		return DigestCard{}, false
	}
	return card.clone(), true
}

// Len 返回当前卡片数（测试与观测用）。
func (s *MemoryDigestStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.cards)
}

// appendActor 追加一个互动者：空值忽略、重复忽略、超出上限截断。
func appendActor(actors []string, actor string, limit int) []string {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		return actors
	}
	for _, a := range actors {
		if a == actor {
			return actors
		}
	}
	if len(actors) >= limit {
		return actors
	}
	return append(actors, actor)
}

// digestEntryID 生成卡片在收件箱里的条目 ID。
//
// 同一张卡（同一作用域 + 同一开卡时刻）恒得同一 ID，于是「原地更新」
// 只是覆盖写入这个 ID；窗口过期后开卡时刻变化，自然得到新 ID → 新条目。
func digestEntryID(scope DigestScope, openedAt time.Time) string {
	sum := sha1.Sum([]byte(scope.ID() + "|" + openedAt.UTC().Format(time.RFC3339Nano)))
	return "digest-" + hex.EncodeToString(sum[:8])
}

// handleDigest 执行一个 digest 步骤：过滤 → 变量上下文 → 聚合 → 渲染 → 落卡。
func (p *Pipeline) handleDigest(ctx *Context, step domain.Step) StepResult {
	res := StepResult{
		StepID:   step.ID,
		StepType: domain.StepTypeDigest,
		Channel:  domain.ChannelDigest,
	}

	// 过滤条件（可选）：不满足条件时不参与聚合，也不产生卡片。
	if p.filter != nil {
		if err := p.filter.Apply(ctx, step); err != nil {
			if errors.Is(err, errSkipStep) {
				return ctx.Results[step.ID]
			}
			res.Error = err.Error()
			ctx.Results[step.ID] = res
			return res
		}
	}

	// 变量上下文：聚合键与模板都要读 payload / 系统变量。
	if p.transform != nil {
		if err := p.transform.Apply(ctx, step); err != nil {
			res.Error = err.Error()
			ctx.Results[step.ID] = res
			return res
		}
	}

	spec, err := ParseDigestSpec(step)
	if err != nil {
		res.Error = err.Error()
		ctx.Results[step.ID] = res
		return res
	}
	res.Channel = spec.Channel

	// 聚合键：缺失即失败（聚合没有键就没有意义，不能静默丢弃互动）。
	key := p.resolveDigestField(ctx, spec.KeyField)
	if key == "" {
		res.Error = fmt.Sprintf("digest 步骤缺少聚合键 %s（payload 未提供该字段）", spec.KeyField)
		ctx.Results[step.ID] = res
		return res
	}

	now := time.Now
	if p.now != nil {
		now = p.now
	}
	at := now()

	scope := DigestScope{
		Workflow:   workflowName(ctx),
		Subscriber: strings.TrimSpace(ctx.Msg.SubscriberID),
		StepID:     step.ID,
		Key:        key,
	}
	if scope.Subscriber == "" {
		res.Error = "digest 步骤缺少订阅者 ID"
		ctx.Results[step.ID] = res
		return res
	}

	store := p.digests
	if store == nil {
		res.Error = "digest 步骤未配置聚合存储（pipeline.WithDigestStore）"
		ctx.Results[step.ID] = res
		return res
	}

	card, outcome, err := store.Apply(DigestEvent{
		Scope:   scope,
		EventID: ctx.Msg.TriggerID,
		Actor:   p.resolveDigestField(ctx, spec.ActorField),
		Channel: spec.Channel,
		Now:     at,
		Window:  spec.Window,
	})
	if err != nil {
		res.Error = "聚合失败: " + err.Error()
		ctx.Results[step.ID] = res
		return res
	}

	// 重放（队列重投、崩溃续跑）：这次互动在窗口内已经计过数，只把当前累计
	// 结果回填给上游，不重复投递——重复发一条邮件比少发一条卡片糟得多。
	// 代价是首次执行若在落卡前崩溃，这条卡片要等下一条互动或下一个窗口补上；
	// 收件箱卡片本身按条目 ID 幂等写入，不依赖这条路径自愈。
	if outcome.Replayed {
		res.AggregateKey = card.Scope.Key
		res.AggregateCount = card.Count
		res.AggregateReplayed = true
		res.AggregateExpiresAt = card.ExpiresAt
		ctx.Results[step.ID] = res
		p.logger.Info("[digest] 事件已计入当前卡片，重放跳过重复累计与投递",
			"workflow", scope.Workflow,
			"subscriberId", scope.Subscriber,
			"stepId", step.ID,
			"key", key,
			"count", card.Count,
			"eventId", ctx.Msg.TriggerID,
			"entryId", card.EntryID,
		)
		return res
	}

	// 把聚合结果注入变量表，卡片文案即可使用 {{ digest.count }} 等变量。
	ctx.Variables["digest.key"] = card.Scope.Key
	ctx.Variables["digest.count"] = card.Count
	ctx.Variables["digest.actor"] = lastActor(card.Actors)
	ctx.Variables["digest.actors"] = strings.Join(card.Actors, "、")
	ctx.Variables["digest.opened"] = outcome.Opened

	title, err := p.renderInline(ctx, spec.Title)
	if err != nil {
		res.Error = "渲染聚合卡片标题失败: " + err.Error()
		ctx.Results[step.ID] = res
		return res
	}
	body, err := p.renderText(ctx, spec.Template)
	if err != nil {
		res.Error = "渲染聚合卡片正文失败: " + err.Error()
		ctx.Results[step.ID] = res
		return res
	}
	if strings.TrimSpace(body) == "" {
		res.Error = "聚合卡片正文为空"
		ctx.Results[step.ID] = res
		return res
	}

	res.Rendered = body
	res.AggregateKey = key
	res.AggregateCount = card.Count
	res.AggregateOpened = outcome.Opened
	res.AggregateExpiresAt = card.ExpiresAt

	deliverStarted := time.Now()
	sent, delivery, deliverErr := p.deliverDigest(ctx, step, spec, card, outcome.Opened, title, body)
	res.Duration = time.Since(deliverStarted)
	if deliverErr != nil {
		res.Error = deliverErr.Error()
		ctx.Results[step.ID] = res
		return res
	}
	// sent 表示本次是否真的投递出去：in_app 卡片总是写/改写成功即 true；
	// 其他渠道窗口内命中既有卡片时不重复投递，为 false。
	res.Sent = sent
	res.UsedProvider = delivery.Provider.ID
	res.ProviderAttempts = delivery.Attempted()
	res.ProviderRetries = delivery.Retries()
	res.ProviderFallback = delivery.FellBack()
	ctx.Results[step.ID] = res

	p.logger.Info("[digest] 聚合卡片已更新",
		"workflow", scope.Workflow,
		"subscriberId", scope.Subscriber,
		"stepId", step.ID,
		"key", key,
		"count", card.Count,
		"opened", outcome.Opened,
		"entryId", card.EntryID,
		"channel", string(spec.Channel),
	)
	return res
}

// deliverDigest 把聚合卡片落库（in_app）或投递给其他渠道。
//
// in_app：首次开卡写入收件箱，命中已有卡片则覆盖同 ID 条目（原地累加）；
// 其他渠道：只在开卡时投递一次，窗口内的后续互动仅累计计数——邮件/短信
// 这类渠道没有「原地改写」能力，窗口结束统一投递属于后续的定时冲刷能力。
// deliverDigest 投递一张聚合卡片，返回是否真的发出去了、渠道链的尝试记录与错误。
func (p *Pipeline) deliverDigest(ctx *Context, step domain.Step, spec DigestSpec, card DigestCard, opened bool, title, body string) (bool, Outcome, error) {
	if spec.Channel == domain.ChannelInApp {
		store, ok := p.inboxStore()
		if !ok {
			return false, Outcome{}, errors.New("未注册 in_app provider，无法写入聚合卡片")
		}
		entry := InboxEntry{
			ID:           card.EntryID,
			TriggerID:    ctx.Msg.TriggerID,
			SubscriberID: card.Scope.Subscriber,
			Workflow:     card.Scope.Workflow,
			StepID:       step.ID,
			Title:        title,
			Body:         body,
			// 开卡时刻固定不变，窗口内的互动只推进 UpdatedAt：收件箱按更新时间
			// 排序能把被顶起来的卡片排到前面，同时保留「这张卡是什么时候开的」。
			CreatedAt: card.OpenedAt.UTC(),
			UpdatedAt: card.LastAt.UTC(),
			// 新的互动让卡片重新回到未读：API 侧用「已读时刻 vs 更新时间」
			// 判定未读，这里写入的 Read 只是引擎侧快照。
			Read: false,
		}
		if opened {
			if _, err := store.Save(entry); err != nil {
				return false, Outcome{}, fmt.Errorf("写入聚合卡片失败: %w", err)
			}
			return true, Outcome{}, nil
		}
		updated, err := store.Update(entry)
		if err != nil {
			return false, Outcome{}, fmt.Errorf("更新聚合卡片失败: %w", err)
		}
		if !updated {
			// 卡片已不在收件箱（文件被清理等）：按新条目落库，别把互动弄丢。
			if _, err := store.Save(entry); err != nil {
				return false, Outcome{}, fmt.Errorf("补写聚合卡片失败: %w", err)
			}
		}
		return true, Outcome{}, nil
	}

	// 其他渠道：只有开卡时投递，命中窗口内的既有卡片不重复发送。
	if !opened {
		p.logger.Info("[digest] 窗口内命中已有聚合卡片，跳过重复投递",
			"workflow", card.Scope.Workflow,
			"key", card.Scope.Key,
			"count", card.Count,
			"channel", string(spec.Channel),
		)
		return false, Outcome{}, nil
	}
	// 与普通步骤走同一条投递链：按渠道策略重试、失败时按优先级回退，
	// 卡片和普通消息在渠道层不该有两套行为。
	outcome := p.dispatch.failover.Send(ctx.Context(), Delivery{
		Channel:      spec.Channel,
		To:           deliveryRecipient(ctx, step, spec.Channel),
		Subject:      sanitizeSingleLine(title),
		Body:         sanitizeMultiLine(body),
		StepID:       step.ID,
		TriggerID:    ctx.Msg.TriggerID,
		WorkflowName: workflowName(ctx),
		SubscriberID: ctx.Msg.SubscriberID,
	})
	if outcome.Candidates == 0 {
		return false, outcome, fmt.Errorf("渠道 %s 未注册 provider，无法投递聚合卡片", spec.Channel)
	}
	if !outcome.Sent {
		return false, outcome, errors.New("投递聚合卡片失败: " + outcome.Result.Reason)
	}
	return true, outcome, nil
}

// inboxStore 从 in_app provider 取回收件箱存储，保证「写收件箱」与
// 「改写聚合计卡片」落在同一份存储上（否则图表写入与改写会各写各的）。
func (p *Pipeline) inboxStore() (InboxStore, bool) {
	if p.registry == nil {
		return nil, false
	}
	provider, ok := p.registry.Lookup(domain.ChannelInApp)
	if !ok {
		return nil, false
	}
	holder, ok := provider.(InboxStoreProvider)
	if !ok {
		return nil, false
	}
	store := holder.InboxStore()
	return store, store != nil
}

// resolveDigestField 解析一个变量表达式为去空白的单行字符串（缺失返回空串）。
//
// 聚合键与互动者都是标识（帖子 ID、昵称），按单行文本清理：它们会进卡片
// 作用域与日志，带换行的键会在日志里造出半行假记录。
func (p *Pipeline) resolveDigestField(ctx *Context, field string) string {
	if field == "" || p.renderer == nil {
		return ""
	}
	v, ok := p.renderer.vs.resolve(ctx, field)
	if !ok || v == nil {
		return ""
	}
	return sanitizeSingleLine(stringify(v))
}

// renderText 用当前变量表渲染一段多行模板（聚合卡片正文）。
func (p *Pipeline) renderText(ctx *Context, tmpl string) (string, error) {
	if p.renderer == nil {
		return sanitizeMultiLine(tmpl), nil
	}
	return p.renderer.Render(ctx, tmpl)
}

// renderInline 用当前变量表渲染一段单行模板（聚合卡片标题）。
func (p *Pipeline) renderInline(ctx *Context, tmpl string) (string, error) {
	if p.renderer == nil {
		return sanitizeSingleLine(tmpl), nil
	}
	return p.renderer.RenderInline(ctx, tmpl)
}

// lastActor 返回最近出现的互动者（互动者列表为空时返回空串）。
func lastActor(actors []string) string {
	if len(actors) == 0 {
		return ""
	}
	return actors[len(actors)-1]
}
