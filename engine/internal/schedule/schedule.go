// Package schedule 实现「延迟续跑任务」的定时调度（ROADMAP Phase 2）。
//
// 用途：工作流中的 delay 步骤把之后的步骤推迟到到期时刻执行。管线只负责
// 算出到期时刻并交出任务（pipeline.DelayEnqueuer），本包负责按时唤醒：
//
//	Submit/EnqueueDelay → Store（按 DueAt 排序）→ Tick 取出到期项 → Handler
//
// 设计取舍：
//   - Store 是接口，两种实现：RedisStore（ZADD score=unix 毫秒 + ZRANGEBYSCORE，
//     跨进程共享、重启不丢，引擎默认走它）与 MemoryStore（进程内，用于测试
//     与显式关闭 Redis 的单实例场景）。上层只依赖接口，不关心具体存储。
//   - 认领带可见性租约：Tick 取到到期任务后不是直接删除，而是把它挪进「在途」
//     集合并登记一个截止时刻（Claim），执行成功才 Ack 清除。认领之后进程被杀、
//     机器掉电、实例被调度走，任务都还在存储里；下一轮 Tick（或重启后的第一个
//     进程）先做一次 Reclaim，把租约过期的任务放回时间轴重新投递。少了这层，
//     「认领」与「执行」之间的任何一次崩溃都会让一条延迟通知静默消失。
//   - Handler 失败按指数退避重新排期，超过重试上限后丢弃并记录日志（死信）；
//     Handler 内的 panic 被就地兜住算作一次失败，不拖垮整个调度循环。
package schedule

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ErrNotFound 表示按 id 未找到任务（已被其他实例认领或已过期执行）。
var ErrNotFound = errors.New("schedule: 任务不存在")

// DefaultClaimLease 是认领后的默认可见性超时。
//
// 取值要大于一条消息续跑剩余步骤的总预算（管线默认 60s），否则任务还在跑就
// 被判为「领取者已失效」，同一份通知会被重复投递。反过来，租约太长会让崩溃
// 后的任务等太久才被捡回，因此这里取两倍余量。
const DefaultClaimLease = 2 * time.Minute

// Item 是一条延迟任务：到期时刻 + 不透明的业务载荷。
type Item struct {
	// ID 是任务标识；Add 时为空则自动生成。
	ID string
	// DueAt 是到期时刻，Store 按其升序取出。
	DueAt time.Time
	// Payload 是业务载荷（本项目里是编码后的 pipeline.Deferral）。
	Payload []byte
	// Attempts 是已执行失败次数，用于退避与重试上限判断。
	Attempts int
}

// Reclaimed 描述一条被回收任务的当前状态。
type Reclaimed struct {
	// ID 是任务标识。
	ID string
	// Attempts 是回收后累计的尝试次数。
	Attempts int
}

// ReclaimResult 是一次租约回收的结果。
type ReclaimResult struct {
	// Rewound 是已放回时间轴、等待重新投递的任务（Attempts 已累加）。
	Rewound []Reclaimed
	// Abandoned 是超过尝试上限或任务体已读不出来的记录，已从存储清除。
	Abandoned []string
}

// Store 是延迟任务的存储抽象。
//
// 生命周期：Add 登记 → Due 取到期 → Claim 认领（登记租约）→ Ack 确认完成，
// 或 Requeue 带退避写回时间轴。Claim 之后没有 Ack 也没有 Requeue 的记录，
// 由 Reclaim 在租约过期后重新投递——这正是「认领后崩溃不丢任务」的落点。
type Store interface {
	// Add 登记任务并返回其 id（item.ID 为空时由实现生成）。
	Add(item Item) (string, error)
	// Due 返回到期时刻 <= now 的任务，按 DueAt 升序，最多 limit 条。
	Due(now time.Time, limit int) ([]Item, error)
	// Claim 认领任务并登记 now+lease 的可见性租约。
	// 已被其他实例认领或任务已不存在时返回 false，不视为错误。
	Claim(id string, now time.Time, lease time.Duration) (bool, error)
	// Ack 确认任务执行完成，清除任务体与租约；重复确认是幂等的。
	Ack(id string) error
	// Requeue 把任务按新的到期时刻写回时间轴并清除租约（失败退避走这里）。
	Requeue(item Item) error
	// Reclaim 回收租约已过期的任务：未超过 maxAttempts 的放回时间轴，
	// 超过的丢弃并记入 Abandoned。maxAttempts <= 0 表示不设上限。
	Reclaim(now time.Time, limit int, maxAttempts int) (ReclaimResult, error)
	// Remove 彻底删除任务（丢弃、清理损坏记录）；不存在时返回 ErrNotFound。
	Remove(id string) error
	// Len 返回当前待执行任务数（不含已认领在途的）。
	Len() int
}

// MemoryStore 是进程内延迟任务存储（按到期时刻排序的切片 + 互斥锁）。
//
// 进程重启会丢失未到期任务，因此只用于测试或显式选择单实例内存存储
// （XW_DELAY_STORE=memory）；默认部署使用 RedisStore。
type MemoryStore struct {
	mu     sync.Mutex
	items  map[string]Item
	leases map[string]time.Time // 已认领任务的租约截止时刻
	seq    atomic.Uint64
}

// NewMemoryStore 创建空的内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		items:  make(map[string]Item),
		leases: make(map[string]time.Time),
	}
}

// Add 登记任务；id 为空时自动生成。
func (s *MemoryStore) Add(item Item) (string, error) {
	if item.DueAt.IsZero() {
		return "", errors.New("schedule: 任务缺少到期时刻")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	id := item.ID
	if id == "" {
		id = fmt.Sprintf("dl_%d_%d", item.DueAt.UTC().UnixNano(), s.seq.Add(1))
	}
	item.ID = id
	item.DueAt = item.DueAt.UTC()

	// 同一 id 重复登记视为覆盖（重试用例走这里），不做去重报错：
	// 业务侧的幂等由调度器「先认领后执行」与上层幂等键保证。
	// 覆盖同时清掉租约：写回时间轴的任务不该还挂在某个实例名下。
	s.items[id] = item
	delete(s.leases, id)
	return id, nil
}

// Due 取回到期任务，按到期时刻升序；已认领在途的不返回。
func (s *MemoryStore) Due(now time.Time, limit int) ([]Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []Item
	for id, item := range s.items {
		if _, claimed := s.leases[id]; claimed {
			continue
		}
		if !item.DueAt.After(now) {
			out = append(out, item)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DueAt.Equal(out[j].DueAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].DueAt.Before(out[j].DueAt)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// Claim 认领任务并登记租约。
func (s *MemoryStore) Claim(id string, now time.Time, lease time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[id]; !ok {
		return false, nil
	}
	if _, claimed := s.leases[id]; claimed {
		return false, nil
	}
	s.leases[id] = now.Add(normalizeLease(lease))
	return true, nil
}

// Ack 确认任务完成，删除任务与租约。
func (s *MemoryStore) Ack(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, id)
	delete(s.leases, id)
	return nil
}

// Requeue 按新的到期时刻写回时间轴并清除租约。
func (s *MemoryStore) Requeue(item Item) error {
	if item.ID == "" {
		return errors.New("schedule: 重新排期缺少任务 id")
	}
	if item.DueAt.IsZero() {
		return errors.New("schedule: 任务缺少到期时刻")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item.DueAt = item.DueAt.UTC()
	s.items[item.ID] = item
	delete(s.leases, item.ID)
	return nil
}

// Reclaim 回收租约已过期的任务。
func (s *MemoryStore) Reclaim(now time.Time, limit int, maxAttempts int) (ReclaimResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]string, 0, len(s.leases))
	for id, deadline := range s.leases {
		if !deadline.After(now) {
			ids = append(ids, id)
		}
	}
	// 按 id 排序，让同一时刻到期的多条任务在测试与日志里顺序稳定。
	sort.Strings(ids)
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}

	var res ReclaimResult
	for _, id := range ids {
		delete(s.leases, id)
		item, ok := s.items[id]
		if !ok {
			res.Abandoned = append(res.Abandoned, id)
			continue
		}
		item.Attempts++
		if maxAttempts > 0 && item.Attempts >= maxAttempts {
			delete(s.items, id)
			res.Abandoned = append(res.Abandoned, id)
			continue
		}
		item.DueAt = now.UTC()
		s.items[id] = item
		res.Rewound = append(res.Rewound, Reclaimed{ID: id, Attempts: item.Attempts})
	}
	return res, nil
}

// Remove 删除任务（含租约）。
func (s *MemoryStore) Remove(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[id]; !ok {
		return ErrNotFound
	}
	delete(s.items, id)
	delete(s.leases, id)
	return nil
}

// Len 返回待执行任务数（不含已认领在途的）。
func (s *MemoryStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := 0
	for id := range s.items {
		if _, claimed := s.leases[id]; claimed {
			continue
		}
		pending++
	}
	return pending
}

// Handler 执行一条到期任务；返回 error 表示本次执行失败（将按退避重试）。
type Handler func(ctx context.Context, item Item) error

// Config 是调度器配置。
type Config struct {
	// Tick 是轮询间隔，默认 1s。
	Tick time.Duration
	// Batch 是单次 Tick 取出的任务上限，默认 32。
	Batch int
	// MaxAttempts 是单条任务的最大执行次数（含首次），默认 3。
	MaxAttempts int
	// BaseBackoff 是首次重试的退避时长，之后按 2 倍递增，上限 1 分钟；默认 5s。
	BaseBackoff time.Duration
	// ClaimLease 是认领后的可见性超时：超过该时长仍没确认的任务会被回收重投；
	// 默认 DefaultClaimLease。
	ClaimLease time.Duration
	// Now 返回当前时刻，测试可注入固定时钟；默认 time.Now。
	Now func() time.Time
}

// DefaultConfig 返回默认配置。
func DefaultConfig() Config {
	return Config{
		Tick:        time.Second,
		Batch:       32,
		MaxAttempts: 3,
		BaseBackoff: 5 * time.Second,
		ClaimLease:  DefaultClaimLease,
		Now:         time.Now,
	}
}

// Option 修改配置。
type Option func(*Config)

// WithNow 注入时钟。
func WithNow(now func() time.Time) Option {
	return func(c *Config) {
		if now != nil {
			c.Now = now
		}
	}
}

// WithTick 设置轮询间隔。
func WithTick(d time.Duration) Option {
	return func(c *Config) {
		if d > 0 {
			c.Tick = d
		}
	}
}

// WithBatch 设置单次 Tick 取出的任务上限。
func WithBatch(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.Batch = n
		}
	}
}

// WithMaxAttempts 设置最大执行次数。
func WithMaxAttempts(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.MaxAttempts = n
		}
	}
}

// WithBaseBackoff 设置首次重试退避。
func WithBaseBackoff(d time.Duration) Option {
	return func(c *Config) {
		if d > 0 {
			c.BaseBackoff = d
		}
	}
}

// WithClaimLease 设置认领后的可见性超时。
func WithClaimLease(d time.Duration) Option {
	return func(c *Config) {
		if d > 0 {
			c.ClaimLease = d
		}
	}
}

// Scheduler 按时唤醒延迟任务并交 Handler 执行。
type Scheduler struct {
	store   Store
	handler Handler
	logger  *slog.Logger
	cfg     Config
}

// New 创建调度器。store/handler 为必需项，logger 为 nil 时使用默认 logger。
func New(store Store, handler Handler, logger *slog.Logger, opts ...Option) *Scheduler {
	cfg := DefaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Scheduler{store: store, handler: handler, logger: logger, cfg: cfg}
}

// EnqueueDelay 按到期时刻登记一条编码后的载荷，返回任务 id。
// 方法签名与 pipeline.DelayEnqueuer 一致，可直接作为延迟任务的出口。
func (s *Scheduler) EnqueueDelay(dueAt time.Time, payload []byte) (string, error) {
	return s.Submit(Item{DueAt: dueAt, Payload: payload})
}

// Submit 登记一条延迟任务。
func (s *Scheduler) Submit(item Item) (string, error) {
	if s.store == nil {
		return "", errors.New("schedule: 调度器未初始化存储")
	}
	return s.store.Add(item)
}

// Len 返回待执行任务数。
func (s *Scheduler) Len() int {
	if s.store == nil {
		return 0
	}
	return s.store.Len()
}

// Tick 取出一批到期任务并执行，返回成功处理的条数（不含失败重排的）。
//
// now 由调用方给定而非内部取时间，保证测试可完全控制时间轴。
//
// 一轮的顺序是「先回收、再取到期」：回收把上一轮（或上一个进程）认领后没跑完
// 的任务放回时间轴，且到期时刻被置为 now，所以同一轮就能被重新取到——重启后
// 的第一个 Tick 会立刻补上崩溃时丢在半路的那条通知。
func (s *Scheduler) Tick(ctx context.Context, now time.Time) (int, error) {
	if s.store == nil {
		return 0, errors.New("schedule: 调度器未初始化存储")
	}
	// 回收失败不该拖住本轮投递：记下首个错误，继续处理到期任务。
	firstErr := s.reclaim(now)

	items, err := s.store.Due(now, s.cfg.Batch)
	if err != nil {
		return 0, err
	}

	handled := 0
	for _, item := range items {
		// 认领失败说明已被其他实例取走（或记录已被清理），跳过即可。
		claimed, err := s.store.Claim(item.ID, now, s.cfg.ClaimLease)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !claimed {
			continue
		}
		if s.handler == nil {
			// 没有执行器时保留旧语义：任务直接出队，不在时间轴上反复出现。
			s.ack(item)
			continue
		}
		if err := s.invoke(ctx, item); err != nil {
			s.retry(item, now, err)
			continue
		}
		s.ack(item)
		handled++
	}
	return handled, firstErr
}

// reclaim 回收租约过期的任务并记录日志，返回首个错误。
func (s *Scheduler) reclaim(now time.Time) error {
	res, err := s.store.Reclaim(now, s.cfg.Batch, s.cfg.MaxAttempts)
	if err != nil {
		s.logger.Warn("[schedule] 回收过期租约失败", "error", err)
		return err
	}
	for _, item := range res.Rewound {
		s.logger.Warn("[schedule] 延迟任务认领后未完成，已重新投递",
			"taskId", item.ID,
			"attempts", item.Attempts,
		)
	}
	for _, id := range res.Abandoned {
		s.logger.Error("[schedule] 延迟任务反复未完成，已丢弃",
			"taskId", id,
			"maxAttempts", s.cfg.MaxAttempts,
		)
	}
	return nil
}

// invoke 执行一次 Handler，并把 panic 转成普通错误。
//
// 一条畸形载荷让 Handler panic 时，整个调度循环不该跟着退出：转成错误后走
// 同一套退避重试，次数耗尽即进死信。
func (s *Scheduler) invoke(ctx context.Context, item Item) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("schedule: 延迟任务执行 panic: %v", r)
		}
	}()
	return s.handler(ctx, item)
}

// ack 确认完成；失败只记日志，租约到期后任务会被回收重投（宁可重复，不可丢）。
func (s *Scheduler) ack(item Item) {
	if err := s.store.Ack(item.ID); err != nil {
		s.logger.Error("[schedule] 确认延迟任务失败，将由租约回收重投",
			"taskId", item.ID, "error", err)
	}
}

// retry 按指数退避重新排期，超过上限则丢弃（死信）并记错误日志。
func (s *Scheduler) retry(item Item, now time.Time, cause error) {
	attempts := item.Attempts + 1
	if attempts >= s.cfg.MaxAttempts {
		s.logger.Error("[schedule] 延迟任务重试超限，丢弃",
			"taskId", item.ID,
			"attempts", attempts,
			"error", cause,
		)
		s.ack(item)
		return
	}
	item.Attempts = attempts
	item.DueAt = now.Add(backoff(s.cfg.BaseBackoff, attempts))
	if err := s.store.Requeue(item); err != nil {
		// 排期写不回去也不能让任务悬在租约里：清掉认领，等回收重投。
		s.logger.Error("[schedule] 延迟任务重新排期失败",
			"taskId", item.ID, "error", err,
			"fallback", "保留记录等待租约回收")
		return
	}
	s.logger.Warn("[schedule] 延迟任务执行失败，已重新排期",
		"taskId", item.ID,
		"attempts", attempts,
		"dueAt", item.DueAt,
		"error", cause,
	)
}

// normalizeLease 收敛租约时长：非正值回退默认，避免租约变成 0 导致任务
// 刚认领就被判为过期、同一份通知被反复重投。
func normalizeLease(lease time.Duration) time.Duration {
	if lease <= 0 {
		return DefaultClaimLease
	}
	return lease
}

// backoff 返回第 attempts 次重试的退避时长：base * 2^(attempts-1)，上限 1 分钟。
func backoff(base time.Duration, attempts int) time.Duration {
	const maxBackoff = time.Minute
	if base <= 0 {
		base = time.Second
	}
	d := base
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= maxBackoff {
			return maxBackoff
		}
	}
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

// Run 按 Tick 间隔轮询直至 ctx 取消。
func (s *Scheduler) Run(ctx context.Context) {
	tick := s.cfg.Tick
	if tick <= 0 {
		tick = time.Second
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	s.logger.Info("[schedule] 延迟调度器已启动",
		"tick", tick,
		"pending", s.Len(),
		"claimLease", normalizeLease(s.cfg.ClaimLease),
	)

	// 启动即跑一轮：上一个进程如果是在「认领之后、确认之前」退出的，它的任务
	// 正挂在过期租约里。等到第一个 tick 才处理虽然也只差一秒，但发布期间重启
	// 与轮询间隔叠在一起时，这轮主动回收能让补投更早发生。
	if ctx.Err() == nil {
		if _, err := s.Tick(ctx, s.cfg.Now()); err != nil {
			s.logger.Warn("[schedule] 启动回收失败", "error", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("[schedule] 延迟调度器已停止", "pending", s.Len())
			return
		case <-ticker.C:
			if _, err := s.Tick(ctx, s.cfg.Now()); err != nil {
				s.logger.Warn("[schedule] 遍历到期任务失败", "error", err)
			}
		}
	}
}
