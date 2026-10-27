// Package schedule 实现「延迟续跑任务」的定时调度（ROADMAP Phase 2）。
//
// 用途：工作流中的 delay 步骤把之后的步骤推迟到到期时刻执行。管线只负责
// 算出到期时刻并交出任务（pipeline.DelayEnqueuer），本包负责按时唤醒：
//
//	Submit/EnqueueDelay → Store（按 DueAt 排序）→ Tick 取出到期项 → Handler
//
// 设计取舍：
//   - Store 是接口，默认提供内存实现（单机 demo / 测试）；生产可替换为
//     Redis ZSET（ZADD score=unix 毫秒 + ZRANGEBYSCORE）而不改上层。
//   - Tick 先「认领（Remove）再执行」，多实例并发 Tick 时同一任务只会被
//     一个实例处理，配合 At-least-once 语义已足够通知场景。
//   - Handler 失败按指数退避重新排期，超过重试上限后丢弃并记录日志（死信）。
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

// Store 是延迟任务的存储抽象。
type Store interface {
	// Add 登记任务并返回其 id（item.ID 为空时由实现生成）。
	Add(item Item) (string, error)
	// Due 返回到期时刻 <= now 的任务，按 DueAt 升序，最多 limit 条。
	Due(now time.Time, limit int) ([]Item, error)
	// Remove 认领/删除任务；不存在时返回 ErrNotFound。
	Remove(id string) error
	// Len 返回当前待执行任务数。
	Len() int
}

// MemoryStore 是进程内延迟任务存储（按到期时刻排序的切片 + 互斥锁）。
//
// 进程重启会丢失未到期任务，因此仅用于单机 demo 与测试；生产请替换为
// Redis ZSET 实现（Store 接口不变）。
type MemoryStore struct {
	mu    sync.Mutex
	items map[string]Item
	seq   atomic.Uint64
}

// NewMemoryStore 创建空的内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{items: make(map[string]Item)}
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
	s.items[id] = item
	return id, nil
}

// Due 取回到期任务，按到期时刻升序。
func (s *MemoryStore) Due(now time.Time, limit int) ([]Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []Item
	for _, item := range s.items {
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

// Remove 删除任务。
func (s *MemoryStore) Remove(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[id]; !ok {
		return ErrNotFound
	}
	delete(s.items, id)
	return nil
}

// Len 返回待执行任务数。
func (s *MemoryStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
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
func (s *Scheduler) Tick(ctx context.Context, now time.Time) (int, error) {
	if s.store == nil {
		return 0, errors.New("schedule: 调度器未初始化存储")
	}
	items, err := s.store.Due(now, s.cfg.Batch)
	if err != nil {
		return 0, err
	}

	handled := 0
	var firstErr error
	for _, item := range items {
		// 先认领再执行：Remove 失败说明已被其他实例处理，跳过即可。
		if err := s.store.Remove(item.ID); err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if s.handler == nil {
			continue
		}
		if err := s.handler(ctx, item); err != nil {
			s.retry(item, now, err)
			continue
		}
		handled++
	}
	return handled, firstErr
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
		return
	}
	item.Attempts = attempts
	item.DueAt = now.Add(backoff(s.cfg.BaseBackoff, attempts))
	if _, err := s.store.Add(item); err != nil {
		s.logger.Error("[schedule] 延迟任务重新排期失败",
			"taskId", item.ID, "error", err)
		return
	}
	s.logger.Warn("[schedule] 延迟任务执行失败，已重新排期",
		"taskId", item.ID,
		"attempts", attempts,
		"dueAt", item.DueAt,
		"error", cause,
	)
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

	s.logger.Info("[schedule] 延迟调度器已启动", "tick", tick, "pending", s.Len())
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
