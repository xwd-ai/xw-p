package pipeline

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// Redis 聚合卡片存储：把窗口状态放到 Redis，让多个引擎实例共享同一张卡片。
//
// MemoryDigestStore 的计数活在进程内存里，两个引擎实例会各自开一张卡，
// 同一篇帖子的 100 次点赞被拆成两张计数 50 的卡片。本实现把「窗口」与
// 「计数」都放到 Redis，用两个原子操作消除竞态：
//
//	SET NX PX   决定谁开卡：窗口内只有一个实例能成功写入窗口键
//	HINCRBY     累加计数：无论多少实例并发，计数都不会丢
//
// 键布局（前缀默认 xw:digest）：
//
//	xw:digest:window:<scopeHash>  string 卡片身份，值形如 entryID|开卡纳秒|到期纳秒，TTL = 窗口
//	xw:digest:card:<entryID>      hash   计数、最近互动时刻、渠道与作用域字段（观测用）
//	xw:digest:actors:<entryID>    list   互动者（去重、上限 maxDigestActors）
//	xw:digest:events:<entryID>    set    已累计的事件 ID，用于重放去重，TTL = 窗口余量
//
// 窗口语义与内存实现一致，仍是固定窗口：互动不刷新窗口键的 TTL，否则热门帖子
// 会一直续期、卡片永远不结算。窗口到期后窗口键被 Redis 淘汰，下一次互动自然
// 开一张新卡；旧卡片的收件箱条目保留为历史。
const (
	// defaultDigestPrefix 是聚合卡片在 Redis 里的键前缀。
	defaultDigestPrefix = "xw:digest"
	// digestCardTTLSlack 是卡片数据在窗口结束后的保留时长：窗口键一到期就
	// 不再命中，但卡片/互动者数据多留一段时间便于排查与观测。
	digestCardTTLSlack = time.Hour
	// digestWindowAttempts 是开卡竞争的最大重试次数（正常一次即成功）。
	digestWindowAttempts = 5
)

// RedisDigestStore 是跨实例共享的聚合卡片存储。
type RedisDigestStore struct {
	client *redis.Client
	prefix string
}

// 两个存储实现都必须满足 DigestStore 接口。
var (
	_ DigestStore = (*MemoryDigestStore)(nil)
	_ DigestStore = (*RedisDigestStore)(nil)
)

// NewRedisDigestStore 使用默认键前缀创建 Redis 聚合卡片存储。
func NewRedisDigestStore(client *redis.Client) *RedisDigestStore {
	return NewRedisDigestStoreWithPrefix(client, defaultDigestPrefix)
}

// NewRedisDigestStoreWithPrefix 使用指定键前缀创建存储（多环境隔离时可用）。
func NewRedisDigestStoreWithPrefix(client *redis.Client, prefix string) *RedisDigestStore {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = defaultDigestPrefix
	}
	return &RedisDigestStore{client: client, prefix: strings.TrimRight(prefix, ":")}
}

// Prefix 返回当前键前缀。
func (s *RedisDigestStore) Prefix() string {
	if s == nil {
		return ""
	}
	return s.prefix
}

// Ping 检查 Redis 连通性。
func (s *RedisDigestStore) Ping(ctx context.Context) error {
	if s == nil || s.client == nil {
		return errors.New("Redis 聚合存储未初始化")
	}
	return s.client.Ping(ctx).Err()
}

// Close 关闭底层连接。
func (s *RedisDigestStore) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Close()
}

// digestWindow 描述一张卡片的窗口身份。
type digestWindow struct {
	entryID   string
	openedAt  time.Time
	expiresAt time.Time
}

// newDigestWindow 按「作用域 + 开卡时刻」生成一个新窗口。
//
// 条目 ID 仍由作用域与开卡时刻推导，于是同一张卡在窗口内恒得同一 ID，
// 收件箱的原地更新只依赖这个 ID，不需要额外映射表。
func newDigestWindow(scope DigestScope, now time.Time, window time.Duration) digestWindow {
	return digestWindow{
		entryID:   digestEntryID(scope, now),
		openedAt:  now,
		expiresAt: now.Add(window),
	}
}

// encodeDigestWindow 把窗口编码成紧凑字符串：entryID|开卡纳秒|到期纳秒。
//
// 值里带上时刻，是为了让「没抢到开卡」的实例读一次就能拿到窗口边界，
// 不必再回读其它键（避免读到尚未写入的空状态）。
func encodeDigestWindow(w digestWindow) string {
	return fmt.Sprintf("%s|%d|%d", w.entryID, w.openedAt.UnixNano(), w.expiresAt.UnixNano())
}

// decodeDigestWindow 解析窗口字符串。
func decodeDigestWindow(raw string) (digestWindow, error) {
	parts := strings.Split(raw, "|")
	if len(parts) != 3 || strings.TrimSpace(parts[0]) == "" {
		return digestWindow{}, fmt.Errorf("聚合窗口数据格式非法：%q", raw)
	}
	opened, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return digestWindow{}, fmt.Errorf("聚合窗口开卡时刻非法：%q", parts[1])
	}
	expires, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return digestWindow{}, fmt.Errorf("聚合窗口到期时刻非法：%q", parts[2])
	}
	return digestWindow{
		entryID:   parts[0],
		openedAt:  time.Unix(0, opened).UTC(),
		expiresAt: time.Unix(0, expires).UTC(),
	}, nil
}

// windowKey / cardKey / actorsKey 是三类键的命名。
func (s *RedisDigestStore) windowKey(scope DigestScope) string {
	return s.prefix + ":window:" + digestScopeHash(scope)
}

func (s *RedisDigestStore) cardKey(entryID string) string {
	return s.prefix + ":card:" + entryID
}

func (s *RedisDigestStore) actorsKey(entryID string) string {
	return s.prefix + ":actors:" + entryID
}

// eventsKey 是重放去重集合的键：成员为已累计的事件 ID。
func (s *RedisDigestStore) eventsKey(entryID string) string {
	return s.prefix + ":events:" + entryID
}

// digestScopeHash 把卡片作用域压成定长键片段：作用域里的帖子 ID、订阅者 ID
// 都是外部输入，不能直接把原始字符串拼进键名（长度与特殊字符都不可控）。
func digestScopeHash(scope DigestScope) string {
	sum := sha1.Sum([]byte(scope.ID()))
	return hex.EncodeToString(sum[:12])
}

// applyScript 把「重放判定 + 计数累加 + 卡片状态写入 + 互动者追加」压成一次
// Lua 调用。
//
// 分成多条命令的写法有一个真实的窗口：副本 A 的 SADD 成功后还没来得及
// HINCRBY，副本 B 的 SADD 已经返回 0（判定为重放），此时 B 回读到的计数是 0，
// 于是「重放返回卡片当前状态」这条约定被打破。脚本在 Redis 里串行执行，
// 判定与累加之间不存在其它命令插入的可能。
//
// KEYS[1] 事件去重集合 KEYS[2] 卡片哈希 KEYS[3] 互动者列表
// ARGV: 事件 ID / TTL 毫秒 / 最近互动时刻 / 聚合键 / 工作流 / 订阅者 / 步骤 /
// 到期时刻 / 渠道 / 互动者 / 互动者上限
var applyScript = redis.NewScript(`
local dedupe = ARGV[1] ~= ''
if dedupe then
  local added = redis.call('SADD', KEYS[1], ARGV[1])
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  if added == 0 then
    return {0, tonumber(redis.call('HGET', KEYS[2], 'count') or '0')}
  end
end

local count = redis.call('HINCRBY', KEYS[2], 'count', 1)
redis.call('HSET', KEYS[2],
  'last_at', ARGV[3],
  'key', ARGV[4],
  'workflow', ARGV[5],
  'subscriber', ARGV[6],
  'step', ARGV[7],
  'expires_at', ARGV[8])
if ARGV[9] ~= '' then
  redis.call('HSET', KEYS[2], 'channel', ARGV[9])
end
redis.call('PEXPIRE', KEYS[2], ARGV[2])

if ARGV[10] ~= '' then
  local existing = redis.call('LRANGE', KEYS[3], 0, -1)
  local seen = false
  for i = 1, #existing do
    if existing[i] == ARGV[10] then seen = true end
  end
  if not seen then
    redis.call('RPUSH', KEYS[3], ARGV[10])
    redis.call('LTRIM', KEYS[3], 0, tonumber(ARGV[11]) - 1)
    redis.call('PEXPIRE', KEYS[3], ARGV[2])
  end
end

return {1, count}
`)

// Apply 实现 DigestStore：在窗口内累计一次互动事件，并按事件 ID 去重。
//
// 步骤：先拿窗口（可能开卡），再由 applyScript 原子完成「判重 → 累加 → 写状态」，
// 最后回读卡片快照。同一事件被多个副本并发重投时只有一个能拿到计数，
// 其余副本读到的是已经累加过的状态。
func (s *RedisDigestStore) Apply(event DigestEvent) (DigestCard, DigestOutcome, error) {
	if err := event.validate(); err != nil {
		return DigestCard{}, DigestOutcome{}, err
	}
	if s == nil || s.client == nil {
		return DigestCard{}, DigestOutcome{}, errors.New("Redis 聚合存储未初始化")
	}

	ctx := context.Background()
	win, opened, err := s.openWindow(ctx, event.Scope, event.Now, event.Window)
	if err != nil {
		return DigestCard{}, DigestOutcome{}, err
	}

	ttl := event.Window + digestCardTTLSlack
	applied, err := applyScript.Run(ctx, s.client,
		[]string{s.eventsKey(win.entryID), s.cardKey(win.entryID), s.actorsKey(win.entryID)},
		strings.TrimSpace(event.EventID),
		ttl.Milliseconds(),
		event.Now.UTC().Format(time.RFC3339Nano),
		event.Scope.Key,
		event.Scope.Workflow,
		event.Scope.Subscriber,
		event.Scope.StepID,
		win.expiresAt.UTC().Format(time.RFC3339Nano),
		string(event.Channel),
		strings.TrimSpace(event.Actor),
		maxDigestActors,
	).Slice()
	if err != nil {
		return DigestCard{}, DigestOutcome{}, fmt.Errorf("累计聚合事件失败: %w", err)
	}
	if len(applied) != 2 {
		return DigestCard{}, DigestOutcome{}, fmt.Errorf("累计聚合事件返回结果异常：%v", applied)
	}
	first, _ := applied[0].(int64)
	replayed := first == 0

	card, err := s.readCard(ctx, win, event.Scope)
	if err != nil {
		return DigestCard{}, DigestOutcome{}, err
	}
	card.LastAt = event.Now
	return card, DigestOutcome{Opened: opened, Replayed: replayed}, nil
}

// Accumulate 实现 DigestStore：不带事件 ID 的累计（不去重）。
func (s *RedisDigestStore) Accumulate(scope DigestScope, actor string, channel domain.Channel, now time.Time, window time.Duration) (DigestCard, bool, error) {
	card, outcome, err := s.Apply(DigestEvent{
		Scope:   scope,
		Actor:   actor,
		Channel: channel,
		Now:     now,
		Window:  window,
	})
	return card, outcome.Opened, err
}

// openWindow 取得当前窗口；窗口不存在或已越过到期时刻时开一张新卡。
//
// 三条路径都是单条原子命令，多实例并发下只有一个实例能开卡：
//   - 窗口键不存在 → SET NX PX，成功者为开卡者；
//   - 窗口键存在且未到期 → 直接复用；
//   - 窗口键存在但时钟已越过到期（TTL 尚未淘汰或多实例时钟偏差）→ SET XX PX
//     原子替换，只有一个实例能替换成功，其余实例重读后命中新窗口。
func (s *RedisDigestStore) openWindow(ctx context.Context, scope DigestScope, now time.Time, window time.Duration) (digestWindow, bool, error) {
	key := s.windowKey(scope)
	var lastErr error

	for attempt := 0; attempt < digestWindowAttempts; attempt++ {
		raw, err := s.client.Get(ctx, key).Result()
		if errors.Is(err, redis.Nil) {
			candidate := newDigestWindow(scope, now, window)
			ok, err := s.client.SetNX(ctx, key, encodeDigestWindow(candidate), window).Result()
			if err != nil {
				lastErr = err
				continue
			}
			if ok {
				return candidate, true, nil
			}
			continue // 被其它实例抢先开卡，重读窗口
		}
		if err != nil {
			return digestWindow{}, false, fmt.Errorf("读取聚合窗口失败: %w", err)
		}

		win, err := decodeDigestWindow(raw)
		if err != nil {
			// 脏数据会让一张坏卡片永久挡住聚合：清掉后重试。
			if derr := s.client.Del(ctx, key).Err(); derr != nil {
				return digestWindow{}, false, fmt.Errorf("聚合窗口数据损坏且清理失败: %w", derr)
			}
			lastErr = err
			continue
		}
		if now.Before(win.expiresAt) {
			return win, false, nil
		}

		candidate := newDigestWindow(scope, now, window)
		_, err = s.client.SetArgs(ctx, key, encodeDigestWindow(candidate), redis.SetArgs{Mode: "XX", TTL: window}).Result()
		if errors.Is(err, redis.Nil) {
			continue // 键在这期间已被淘汰，回到 SETNX 分支
		}
		if err != nil {
			lastErr = err
			continue
		}
		return candidate, true, nil
	}

	if lastErr == nil {
		lastErr = errors.New("开卡竞争过于激烈，重试次数已用尽")
	}
	return digestWindow{}, false, fmt.Errorf("打开聚合窗口失败: %w", lastErr)
}

// Snapshot 实现 DigestStore：读取未过期卡片的当前状态。
//
// 窗口键缺失即视为窗口已结束——这正是「窗口过期后开新卡」的判定依据，
// 不需要额外维护一张过期表。
func (s *RedisDigestStore) Snapshot(scope DigestScope, now time.Time) (DigestCard, bool) {
	if s == nil || s.client == nil {
		return DigestCard{}, false
	}
	ctx := context.Background()

	raw, err := s.client.Get(ctx, s.windowKey(scope)).Result()
	if err != nil {
		return DigestCard{}, false
	}
	win, err := decodeDigestWindow(raw)
	if err != nil || !now.Before(win.expiresAt) {
		return DigestCard{}, false
	}
	card, err := s.readCard(ctx, win, scope)
	if err != nil {
		return DigestCard{}, false
	}
	return card, true
}

// readCard 按窗口身份回读卡片当前状态（计数、互动者、渠道、最近互动时刻）。
func (s *RedisDigestStore) readCard(ctx context.Context, win digestWindow, scope DigestScope) (DigestCard, error) {
	cardKey := s.cardKey(win.entryID)
	count, err := s.client.HGet(ctx, cardKey, "count").Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return DigestCard{}, fmt.Errorf("读取聚合计数失败: %w", err)
	}

	card := DigestCard{
		Scope:     scope,
		EntryID:   win.entryID,
		Count:     count,
		OpenedAt:  win.openedAt,
		LastAt:    win.openedAt,
		ExpiresAt: win.expiresAt,
	}
	if actors, err := s.client.LRange(ctx, s.actorsKey(win.entryID), 0, -1).Result(); err == nil {
		card.Actors = actors
	}
	if channel, err := s.client.HGet(ctx, cardKey, "channel").Result(); err == nil {
		card.Channel = domain.Channel(channel)
	}
	if lastAt, err := s.client.HGet(ctx, cardKey, "last_at").Result(); err == nil {
		if t, perr := time.Parse(time.RFC3339Nano, lastAt); perr == nil {
			card.LastAt = t
		}
	}
	return card, nil
}
