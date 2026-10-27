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

	"github.com/cdgjhg/xw-p/engine/internal/domain"
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

// digestScopeHash 把卡片作用域压成定长键片段：作用域里的帖子 ID、订阅者 ID
// 都是外部输入，不能直接把原始字符串拼进键名（长度与特殊字符都不可控）。
func digestScopeHash(scope DigestScope) string {
	sum := sha1.Sum([]byte(scope.ID()))
	return hex.EncodeToString(sum[:12])
}

// Accumulate 实现 DigestStore：在窗口内累计一次互动。
//
// 步骤：先拿窗口（可能开卡）→ HINCRBY 计数 → 写卡片状态 → 追加互动者。
// 开卡与计数都是原子的，因此并发互动既不会重复开卡，也不会丢计数。
func (s *RedisDigestStore) Accumulate(scope DigestScope, actor string, channel domain.Channel, now time.Time, window time.Duration) (DigestCard, bool, error) {
	if window <= 0 {
		return DigestCard{}, false, fmt.Errorf("聚合窗口必须为正，实际为 %s", window)
	}
	if strings.TrimSpace(scope.Key) == "" {
		return DigestCard{}, false, errors.New("聚合键为空，无法聚合")
	}
	if s == nil || s.client == nil {
		return DigestCard{}, false, errors.New("Redis 聚合存储未初始化")
	}

	ctx := context.Background()
	win, opened, err := s.openWindow(ctx, scope, now, window)
	if err != nil {
		return DigestCard{}, false, err
	}

	cardKey := s.cardKey(win.entryID)
	count, err := s.client.HIncrBy(ctx, cardKey, "count", 1).Result()
	if err != nil {
		return DigestCard{}, false, fmt.Errorf("累加聚合计数失败: %w", err)
	}

	ttl := window + digestCardTTLSlack
	fields := map[string]any{
		"last_at":    now.UTC().Format(time.RFC3339Nano),
		"key":        scope.Key,
		"workflow":   scope.Workflow,
		"subscriber": scope.Subscriber,
		"step":       scope.StepID,
		"expires_at": win.expiresAt.UTC().Format(time.RFC3339Nano),
	}
	if channel != "" {
		fields["channel"] = string(channel)
	}
	if err := s.client.HSet(ctx, cardKey, fields).Err(); err != nil {
		return DigestCard{}, false, fmt.Errorf("写入聚合卡片状态失败: %w", err)
	}
	if err := s.client.Expire(ctx, cardKey, ttl).Err(); err != nil {
		return DigestCard{}, false, fmt.Errorf("设置聚合卡片过期时间失败: %w", err)
	}

	actorsKey := s.actorsKey(win.entryID)
	actor = strings.TrimSpace(actor)
	if actor != "" {
		if err := s.appendActor(ctx, actorsKey, actor, ttl); err != nil {
			return DigestCard{}, false, err
		}
	}
	actors, err := s.client.LRange(ctx, actorsKey, 0, -1).Result()
	if err != nil {
		return DigestCard{}, false, fmt.Errorf("读取聚合卡片互动者失败: %w", err)
	}

	card := DigestCard{
		Scope:     scope,
		EntryID:   win.entryID,
		Count:     int(count),
		Actors:    actors,
		Channel:   channel,
		OpenedAt:  win.openedAt,
		LastAt:    now,
		ExpiresAt: win.expiresAt,
	}
	return card, opened, nil
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

// appendActor 追加一个互动者：已存在则跳过，超出上限则不追加。
//
// 互动者只用于卡片文案展示，去重是「读-判-写」的尽力而为：极端并发下列表
// 尾部可能有重复项，但不影响计数正确性（计数只走 HINCRBY）。
func (s *RedisDigestStore) appendActor(ctx context.Context, key, actor string, ttl time.Duration) error {
	existing, err := s.client.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("读取聚合卡片互动者失败: %w", err)
	}
	for _, a := range existing {
		if a == actor {
			return nil
		}
	}
	if len(existing) >= maxDigestActors {
		return nil
	}
	if err := s.client.RPush(ctx, key, actor).Err(); err != nil {
		return fmt.Errorf("追加聚合卡片互动者失败: %w", err)
	}
	if err := s.client.LTrim(ctx, key, 0, int64(maxDigestActors-1)).Err(); err != nil {
		return fmt.Errorf("截断聚合卡片互动者失败: %w", err)
	}
	if err := s.client.Expire(ctx, key, ttl).Err(); err != nil {
		return fmt.Errorf("设置聚合卡片互动者过期时间失败: %w", err)
	}
	return nil
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

	cardKey := s.cardKey(win.entryID)
	count, err := s.client.HGet(ctx, cardKey, "count").Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return DigestCard{}, false
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
	return card, true
}
