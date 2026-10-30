package schedule

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis 延迟任务存储：把未到期的 delay 任务放进 ZSET，进程重启不会丢。
//
// MemoryStore 的任务活在进程内存里，引擎一重启（发布、OOM、机器漂移）
// 所有未到期的 delay 步骤就没了——用户看到的是一条「以后再说」的通知永远
// 不来，日志里却什么都没有。本实现把任务与到期时刻都放到 Redis：
//
//	ZADD <prefix>:due <unix 毫秒> <id>       到期时间轴，取到期项走 ZRANGEBYSCORE
//	HSET <prefix>:items <id> <json>          任务体（载荷、已重试次数）
//	ZADD <prefix>:inflight <认领截止毫秒> <id> 已认领、尚未确认的任务
//
// 认领（Claim）把 id 从时间轴挪到在途集合，租约到期而没确认的记录由 Reclaim
// 挪回时间轴重新投递——进程在「认领之后、确认之前」被杀，任务依然在 Redis 上，
// 只是晚了最多一个租约周期才被捡回。有了在途集合，Ack 与 Requeue 才有明确的
// 清理对象：确认删干净，重排则连同新的到期时刻一起写回时间轴。
//
// 三个键的每次改动都在同一个 Lua 脚本里完成，因此不存在「时间轴上有 id、
// 任务体却读不到」的中间状态：登记是 HSET + ZADD 一条脚本，认领是 ZREM +
// ZADD 一条脚本，取出到期任务是 ZRANGEBYSCORE + HGET 一条脚本（取到的是
// 一个时刻的完整快照，不会被别的实例的写入切成两半），回收是「改成新任务体 +
// 写回时间轴 + 清在途」一条脚本（并发回收同一 id 也只有一次能删掉在途记录）。
//
// 到期时刻的精度是毫秒：delay 步骤的窗口以分钟计，毫秒级截断不影响语义，
// 换来的是 ZSET 分数可以直接比较、也便于用 redis-cli 排查。
const (
	// DefaultRedisPrefix 是延迟任务在 Redis 里的默认键前缀，用于环境隔离。
	DefaultRedisPrefix = "xw:delay"
	// redisPoolTimeout 是单次 Redis 操作的超时上限。调度器每秒扫描一次，
	// 一次卡住不该把整条 Tick 拖到下一轮之后。
	redisPoolTimeout = 5 * time.Second
)

// 三个脚本共用的键位约定：KEYS[1] = 到期时间轴，KEYS[2] = 任务体哈希，
// KEYS[3] = 在途集合（分数为认领截止时刻）。
const (
	// addScript 登记任务：先写任务体再写时间轴，两条命令同一脚本内完成。
	// 顺带清掉在途记录，让「重排」与「登记」共用同一条脚本：同一 id 不
	// 可能同时挂在时间轴和某个实例的租约下。
	addScript = `
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
return 1
`
	// dueScript 取出到期任务：返回扁平数组 [id1, 任务体1, id2, 任务体2, ...]。
	// 极限情况下时间轴上有 id 而任务体缺失（外部误删），这类记录直接跳过，
	// 不进返回值，避免把一条读不出载荷的任务交给上层。
	dueScript = `
local limit = tonumber(ARGV[2])
local ids
if limit and limit > 0 then
  ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, limit)
else
  ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
end
local out = {}
for i = 1, #ids do
  local raw = redis.call('HGET', KEYS[2], ids[i])
  if raw then
    out[#out + 1] = ids[i]
    out[#out + 1] = raw
  end
end
return out
`
	// claimScript 认领任务并登记租约：ZREM 的返回值决定这次认领是否成功，
	// 并发认领同一 id 时只有一个实例拿到 1。返回 -1 表示时间轴上有 id 但
	// 任务体已不存在，这条记录当场清掉，不算认领成功。
	claimScript = `
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 0 then
  return 0
end
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 0 then
  redis.call('ZREM', KEYS[3], ARGV[1])
  return -1
end
redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])
return 1
`
	// ackScript 确认完成：清在途记录与任务体。重复确认没有副作用，
	// 确认晚到（任务已被回收重投）也不会误删别人的在途记录之外的东西。
	ackScript = `
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
`
	// reclaimScript 回收过期租约：取在途集合里截止时刻 <= now 的记录，逐条
	// 累加尝试次数后放回时间轴（到期时刻置为 now，本轮即可再被取到）。
	// 超过 maxAttempts 或任务体读不出来的记录直接丢弃。
	// 返回扁平数组 [状态, id, 次数, 状态, id, 次数, ...]，状态为 r/a。
	reclaimScript = `
local limit = tonumber(ARGV[2])
local max = tonumber(ARGV[3])
local ids
if limit and limit > 0 then
  ids = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', ARGV[1], 'LIMIT', 0, limit)
else
  ids = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', ARGV[1])
end
local out = {}
for i = 1, #ids do
  local id = ids[i]
  redis.call('ZREM', KEYS[3], id)
  local raw = redis.call('HGET', KEYS[2], id)
  local ok, item = false, nil
  if raw then
    ok, item = pcall(cjson.decode, raw)
  end
  if not ok or type(item) ~= 'table' then
    redis.call('HDEL', KEYS[2], id)
    out[#out + 1] = 'a'
    out[#out + 1] = id
    out[#out + 1] = 0
  else
    local attempts = tonumber(item['attempts']) or 0
    attempts = attempts + 1
    if max and max > 0 and attempts >= max then
      redis.call('HDEL', KEYS[2], id)
      out[#out + 1] = 'a'
      out[#out + 1] = id
      out[#out + 1] = attempts
    else
      item['attempts'] = attempts
      item['due_at_ms'] = tonumber(ARGV[1])
      redis.call('HSET', KEYS[2], id, cjson.encode(item))
      redis.call('ZADD', KEYS[1], ARGV[1], id)
      out[#out + 1] = 'r'
      out[#out + 1] = id
      out[#out + 1] = attempts
    end
  end
end
return out
`
	// removeScript 彻底删除任务：时间轴与在途集合都清，任一命中即为找到。
	removeScript = `
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
local claimed = redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return removed + claimed
`
)

// RedisStore 是基于 Redis ZSET 的延迟任务存储，可跨进程、跨实例共享。
type RedisStore struct {
	client *redis.Client
	prefix string
	logger *slog.Logger
	seq    atomic.Uint64
}

// 两个存储实现都必须满足 Store 接口。
var (
	_ Store = (*MemoryStore)(nil)
	_ Store = (*RedisStore)(nil)
)

// NewRedisStore 使用默认键前缀创建 Redis 延迟任务存储。
func NewRedisStore(client *redis.Client) *RedisStore {
	return NewRedisStoreWithPrefix(client, DefaultRedisPrefix)
}

// NewRedisStoreWithPrefix 使用指定键前缀创建 Redis 延迟任务存储；
// 前缀为空时回退默认值，避免键名退化成 ":" 开头的裸键。
func NewRedisStoreWithPrefix(client *redis.Client, prefix string) *RedisStore {
	if prefix == "" {
		prefix = DefaultRedisPrefix
	}
	return &RedisStore{client: client, prefix: prefix, logger: slog.Default()}
}

// Prefix 返回生效的键前缀。
func (s *RedisStore) Prefix() string { return s.prefix }

// WithLogger 替换日志实现，便于测试静默或接入引擎的 logger。
func (s *RedisStore) WithLogger(logger *slog.Logger) *RedisStore {
	if logger != nil {
		s.logger = logger
	}
	return s
}

// DueKey 返回到期时间轴的键名（观测与运维用）。
func (s *RedisStore) DueKey() string { return s.prefix + ":due" }

// ItemsKey 返回任务体哈希的键名（观测与运维用）。
func (s *RedisStore) ItemsKey() string { return s.prefix + ":items" }

// InflightKey 返回在途集合的键名（观测与运维用）。
func (s *RedisStore) InflightKey() string { return s.prefix + ":inflight" }

// Add 登记任务；item.ID 为空时按到期时刻与随机后缀生成。
func (s *RedisStore) Add(item Item) (string, error) {
	if item.DueAt.IsZero() {
		return "", errors.New("schedule: 任务缺少到期时刻")
	}
	if s.client == nil {
		return "", errors.New("schedule: Redis 存储未初始化连接")
	}

	id := item.ID
	if id == "" {
		var err error
		if id, err = s.newID(item.DueAt); err != nil {
			return "", err
		}
	}
	item.ID = id
	item.DueAt = item.DueAt.UTC()

	raw, err := encodeItem(item)
	if err != nil {
		return "", err
	}

	ctx, cancel := s.timeout()
	defer cancel()
	_, err = s.client.Eval(ctx, addScript, s.keys(),
		id, strconv.FormatInt(item.DueAt.UnixMilli(), 10), raw).Result()
	if err != nil {
		return "", fmt.Errorf("schedule: 登记延迟任务失败: %w", err)
	}
	return id, nil
}

// Due 返回到期任务，按到期时刻升序。
func (s *RedisStore) Due(now time.Time, limit int) ([]Item, error) {
	if s.client == nil {
		return nil, errors.New("schedule: Redis 存储未初始化连接")
	}
	ctx, cancel := s.timeout()
	defer cancel()

	res, err := s.client.Eval(ctx, dueScript, s.keys(),
		strconv.FormatInt(now.UTC().UnixMilli(), 10), strconv.Itoa(limit)).Result()
	if err != nil {
		return nil, fmt.Errorf("schedule: 读取到期任务失败: %w", err)
	}
	pairs, ok := res.([]any)
	if !ok {
		return nil, fmt.Errorf("schedule: 到期任务返回值格式异常: %T", res)
	}

	items := make([]Item, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		id, okID := pairs[i].(string)
		raw, okRaw := pairs[i+1].(string)
		if !okID || !okRaw {
			continue
		}
		item, err := decodeItem(raw)
		if err != nil {
			// 任务体解不出来就没法执行，留着只会每轮都被读一次。直接清掉，
			// 并把 id 记进日志，方便对账时知道少执行了哪一条。
			s.logger.Error("[schedule] 延迟任务体解析失败，已丢弃",
				"taskId", id, "error", err)
			s.drop(id)
			continue
		}
		if item.ID == "" {
			item.ID = id
		}
		items = append(items, item)
	}
	return items, nil
}

// Claim 认领任务并登记 now+lease 的可见性租约。
//
// 返回 false 的两种情况都不算错误：id 已被其他实例认领（ZREM 拿到 0），
// 或时间轴上有 id 而任务体已不存在（脚本顺手清掉那条记录）。
func (s *RedisStore) Claim(id string, now time.Time, lease time.Duration) (bool, error) {
	if s.client == nil {
		return false, errors.New("schedule: Redis 存储未初始化连接")
	}
	deadline := now.Add(normalizeLease(lease)).UTC()
	ctx, cancel := s.timeout()
	defer cancel()

	n, err := s.client.Eval(ctx, claimScript, s.keys(),
		id, strconv.FormatInt(deadline.UnixMilli(), 10)).Int64()
	if err != nil {
		return false, fmt.Errorf("schedule: 认领延迟任务失败: %w", err)
	}
	return n == 1, nil
}

// Ack 确认任务完成，清除任务体与在途记录；重复确认是幂等的。
func (s *RedisStore) Ack(id string) error {
	if s.client == nil {
		return errors.New("schedule: Redis 存储未初始化连接")
	}
	ctx, cancel := s.timeout()
	defer cancel()

	if _, err := s.client.Eval(ctx, ackScript, s.keys(), id).Result(); err != nil {
		return fmt.Errorf("schedule: 确认延迟任务失败: %w", err)
	}
	return nil
}

// Requeue 把任务按新的到期时刻写回时间轴，并清除在途记录。
func (s *RedisStore) Requeue(item Item) error {
	if item.ID == "" {
		return errors.New("schedule: 重新排期缺少任务 id")
	}
	if item.DueAt.IsZero() {
		return errors.New("schedule: 任务缺少到期时刻")
	}
	if s.client == nil {
		return errors.New("schedule: Redis 存储未初始化连接")
	}
	item.DueAt = item.DueAt.UTC()
	raw, err := encodeItem(item)
	if err != nil {
		return err
	}

	ctx, cancel := s.timeout()
	defer cancel()
	if _, err := s.client.Eval(ctx, addScript, s.keys(),
		item.ID, strconv.FormatInt(item.DueAt.UnixMilli(), 10), raw).Result(); err != nil {
		return fmt.Errorf("schedule: 重新排期延迟任务失败: %w", err)
	}
	return nil
}

// Reclaim 回收租约已过期的任务：未超过 maxAttempts 的放回时间轴并累加次数，
// 超过的丢弃。并发回收同一批记录时，只有先 ZREM 成功的那一方会处理它。
func (s *RedisStore) Reclaim(now time.Time, limit int, maxAttempts int) (ReclaimResult, error) {
	if s.client == nil {
		return ReclaimResult{}, errors.New("schedule: Redis 存储未初始化连接")
	}
	ctx, cancel := s.timeout()
	defer cancel()

	res, err := s.client.Eval(ctx, reclaimScript, s.keys(),
		strconv.FormatInt(now.UTC().UnixMilli(), 10),
		strconv.Itoa(limit),
		strconv.Itoa(maxAttempts),
	).Result()
	if err != nil {
		return ReclaimResult{}, fmt.Errorf("schedule: 回收过期延迟任务失败: %w", err)
	}
	flat, ok := res.([]any)
	if !ok {
		return ReclaimResult{}, fmt.Errorf("schedule: 回收结果格式异常: %T", res)
	}

	var out ReclaimResult
	for i := 0; i+2 < len(flat); i += 3 {
		status, okStatus := flat[i].(string)
		id, okID := flat[i+1].(string)
		attempts, _ := flat[i+2].(int64)
		if !okStatus || !okID {
			continue
		}
		switch status {
		case "r":
			out.Rewound = append(out.Rewound, Reclaimed{ID: id, Attempts: int(attempts)})
		case "a":
			out.Abandoned = append(out.Abandoned, id)
		}
	}
	return out, nil
}

// Remove 彻底删除任务（丢弃或清理损坏记录）；时间轴与在途集合都算命中。
func (s *RedisStore) Remove(id string) error {
	if s.client == nil {
		return errors.New("schedule: Redis 存储未初始化连接")
	}
	ctx, cancel := s.timeout()
	defer cancel()

	n, err := s.client.Eval(ctx, removeScript, s.keys(), id).Int64()
	if err != nil {
		return fmt.Errorf("schedule: 删除延迟任务失败: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// Len 返回待执行任务数；读取失败时返回 0（仅用于日志与观测）。
func (s *RedisStore) Len() int {
	if s.client == nil {
		return 0
	}
	ctx, cancel := s.timeout()
	defer cancel()
	n, err := s.client.ZCard(ctx, s.DueKey()).Result()
	if err != nil {
		s.logger.Warn("[schedule] 读取延迟任务数失败", "error", err)
		return 0
	}
	return int(n)
}

// Ping 检查 Redis 是否可用，供启动时选择存储实现。
func (s *RedisStore) Ping(ctx context.Context) error {
	if s.client == nil {
		return errors.New("schedule: Redis 存储未初始化连接")
	}
	return s.client.Ping(ctx).Err()
}

// drop 清理一条读不出载荷的记录。
func (s *RedisStore) drop(id string) {
	ctx, cancel := s.timeout()
	defer cancel()
	if _, err := s.client.Eval(ctx, removeScript, s.keys(), id).Result(); err != nil {
		s.logger.Warn("[schedule] 清理损坏的延迟任务失败", "taskId", id, "error", err)
	}
}

// keys 返回脚本统一使用的三个键：时间轴、任务体、在途集合。
func (s *RedisStore) keys() []string {
	return []string{s.DueKey(), s.ItemsKey(), s.InflightKey()}
}

func (s *RedisStore) timeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), redisPoolTimeout)
}

// newID 生成任务 id：到期时刻（纳秒）+ 随机后缀。
//
// 不带进程号或实例标识，是为了让 id 在多实例下也可比较、可排序；
// 随机后缀保证同一毫秒内多条任务的 id 不撞。
func (s *RedisStore) newID(dueAt time.Time) (string, error) {
	buf := make([]byte, 6)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("schedule: 生成任务 id 失败: %w", err)
	}
	return fmt.Sprintf("dl_%d_%s", dueAt.UTC().UnixNano(), hex.EncodeToString(buf)), nil
}

// wireItem 是任务在 Redis 里的序列化形态。字段名带单位，避免后面
// 有人把秒当毫秒读；载荷走 JSON 的 base64 编码，二进制也安全。
type wireItem struct {
	ID       string `json:"id"`
	DueAtMS  int64  `json:"due_at_ms"`
	Payload  []byte `json:"payload"`
	Attempts int    `json:"attempts"`
}

func encodeItem(item Item) (string, error) {
	raw, err := json.Marshal(wireItem{
		ID:       item.ID,
		DueAtMS:  item.DueAt.UTC().UnixMilli(),
		Payload:  item.Payload,
		Attempts: item.Attempts,
	})
	if err != nil {
		return "", fmt.Errorf("schedule: 序列化延迟任务失败: %w", err)
	}
	return string(raw), nil
}

func decodeItem(raw string) (Item, error) {
	var wire wireItem
	if err := json.Unmarshal([]byte(raw), &wire); err != nil {
		return Item{}, fmt.Errorf("schedule: 反序列化延迟任务失败: %w", err)
	}
	if wire.DueAtMS <= 0 {
		return Item{}, errors.New("schedule: 延迟任务缺少到期时刻")
	}
	return Item{
		ID:       wire.ID,
		DueAt:    time.UnixMilli(wire.DueAtMS).UTC(),
		Payload:  wire.Payload,
		Attempts: wire.Attempts,
	}, nil
}
