package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

// 失败重试的默认参数。
//
// 选择这几组数字的依据：基础延迟要短到「下游抖一下」的重试几乎无感，
// 上限要长到「下游真的挂了」时不会把队列刷成一堆重试；最大次数取 5，
// 配合 2s 起步约 30 秒内跑完一轮，比「等租约到期再投递」（默认 60s 一轮、
// 且没有次数上限）更可控。
const (
	// DefaultMaxAttempts 是单条消息的最大处理次数（含首次）。最后一次失败后
	// 不再安排重试，直接转死信——否则「重试 N 次」实际会被处理 N+1 次。
	DefaultMaxAttempts = 5
	// DefaultRetryBase 是第一次重试的等待时长。
	DefaultRetryBase = 2 * time.Second
	// DefaultRetryMax 是重试等待的上限，指数增长到这里就不再翻倍。
	DefaultRetryMax = time.Minute
	// DefaultRetryJitter 是抖动比例：实际等待在 [d*(1-jitter), d] 之间随机。
	DefaultRetryJitter = 0.3
	// DefaultRetryBatch 限制单轮提升（把到期的重试写回主队列）的条数。
	DefaultRetryBatch = 100
)

// RetryPolicy 描述失败重试的退避策略。
//
// MaxAttempts <= 0 表示关闭重试：失败的消息退回「保留在途记录、等租约回收」
// 的旧路径。
type RetryPolicy struct {
	MaxAttempts int
	Base        time.Duration
	Max         time.Duration
	// Jitter 取 [0,1]，0 表示不抖动。
	Jitter float64
	// rnd 供测试注入固定随机源；为空时用全局随机数。
	rnd func() float64
}

// DefaultRetryPolicy 返回默认策略。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: DefaultMaxAttempts,
		Base:        DefaultRetryBase,
		Max:         DefaultRetryMax,
		Jitter:      DefaultRetryJitter,
	}
}

// Enabled 表示是否启用失败重试。
func (p RetryPolicy) Enabled() bool { return p.MaxAttempts > 0 }

// Normalize 补全非法参数，避免 0 值把等待时间压成「立即重试」的忙循环。
func (p RetryPolicy) Normalize() RetryPolicy {
	if p.Base <= 0 {
		p.Base = DefaultRetryBase
	}
	if p.Max <= 0 {
		p.Max = DefaultRetryMax
	}
	if p.Max < p.Base {
		p.Max = p.Base
	}
	if p.Jitter < 0 {
		p.Jitter = 0
	}
	if p.Jitter > 1 {
		p.Jitter = 1
	}
	return p
}

// Delay 返回第 attempt 次重试前的等待时长（attempt 从 1 开始）。
//
// 指数增长：base、2*base、4*base……到 Max 封顶；随后按 Jitter 向下抖动，
// 让同一批同时失败的消息不会在同一毫秒一起回到队列。
func (p RetryPolicy) Delay(attempt int) time.Duration {
	p = p.Normalize()
	if attempt < 1 {
		attempt = 1
	}
	d := p.Base
	// attempt 很大时 2 的幂会溢出，超过 20 次直接取上限。
	for i := 1; i < attempt && d < p.Max; i++ {
		if d > p.Max/2 {
			d = p.Max
			break
		}
		d *= 2
	}
	if d > p.Max {
		d = p.Max
	}
	if p.Jitter == 0 {
		return d
	}
	r := p.rnd
	if r == nil {
		r = rand.Float64
	}
	// 只向下抖动：等待时间始终不超过理论值，退避上界仍然可预期。
	return time.Duration(float64(d) * (1 - p.Jitter*r()))
}

// StateTTL 是重试计数等辅助键的存活时间。
//
// 必须覆盖「一条消息从首次失败到耗尽次数」的整个窗口，否则计数键先过期，
// 重试次数会被清零，消息就能无限重试下去。按无抖动的最坏情况求和，再留一
// 分钟余量；上限 24 小时，避免参数配得离谱时留下长期驻留的键。
func (p RetryPolicy) StateTTL() time.Duration {
	p = p.Normalize()
	if !p.Enabled() {
		return time.Minute
	}
	bare := p
	bare.Jitter = 0
	var total time.Duration
	for i := 1; i <= p.MaxAttempts && i <= 32; i++ {
		total += bare.Delay(i)
	}
	total += time.Minute
	if total < time.Minute {
		total = time.Minute
	}
	if total > 24*time.Hour {
		total = 24 * time.Hour
	}
	return total
}

// RetryOutcome 是一次失败处理的结果。
type RetryOutcome struct {
	// Attempt 是本次失败后累计的处理次数（首次失败为 1）。
	Attempt int
	// Delay 是到下次重试的等待时长；Dead 为真时无意义。
	Delay time.Duration
	// FirstFailedAt 是这条消息第一次失败的时刻，跨重试保持不变。
	FirstFailedAt time.Time
	// Dead 为真表示次数已耗尽，消息已转入死信列表。
	Dead bool
}

// DeadLetter 是耗尽重试次数后被搁置的消息。
//
// 保留原始消息体的原因：失败可能是代码缺陷或下游临时不可用，
// 修好之后需要能把消息原样取回重放。
type DeadLetter struct {
	Payload       []byte    `json:"payload"`
	Attempts      int       `json:"attempts"`
	LastError     string    `json:"last_error"`
	FirstFailedAt time.Time `json:"first_failed_at"`
	DeadAt        time.Time `json:"dead_at"`
	Queue         string    `json:"queue"`
}

// Promoted 是一条到期并被写回主队列的重试消息。
type Promoted struct {
	// ID 是当初那次领取的凭据，仅用于日志串联同一条消息的多次尝试。
	ID string
	// Attempts 是已处理次数。
	Attempts int
	// Raw 是被重新写回队列的原始消息体。
	Raw []byte
}

// retryKeys 是重试相关的键，均以队列键为前缀。
type retryKeys struct {
	// retry 是待重试消息的 zset：member=重试记录 ID，score=可重试时间（毫秒）。
	retry string
	// records 存重试元信息（JSON：次数 + 首次失败时间 + 最近错误）。
	records string
	// bodies 按重试记录 ID 存原始消息体。消息体单独存放而不是塞进 JSON：
	// 重投时必须原样写回，任何序列化往返（尤其是 []byte 会被 base64 编码）
	// 都会让写回的消息体与原始 JSON 不再一致。
	bodies string
	// dead 是次数耗尽的消息列表（JSON），供排查与后续重放。
	dead string
}

func retryKeysFor(queueKey string) retryKeys {
	return retryKeys{
		retry:   queueKey + ":retry",
		records: queueKey + ":retry:record",
		bodies:  queueKey + ":retry:body",
		dead:    queueKey + ":dead",
	}
}

// attemptKey 是单条消息的处理次数。按消息体哈希定位，因此重投时无需
// 让次数随消息体一起传递——消息体本身始终保持原样，Python 侧的契约不变。
func (c *ReliableConsumer) attemptKey(raw []byte) string {
	sum := sha256.Sum256(raw)
	return c.keys.queue + ":retry:attempt:" + hex.EncodeToString(sum[:8])
}

func (c *ReliableConsumer) firstFailKey(raw []byte) string {
	sum := sha256.Sum256(raw)
	return c.keys.queue + ":retry:first:" + hex.EncodeToString(sum[:8])
}

// bumpAttemptScript 累加处理次数，并给两个辅助键续期。
// 首次失败的记录用 SET NX 写入，后续失败不改写它。
var bumpAttemptScript = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('SET', KEYS[2], ARGV[1], 'PX', ARGV[2], 'NX')
local first = redis.call('GET', KEYS[2])
return {tostring(n), first or ARGV[1]}
`)

// scheduleRetryScript 把消息从「在途」搬到「待重试」。
//
// 两个动作放在同一个脚本里：若先删在途记录再写重试记录，中间崩溃会丢消息；
// 反过来则会被租约回收重复投递。
var scheduleRetryScript = redis.NewScript(`
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
redis.call('ZADD', KEYS[3], ARGV[4], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
redis.call('ZREM', KEYS[5], ARGV[1])
return 1
`)

// deadLetterScript 把消息转入死信列表并清掉辅助键，同样是一次原子搬家。
var deadLetterScript = redis.NewScript(`
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[2])
redis.call('DEL', KEYS[4])
redis.call('DEL', KEYS[5])
return 1
`)

// promoteScript 取出到期的重试记录，把消息体写回主队列。
//
// 「选到期 → 读记录 → 写回主队列 → 删记录」必须原子：多个实例同时提升时，
// 后执行的一方已经看不到被摘走的成员，同一条消息不会被写回两次。
var promoteScript = redis.NewScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local out = {}
for i = 1, #ids do
  local id = ids[i]
  local meta = redis.call('HGET', KEYS[3], id)
  local raw = redis.call('HGET', KEYS[4], id)
  redis.call('ZREM', KEYS[2], id)
  if raw then
    redis.call('HDEL', KEYS[3], id)
    redis.call('HDEL', KEYS[4], id)
    redis.call('RPUSH', KEYS[1], raw)
    local attempts = 0
    if meta then
      local ok, decoded = pcall(cjson.decode, meta)
      if ok and decoded['attempts'] then
        attempts = decoded['attempts']
      end
    end
    out[#out + 1] = id
    out[#out + 1] = tostring(attempts)
    out[#out + 1] = raw
  end
end
return out
`)

// Retry 处理一条处理失败的消息：累加次数并决定「稍后重试」还是「进死信」。
//
// 无论哪种结果，本次领取都会从在途记录里移除——消息的去向已经由重试队列
// 或死信列表接管，再留着在途记录只会让租约回收把同一条消息重复投递一次，
// 把「有节奏的重试」打回「无节奏的重投」。
func (c *ReliableConsumer) Retry(ctx context.Context, d *Delivery, cause error, policy RetryPolicy) (RetryOutcome, error) {
	policy = policy.Normalize()
	if d == nil {
		return RetryOutcome{}, errors.New("重试缺少消息凭据")
	}
	if !policy.Enabled() {
		return RetryOutcome{}, errors.New("重试策略未启用")
	}

	now := c.now()
	ttl := policy.StateTTL()
	vals, err := bumpAttemptScript.Run(ctx, c.client,
		[]string{c.attemptKey(d.Raw), c.firstFailKey(d.Raw)},
		now.Format(time.RFC3339Nano), ttl.Milliseconds(),
	).Slice()
	if err != nil {
		return RetryOutcome{}, err
	}
	attempt := 1
	firstFailedAt := now
	if len(vals) >= 2 {
		if n, ok := asInt(vals[0]); ok && n > 0 {
			attempt = n
		}
		if s, ok := vals[1].(string); ok {
			if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
				firstFailedAt = ts
			}
		}
	}

	if attempt >= policy.MaxAttempts {
		if err := c.deadLetter(ctx, d, attempt, cause, firstFailedAt, now); err != nil {
			return RetryOutcome{}, err
		}
		return RetryOutcome{Attempt: attempt, FirstFailedAt: firstFailedAt, Dead: true}, nil
	}

	delay := policy.Delay(attempt)
	rec, err := json.Marshal(retryRecord{
		Attempts:      attempt,
		LastError:     causeMessage(cause),
		FirstFailedAt: firstFailedAt,
	})
	if err != nil {
		return RetryOutcome{}, err
	}
	err = scheduleRetryScript.Run(ctx, c.client,
		[]string{c.retryKeys().records, c.retryKeys().bodies, c.retryKeys().retry, c.keys.processing, c.keys.leases},
		d.ID, string(rec), string(d.Raw), now.Add(delay).UnixMilli(),
	).Err()
	if err != nil {
		return RetryOutcome{}, err
	}
	return RetryOutcome{Attempt: attempt, Delay: delay, FirstFailedAt: firstFailedAt}, nil
}

func (c *ReliableConsumer) deadLetter(ctx context.Context, d *Delivery, attempt int, cause error, firstFailedAt, now time.Time) error {
	rec, err := json.Marshal(DeadLetter{
		Payload:       d.Raw,
		Attempts:      attempt,
		LastError:     causeMessage(cause),
		FirstFailedAt: firstFailedAt,
		DeadAt:        now,
		Queue:         c.keys.queue,
	})
	if err != nil {
		return err
	}
	return deadLetterScript.Run(ctx, c.client,
		[]string{c.retryKeys().dead, c.keys.processing, c.keys.leases, c.attemptKey(d.Raw), c.firstFailKey(d.Raw)},
		string(rec), d.ID,
	).Err()
}

// PromoteDueRetries 把已到期的重试消息写回主队列，返回本次提升的条数。
//
// 写回方向与回收流程一致（RPUSH 到右端）：主队列从右端消费，重试消息等待
// 已久，应该排在首次投递的新消息前面，而不是再等一整轮。
func (c *ReliableConsumer) PromoteDueRetries(ctx context.Context, batch int) ([]Promoted, error) {
	if batch <= 0 {
		batch = DefaultRetryBatch
	}
	vals, err := promoteScript.Run(ctx, c.client,
		[]string{c.keys.queue, c.retryKeys().retry, c.retryKeys().records, c.retryKeys().bodies},
		c.now().UnixMilli(), batch,
	).Slice()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	out := make([]Promoted, 0, len(vals)/3)
	for i := 0; i+2 < len(vals); i += 3 {
		id, _ := vals[i].(string)
		attempts, _ := asInt(vals[i+1])
		raw, _ := vals[i+2].(string)
		out = append(out, Promoted{ID: id, Attempts: attempts, Raw: []byte(raw)})
	}
	return out, nil
}

// DeadLetters 读取死信列表（按写入顺序，最多 limit 条）。
func (c *ReliableConsumer) DeadLetters(ctx context.Context, limit int64) ([]DeadLetter, error) {
	if limit <= 0 {
		limit = DefaultRetryBatch
	}
	items, err := c.client.LRange(ctx, c.retryKeys().dead, 0, limit-1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]DeadLetter, 0, len(items))
	for _, item := range items {
		var rec DeadLetter
		if err := json.Unmarshal([]byte(item), &rec); err != nil {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

func (c *ReliableConsumer) retryKeys() retryKeys { return retryKeysFor(c.keys.queue) }

type retryRecord struct {
	Attempts      int       `json:"attempts"`
	LastError     string    `json:"last_error"`
	FirstFailedAt time.Time `json:"first_failed_at"`
}

func causeMessage(cause error) string {
	if cause == nil {
		return ""
	}
	return cause.Error()
}

// Retrier 把一个固定的重试策略绑到消费者上，供消费侧直接调用。
type Retrier struct {
	consumer *ReliableConsumer
	policy   RetryPolicy
}

// Retrier 返回绑定策略后的重试处理器。
func (c *ReliableConsumer) Retrier(policy RetryPolicy) *Retrier {
	return &Retrier{consumer: c, policy: policy.Normalize()}
}

// Retry 按绑定策略处理一次失败。
func (r *Retrier) Retry(ctx context.Context, d *Delivery, cause error) (RetryOutcome, error) {
	return r.consumer.Retry(ctx, d, cause, r.policy)
}

// Policy 返回实际生效的策略。
func (r *Retrier) Policy() RetryPolicy { return r.policy }
