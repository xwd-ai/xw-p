package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultClaimLease 是在途记录的默认租约长度。
// 超过租约仍未确认的消息视为「领取者已失效」，由回收流程重新投递
// （见 reclaim.go）。
const DefaultClaimLease = time.Minute

// Delivery 是一条已从队列取出、尚未确认处理完成的消息。
//
// ID 标识「这一次领取」而不是消息本身：同一条消息被重新投递时会拿到新的 ID，
// 因此确认操作只可能删掉自己那份在途记录，不会误删其他消费者的。
type Delivery struct {
	ID        string
	Raw       []byte
	ClaimedAt time.Time
}

// ReliableConsumer 以「取出 → 登记在途 → 确认后删除」的方式消费 Redis List。
//
// 与朴素 BRPOP 的区别：消息被取出的同时会写一份在途记录（payload + 租约），
// 只有处理成功调用 Ack 才真正删除。进程在两步之间崩溃时，消息要么还在
// staging 列表里、要么在在途记录里，两种位置都能被重新取回，不会随进程消失。
//
// 键布局（都以队列键为前缀）：
//
//	<queue>                  主队列，Python 侧 LPUSH 入队，本包从右侧取出
//	<queue>:staging          BLMOVE 的落点，等待登记的在途消息
//	<queue>:processing       hash，deliveryID → 原始 payload
//	<queue>:processing:lease zset，deliveryID → 租约到期时间（毫秒时间戳）
type ReliableConsumer struct {
	client *redis.Client
	keys   claimKeys
	lease  time.Duration
	// grace 是租约到期后的额外宽限，回收阈值取「当前时间 - grace」，
	// 让逼近租约上限的慢处理不被立刻判死。见 ReclaimExpired。
	grace time.Duration
	now   func() time.Time
}

type claimKeys struct {
	queue      string
	staging    string
	processing string
	leases     string
}

func claimKeysFor(queueKey string) claimKeys {
	return claimKeys{
		queue:      queueKey,
		staging:    queueKey + ":staging",
		processing: queueKey + ":processing",
		leases:     queueKey + ":processing:lease",
	}
}

// claimScript 原子地完成「取出 + 登记在途」。
//
// 先取 staging：那里放的是上一次 BLMOVE 已经取出、但还没来得及登记的消息，
// 先清空它能保证不因队列持续有货而被无限搁置，也能保持大致先进先出的顺序。
var claimScript = redis.NewScript(`
local raw = redis.call('LPOP', KEYS[2])
if not raw then
  raw = redis.call('RPOP', KEYS[1])
end
if not raw then
  return false
end
redis.call('HSET', KEYS[3], ARGV[1], raw)
redis.call('ZADD', KEYS[4], ARGV[2], ARGV[1])
return raw
`)

// ackScript 删除在途记录；记录不存在时也是空操作，重复确认安全。
var ackScript = redis.NewScript(`
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
`)

// NewReliableConsumer 创建可靠消费者。
func NewReliableConsumer(addr, password string, db int, key string, lease time.Duration) *ReliableConsumer {
	client := redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db})
	return newReliableConsumer(client, key, lease)
}

func newReliableConsumer(client *redis.Client, key string, lease time.Duration) *ReliableConsumer {
	if lease <= 0 {
		lease = DefaultClaimLease
	}
	return &ReliableConsumer{
		client: client,
		keys:   claimKeysFor(key),
		lease:  lease,
		grace:  lease / 2,
		now:    time.Now,
	}
}

// Claim 领取一条消息并登记在途记录；timeout 内没有消息时返回 (nil, nil)。
//
// 队列有货时脚本一步完成取出与登记；队列为空时才退到 BLMOVE 阻塞等待：
// Redis 的阻塞命令无法和脚本合并成一次原子操作，所以先把消息挪进 staging
// （此刻它已落在 Redis 上，进程崩溃也不会丢），再交给脚本登记。
//
// 阻塞等待的最小粒度是 1 秒（客户端会把更小的值截断），传更短的 timeout 没有意义。
func (c *ReliableConsumer) Claim(ctx context.Context, timeout time.Duration) (*Delivery, error) {
	if d, err := c.claimOnce(ctx); d != nil || err != nil {
		return d, err
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	// RIGHT → LEFT：与主队列的 RPOP 方向一致，先入队的先出。
	_, err := c.client.BLMove(ctx, c.keys.queue, c.keys.staging, "RIGHT", "LEFT", timeout).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil // 等待超时，没有新消息
	}
	if err != nil {
		return nil, err
	}
	// 消息已在 staging 落地；若被并发的消费者抢先登记，这里返回 nil，调用方继续下一轮。
	return c.claimOnce(ctx)
}

func (c *ReliableConsumer) claimOnce(ctx context.Context) (*Delivery, error) {
	now := c.now()
	id := newDeliveryID(now)
	raw, err := claimScript.Run(ctx, c.client,
		[]string{c.keys.queue, c.keys.staging, c.keys.processing, c.keys.leases},
		id, now.Add(c.lease).UnixMilli(),
	).Text()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &Delivery{ID: id, Raw: []byte(raw), ClaimedAt: now}, nil
}

// Ack 确认一条消息已处理完成，删除其在途记录。
func (c *ReliableConsumer) Ack(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	return ackScript.Run(ctx, c.client, []string{c.keys.processing, c.keys.leases}, id).Err()
}

// Ping 检查 Redis 连通性。
func (c *ReliableConsumer) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// Close 关闭连接。
func (c *ReliableConsumer) Close() error {
	return c.client.Close()
}

// newDeliveryID 生成领取凭据：时间戳前缀便于按领取顺序排查，随机后缀保证唯一。
func newDeliveryID(now time.Time) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", now.UnixNano())
	}
	return fmt.Sprintf("%d-%s", now.UnixNano(), hex.EncodeToString(b[:]))
}
