package queue

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisConsumer 从 Redis List 消费消息。
// 与 Python 侧 RedisMessageBus 配对：Python 用 LPUSH 入队，本实现用 BRPOP 阻塞出队。
type RedisConsumer struct {
	client *redis.Client
	key    string
}

// NewRedisConsumer 创建 Redis 消费者。
func NewRedisConsumer(addr, password string, db int, key string) *RedisConsumer {
	return &RedisConsumer{
		client: redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db}),
		key:    key,
	}
}

// Next 阻塞弹出下一条消息；超时返回 (nil, nil)。
func (c *RedisConsumer) Next(ctx context.Context, timeout time.Duration) ([]byte, error) {
	res, err := c.client.BRPop(ctx, timeout, c.key).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(res) < 2 {
		return nil, nil
	}
	return []byte(res[1]), nil
}

// Ping 检查 Redis 连通性。
func (c *RedisConsumer) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// Close 关闭连接。
func (c *RedisConsumer) Close() error {
	return c.client.Close()
}
