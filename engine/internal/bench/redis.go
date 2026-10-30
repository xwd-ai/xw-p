package bench

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RedisSink 把消息 LPUSH 进队列，与 Python 触发侧的方向一致
// （入队端 LPUSH，消费端从右侧取出）。
type RedisSink struct {
	client *redis.Client
	key    string
}

// NewRedisSink 创建 Redis 写入端。
func NewRedisSink(client *redis.Client, key string) *RedisSink {
	return &RedisSink{client: client, key: key}
}

// Enqueue 写入一条消息。
func (s *RedisSink) Enqueue(ctx context.Context, payload []byte) error {
	return s.client.LPush(ctx, s.key, payload).Err()
}
