package queue

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultReclaimBatch 限制单次回收扫描的在途记录条数。
//
// 回收要遍历过期记录并把消息写回队列，一次性处理过多会让脚本长时间占用
// Redis；分批处理后，剩下的记录会在下一轮扫描里继续被捡起。
const DefaultReclaimBatch = 100

// Reclaimed 描述一条因租约过期而被判定为「领取者已失效」的消息。
type Reclaimed struct {
	// ID 是失效的那次领取凭据，仅用于日志排查。
	ID string
	// Raw 是重新入队的原始消息体。
	Raw []byte
	// LeaseExpiredAt 是原租约的到期时刻。
	LeaseExpiredAt time.Time
}

// ReclaimReport 是一次回收扫描的结果。
type ReclaimReport struct {
	// Scanned 是扫描到的过期租约条数，含找不到消息体的残留记录。
	Scanned int
	// Requeued 是已写回队列的消息。
	Requeued []Reclaimed
	// Orphans 是有租约、但在途 payload 已不存在的残留条数（多为历史脏数据），
	// 扫描时一并清理。
	Orphans int
}

// ReclaimExpired 把租约已过期的在途消息写回主队列，供消费者重新领取。
//
// 语义是「至少一次」：重新入队的消息可能被处理第二次，因此下游步骤必须
// 幂等（digest 以 trigger_id 去重，站内信以条目 ID 覆盖）。相对地，宁可重复
// 也不能丢——原实现弹出即删，进程崩一次消息就永远消失。
//
// 判定阈值是「租约到期 + 宽限期」：租约本身按领取时刻计算，若某条消息的
// 处理耗时恰好逼近租约，直接在到期瞬间回收会把它交给第二个消费者，造成
// 无谓的重复投递。宽限期取租约的一半，随租约一起放大或收紧。
func (c *ReliableConsumer) ReclaimExpired(ctx context.Context, batch int) (ReclaimReport, error) {
	if batch <= 0 {
		batch = DefaultReclaimBatch
	}
	threshold := c.now().Add(-c.grace).UnixMilli()

	vals, err := reclaimScript.Run(ctx, c.client,
		[]string{c.keys.queue, c.keys.processing, c.keys.leases},
		threshold, batch,
	).Slice()
	if errors.Is(err, redis.Nil) {
		return ReclaimReport{}, nil
	}
	if err != nil {
		return ReclaimReport{}, err
	}

	report := ReclaimReport{}
	if len(vals) == 0 {
		return report, nil
	}
	if n, ok := asInt(vals[0]); ok {
		report.Scanned = n
	}
	for _, v := range vals[1:] {
		fields, ok := v.([]interface{})
		if !ok || len(fields) < 3 {
			continue
		}
		id, _ := fields[0].(string)
		raw, _ := fields[1].(string)
		item := Reclaimed{ID: id, Raw: []byte(raw)}
		if ms, ok := asInt(fields[2]); ok {
			item.LeaseExpiredAt = time.UnixMilli(int64(ms))
		}
		report.Requeued = append(report.Requeued, item)
	}
	report.Orphans = report.Scanned - len(report.Requeued)
	return report, nil
}

// reclaimScript 原子地完成「挑出过期租约 → 取出消息体 → 写回队列 → 清理在途记录」。
//
// 全过程在一个脚本里执行，多个消费者实例同时回收也不会把同一条消息写回两次：
// 脚本在 Redis 内串行，后执行的一方已经看不到被别人摘走的租约。
//
// 返回值第一项是扫描到的过期条数，其后每项是三元素数组 {id, raw, 租约到期毫秒}。
var reclaimScript = redis.NewScript(`
local flat = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', ARGV[1], 'WITHSCORES', 'LIMIT', 0, ARGV[2])
local scanned = 0
local out = {}
for i = 1, #flat, 2 do
  local id = flat[i]
  local score = flat[i + 1]
  scanned = scanned + 1
  redis.call('ZREM', KEYS[3], id)
  local raw = redis.call('HGET', KEYS[2], id)
  if raw then
    redis.call('HDEL', KEYS[2], id)
    -- RPUSH 到主队列右端：主队列由右向左消费，重新入队的消息会被优先领到，
    -- 不会排在新消息后面等到很晚。
    redis.call('RPUSH', KEYS[1], raw)
    out[#out + 1] = {id, raw, tostring(score)}
  end
end
local result = {tostring(scanned)}
for i = 1, #out do
  result[#result + 1] = out[i]
end
return result
`)

// asInt 兼容脚本返回的整数与字符串两种形态（不同客户端/服务端序列化不同）。
func asInt(v interface{}) (int, bool) {
	switch n := v.(type) {
	case int64:
		return int(n), true
	case int:
		return n, true
	case string:
		if parsed, err := strconv.ParseInt(n, 10, 64); err == nil {
			return int(parsed), true
		}
		// Lua 的 tostring 在大数值上可能给出指数形式，补一条浮点解析路径。
		if parsed, err := strconv.ParseFloat(n, 64); err == nil {
			return int(parsed), true
		}
	}
	return 0, false
}
