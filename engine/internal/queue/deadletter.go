package queue

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/redis/go-redis/v9"
)

// 死信查询的分页参数。
const (
	// DefaultDeadLetterLimit 是一页的默认条数。
	DefaultDeadLetterLimit = 20
	// MaxDeadLetterLimit 限制单页条数：死信记录带着完整原始消息，
	// 一页几百条就足够一次排查，再大只会把响应撑肥。
	MaxDeadLetterLimit = 200
	// MaxReplayIDs 限制一次重放/丢弃涉及的死信条数。
	// 重放会把消息重新排进队列，一次性放回去太多等于自造一波流量峰值。
	MaxReplayIDs = 100
)

// DeadLetterEntry 是死信列表里的一条记录，附带一个定位凭据。
//
// ID 取「写入时那份原始 JSON」的 SHA-1：记录落库后内容不再变化，
// 因此这个 ID 在查询与随后的重放之间保持稳定，也不必额外维护一份索引键。
// 重放时按 ID 找回原始记录串，再用它做条件删除，保证同一条死信只会被
// 放回队列一次。
type DeadLetterEntry struct {
	ID string `json:"id"`
	DeadLetter
}

// DeadLetterQuery 是死信的翻页参数。Offset 从 0 开始，按最新优先计。
type DeadLetterQuery struct {
	Offset int
	Limit  int
}

// DeadLetterPage 是一页死信。
type DeadLetterPage struct {
	Items []DeadLetterEntry `json:"items"`
	// Total 是死信列表当前长度，与本次返回的条数无关：用来算总页数。
	Total  int64 `json:"total"`
	Offset int   `json:"offset"`
	Limit  int   `json:"limit"`
}

// 重放结果，按 ID 汇报去向。
type ReplayReport struct {
	Replayed []string `json:"replayed"`
	// Missing 是不在死信列表里的 ID：可能已被其他运维操作取走，
	// 也可能传错了。这种情况不算错误，重放是幂等的。
	Missing []string `json:"missing"`
}

// ListDeadLetters 按「最新在前」读取一页死信。
//
// 死信以 RPUSH 追加到列表尾部，所以最新的在尾部：从尾部往前取，
// 翻页顺序才符合「先看最近的失败」这个习惯。
func (c *ReliableConsumer) ListDeadLetters(ctx context.Context, q DeadLetterQuery) (DeadLetterPage, error) {
	if q.Offset < 0 {
		q.Offset = 0
	}
	limit := q.Limit
	if limit <= 0 {
		limit = DefaultDeadLetterLimit
	}
	if limit > MaxDeadLetterLimit {
		limit = MaxDeadLetterLimit
	}

	key := c.retryKeys().dead
	total, err := c.client.LLen(ctx, key).Result()
	if err != nil {
		return DeadLetterPage{}, err
	}
	page := DeadLetterPage{
		Items:  make([]DeadLetterEntry, 0, limit),
		Total:  total,
		Offset: q.Offset,
		Limit:  limit,
	}
	if total == 0 || int64(q.Offset) >= total {
		return page, nil
	}

	hi := total - 1 - int64(q.Offset)
	lo := hi - int64(limit) + 1
	if lo < 0 {
		lo = 0
	}
	items, err := c.client.LRange(ctx, key, lo, hi).Result()
	if err != nil {
		return DeadLetterPage{}, err
	}
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
	for _, item := range items {
		var rec DeadLetter
		if err := json.Unmarshal([]byte(item), &rec); err != nil {
			// 单条脏记录不该让整页读不出来：跳过并保留在列表里，等人工清理。
			continue
		}
		page.Items = append(page.Items, DeadLetterEntry{ID: deadLetterID(item), DeadLetter: rec})
	}
	return page, nil
}

// ReplayDeadLetters 把指定死信原样放回主队列，并把它们从死信列表移除。
//
// 重放的前提是「导致失败的原因已经排除」，所以重放后的消息重新获得完整的
// 重试次数（转入死信时次数键已被清除）。放入队列的方向与重试提升一致，
// 走 RPUSH：主队列从右端消费，重放的消息会被优先处理，而不是压在
// 新产生的消息后面再等一整轮。
func (c *ReliableConsumer) ReplayDeadLetters(ctx context.Context, ids []string) (ReplayReport, error) {
	report := ReplayReport{Replayed: make([]string, 0, len(ids)), Missing: make([]string, 0)}
	ids = dedupeIDs(ids)
	if len(ids) == 0 {
		return report, nil
	}
	if len(ids) > MaxReplayIDs {
		ids = ids[:MaxReplayIDs]
	}

	index, err := c.deadLetterIndex(ctx)
	if err != nil {
		return report, err
	}
	for _, id := range ids {
		raw, ok := index[id]
		if !ok {
			report.Missing = append(report.Missing, id)
			continue
		}
		var rec DeadLetter
		if err := json.Unmarshal([]byte(raw), &rec); err != nil || len(rec.Payload) == 0 {
			report.Missing = append(report.Missing, id)
			continue
		}
		// 条件删除 + 入队必须原子：并发重放同一条时只有一个能把 LREM 删掉，
		// 另一个拿不到 1 就不会重复入队。先删后推的顺序反过来会留下
		// 「已入队但没删掉」的重复记录。
		moved, err := replayDeadLetterScript.Run(ctx, c.client,
			[]string{c.retryKeys().dead, c.keys.queue},
			raw, string(rec.Payload),
		).Int()
		if err != nil {
			return report, err
		}
		if moved == 1 {
			report.Replayed = append(report.Replayed, id)
		} else {
			report.Missing = append(report.Missing, id)
		}
	}
	return report, nil
}

// DiscardDeadLetters 丢弃指定死信，返回真正删掉的条数。
//
// 与重放的区别只在于不给消息第二次机会：彻底放弃的消息需要能主动清掉，
// 否则死信列表只增不减，排查时反而找不到真正要看的那几条。
func (c *ReliableConsumer) DiscardDeadLetters(ctx context.Context, ids []string) (int, error) {
	ids = dedupeIDs(ids)
	if len(ids) == 0 {
		return 0, nil
	}
	if len(ids) > MaxReplayIDs {
		ids = ids[:MaxReplayIDs]
	}

	index, err := c.deadLetterIndex(ctx)
	if err != nil {
		return 0, err
	}
	removed := 0
	pipe := c.client.Pipeline()
	for _, id := range ids {
		raw, ok := index[id]
		if !ok {
			continue
		}
		pipe.LRem(ctx, c.retryKeys().dead, 1, raw)
	}
	results, err := pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}
	for _, res := range results {
		if n, ok := res.(*redis.IntCmd); ok {
			removed += int(n.Val())
		}
	}
	return removed, nil
}

// deadLetterIndex 读取整份死信列表并建立 ID → 原始记录串的索引。
//
// 重放与丢弃都是低频的运维动作，为此扫一遍列表比为每条记录维护一个
// 索引键更简单，也不会因为索引与列表不同步而出现「查得到却删不掉」。
func (c *ReliableConsumer) deadLetterIndex(ctx context.Context) (map[string]string, error) {
	items, err := c.client.LRange(ctx, c.retryKeys().dead, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	index := make(map[string]string, len(items))
	for _, item := range items {
		index[deadLetterID(item)] = item
	}
	return index, nil
}

// deadLetterID 计算死信记录的定位凭据。
func deadLetterID(raw string) string {
	sum := sha1.Sum([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func dedupeIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// replayDeadLetterScript 原子地完成「从死信列表摘走 + 写回主队列」。
//
// 以被摘走的那条记录串本身作为删除条件（而不是下标）：两次并发重放
// 拿到的是同一条记录，LREM 只会成功一次。若改按下标删，后执行的一方
// 会因为列表前移删到邻座那条。
var replayDeadLetterScript = redis.NewScript(`
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 0 then
  return 0
end
redis.call('RPUSH', KEYS[2], ARGV[2])
return 1
`)
