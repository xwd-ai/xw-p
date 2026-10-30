package bench

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Enqueuer 是压测的写入端：把一条消息体放进队列。
//
// 抽象成接口是为了让压测能跑在进程内 Redis（测试）和真实 Redis（实测）上，
// 也方便以后换成其它传输做对比。
type Enqueuer interface {
	Enqueue(ctx context.Context, payload []byte) error
}

// ProduceStats 是写入端的统计。
type ProduceStats struct {
	// Sent / Failed 是成功与失败的消息条数。
	Sent   int
	Failed int
	// Latency 是单次入队调用的耗时分布。
	Latency Samples
	// Errors 是失败原因样本（最多保留 errSampleLimit 条，避免刷内存）。
	Errors []string
	// Started / Finished 是写入窗口的起止时刻。
	Started  time.Time
	Finished time.Time
}

// Elapsed 返回写入窗口时长。
func (s ProduceStats) Elapsed() time.Duration { return s.Finished.Sub(s.Started) }

// errSampleLimit 是错误原因样本的保留上限。
const errSampleLimit = 5

// Producer 按 Plan 的并发与速率灌入消息。
type Producer struct {
	sink  Enqueuer
	plan  Plan
	pacer *Pacer
	// build 生成第 i 条消息体；由调用方决定消息结构，生产者只管节奏与计数。
	build func(seq int, now time.Time) ([]byte, error)
	now   func() time.Time
}

// NewProducer 创建生产者。
func NewProducer(sink Enqueuer, plan Plan, build func(seq int, now time.Time) ([]byte, error)) *Producer {
	plan = plan.Normalize()
	return &Producer{
		sink:  sink,
		plan:  plan,
		pacer: NewPacer(plan.Rate),
		build: build,
		now:   time.Now,
	}
}

// Run 灌入 Total 条消息并返回统计。ctx 取消时提前结束，已写入的条数照实统计。
func (p *Producer) Run(ctx context.Context) ProduceStats {
	stats := ProduceStats{Started: p.now()}
	var (
		next  atomic.Int64
		wg    sync.WaitGroup
		mu    sync.Mutex
		local = make([][]time.Duration, p.plan.Concurrency)
	)

	for i := 0; i < p.plan.Concurrency; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			lat := local[worker] // 各 goroutine 写自己的切片，避免锁竞争
			for {
				seq := int(next.Add(1)) - 1
				if seq >= p.plan.Total || ctx.Err() != nil {
					break
				}
				if err := p.pacer.Wait(ctx); err != nil {
					break
				}
				payload, err := p.build(seq, p.now())
				if err != nil {
					mu.Lock()
					stats.Failed++
					recordErr(&stats.Errors, err)
					mu.Unlock()
					continue
				}
				start := p.now()
				err = p.sink.Enqueue(ctx, payload)
				lat = append(lat, p.now().Sub(start))

				mu.Lock()
				if err != nil {
					stats.Failed++
					recordErr(&stats.Errors, err)
				} else {
					stats.Sent++
				}
				mu.Unlock()
			}
			local[worker] = lat
		}(i)
	}
	wg.Wait()

	for _, ds := range local {
		stats.Latency.AddAll(ds)
	}
	stats.Finished = p.now()
	return stats
}

// recordErr 记录一条错误原因样本，超出上限后丢弃。
func recordErr(dst *[]string, err error) {
	if len(*dst) >= errSampleLimit {
		return
	}
	*dst = append(*dst, err.Error())
}
