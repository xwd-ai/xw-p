package bench

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// Options 描述一轮压测的装配：负载形状、写入端、读取端与消息处理逻辑。
type Options struct {
	// Plan 是负载形状。
	Plan Plan
	// Queue 是队列键名，仅用于报告。
	Queue string
	// Sink 是写入端。
	Sink Enqueuer
	// Source 是读取端。
	Source Claimer
	// Handler 处理一条消息；为空时退化为「只解码」。
	Handler Handler
	// Workflow 随消息下发的步骤快照；为空时消息不带快照。
	Workflow *queue.WorkflowSnapshot
}

// Result 是一轮压测的完整结果。
type Result struct {
	Queue          string
	Plan           Plan
	Produced       ProduceStats
	Consumed       ConsumeStats
	ProduceElapsed time.Duration
	// Window 是端到端窗口：首条入队 → 最后一条确认完成。
	Window time.Duration
	// Throughput 是端到端吞吐（已处理条数 / Window，单位 条/秒）。
	Throughput float64
	// ProduceRate 是写入端吞吐（成功条数 / 写入窗口，单位 条/秒）。
	ProduceRate float64
	// ErrorRate 是消费侧失败占比（0–1）。
	ErrorRate float64
	// DrainTimedOut 表示在 DrainTimeout 内没有处理完全部消息，
	// 此时吞吐与分位只覆盖已完成的部分。
	DrainTimedOut bool
	// Memory 是收尾时的进程内存快照。
	Memory MemStats
}

// MemStats 是进程内存快照。取 Go 运行时口径而非 RSS：
// 前者跨平台可得，且压测关心的是堆分配与 GC 是否随消息量线性增长。
type MemStats struct {
	HeapAlloc  uint64
	HeapInuse  uint64
	TotalAlloc uint64
	Sys        uint64
	NumGC      uint32
	Goroutines int
}

// ReadMemStats 采集当前进程内存。
func ReadMemStats() MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return MemStats{
		HeapAlloc:  m.HeapAlloc,
		HeapInuse:  m.HeapInuse,
		TotalAlloc: m.TotalAlloc,
		Sys:        m.Sys,
		NumGC:      m.NumGC,
		Goroutines: runtime.NumGoroutine(),
	}
}

// ErrNoSink 表示没有指定写入端。
var ErrNoSink = errors.New("bench: 未指定写入端")

// ErrNoSource 表示没有指定读取端。
var ErrNoSource = errors.New("bench: 未指定读取端")

// Run 执行一轮压测：先起消费者，再按负载灌入，最后等待排空。
//
// 顺序不能反：先灌入再起消费者，队列积压会让「写入窗口」和「消费窗口」
// 之间的边界变得模糊，测出来的吞吐反映的是 Redis List 的堆积速度，
// 而不是链路处理速度。
func Run(ctx context.Context, opts Options) (Result, error) {
	if opts.Sink == nil {
		return Result{}, ErrNoSink
	}
	if opts.Source == nil {
		return Result{}, ErrNoSource
	}
	plan := opts.Plan.Normalize()
	handler := opts.Handler
	if handler == nil {
		handler = DecodeHandler(plan.Work)
	}

	consumer := NewConsumer(opts.Source, plan.Workers, handler).
		WithSentAt(StampOf).
		WithPrefetch(plan.Prefetch)
	consumer.Start(ctx)

	producer := NewProducer(opts.Sink, plan, func(seq int, now time.Time) ([]byte, error) {
		return BuildEnvelope(seq, now, opts.Workflow).Encode()
	})
	produced := producer.Run(ctx)

	result := Result{
		Queue:          opts.Queue,
		Plan:           plan,
		Produced:       produced,
		ProduceElapsed: produced.Elapsed(),
	}

	// 排空：以「已确认条数」为准，而不是「队列长度为 0」——消息被取出后
	// 仍在处理中时队列也是空的，那时收工会把在途消息算成丢失。
	if !waitDrain(ctx, consumer, int64(produced.Sent), plan.DrainTimeout) {
		result.DrainTimedOut = true
	}
	result.Consumed = consumer.Stop()

	if d := produced.Elapsed(); d > 0 {
		result.ProduceRate = float64(produced.Sent) / d.Seconds()
	}
	if w := result.Consumed.Finished.Sub(produced.Started); w > 0 {
		result.Window = w
		if attempted := result.Consumed.Handled + result.Consumed.Failed; attempted > 0 {
			result.Throughput = float64(attempted) / w.Seconds()
		}
	}
	if attempted := result.Consumed.Handled + result.Consumed.Failed; attempted > 0 {
		result.ErrorRate = float64(result.Consumed.Failed) / float64(attempted)
	}
	result.Memory = ReadMemStats()
	return result, nil
}

// waitDrain 等待已确认条数达到目标，或超时/取消；返回是否排空。
func waitDrain(ctx context.Context, c *Consumer, target int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if c.Done() >= target {
			return true
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

// Summary 返回一行不超过 200 字符的概述，便于脚本直接取用。
func (r Result) Summary() string {
	return fmt.Sprintf("%d 条 / 并发 %d / 限速 %.0f 条每秒 / 消费者 %d → %.0f 条每秒，P50 %s，P99 %s，错误率 %.2f%%",
		r.Produced.Sent, r.Plan.Concurrency, r.Plan.Rate, r.Plan.Workers,
		r.Throughput, roundMS(r.Consumed.Latency.Percentile(0.5)), roundMS(r.Consumed.Latency.Percentile(0.99)),
		r.ErrorRate*100)
}

// roundMS 把耗时收敛到 0.1ms，避免报告出现 3.000417ms 这种噪音。
func roundMS(d time.Duration) time.Duration {
	if d == 0 {
		return 0
	}
	return d.Round(100 * time.Microsecond)
}
