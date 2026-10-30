package bench

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Report 把结果渲染成可直接贴进文档的多行文本。
//
// 只陈述测到的事实：生效的负载形状、写入与消费两侧的计数、端到端分位、
// 错误率、进程内存。没有「优化前后提升 N 倍」这类结论——那需要两组
// 参数一致的数据放在一起比，由读报告的人自己判断。
func (r Result) Report() string {
	var b strings.Builder
	b.WriteString("压测结果")
	if r.Queue != "" {
		fmt.Fprintf(&b, "  [%s]", r.Queue)
	}
	b.WriteString("\n")

	fmt.Fprintf(&b, "  负载      %s 条 · 生产者并发 %d · %s · 消费者 %d · 每条模拟处理 %s\n",
		thousands(r.Produced.Sent+r.Produced.Failed), r.Plan.Concurrency, rateText(r.Plan.Rate),
		r.Plan.Workers, workText(r.Plan.Work))

	fmt.Fprintf(&b, "  写入      %s 成功 / %s 失败，用时 %s（%s 条/秒）\n",
		thousands(r.Produced.Sent), thousands(r.Produced.Failed),
		roundMS(r.ProduceElapsed), thousands(int(r.ProduceRate)))

	drain := ""
	if r.DrainTimedOut {
		drain = "，未在超时前排空"
	}
	fmt.Fprintf(&b, "  消费      %s 处理 / %s 失败%s，窗口 %s\n",
		thousands(r.Consumed.Handled), thousands(r.Consumed.Failed), drain,
		roundMS(r.Window))

	fmt.Fprintf(&b, "  吞吐      %s 条/秒\n", thousands(int(r.Throughput)))

	lat := r.Consumed.Latency
	fmt.Fprintf(&b, "  端到端    P50 %s · P90 %s · P99 %s · 最大 %s · 平均 %s\n",
		roundMS(lat.Percentile(0.5)), roundMS(lat.Percentile(0.9)), roundMS(lat.Percentile(0.99)),
		roundMS(lat.Max()), roundMS(lat.Mean()))

	fmt.Fprintf(&b, "  错误率    %.2f%%\n", r.ErrorRate*100)

	mem := r.Memory
	fmt.Fprintf(&b, "  内存      堆 %s（在用 %s）· 保留 %s · 累计分配 %s · GC %d 次 · goroutine %d\n",
		mib(mem.HeapAlloc), mib(mem.HeapInuse), mib(mem.Sys), mib(mem.TotalAlloc), mem.NumGC, mem.Goroutines)

	if len(r.Produced.Errors) > 0 {
		fmt.Fprintf(&b, "  写入错误  %s\n", strings.Join(r.Produced.Errors, " | "))
	}
	if len(r.Consumed.Errors) > 0 {
		fmt.Fprintf(&b, "  消费错误  %s\n", strings.Join(r.Consumed.Errors, " | "))
	}
	return b.String()
}

// jsonResult 是留档用的 JSON 形态：时间统一换成毫秒浮点，
// 便于后续把多组结果画在同一张图里比较。
type jsonResult struct {
	Queue     string  `json:"queue"`
	Total     int     `json:"total"`
	Rate      float64 `json:"rate"`
	Producers int     `json:"producers"`
	Workers   int     `json:"workers"`

	Sent       int     `json:"sent"`
	SendFailed int     `json:"send_failed"`
	SentRate   float64 `json:"send_rate"`

	Handled       int     `json:"handled"`
	RecvFailed    int     `json:"recv_failed"`
	Throughput    float64 `json:"throughput"`
	ErrorRate     float64 `json:"error_rate"`
	DrainTimedOut bool    `json:"drain_timed_out"`

	LatencyMS map[string]float64 `json:"latency_ms"`

	MemoryMB  map[string]float64 `json:"memory_mb"`
	NumGC     uint32             `json:"num_gc"`
	Goroutine int                `json:"goroutines"`
	WindowMS  float64            `json:"window_ms"`
}

// JSON 返回留档用的 JSON。
func (r Result) JSON() ([]byte, error) {
	lat := r.Consumed.Latency
	out := jsonResult{
		Queue: r.Queue, Total: r.Produced.Sent + r.Produced.Failed, Rate: r.Plan.Rate,
		Producers: r.Plan.Concurrency, Workers: r.Plan.Workers,
		Sent: r.Produced.Sent, SendFailed: r.Produced.Failed, SentRate: r.ProduceRate,
		Handled: r.Consumed.Handled, RecvFailed: r.Consumed.Failed,
		Throughput: r.Throughput, ErrorRate: r.ErrorRate, DrainTimedOut: r.DrainTimedOut,
		LatencyMS: map[string]float64{
			"p50": ms(lat.Percentile(0.5)), "p90": ms(lat.Percentile(0.9)),
			"p99": ms(lat.Percentile(0.99)), "max": ms(lat.Max()), "avg": ms(lat.Mean()),
		},
		MemoryMB: map[string]float64{
			"heap_alloc": mb(r.Memory.HeapAlloc), "heap_inuse": mb(r.Memory.HeapInuse),
			"sys": mb(r.Memory.Sys), "total_alloc": mb(r.Memory.TotalAlloc),
		},
		NumGC:     r.Memory.NumGC,
		Goroutine: r.Memory.Goroutines,
		WindowMS:  ms(r.Window),
	}
	return json.MarshalIndent(out, "", "  ")
}

// rateText 描述限速配置，0 表示不限速。
func rateText(rate float64) string {
	if rate <= 0 {
		return "不限速"
	}
	return thousands(int(rate)) + " 条/秒"
}

// workText 描述模拟处理耗时。
func workText(d time.Duration) string {
	if d <= 0 {
		return "无"
	}
	return d.String()
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func mb(v uint64) float64 { return float64(v) / (1024 * 1024) }

func mib(v uint64) string { return fmt.Sprintf("%.1f MiB", mb(v)) }

// thousands 给整数加千位分隔符，报告里的数字动辄五六位，不分组很难读。
func thousands(n int) string {
	s := fmt.Sprintf("%d", n)
	if n < 0 {
		return "-" + thousands(-n)
	}
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		b.WriteString(s[:pre])
	}
	for i := pre; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteString(",")
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}
