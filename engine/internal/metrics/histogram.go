package metrics

import (
	"math"
	"sort"
)

// 延迟直方图用固定桶而不是保留全部原始样本：引擎是长跑进程，一条消息一个
// 样本，保留原始值意味着内存随流量单调增长，而且迟早要面对「保留多久」这个
// 没有正确答案的问题。固定桶的内存是常量，分位数以桶内线性插值估算，误差被
// 桶宽限制住——对于「这条渠道的 P99 是几十毫秒还是几秒」这类判断完全够用。
//
// 桶上界取毫秒，覆盖 mock 投递（亚毫秒）到超时兜底（十秒级）。最后一个桶之后
// 的值进溢出桶，分位数落在溢出桶时回报实际观测到的最大值。
var DefaultBucketBoundsMS = []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

// histogram 是只增不减的定桶直方图。零值不可用，需经 newHistogram 构造。
type histogram struct {
	bounds []float64 // 有限上界，升序
	counts []int64   // 与 bounds 等长，末尾再加一个溢出桶
	sum    float64
	max    float64
	n      int64
}

func newHistogram(bounds []float64) histogram {
	clean := normalizeBounds(bounds)
	return histogram{
		bounds: clean,
		counts: make([]int64, len(clean)+1),
	}
}

// normalizeBounds 复制并校验桶上界：只接受有限正值，去重后升序。
//
// 非法上界（负数、NaN、Inf）直接丢掉而不是报错——指标是旁路，不该因为调用方
// 传了一组奇怪的桶就让投递路径起不来。丢光时回落到默认桶。
func normalizeBounds(bounds []float64) []float64 {
	out := make([]float64, 0, len(bounds))
	for _, b := range bounds {
		if math.IsNaN(b) || math.IsInf(b, 0) || b <= 0 {
			continue
		}
		out = append(out, b)
	}
	sort.Float64s(out)
	dedup := out[:0]
	for i, b := range out {
		if i > 0 && b == out[i-1] {
			continue
		}
		dedup = append(dedup, b)
	}
	if len(dedup) == 0 {
		return append([]float64(nil), DefaultBucketBoundsMS...)
	}
	return dedup
}

// observe 记录一个样本（单位毫秒）。
func (h *histogram) observe(ms float64) {
	if len(h.counts) == 0 {
		// 零值直方图按默认桶补齐：结构体被内嵌在其它类型里时，漏掉构造
		// 不该变成一次越界崩溃。
		*h = newHistogram(DefaultBucketBoundsMS)
	}
	if math.IsNaN(ms) || ms < 0 {
		ms = 0
	}
	idx := sort.SearchFloat64s(h.bounds, ms)
	h.counts[idx]++
	h.n++
	h.sum += ms
	if ms > h.max {
		h.max = ms
	}
}

// quantile 估算分位数（p 在 (0,1] 内）。样本为空时返回 0。
//
// 采用最近秩：第 k = ceil(p*n) 小的样本就是要求的秩，落在哪个桶就在该桶的
// [下界, 上界] 之间按桶内位置线性插值。桶内插值只是估计，桶越宽误差越大，
// 这一点在接口文档里如实写明，不把它当成精确分位。
func (h *histogram) quantile(p float64) float64 {
	if h.n == 0 {
		return 0
	}
	if p <= 0 {
		return 0
	}
	if p > 1 {
		p = 1
	}
	rank := int64(math.Ceil(p * float64(h.n)))
	if rank < 1 {
		rank = 1
	}

	var seen int64
	for i, c := range h.counts {
		if c == 0 {
			continue
		}
		if seen+c < rank {
			seen += c
			continue
		}
		lo := 0.0
		if i > 0 {
			lo = h.bounds[i-1]
		}
		if i >= len(h.bounds) {
			// 溢出桶：只知道「比最大上界还大」，用实际最大值回报，
			// 它是真实观测值，也不会低于桶下界。
			return h.max
		}
		hi := h.bounds[i]
		// 桶内第 (rank-seen) 个样本的相对位置，取值 (0,1]。
		pos := float64(rank-seen) / float64(c)
		return clampToMax(lo+(hi-lo)*pos, h.max)
	}
	return h.max
}

// clampToMax 把插值结果压回实际观测到的最大值。
//
// 桶内插值可能给出比真实最大样本还大的值（样本恰好落在桶下界时尤其明显：
// 单条 0ms 的样本会插成整个桶宽）。最大值是真实观测，作为上界用它收口，
// 估算因此不会凭空造出没发生过的延迟。
func clampToMax(v, max float64) float64 {
	if v > max {
		return max
	}
	return v
}

// mean 返回样本均值；无样本时为 0。
func (h *histogram) mean() float64 {
	if h.n == 0 {
		return 0
	}
	return h.sum / float64(h.n)
}

// Bucket 是延迟分布里的一个桶。
type Bucket struct {
	// UpperMS 是桶上界（毫秒），本桶统计 (上一上界, UpperMS] 区间。
	UpperMS float64 `json:"upperMs"`
	// Count 是落在这个桶里的样本数。
	Count int64 `json:"count"`
}

// Latency 是延迟分布的对外形态。
type Latency struct {
	// Count 是样本数（含失败的投递）。
	Count int64 `json:"count"`
	// MaxMS / MeanMS 是观测到的最大与平均耗时。
	MaxMS  float64 `json:"maxMs"`
	MeanMS float64 `json:"meanMs"`
	// P50MS / P90MS / P99MS 由直方图估算，桶宽决定精度。
	P50MS float64 `json:"p50Ms"`
	P90MS float64 `json:"p90Ms"`
	P99MS float64 `json:"p99Ms"`
	// Buckets 按上界升序列出全部有限桶（含计数为 0 的），便于直接画分布。
	Buckets []Bucket `json:"buckets"`
	// Overflow 是超过最大上界的样本数。
	Overflow int64 `json:"overflow"`
}

// snapshot 把直方图折成对外结构。调用方必须持有 Registry 的锁。
func (h *histogram) snapshot() Latency {
	out := Latency{
		Count:    h.n,
		MaxMS:    round3(h.max),
		MeanMS:   round3(h.mean()),
		P50MS:    round3(h.quantile(0.5)),
		P90MS:    round3(h.quantile(0.9)),
		P99MS:    round3(h.quantile(0.99)),
		Buckets:  make([]Bucket, 0, len(h.bounds)),
		Overflow: h.counts[len(h.bounds)],
	}
	for i, b := range h.bounds {
		out.Buckets = append(out.Buckets, Bucket{UpperMS: b, Count: h.counts[i]})
	}
	return out
}

// round3 把毫秒值收敛到微秒精度：JSON 里出现一长串浮点尾数只会让读数变难。
func round3(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return math.Round(v*1000) / 1000
}
