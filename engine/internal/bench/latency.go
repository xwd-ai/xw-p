package bench

import (
	"sort"
	"time"
)

// Samples 是一组耗时样本，用于计算分位数。
//
// 样本按原始值保存、取分位时才排序：压测关心的是「有多少条超出了 SLO」，
// 分桶直方图的桶边界会把 P99 抹成整数毫秒，反而不如直接留样本。
// 单轮压测的样本量与消息条数同阶，内存开销可接受。
type Samples struct {
	vals []time.Duration
}

// Add 追加一个样本。
func (s *Samples) Add(d time.Duration) {
	if d < 0 {
		d = 0
	}
	s.vals = append(s.vals, d)
}

// AddAll 批量追加样本，用于把各 goroutine 的本地样本合并回来。
func (s *Samples) AddAll(ds []time.Duration) {
	for _, d := range ds {
		s.Add(d)
	}
}

// Len 返回样本数量。
func (s *Samples) Len() int { return len(s.vals) }

// Percentile 返回 q 分位耗时（q 取 0–1，最近秩法）。
// 样本为空时返回 0，便于报告直接打印而不用先判空。
func (s *Samples) Percentile(q float64) time.Duration {
	n := len(s.vals)
	if n == 0 {
		return 0
	}
	if q <= 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}
	sorted := s.sorted()
	idx := int(float64(n)*q+0.999999) - 1 // 向上取整再减一
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}

// Mean 返回平均耗时。
func (s *Samples) Mean() time.Duration {
	if len(s.vals) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range s.vals {
		sum += d
	}
	return sum / time.Duration(len(s.vals))
}

// Max 返回最大耗时。
func (s *Samples) Max() time.Duration {
	var max time.Duration
	for _, d := range s.vals {
		if d > max {
			max = d
		}
	}
	return max
}

// sorted 返回升序副本，不改动原样本顺序。
func (s *Samples) sorted() []time.Duration {
	out := make([]time.Duration, len(s.vals))
	copy(out, s.vals)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
