package bench

import (
	"testing"
	"time"
)

func TestSamplesPercentile(t *testing.T) {
	var s Samples
	for _, d := range []time.Duration{5, 1, 3, 2, 4} {
		s.Add(d * time.Millisecond)
	}
	cases := []struct {
		q    float64
		want time.Duration
	}{
		{0, 1 * time.Millisecond},
		{0.5, 3 * time.Millisecond},
		{0.99, 5 * time.Millisecond},
		{1, 5 * time.Millisecond},
	}
	for _, c := range cases {
		if got := s.Percentile(c.q); got != c.want {
			t.Errorf("Percentile(%.2f) = %s, 期望 %s", c.q, got, c.want)
		}
	}
}

func TestSamplesEmptyAndMean(t *testing.T) {
	var empty Samples
	if got := empty.Percentile(0.99); got != 0 {
		t.Errorf("空样本分位应为 0, 得到 %s", got)
	}
	if got := empty.Mean(); got != 0 {
		t.Errorf("空样本均值应为 0, 得到 %s", got)
	}
	if got := empty.Max(); got != 0 {
		t.Errorf("空样本最大值应为 0, 得到 %s", got)
	}

	var s Samples
	s.AddAll([]time.Duration{2 * time.Millisecond, 4 * time.Millisecond})
	if got := s.Mean(); got != 3*time.Millisecond {
		t.Errorf("均值 = %s, 期望 3ms", got)
	}
	if got := s.Max(); got != 4*time.Millisecond {
		t.Errorf("最大值 = %s, 期望 4ms", got)
	}
	if s.Len() != 2 {
		t.Errorf("样本数 = %d, 期望 2", s.Len())
	}
}

func TestSamplesAddClampsNegative(t *testing.T) {
	var s Samples
	s.Add(-time.Second)
	if got := s.Percentile(0.5); got != 0 {
		t.Errorf("负耗时应归零, 得到 %s", got)
	}
}

func TestSamplesPercentileDoesNotReorderInput(t *testing.T) {
	var s Samples
	s.AddAll([]time.Duration{3 * time.Millisecond, 1 * time.Millisecond, 2 * time.Millisecond})
	_ = s.Percentile(0.5)
	if s.vals[0] != 3*time.Millisecond {
		t.Errorf("取分位不应改动原始样本顺序: %v", s.vals)
	}
}
