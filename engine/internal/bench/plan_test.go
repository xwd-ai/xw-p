package bench

import (
	"testing"
	"time"
)

func TestPlanNormalizeFillsDefaults(t *testing.T) {
	got := Plan{}.Normalize()
	if got.Total != DefaultTotal {
		t.Errorf("Total = %d, 期望 %d", got.Total, DefaultTotal)
	}
	if got.Concurrency != DefaultConcurrency {
		t.Errorf("Concurrency = %d, 期望 %d", got.Concurrency, DefaultConcurrency)
	}
	if got.Workers != DefaultWorkers {
		t.Errorf("Workers = %d, 期望 %d", got.Workers, DefaultWorkers)
	}
	if got.DrainTimeout != DefaultDrainTimeout {
		t.Errorf("DrainTimeout = %s, 期望 %s", got.DrainTimeout, DefaultDrainTimeout)
	}
	if got.Rate != 0 || got.RateLimited() {
		t.Errorf("Rate = %v，默认应为不限速", got.Rate)
	}
}

func TestPlanNormalizeClamps(t *testing.T) {
	got := Plan{
		Total:       -1,
		Concurrency: MaxConcurrency + 1000,
		Workers:     MaxConcurrency + 1000,
		Rate:        MaxRate * 10,
		Work:        -time.Second,
	}.Normalize()

	if got.Total != DefaultTotal {
		t.Errorf("越界总条数未被修正: %d", got.Total)
	}
	if got.Concurrency != MaxConcurrency {
		t.Errorf("并发数未被截断: %d", got.Concurrency)
	}
	if got.Workers != MaxConcurrency {
		t.Errorf("消费者数未被截断: %d", got.Workers)
	}
	if got.Rate != MaxRate {
		t.Errorf("速率未被截断: %v", got.Rate)
	}
	if got.Work != 0 {
		t.Errorf("负耗时未归零: %s", got.Work)
	}
}

func TestPlanRateLimited(t *testing.T) {
	if (Plan{Rate: 0}).RateLimited() {
		t.Error("Rate=0 不应视为限速")
	}
	if !(Plan{Rate: 10}).RateLimited() {
		t.Error("Rate>0 应视为限速")
	}
}
