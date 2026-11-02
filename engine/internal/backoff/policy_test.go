package backoff

import (
	"testing"
	"time"
)

func TestDelayGrowsExponentiallyUntilCap(t *testing.T) {
	p := Policy{MaxAttempts: 10, Base: 100 * time.Millisecond, Max: time.Second}

	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		time.Second,
		time.Second,
	}
	for i, expect := range want {
		if got := p.Delay(i + 1); got != expect {
			t.Errorf("第 %d 次等待 = %s, 期望 %s", i+1, got, expect)
		}
	}
}

func TestDelayTreatsNonPositiveAttemptAsFirst(t *testing.T) {
	p := Policy{Base: 50 * time.Millisecond, Max: time.Second}
	for _, attempt := range []int{0, -3} {
		if got := p.Delay(attempt); got != 50*time.Millisecond {
			t.Errorf("attempt=%d 的等待 = %s, 期望首次的 50ms", attempt, got)
		}
	}
}

func TestDelayJitterStaysWithinBounds(t *testing.T) {
	p := Policy{Base: time.Second, Max: time.Minute, Jitter: 0.5}

	p.Rand = func() float64 { return 1 }
	if got := p.Delay(1); got != 500*time.Millisecond {
		t.Errorf("抖动到底时等待 = %s, 期望 500ms", got)
	}
	p.Rand = func() float64 { return 0 }
	if got := p.Delay(1); got != time.Second {
		t.Errorf("不抖动时等待 = %s, 期望 1s", got)
	}
	p.Rand = func() float64 { return 0.5 }
	if got := p.Delay(1); got != 750*time.Millisecond {
		t.Errorf("抖动一半时等待 = %s, 期望 750ms", got)
	}
}

func TestDelayNeverExceedsCapWithJitter(t *testing.T) {
	p := Policy{Base: time.Second, Max: 3 * time.Second, Jitter: 0.3}
	for attempt := 1; attempt <= 12; attempt++ {
		if got := p.Delay(attempt); got > 3*time.Second {
			t.Errorf("第 %d 次等待 %s 超过上限", attempt, got)
		}
	}
}

func TestNormalizeFillsZeroValues(t *testing.T) {
	p := Policy{MaxAttempts: 3}.Normalize()

	if p.Base != DefaultBase || p.Max != DefaultMax {
		t.Errorf("零值应补成 %s→%s，实际 %s→%s", DefaultBase, DefaultMax, p.Base, p.Max)
	}
	if p.MaxAttempts != 3 {
		t.Errorf("Normalize 不应改动次数，实际 %d", p.MaxAttempts)
	}
}

func TestNormalizeClampsJitterAndKeepsMaxAboveBase(t *testing.T) {
	if got := (Policy{Base: time.Second, Max: time.Second, Jitter: 2}).Normalize(); got.Jitter != 1 {
		t.Errorf("抖动上限应为 1，实际 %g", got.Jitter)
	}
	if got := (Policy{Base: time.Second, Max: time.Second, Jitter: -1}).Normalize(); got.Jitter != 0 {
		t.Errorf("抖动下限应为 0，实际 %g", got.Jitter)
	}
	got := Policy{Base: 10 * time.Second, Max: time.Second}.Normalize()
	if got.Max != got.Base {
		t.Errorf("上限小于基础值时应对齐，实际 %s", got.Max)
	}
}

func TestAttemptsAndRetries(t *testing.T) {
	cases := []struct {
		attempts int
		wantTry  int
		wantRet  int
	}{
		{attempts: 0, wantTry: 1, wantRet: 0},
		{attempts: 1, wantTry: 1, wantRet: 0},
		{attempts: 4, wantTry: 4, wantRet: 3},
	}
	for _, c := range cases {
		p := Policy{MaxAttempts: c.attempts}
		if got := p.Attempts(); got != c.wantTry {
			t.Errorf("次数 %d 的 Attempts = %d, 期望 %d", c.attempts, got, c.wantTry)
		}
		if got := p.Retries(); got != c.wantRet {
			t.Errorf("次数 %d 的 Retries = %d, 期望 %d", c.attempts, got, c.wantRet)
		}
	}
}

func TestString(t *testing.T) {
	if got := (Policy{}).String(); got != "关闭" {
		t.Errorf("未配置时摘要 = %q, 期望 关闭", got)
	}
	p := Policy{MaxAttempts: 3, Base: 500 * time.Millisecond, Max: 2 * time.Second, Jitter: 0.25}
	if got, want := p.String(), "3 次 500ms→2s 抖动 0.25"; got != want {
		t.Errorf("摘要 = %q, 期望 %q", got, want)
	}
	p.Jitter = 0
	if got, want := p.String(), "3 次 500ms→2s"; got != want {
		t.Errorf("无抖动时摘要 = %q, 期望 %q", got, want)
	}
}
