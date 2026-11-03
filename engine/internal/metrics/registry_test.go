package metrics

import (
	"math"
	"sync"
	"testing"
	"time"
)

// within 断言浮点结果落在容差内。
func within(t *testing.T, label string, got, want, tol float64) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Fatalf("%s = %v，期望 %v（容差 %v）", label, got, want, tol)
	}
}

// findStat 按标签在快照里取一项。
func findStat(t *testing.T, stats []Stat, label string) Stat {
	t.Helper()
	for _, s := range stats {
		if s.Channel == label || s.Provider == label {
			return s
		}
	}
	t.Fatalf("快照里没有标签 %q 的条目", label)
	return Stat{}
}

func TestRecordCountsByChannelAndProvider(t *testing.T) {
	reg := New()
	reg.Record(Sample{Channel: "in_app", Provider: "inapp", Status: StatusSent, Duration: 3 * time.Millisecond})
	reg.Record(Sample{Channel: "email", Provider: "console-email", Status: StatusSent, Duration: 4 * time.Millisecond})
	reg.Record(Sample{Channel: "email", Provider: "console-email", Status: StatusFailed, Duration: 5 * time.Millisecond})

	snap := reg.Snapshot()
	if snap.Total.Attempted != 3 || snap.Total.Sent != 2 || snap.Total.Failed != 1 {
		t.Fatalf("合计不符: %+v", snap.Total)
	}

	email := findStat(t, snap.Channels, "email")
	if email.Sent != 1 || email.Failed != 1 || email.Attempted != 2 {
		t.Fatalf("email 渠道计数不符: %+v", email)
	}
	if email.ErrorRate != 0.5 {
		t.Fatalf("email 错误率 = %v，期望 0.5", email.ErrorRate)
	}
	// 同一个实现可能服务多个渠道，实现维度单独计数。
	console := findStat(t, snap.Providers, "console-email")
	if console.Attempted != 2 || console.Sent != 1 {
		t.Fatalf("console-email 实现计数不符: %+v", console)
	}
}

func TestStatsSortedByVolume(t *testing.T) {
	reg := New()
	for i := 0; i < 3; i++ {
		reg.Record(Sample{Channel: "email", Provider: "console", Status: StatusSent})
	}
	reg.Record(Sample{Channel: "in_app", Provider: "console", Status: StatusSent})

	snap := reg.Snapshot()
	if len(snap.Channels) != 2 {
		t.Fatalf("渠道数 = %d，期望 2", len(snap.Channels))
	}
	if snap.Channels[0].Channel != "email" {
		t.Fatalf("投递量大的渠道应排在前面，实际顺序 %v", snap.Channels)
	}
}

func TestRetriesFallbackAndTimeoutsAreCounted(t *testing.T) {
	reg := New()
	reg.Record(Sample{
		Channel:  "sms",
		Provider: "twilio",
		Status:   StatusSent,
		Duration: 10 * time.Millisecond,
		Retries:  2,
		Fallback: true,
	})
	reg.Record(Sample{
		Channel:  "sms",
		Provider: "twilio",
		Status:   StatusFailed,
		Duration: 10 * time.Second,
		TimedOut: true,
	})

	stat := findStat(t, reg.Snapshot().Channels, "sms")
	if stat.Retried != 2 {
		t.Fatalf("重试次数 = %d，期望 2", stat.Retried)
	}
	if stat.Fallback != 1 {
		t.Fatalf("回退次数 = %d，期望 1", stat.Fallback)
	}
	if stat.Timeouts != 1 {
		t.Fatalf("超时次数 = %d，期望 1", stat.Timeouts)
	}
	if stat.Attempted != 2 {
		t.Fatalf("投递次数 = %d，期望 2（失败与超时同样计入）", stat.Attempted)
	}
}

func TestLatencyQuantiles(t *testing.T) {
	// 1..100 毫秒各一条：P50 落在 50~51 之间，P99 落在 99~100 之间。
	// 桶宽 1ms，插值误差不超过一个桶，据此校验估算没有跑偏。
	reg := New()
	for i := 1; i <= 100; i++ {
		reg.Record(Sample{
			Channel:  "email",
			Provider: "console",
			Status:   StatusSent,
			Duration: time.Duration(i) * time.Millisecond,
		})
	}

	lat := findStat(t, reg.Snapshot().Channels, "email").Latency
	if lat.Count != 100 {
		t.Fatalf("样本数 = %d，期望 100", lat.Count)
	}
	within(t, "P50", lat.P50MS, 50.5, 1)
	within(t, "P90", lat.P90MS, 90.5, 1)
	within(t, "P99", lat.P99MS, 99.5, 1)
	within(t, "Mean", lat.MeanMS, 50.5, 0.5)
	within(t, "Max", lat.MaxMS, 100, 0)
}

func TestLatencyOverflowBeyondLargestBucket(t *testing.T) {
	// 默认最大桶上界 10s；30s 的样本只能进溢出桶，分位数回报实际最大值
	// 而不是桶下界，否则「P99 是 10 秒」会把更慢的渠道藏起来。
	reg := New()
	for i := 0; i < 10; i++ {
		reg.Record(Sample{
			Channel:  "chat",
			Provider: "slack",
			Status:   StatusFailed,
			Duration: 30 * time.Second,
		})
	}

	lat := findStat(t, reg.Snapshot().Channels, "chat").Latency
	if lat.Overflow != 10 {
		t.Fatalf("溢出桶条数 = %d，期望 10", lat.Overflow)
	}
	within(t, "P99", lat.P99MS, 30000, 0)
	within(t, "Max", lat.MaxMS, 30000, 0)
}

func TestBucketListCoversAllBounds(t *testing.T) {
	reg := New(WithBuckets([]float64{5, 50}))
	reg.Record(Sample{Channel: "email", Provider: "console", Status: StatusSent, Duration: 2 * time.Millisecond})
	reg.Record(Sample{Channel: "email", Provider: "console", Status: StatusSent, Duration: 20 * time.Millisecond})
	reg.Record(Sample{Channel: "email", Provider: "console", Status: StatusSent, Duration: 200 * time.Millisecond})

	lat := findStat(t, reg.Snapshot().Channels, "email").Latency
	if len(lat.Buckets) != 2 {
		t.Fatalf("桶数 = %d，期望 2", len(lat.Buckets))
	}
	if lat.Buckets[0].Count != 1 || lat.Buckets[1].Count != 1 || lat.Overflow != 1 {
		t.Fatalf("桶分布不符: %+v overflow=%d", lat.Buckets, lat.Overflow)
	}
}

func TestInvalidBoundsFallBackToDefault(t *testing.T) {
	reg := New(WithBuckets([]float64{-1, math.NaN(), math.Inf(1), 0}))
	reg.Record(Sample{Channel: "email", Provider: "console", Status: StatusSent, Duration: time.Millisecond})

	lat := findStat(t, reg.Snapshot().Channels, "email").Latency
	if len(lat.Buckets) != len(DefaultBucketBoundsMS) {
		t.Fatalf("非法桶被丢光后应回落到默认桶，实际 %d 个", len(lat.Buckets))
	}
}

func TestNegativeDurationTreatedAsZero(t *testing.T) {
	reg := New()
	reg.Record(Sample{Channel: "email", Provider: "console", Status: StatusSent, Duration: -5 * time.Millisecond})

	lat := findStat(t, reg.Snapshot().Channels, "email").Latency
	if lat.MaxMS != 0 || lat.MeanMS != 0 {
		t.Fatalf("负耗时按 0 记账，实际 max=%v mean=%v", lat.MaxMS, lat.MeanMS)
	}
}

func TestEmptyLabelsBecomeUnknown(t *testing.T) {
	reg := New()
	reg.Record(Sample{Status: StatusFailed, Duration: time.Millisecond})

	snap := reg.Snapshot()
	stat := findStat(t, snap.Channels, UnknownLabel)
	if stat.Failed != 1 {
		t.Fatalf("缺标签的样本应归到 %q，实际 %+v", UnknownLabel, stat)
	}
	findStat(t, snap.Providers, UnknownLabel)
}

func TestEmptySnapshot(t *testing.T) {
	snap := New().Snapshot()
	if snap.Total.Attempted != 0 || snap.Total.Latency.Count != 0 {
		t.Fatalf("空快照应全为零: %+v", snap.Total)
	}
	if len(snap.Channels) != 0 || len(snap.Providers) != 0 {
		t.Fatalf("空快照不应有渠道或实现条目")
	}
	if snap.Total.ErrorRate != 0 {
		t.Fatalf("没有投递时错误率应为 0")
	}
}

func TestSnapshotIsIndependentCopy(t *testing.T) {
	reg := New()
	reg.Record(Sample{Channel: "email", Provider: "console", Status: StatusSent, Duration: time.Millisecond})

	snap := reg.Snapshot()
	snap.Channels[0].Sent = 99
	snap.Channels[0].Latency.Buckets[0].Count = 99
	snap.Total.Sent = 99

	again := reg.Snapshot()
	if again.Channels[0].Sent != 1 || again.Total.Sent != 1 {
		t.Fatalf("修改快照不应影响内部计数: %+v", again.Channels[0])
	}
	if again.Channels[0].Latency.Buckets[0].Count != 1 {
		t.Fatalf("快照里的桶数组也应是拷贝")
	}
}

func TestResetClearsCounters(t *testing.T) {
	clock := time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC)
	reg := New(WithClock(func() time.Time { return clock }))
	reg.Record(Sample{Channel: "email", Provider: "console", Status: StatusSent})

	clock = clock.Add(90 * time.Second)
	if snap := reg.Snapshot(); snap.UptimeSeconds != 90 {
		t.Fatalf("运行时长 = %v，期望 90", snap.UptimeSeconds)
	}

	reg.Reset()
	snap := reg.Snapshot()
	if snap.Total.Attempted != 0 || len(snap.Channels) != 0 {
		t.Fatalf("重置后应清空计数: %+v", snap)
	}
	if !snap.StartedAt.Equal(clock) {
		t.Fatalf("重置后应从当前时刻重新计时，实际 %v", snap.StartedAt)
	}
}

func TestNilRegistryIsSafe(t *testing.T) {
	var reg *Registry
	reg.Record(Sample{Channel: "email", Status: StatusSent})
	reg.Reset()
	if snap := reg.Snapshot(); snap.Total.Attempted != 0 {
		t.Fatalf("nil 收集器应返回空快照")
	}
}

func TestConcurrentRecord(t *testing.T) {
	reg := New()
	const workers, perWorker = 16, 500

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				reg.Record(Sample{
					Channel:  "in_app",
					Provider: "inapp",
					Status:   StatusSent,
					Duration: time.Duration(j%10) * time.Millisecond,
				})
				_ = reg.Snapshot() // 读写并发下快照不应拿到半截状态
			}
		}()
	}
	wg.Wait()

	snap := reg.Snapshot()
	want := int64(workers * perWorker)
	if snap.Total.Attempted != want {
		t.Fatalf("合计 = %d，期望 %d", snap.Total.Attempted, want)
	}
	if snap.Total.Latency.Count != want {
		t.Fatalf("延迟样本 = %d，期望 %d", snap.Total.Latency.Count, want)
	}
	stat := findStat(t, snap.Channels, "in_app")
	if stat.Attempted != want {
		t.Fatalf("渠道计数 = %d，期望 %d", stat.Attempted, want)
	}
	// 实现维度与渠道维度是同一批样本的两种切法，总和必须一致。
	if stat.Attempted != findStat(t, snap.Providers, "inapp").Attempted {
		t.Fatalf("渠道与实现维度的总量不一致")
	}
}

func TestQuantileEdgeCases(t *testing.T) {
	h := newHistogram(DefaultBucketBoundsMS)
	if got := h.quantile(0.5); got != 0 {
		t.Fatalf("空直方图分位应为 0，实际 %v", got)
	}

	h.observe(0)
	if got := h.quantile(0.5); got != 0 {
		t.Fatalf("单样本 0ms 的 P50 应为 0，实际 %v", got)
	}

	single := newHistogram(DefaultBucketBoundsMS)
	single.observe(7)
	within(t, "P99 单样本", single.quantile(0.99), 7, 1)
}
