package runner

import (
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/metrics"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
)

// findChannel 在快照里按渠道取一项。
func findChannel(t *testing.T, snap metrics.Snapshot, channel string) metrics.Stat {
	t.Helper()
	for _, s := range snap.Channels {
		if s.Channel == channel {
			return s
		}
	}
	t.Fatalf("快照里没有渠道 %q，实际 %+v", channel, snap.Channels)
	return metrics.Stat{}
}

// 一次成功的投递应同时进合计、渠道与实现三个维度。
func TestRunRecordsMetrics(t *testing.T) {
	r := newRig(t, nil)
	sink := metrics.New()
	r.runner.WithMetrics(sink)

	if _, err := r.runner.Run(likedMessage("t-m1", inAppStep("step-1", "{{ payload.actor }} 赞了你的帖子"))); err != nil {
		t.Fatalf("执行失败: %v", err)
	}

	snap := sink.Snapshot()
	if snap.Total.Sent != 1 || snap.Total.Failed != 0 {
		t.Fatalf("合计不符: %+v", snap.Total)
	}
	ch := findChannel(t, snap, "in_app")
	if ch.Sent != 1 || ch.Attempted != 1 || ch.ErrorRate != 0 {
		t.Fatalf("in_app 渠道计数不符: %+v", ch)
	}
	if ch.Latency.Count != 1 {
		t.Fatalf("延迟样本数 = %d，期望 1", ch.Latency.Count)
	}

	var provider metrics.Stat
	for _, p := range snap.Providers {
		if p.Provider == "console-in-app" {
			provider = p
		}
	}
	if provider.Attempted != 1 {
		t.Fatalf("实现维度应记到 console-in-app，实际 %+v", snap.Providers)
	}
}

// 被过滤条件拦下的步骤没有投递动作，不该进指标——否则失败数里会混进
// 「按设计没发」的噪音。
func TestRunDoesNotRecordSkippedSteps(t *testing.T) {
	r := newRig(t, nil)
	sink := metrics.New()
	r.runner.WithMetrics(sink)

	step := inAppStep("step-1", "{{ payload.actor }} 赞了你的帖子")
	step.Config["filters"] = []any{
		map[string]any{"field": "payload.post_id", "operator": "eq", "value": "P-0000"},
	}
	if _, err := r.runner.Run(likedMessage("t-m2", step)); err != nil {
		t.Fatalf("执行失败: %v", err)
	}

	if snap := sink.Snapshot(); snap.Total.Attempted != 0 {
		t.Fatalf("被过滤的步骤不应计数，实际 %+v", snap.Total)
	}
}

// RecordMetrics 的折算口径：只认有投递动作的步骤，超时按失败计。
func TestRecordMetricsConversion(t *testing.T) {
	sink := metrics.New()

	cases := []struct {
		name       string
		res        pipeline.StepResult
		wantCount  int
		wantStat   string
		wantStatus metrics.Status
	}{
		{
			name:       "成功",
			res:        pipeline.StepResult{Channel: domain.ChannelInApp, Sent: true, UsedProvider: "console-in-app"},
			wantCount:  1,
			wantStat:   "in_app",
			wantStatus: metrics.StatusSent,
		},
		{
			name:       "失败",
			res:        pipeline.StepResult{Channel: domain.ChannelEmail, Error: "收件人被拒", UsedProvider: "smtp-email"},
			wantCount:  1,
			wantStat:   "email",
			wantStatus: metrics.StatusFailed,
		},
		{
			name:       "超时同样计数",
			res:        pipeline.StepResult{Channel: domain.ChannelPush, Error: "步骤执行超时", TimedOut: true, UsedProvider: "console-push"},
			wantCount:  1,
			wantStat:   "push",
			wantStatus: metrics.StatusFailed,
		},
		{
			name:      "被过滤",
			res:       pipeline.StepResult{Channel: domain.ChannelInApp, Skipped: true, SkipReason: "filter"},
			wantCount: 0,
		},
		{
			name:      "上游失败短路",
			res:       pipeline.StepResult{Channel: domain.ChannelEmail, ShortCircuited: true},
			wantCount: 0,
		},
		{
			name:      "已登记延迟续跑",
			res:       pipeline.StepResult{Channel: domain.ChannelDelay, Deferred: true},
			wantCount: 0,
		},
		{
			name:      "聚合卡片窗口内命中",
			res:       pipeline.StepResult{Channel: domain.ChannelInApp},
			wantCount: 0,
		},
		{
			name:      "没有渠道",
			res:       pipeline.StepResult{Sent: true},
			wantCount: 0,
		},
	}

	for _, tc := range cases {
		before := sink.Snapshot().Total.Attempted
		got := RecordMetrics(sink, []pipeline.StepResult{tc.res})
		if got != tc.wantCount {
			t.Fatalf("%s: 记录条数 = %d，期望 %d", tc.name, got, tc.wantCount)
		}
		after := sink.Snapshot().Total.Attempted
		if after-before != int64(tc.wantCount) {
			t.Fatalf("%s: 合计增量 = %d，期望 %d", tc.name, after-before, tc.wantCount)
		}
		if tc.wantCount == 0 {
			continue
		}
		stat := findChannel(t, sink.Snapshot(), tc.wantStat)
		switch tc.wantStatus {
		case metrics.StatusSent:
			if stat.Sent == 0 {
				t.Fatalf("%s: 渠道 %s 的 sent 未累加: %+v", tc.name, tc.wantStat, stat)
			}
		case metrics.StatusFailed:
			if stat.Failed == 0 {
				t.Fatalf("%s: 渠道 %s 的 failed 未累加: %+v", tc.name, tc.wantStat, stat)
			}
		}
	}
}

// 重试与回退的次数要如实带进指标：它们解释「成功数正常但延迟偏高」时的原因。
func TestRecordMetricsKeepsRetryAndFallback(t *testing.T) {
	sink := metrics.New()
	RecordMetrics(sink, []pipeline.StepResult{{
		Channel:          domain.ChannelEmail,
		Sent:             true,
		UsedProvider:     "console-email",
		ProviderAttempts: []string{"smtp-email", "smtp-email", "console-email"},
		ProviderRetries:  2,
		ProviderFallback: true,
		Duration:         320 * time.Millisecond,
	}})

	stat := findChannel(t, sink.Snapshot(), "email")
	if stat.Retried != 2 {
		t.Fatalf("重试次数 = %d，期望 2", stat.Retried)
	}
	if stat.Fallback != 1 {
		t.Fatalf("回退次数 = %d，期望 1", stat.Fallback)
	}
	if stat.Sent != 1 {
		t.Fatalf("回退成功后仍计一次成功，实际 %+v", stat)
	}
	if stat.Latency.MaxMS < 300 || stat.Latency.MaxMS > 340 {
		t.Fatalf("延迟应包含回退链上的耗时，实际 max=%v", stat.Latency.MaxMS)
	}
}

// 未装配收集器时执行照常进行。
func TestRecordMetricsWithoutRegistry(t *testing.T) {
	if got := RecordMetrics(nil, []pipeline.StepResult{{Channel: domain.ChannelInApp, Sent: true}}); got != 0 {
		t.Fatalf("nil 收集器应返回 0，实际 %d", got)
	}

	r := newRig(t, nil)
	if _, err := r.runner.Run(likedMessage("t-m3", inAppStep("step-1", "x"))); err != nil {
		t.Fatalf("未装配指标时执行不应失败: %v", err)
	}
}

// 并发执行同一份收集器：总量必须精确等于投递次数。
func TestRunMetricsConcurrent(t *testing.T) {
	r := newRig(t, nil)
	sink := metrics.New()
	r.runner.WithMetrics(sink)

	const total = 200
	done := make(chan struct{}, total)
	for i := 0; i < total; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			if _, err := r.runner.Run(likedMessage("t-c", inAppStep("step-1", "{{ payload.actor }} 赞了你的帖子"))); err != nil {
				t.Errorf("执行失败: %v", err)
			}
		}()
	}
	for i := 0; i < total; i++ {
		<-done
	}

	snap := sink.Snapshot()
	if snap.Total.Attempted != total {
		t.Fatalf("合计 = %d，期望 %d", snap.Total.Attempted, total)
	}
	if findChannel(t, snap, "in_app").Sent != total {
		t.Fatalf("渠道成功数不符: %+v", snap.Channels)
	}
}
