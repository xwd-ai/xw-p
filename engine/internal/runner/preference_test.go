package runner

import (
	"strings"
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/execution"
	"github.com/xwd-ai/xw-p/engine/internal/metrics"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/preference"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// 偏好跳过在执行明细里的形态：一条 status=skipped 的记录，带命中的层级；
// 它不该出现在投递指标里——没发不等于发失败，否则错误率会被偏好设置污染。

func TestRecordResultsWritesPreferenceSkip(t *testing.T) {
	inbox := pipeline.NewMemoryInboxStore()
	registry := pipeline.NewProviderRegistry()
	registry.Register(pipeline.NewInAppProvider(testLogger(), inbox))
	registry.Register(pipeline.NewConsoleEmailProvider(testLogger()))

	store := execution.NewMemoryStore(nil)
	sink := metrics.New()
	pipe := pipeline.NewWithRegistry(testLogger(), registry).
		WithPreferences(preference.NewGate(preference.NewStatic(
			preference.Record{SubscriberID: "creator_1", Scope: preference.ScopeChannel, Target: "email", Enabled: false},
		), testLogger()))

	r := New(pipe, nil, testLogger()).
		WithRecorder(execution.NewRecorder(store, testLogger())).
		WithMetrics(sink)

	msg := emailMessage("t-pref-1",
		inAppStep("notify", "{{ payload.actor }} 赞了你的帖子"),
		queue.StepSpec{ID: "mail", Type: "email", Config: map[string]any{"template": "有新互动"}},
	)
	rep, err := r.Run(msg)
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if rep.Sent != 1 || rep.Skipped != 1 || rep.Failed != 0 {
		t.Fatalf("期望 1 成功 1 跳过 0 失败，实际 %+v", rep)
	}
	if rep.Recorded != 2 {
		t.Fatalf("偏好跳过也要留一条明细，应回写 2 条，实际 %d", rep.Recorded)
	}

	records := store.Records()
	if len(records) != 2 {
		t.Fatalf("应有 2 条明细，实际 %d", len(records))
	}

	skipped := records[1]
	if skipped.Status != execution.StatusSkipped {
		t.Fatalf("邮件应记为 skipped: %+v", skipped)
	}
	if skipped.Channel != "email" || skipped.StepID != "mail" {
		t.Errorf("明细缺少归属信息: %+v", skipped)
	}
	if skipped.Reason == nil || *skipped.Reason != "偏好跳过：渠道 email 已关闭" {
		t.Errorf("跳过原因 = %v", skipped.Reason)
	}
	if skipped.Provider != "" || skipped.DurationMS != 0 {
		t.Errorf("没有投递动作的记录不该有实现与耗时: %+v", skipped)
	}
	if skipped.ID == "" || skipped.CreatedAt.IsZero() {
		t.Errorf("记录应补齐标识与时间: %+v", skipped)
	}

	if entries, err := inbox.List("creator_1"); err != nil || len(entries) != 1 {
		t.Fatalf("站内信应照常写入收件箱: len=%d err=%v", len(entries), err)
	}

	snap := sink.Snapshot()
	if snap.Total.Attempted != 1 {
		t.Errorf("被偏好跳过的渠道不该计入投递指标，attempted=%d", snap.Total.Attempted)
	}
	if snap.Total.Failed != 0 {
		t.Errorf("偏好跳过不是失败，failed=%d", snap.Total.Failed)
	}
}

func TestRecordResultsIgnoresFilterSkippedSteps(t *testing.T) {
	// 被过滤条件拦下的步骤没有投递意图：与偏好跳过不同，不该留明细。
	inbox := pipeline.NewMemoryInboxStore()
	registry := pipeline.NewProviderRegistry()
	registry.Register(pipeline.NewInAppProvider(testLogger(), inbox))

	store := execution.NewMemoryStore(nil)
	pipe := pipeline.NewWithRegistry(testLogger(), registry).
		WithPreferences(preference.NewGate(preference.NewStatic(), testLogger()))
	r := New(pipe, nil, testLogger()).WithRecorder(execution.NewRecorder(store, testLogger()))

	msg := likedMessage("t-pref-2", queue.StepSpec{
		ID:   "notify",
		Type: "in_app",
		Config: map[string]any{
			"template": "有新互动",
			"filters": []any{
				map[string]any{"field": "payload.actor", "operator": "eq", "value": "别人"},
			},
		},
	})
	rep, err := r.Run(msg)
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if rep.Recorded != 0 || len(store.Records()) != 0 {
		t.Fatalf("被过滤的步骤不该回写明细，实际 %d 条", len(store.Records()))
	}
}

func TestPreferenceSkipKeepsCustomReason(t *testing.T) {
	// 记录缺 SkipMessage 时给一个兜底文案，而不是让 reason 为空——空的 skipped
	// 记录在后台和「没有这条记录」没区别。
	record, ok := buildRecord(queue.QueuedMessage{TriggerID: "t-pref-3", Name: "post_liked"}, pipeline.StepResult{
		StepID:     "mail",
		Channel:    "email",
		Skipped:    true,
		SkipReason: pipeline.SkipReasonPreference,
	})
	if !ok {
		t.Fatal("偏好跳过应产生一条明细")
	}
	if record.Reason == nil || strings.TrimSpace(*record.Reason) == "" {
		t.Fatalf("跳过记录必须带原因: %+v", record)
	}
	if err := record.Validate(); err != nil {
		t.Errorf("记录应通过校验: %v", err)
	}
}
