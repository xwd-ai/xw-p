package pipeline

import (
	"errors"
	"strings"
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/preference"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// 投递前的偏好判定：被拦下的渠道不取实现、不发请求、不短路后续步骤，
// 但要在结果里留下「因为哪一层没发」的结论。

// prefMessage 构造一条带订阅者与邮箱的互动消息。
func prefMessage(triggerID string) queue.QueuedMessage {
	return queue.QueuedMessage{
		TriggerID:    triggerID,
		Name:         "post_liked",
		SubscriberID: "creator_1",
		Email:        "creator@example.com",
		Payload: map[string]any{
			"post_id": "P3001",
			"actor":   "阿岚",
		},
	}
}

// newPreferencePipeline 构造一条带桩渠道与偏好快照的管线。
func newPreferencePipeline(t *testing.T, records ...preference.Record) (*Pipeline, *stubProvider, *stubProvider) {
	t.Helper()
	mail := &stubProvider{id: "console-email", ch: domain.ChannelEmail, res: SendResult{Sent: true}}
	inbox := &stubProvider{id: "console-in-app", ch: domain.ChannelInApp, res: SendResult{Sent: true}}
	registry := NewProviderRegistry()
	registerStub(t, registry, mail, "console-email", 100)
	registerStub(t, registry, inbox, "console-in-app", 100)
	pipe := NewWithRegistry(discardLogger(), registry).
		WithPreferences(preference.NewGate(preference.NewStatic(records...), discardLogger()))
	return pipe, mail, inbox
}

// twoChannelWorkflow 是「邮件 → 站内信」的两步工作流，用于观察单个渠道被跳过。
func twoChannelWorkflow() domain.Workflow {
	return domain.Workflow{
		ID:      "wf_post_liked",
		Name:    "post_liked",
		Version: 1,
		Active:  true,
		Steps: []domain.Step{
			{ID: "mail", Type: domain.ChannelEmail, Config: map[string]any{"template": "{{ payload.actor }} 赞了你的帖子"}},
			{ID: "notify", Type: domain.ChannelInApp, Config: map[string]any{"title": "新的点赞", "template": "{{ payload.actor }} 赞了你的帖子"}},
		},
	}
}

func TestDispatchSkipsDeniedChannel(t *testing.T) {
	pipe, mail, inbox := newPreferencePipeline(t,
		preference.Record{SubscriberID: "creator_1", Scope: preference.ScopeChannel, Target: "email", Enabled: false},
	)

	results := pipe.Run(NewContext(prefMessage("t-pref-1")), twoChannelWorkflow())
	if len(results) != 2 {
		t.Fatalf("应有两个步骤结果，实际 %d", len(results))
	}

	if mail.count() != 0 {
		t.Errorf("被偏好拦下的渠道不该取实现投递，实际投递 %d 次", mail.count())
	}
	first := results[0]
	if !first.Skipped || first.SkipReason != SkipReasonPreference {
		t.Fatalf("邮件步骤应标记为偏好跳过: %+v", first)
	}
	if got := first.SkipMessage; got != "偏好跳过：渠道 email 已关闭" {
		t.Errorf("跳过原因 = %q", got)
	}
	if first.ShortCircuited || first.Error != "" {
		t.Errorf("偏好跳过不是失败，不该记错误或短路: %+v", first)
	}

	if inbox.count() != 1 || !results[1].Sent {
		t.Errorf("站内信应照常投递: count=%d result=%+v", inbox.count(), results[1])
	}
}

func TestDispatchGlobalPreferenceSkipsAllChannels(t *testing.T) {
	pipe, mail, inbox := newPreferencePipeline(t,
		preference.Record{SubscriberID: "creator_1", Scope: preference.ScopeGlobal, Enabled: false},
	)

	results := pipe.Run(NewContext(prefMessage("t-pref-2")), twoChannelWorkflow())
	if mail.count() != 0 || inbox.count() != 0 {
		t.Fatalf("总开关关闭时不该有任何投递：mail=%d inbox=%d", mail.count(), inbox.count())
	}
	for _, res := range results {
		if !res.Skipped || res.SkipMessage != "偏好跳过：全局总开关已关闭" {
			t.Errorf("步骤 %s 应因全局关闭被跳过: %+v", res.StepID, res)
		}
		if res.ShortCircuited {
			t.Errorf("步骤 %s 不该被短路：关掉通知不是投递失败", res.StepID)
		}
	}
}

func TestDispatchWorkflowPreferenceSkipsAllStepsOfThatWorkflow(t *testing.T) {
	pipe, mail, inbox := newPreferencePipeline(t,
		preference.Record{SubscriberID: "creator_1", Scope: preference.ScopeWorkflow, Target: "post_liked", Enabled: false},
	)

	results := pipe.Run(NewContext(prefMessage("t-pref-3")), twoChannelWorkflow())
	if mail.count() != 0 || inbox.count() != 0 {
		t.Fatalf("工作流关闭时不该有任何投递：mail=%d inbox=%d", mail.count(), inbox.count())
	}
	if got := results[0].SkipMessage; got != "偏好跳过：工作流 post_liked 已关闭" {
		t.Errorf("跳过原因 = %q", got)
	}
}

func TestDispatchUnrelatedPreferenceDoesNotBlock(t *testing.T) {
	pipe, mail, inbox := newPreferencePipeline(t,
		// 关的是别的工作流，与 post_liked 无关。
		preference.Record{SubscriberID: "creator_1", Scope: preference.ScopeWorkflow, Target: "post_commented", Enabled: false},
		// 关的是别人的渠道。
		preference.Record{SubscriberID: "creator_2", Scope: preference.ScopeChannel, Target: "email", Enabled: false},
	)

	results := pipe.Run(NewContext(prefMessage("t-pref-4")), twoChannelWorkflow())
	if mail.count() != 1 || inbox.count() != 1 {
		t.Fatalf("无关的偏好不该拦下投递：mail=%d inbox=%d", mail.count(), inbox.count())
	}
	for _, res := range results {
		if res.Skipped {
			t.Errorf("步骤 %s 不该被跳过: %+v", res.StepID, res)
		}
	}
}

// brokenSource 是读不出快照的偏好来源。
type brokenSource struct{}

func (brokenSource) Snapshot() (*preference.Set, error) {
	return nil, errors.New("偏好文件不可读")
}

func TestDispatchAllowsWhenPreferenceUnreadable(t *testing.T) {
	mail := &stubProvider{id: "console-email", ch: domain.ChannelEmail, res: SendResult{Sent: true}}
	registry := NewProviderRegistry()
	registerStub(t, registry, mail, "console-email", 100)
	pipe := NewWithRegistry(discardLogger(), registry).
		WithPreferences(preference.NewGate(brokenSource{}, discardLogger()))

	wf := domain.Workflow{Name: "post_liked", Steps: []domain.Step{
		{ID: "mail", Type: domain.ChannelEmail, Config: map[string]any{"template": "有新互动"}},
	}}
	results := pipe.Run(NewContext(prefMessage("t-pref-5")), wf)
	if mail.count() != 1 || !results[0].Sent {
		t.Fatalf("读不到偏好时应照常投递: count=%d result=%+v", mail.count(), results[0])
	}
}

func TestDigestHonorsChannelPreference(t *testing.T) {
	store := NewMemoryInboxStore()
	registry := NewProviderRegistry()
	registry.Register(NewInAppProvider(discardLogger(), store))
	pipe := NewWithRegistry(discardLogger(), registry).
		WithPreferences(preference.NewGate(preference.NewStatic(
			preference.Record{SubscriberID: "creator_1", Scope: preference.ScopeChannel, Target: "in_app", Enabled: false},
		), discardLogger()))

	results := pipe.Run(NewContext(digestMessage("t-dg-pref-1", "P3101", "阿岚")), likedDigestWorkflow(nil))
	if len(results) != 1 {
		t.Fatalf("应只有一个 digest 结果，实际 %d", len(results))
	}
	res := results[0]
	if !res.Skipped || res.SkipReason != SkipReasonPreference {
		t.Fatalf("聚合卡片应标记为偏好跳过: %+v", res)
	}
	if !strings.Contains(res.SkipMessage, "渠道 in_app 已关闭") {
		t.Errorf("跳过原因 = %q", res.SkipMessage)
	}
	// 计数照常累加：互动是既成事实，偏好只决定这张卡片发不发。
	if res.AggregateCount != 1 {
		t.Errorf("聚合计数应为 1，实际 %d", res.AggregateCount)
	}
	if entries, err := store.List("creator_1"); err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	} else if len(entries) != 0 {
		t.Errorf("关掉站内信后不该写收件箱，实际 %d 条", len(entries))
	}
}
