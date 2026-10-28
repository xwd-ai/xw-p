package pipeline

import (
	"strings"
	"testing"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

// countingPipeline 返回注册了 in_app / push / email 三个计数 provider 的管线，
// 便于断言「哪些步骤真的发出去了」。
func countingPipeline(t *testing.T) (*Pipeline, map[domain.Channel]*countingProvider) {
	t.Helper()
	registry := NewProviderRegistry()
	providers := make(map[domain.Channel]*countingProvider)
	for _, ch := range []domain.Channel{domain.ChannelInApp, domain.ChannelPush, domain.ChannelEmail} {
		p := &countingProvider{ch: ch}
		providers[ch] = p
		registry.Register(p)
	}
	return NewWithRegistry(discardLogger(), registry), providers
}

// cyclicWorkflow 构造一条依赖成环的互动触达工作流（站内信 ↔ 推送）。
func cyclicWorkflow() domain.Workflow {
	return domain.Workflow{
		ID:   "wf_post_liked_cycle",
		Name: "post_liked",
		Steps: []domain.Step{
			{
				ID:   "inbox",
				Type: domain.ChannelInApp,
				Config: map[string]any{
					"template":   "{{ payload.actor }} 赞了你的帖子",
					"depends_on": []any{"push"},
				},
			},
			{
				ID:   "push",
				Type: domain.ChannelPush,
				Config: map[string]any{
					"template":   "{{ payload.actor }} 赞了你的帖子",
					"depends_on": []any{"inbox"},
				},
			},
		},
	}
}

func TestRunRejectsCyclicWorkflow(t *testing.T) {
	p, providers := countingPipeline(t)

	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t_cycle",
		Name:         "post_liked",
		SubscriberID: "creator_1",
		Payload:      map[string]any{"post_id": "P2001", "actor": "阿岚"},
	})

	results := p.Run(ctx, cyclicWorkflow())

	if len(results) != 1 {
		t.Fatalf("校验失败应只返回一条结果，实际 %d 条", len(results))
	}
	res := results[0]
	if res.StepID != validationStepID {
		t.Errorf("校验结果应挂在占位步骤上，实际 %q", res.StepID)
	}
	if !strings.Contains(res.Error, "环") {
		t.Errorf("错误信息应说明依赖成环，实际：%q", res.Error)
	}
	if res.Sent {
		t.Error("校验失败时不应标记为已投递")
	}
	for ch, provider := range providers {
		if provider.Sends() != 0 {
			t.Errorf("畸形工作流不应有任何投递，渠道 %s 被调用 %d 次", ch, provider.Sends())
		}
	}
	if len(ctx.Results) != 0 {
		t.Errorf("校验失败时不应写入步骤结果，实际 %d 条", len(ctx.Results))
	}
}

func TestRunExecutesStepsInDependencyOrder(t *testing.T) {
	p, providers := countingPipeline(t)

	// 声明顺序与依赖顺序相反：邮件依赖推送，推送依赖站内信。
	wf := domain.Workflow{
		ID:   "wf_post_commented_ordered",
		Name: "post_commented",
		Steps: []domain.Step{
			{
				ID:     "mail",
				Type:   domain.ChannelEmail,
				Config: map[string]any{"template": "帖子有新评论", "depends_on": []any{"push"}},
			},
			{
				ID:     "push",
				Type:   domain.ChannelPush,
				Config: map[string]any{"template": "帖子有新评论", "depends_on": []any{"inbox"}},
			},
			{
				ID:     "inbox",
				Type:   domain.ChannelInApp,
				Config: map[string]any{"template": "帖子有新评论"},
			},
		},
	}

	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t_order",
		Name:         "post_commented",
		SubscriberID: "creator_1",
		Payload:      map[string]any{"post_id": "P2002", "actor": "阿岚"},
	})

	results := p.Run(ctx, wf)

	if len(results) != 3 {
		t.Fatalf("应执行 3 个步骤，实际 %d 条结果", len(results))
	}
	wantOrder := []string{"inbox", "push", "mail"}
	for i, id := range wantOrder {
		if results[i].StepID != id {
			t.Fatalf("执行顺序应符合依赖，第 %d 步期望 %s，实际 %s", i, id, results[i].StepID)
		}
		if !results[i].Sent {
			t.Errorf("步骤 %s 应投递成功，实际 error=%q", id, results[i].Error)
		}
	}
	for _, ch := range []domain.Channel{domain.ChannelInApp, domain.ChannelPush, domain.ChannelEmail} {
		if got := providers[ch].Sends(); got != 1 {
			t.Errorf("渠道 %s 应收到一次投递，实际 %d 次", ch, got)
		}
	}
}

func TestRunRejectsDanglingDependency(t *testing.T) {
	p, providers := countingPipeline(t)

	wf := domain.Workflow{
		ID:   "wf_post_mentioned_dangling",
		Name: "post_mentioned",
		Steps: []domain.Step{
			{
				ID:     "inbox",
				Type:   domain.ChannelInApp,
				Config: map[string]any{"template": "有人在帖子里 @ 了你", "depends_on": []any{"ghost_step"}},
			},
		},
	}

	results := p.Run(NewContext(queue.QueuedMessage{
		TriggerID:    "t_dangling",
		Name:         "post_mentioned",
		SubscriberID: "creator_1",
		Payload:      map[string]any{"post_id": "P2003"},
	}), wf)

	if len(results) != 1 || results[0].StepID != validationStepID {
		t.Fatalf("悬空依赖应整体拒绝执行，实际结果：%+v", results)
	}
	if providers[domain.ChannelInApp].Sends() != 0 {
		t.Error("悬空依赖时不应投递")
	}
}
