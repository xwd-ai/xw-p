package pipeline

import (
	"testing"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

func interactionWorkflow() domain.Workflow {
	return domain.Workflow{
		ID:      "wf_post_commented",
		Name:    "post_commented",
		Version: 1,
		Active:  true,
		Steps: []domain.Step{
			{
				ID:   "s1",
				Type: domain.ChannelEmail,
				Name: "评论通知邮件",
				Config: map[string]any{
					"template": "帖子 {{ payload.post_id }} 收到新评论",
				},
			},
			{
				ID:   "s2",
				Type: domain.ChannelSMS,
				Name: "评论通知短信",
				Config: map[string]any{
					"template": "你的帖子 {{ payload.post_id }} 有新评论",
					"filters":  []any{"post_id"},
				},
			},
			{
				ID:   "s3",
				Type: domain.ChannelDelay,
				Name: "延迟一步",
			},
		},
	}
}

func TestPipelineRendersAndDispatches(t *testing.T) {
	p := newMockPipeline()
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "post_commented",
		SubscriberID: "sub_1",
		Payload: map[string]any{
			"post_id": "P1001",
			"actor":   "小林",
		},
	})

	results := p.Run(ctx, interactionWorkflow())

	// 编排步骤 delay 应被跳过，仅 2 个投递步骤
	if len(results) != 2 {
		t.Fatalf("投递结果数 = %d, 期望 2", len(results))
	}

	// email 步骤应渲染成功
	email := results[0]
	if email.Error != "" {
		t.Errorf("email 步骤出错: %s", email.Error)
	}
	wantRendered := "帖子 P1001 收到新评论"
	if email.Rendered != wantRendered {
		t.Errorf("email 渲染 = %q, 期望 %q", email.Rendered, wantRendered)
	}

	// sms 步骤应渲染成功（filters 条件满足）
	sms := results[1]
	if sms.Skipped {
		t.Errorf("sms 步骤不应被跳过")
	}
	if sms.Rendered != "你的帖子 P1001 有新评论" {
		t.Errorf("sms 渲染 = %q", sms.Rendered)
	}
}

func TestPipelineFilterSkipsWhenMissing(t *testing.T) {
	p := newMockPipeline()
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t2",
		Name:         "post_commented",
		SubscriberID: "sub_1",
		Payload:      map[string]any{}, // 缺 post_id
	})

	results := p.Run(ctx, interactionWorkflow())

	// email 步骤无 filters，仍执行
	if results[0].Skipped {
		t.Errorf("email 步骤不应被跳过")
	}
	// sms 步骤因缺少 post_id 被跳过
	if !results[1].Skipped {
		t.Errorf("sms 步骤应被过滤跳过")
	}
}

func TestContextLoadPayload(t *testing.T) {
	ctx := NewContext(queue.QueuedMessage{
		Payload: map[string]any{
			"post_id": "P2001",
			"count":   42,
		},
	})
	ctx.LoadPayload()

	if v, ok := ctx.Get("payload.post_id"); !ok || v != "P2001" {
		t.Errorf("payload.post_id = %v, %v", v, ok)
	}
	if v, ok := ctx.Get("payload.count"); !ok || v != 42 {
		t.Errorf("payload.count = %v, %v", v, ok)
	}
	if _, ok := ctx.Get("payload.missing"); ok {
		t.Errorf("payload.missing 不应存在")
	}
}
