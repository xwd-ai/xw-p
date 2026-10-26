package pipeline

import (
	"testing"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

// branchWorkflow 构造带条件分支的内容社区互动工作流：
//   - s1 email：无条件，恒执行。
//   - s2 sms：仅当点赞数 likes >= 100 时执行（只有热门帖子才走短信）。
//   - s3 push：仅当 actor 存在且非空时执行。
func branchWorkflow() domain.Workflow {
	return domain.Workflow{
		ID:      "wf_branch",
		Name:    "post_liked",
		Version: 1,
		Active:  true,
		Steps: []domain.Step{
			{
				ID:   "s1",
				Type: domain.ChannelEmail,
				Config: map[string]any{
					"template": "帖子 {{ payload.post_id }} 收到新点赞",
				},
			},
			{
				ID:   "s2",
				Type: domain.ChannelSMS,
				Config: map[string]any{
					"template": "热门帖子提醒",
					"if": map[string]any{
						"field":    "payload.likes",
						"operator": "gte",
						"value":    float64(100),
					},
				},
			},
			{
				ID:   "s3",
				Type: domain.ChannelPush,
				Config: map[string]any{
					"template": "互动推送",
					"if":       []any{"payload.actor"},
				},
			},
		},
	}
}

func TestConditionalBranchSkipsWhenFalse(t *testing.T) {
	p := newMockPipeline()
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "post_liked",
		SubscriberID: "sub_1",
		Payload: map[string]any{
			"post_id": "P1001",
			"likes":   float64(199), // 无 actor，s3 应跳过
		},
	})

	results := p.Run(ctx, branchWorkflow())
	if len(results) != 3 {
		t.Fatalf("结果数 = %d, 期望 3", len(results))
	}

	// s1 无条件恒执行
	if results[0].Skipped {
		t.Errorf("s1 不应被跳过")
	}
	// s2 likes=199 满足 gte 100，应执行
	if results[1].Skipped {
		t.Errorf("s2 满足分支条件，不应被跳过")
	}
	// s3 缺 actor，应被条件分支跳过
	if !results[2].Skipped {
		t.Errorf("s3 应被分支跳过")
	}
	if results[2].SkipReason != "condition" {
		t.Errorf("s3 SkipReason = %q, 期望 condition", results[2].SkipReason)
	}
}

func TestConditionalBranchArrayAllMustMatch(t *testing.T) {
	// 用 if 数组（与逻辑）验证：两个条件同时满足才执行。
	wf := branchWorkflow()
	wf.Steps = []domain.Step{
		{
			ID:   "s1",
			Type: domain.ChannelEmail,
			Config: map[string]any{
				"if": []any{
					map[string]any{"field": "payload.status", "operator": "eq", "value": "published"},
					map[string]any{"field": "payload.likes", "operator": "gte", "value": 100},
				},
			},
		},
	}

	p := newMockPipeline()
	// 仅满足一个条件：应跳过
	ctx := NewContext(queue.QueuedMessage{
		Name: "post_published",
		Payload: map[string]any{
			"status": "published",
			"likes":  float64(50),
		},
	})
	res := p.Run(ctx, wf)
	if !res[0].Skipped {
		t.Errorf("仅满足一个条件时应被跳过")
	}

	// 两个条件均满足：应执行
	ctx2 := NewContext(queue.QueuedMessage{
		Name: "post_published",
		Payload: map[string]any{
			"status": "published",
			"likes":  float64(150),
		},
	})
	res2 := p.Run(ctx2, wf)
	if res2[0].Skipped {
		t.Errorf("两条件均满足时应执行")
	}
}

func TestFailureShortCircuitStopsSubsequentSteps(t *testing.T) {
	// 第一个投递步骤模板超限（真实失败），后续步骤应被短路。
	oversized := make([]byte, maxTemplateBytes+1)
	for i := range oversized {
		oversized[i] = 'x'
	}

	wf := domain.Workflow{
		ID:   "wf_short",
		Name: "post_commented",
		Steps: []domain.Step{
			{
				ID:     "s1",
				Type:   domain.ChannelEmail,
				Config: map[string]any{"template": string(oversized)},
			},
			{
				ID:     "s2",
				Type:   domain.ChannelSMS,
				Config: map[string]any{"template": "短信"},
			},
			{
				ID:     "s3",
				Type:   domain.ChannelPush,
				Config: map[string]any{"template": "推送"},
			},
		},
	}

	p := newMockPipeline()
	ctx := NewContext(queue.QueuedMessage{Name: "post_commented"})
	results := p.Run(ctx, wf)

	if len(results) != 3 {
		t.Fatalf("结果数 = %d, 期望 3", len(results))
	}
	if results[0].Error == "" {
		t.Errorf("s1 应失败")
	}
	if !results[1].ShortCircuited {
		t.Errorf("s2 应因上游失败被短路")
	}
	if !results[2].ShortCircuited {
		t.Errorf("s3 应因上游失败被短路")
	}
	if results[1].Error == "" {
		t.Errorf("s2 短路结果应记录上游失败原因")
	}
}
