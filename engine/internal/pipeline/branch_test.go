package pipeline

import (
	"testing"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

// branchWorkflow 构造带条件分支的电商订单工作流：
//   - s1 email：无条件，恒执行。
//   - s2 sms：仅当 amount >= 100 时执行。
//   - s3 push：仅当 order_id 存在且非空时执行。
func branchWorkflow() domain.Workflow {
	return domain.Workflow{
		ID:      "wf_branch",
		Name:    "order_confirmed",
		Version: 1,
		Active:  true,
		Steps: []domain.Step{
			{
				ID:   "s1",
				Type: domain.ChannelEmail,
				Config: map[string]any{
					"template": "订单 {{ payload.order_id }} 已确认",
				},
			},
			{
				ID:   "s2",
				Type: domain.ChannelSMS,
				Config: map[string]any{
					"template": "高价值订单提醒",
					"if": map[string]any{
						"field":    "payload.amount",
						"operator": "gte",
						"value":    float64(100),
					},
				},
			},
			{
				ID:   "s3",
				Type: domain.ChannelPush,
				Config: map[string]any{
					"template": "物流推送",
					"if":      []any{"payload.tracking_no"},
				},
			},
		},
	}
}

func TestConditionalBranchSkipsWhenFalse(t *testing.T) {
	p := New(nil)
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "order_confirmed",
		SubscriberID: "sub_1",
		Payload: map[string]any{
			"order_id": "A1001",
			"amount":   float64(199), // 无 tracking_no，s3 应跳过
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
	// s2 amount=199 满足 gte 100，应执行
	if results[1].Skipped {
		t.Errorf("s2 满足分支条件，不应被跳过")
	}
	// s3 缺 tracking_no，应被条件分支跳过
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
					map[string]any{"field": "payload.status", "operator": "eq", "value": "paid"},
					map[string]any{"field": "payload.amount", "operator": "gte", "value": 100},
				},
			},
		},
	}

	p := New(nil)
	// 仅满足一个条件：应跳过
	ctx := NewContext(queue.QueuedMessage{
		Name: "order_paid",
		Payload: map[string]any{
			"status": "paid",
			"amount": float64(50),
		},
	})
	res := p.Run(ctx, wf)
	if !res[0].Skipped {
		t.Errorf("仅满足一个条件时应被跳过")
	}

	// 两个条件均满足：应执行
	ctx2 := NewContext(queue.QueuedMessage{
		Name: "order_paid",
		Payload: map[string]any{
			"status": "paid",
			"amount": float64(150),
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
		Name: "order_confirmed",
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

	p := New(nil)
	ctx := NewContext(queue.QueuedMessage{Name: "order_confirmed"})
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
