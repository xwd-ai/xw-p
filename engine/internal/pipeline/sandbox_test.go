package pipeline

import (
	"strings"
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// 安全沙箱（Phase 2）单元测试：
// 覆盖危险路径段拒绝、路径长度限制、模板大小限制、递归深度限制。

func TestSafePathRejectsDangerousSegments(t *testing.T) {
	cases := []string{
		"__class__",
		"payload.__dict__",
		"customer..name",   // 空段
		".post_id",         // 前导点产生空段
		"customer.__name",  // __ 前缀段
		"items[0].__proto", // 数组下标后接内部字段
	}

	for _, p := range cases {
		if _, ok := safeLookupPath(map[string]any{"x": 1}, p); ok {
			t.Errorf("危险路径 %q 不应解析成功", p)
		}
	}
}

func TestSafePathAllowsNormalSegments(t *testing.T) {
	root := map[string]any{
		"post_id": "P1001",
		"customer": map[string]any{
			"name": "张三",
		},
	}
	if v, ok := safeLookupPath(root, "customer.name"); !ok || v != "张三" {
		t.Errorf("正常嵌套路径解析失败: %v, %v", v, ok)
	}
	if v, ok := safeLookupPath(root, "post_id"); !ok || v != "P1001" {
		t.Errorf("顶层路径解析失败: %v, %v", v, ok)
	}
}

func TestSafePathTooManySegments(t *testing.T) {
	path := strings.Repeat("a.", maxPathSegments) + "x"
	if _, ok := safeLookupPath(map[string]any{}, path); ok {
		t.Errorf("超长路径应被拒绝")
	}
}

func TestValidateTemplateSizeLimit(t *testing.T) {
	if err := validateTemplate("正常模板"); err != nil {
		t.Errorf("正常模板不应报错: %v", err)
	}
	big := strings.Repeat("x", maxTemplateBytes+1)
	if err := validateTemplate(big); err == nil {
		t.Errorf("超大模板应被拒绝")
	}
}

func TestRenderRejectsDangerousPathInTemplate(t *testing.T) {
	p := newMockPipeline()
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t3",
		Name:         "post_commented",
		SubscriberID: "sub_1",
		Payload: map[string]any{
			"post_id": "P1001",
		},
	})

	wf := domain.Workflow{
		ID:   "wf",
		Name: "post_commented",
		Steps: []domain.Step{
			{
				ID:   "s1",
				Type: domain.ChannelEmail,
				Config: map[string]any{
					"template": "帖子 {{ payload.__dict__ }} 有新评论",
				},
			},
		},
	}

	results := p.Run(ctx, wf)
	got := results[0].Rendered
	if !strings.Contains(got, "{{ payload.__dict__ }}") {
		t.Errorf("危险路径应被拒绝并原样保留，实际 = %q", got)
	}
}

func TestRenderDepthLimit(t *testing.T) {
	// 构造一个通过函数参数自引用的模板不应导致栈溢出：
	// 实际语法无法真正自引用，这里验证 eval 深度上限不 panic。
	r := NewRenderStage().(*renderStage)
	ctx := NewContext(queue.QueuedMessage{
		Payload: map[string]any{"a": "1"},
	})
	// 构造一个深度有限的嵌套调用，确保 eval 能正常返回。
	out := r.eval(ctx, "upper payload.a", 0)
	if out != "1" {
		t.Errorf("深层嵌套求值异常: %q", out)
	}
	// 触发深度上限：直接以超限深度调用，应原样保留。
	out = r.eval(ctx, "upper payload.a", maxRenderDepth+1)
	if out != "{{ upper payload.a }}" {
		t.Errorf("超限深度应原样保留，实际 = %q", out)
	}
}

func TestRenderNormalInteractionStillWorks(t *testing.T) {
	p := newMockPipeline()
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t4",
		Name:         "post_mentioned",
		SubscriberID: "sub_1",
		Payload: map[string]any{
			"post_id":       "P2001",
			"actor":         "小林",
			"comment_count": 5,
		},
	})

	wf := domain.Workflow{
		ID:   "wf",
		Name: "post_mentioned",
		Steps: []domain.Step{
			{
				ID:   "s1",
				Type: domain.ChannelEmail,
				Config: map[string]any{
					"template": "你的帖子 {{ payload.post_id }} 被 {{ payload.actor }} 评论了 {{ payload.comment_count }} 次",
				},
			},
		},
	}

	results := p.Run(ctx, wf)
	want := "你的帖子 P2001 被 小林 评论了 5 次"
	if results[0].Rendered != want {
		t.Errorf("互动场景渲染 = %q, 期望 %q", results[0].Rendered, want)
	}
}
