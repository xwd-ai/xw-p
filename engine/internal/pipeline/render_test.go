package pipeline

import (
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// 标题渲染（Phase 2 渲染阶段）：email 的 subject 与 push/in_app 的 title
// 与正文共用变量系统。标题里带帖子名是最常见的写法，不渲染就会把占位符
// 原样投递出去。

func TestRenderStageRendersSubjectAndTitle(t *testing.T) {
	cases := []struct {
		name       string
		step       domain.Step
		msg        queue.QueuedMessage
		want       string
		body       string
		hasSubject bool
	}{
		{
			name: "email 的 subject",
			step: domain.Step{ID: "s1", Type: domain.ChannelEmail, Config: map[string]any{
				"subject":  "《{{ payload.post_title }}》有新评论",
				"template": "{{ payload.actor }} 评论了你的帖子",
			}},
			msg: queue.QueuedMessage{Payload: map[string]any{
				"post_title": "窗口边界怎么定",
				"actor":      "小林",
			}},
			want:       "《窗口边界怎么定》有新评论",
			body:       "小林 评论了你的帖子",
			hasSubject: true,
		},
		{
			name: "push 的 title 用内置函数",
			step: domain.Step{ID: "s2", Type: domain.ChannelPush, Config: map[string]any{
				"title":    "{{ upper payload.actor }} 赞了你",
				"template": "{{ payload.actor }} 赞了你的帖子",
			}},
			msg:        queue.QueuedMessage{Payload: map[string]any{"actor": "momo"}},
			want:       "MOMO 赞了你",
			body:       "momo 赞了你的帖子",
			hasSubject: true,
		},
		{
			name: "标题里的缺失变量原样保留",
			step: domain.Step{ID: "s3", Type: domain.ChannelInApp, Config: map[string]any{
				"title":    "{{ payload.missing }} 的互动",
				"template": "正文",
			}},
			msg:        queue.QueuedMessage{},
			want:       "{{ payload.missing }} 的互动",
			body:       "正文",
			hasSubject: true,
		},
		{
			name: "没有标题配置时为空",
			step: domain.Step{ID: "s4", Type: domain.ChannelSMS, Config: map[string]any{
				"template": "你的帖子有 {{ payload.count }} 条互动",
			}},
			msg:        queue.QueuedMessage{Payload: map[string]any{"count": 3}},
			body:       "你的帖子有 3 条互动",
			hasSubject: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := NewContext(c.msg)
			// transform 阶段注入系统变量，render 阶段随后渲染标题与正文。
			if err := NewTransformStage().Apply(ctx, c.step); err != nil {
				t.Fatalf("transform 失败: %v", err)
			}
			if err := NewRenderStage().Apply(ctx, c.step); err != nil {
				t.Fatalf("render 失败: %v", err)
			}

			res, ok := ctx.Results[c.step.ID]
			if !ok {
				t.Fatal("render 阶段未写入结果")
			}
			if res.RenderedSubject != c.want {
				t.Errorf("渲染后的标题 = %q, 期望 %q", res.RenderedSubject, c.want)
			}
			if res.Rendered != c.body {
				t.Errorf("渲染后的正文 = %q, 期望 %q", res.Rendered, c.body)
			}
			if (res.RenderedSubject != "") != c.hasSubject {
				t.Errorf("标题空值语义不符合预期: %q", res.RenderedSubject)
			}
			if res.Error != "" {
				t.Errorf("不应产生错误: %s", res.Error)
			}
		})
	}
}

// TestDispatchUsesRenderedSubject 固定 dispatch 的取值顺序：
// 渲染后的标题优先于步骤原始配置，避免「正文渲染了、标题没渲染」的错位。
func TestDispatchUsesRenderedSubject(t *testing.T) {
	sms := &countingProvider{ch: domain.ChannelSMS}
	registry := NewProviderRegistry()
	registry.Register(sms)
	p := NewWithRegistry(discardLogger(), registry)

	wf := domain.Workflow{
		ID:   "wf_post_liked",
		Name: "post_liked",
		Steps: []domain.Step{{
			ID:   "s1",
			Type: domain.ChannelSMS,
			Name: "点赞短信",
			Config: map[string]any{
				"title":    "{{ payload.actor }} 赞了你",
				"template": "你的帖子收到一个赞",
			},
		}},
	}
	msg := queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "post_liked",
		SubscriberID: "creator_1",
		Phone:        "13800000000",
		Payload:      map[string]any{"actor": "阿岚"},
	}

	res := p.Run(NewContext(msg), wf)[0]
	if res.Error != "" {
		t.Fatalf("执行失败: %s", res.Error)
	}
	if got := sms.last.Subject; got != "阿岚 赞了你" {
		t.Errorf("投递标题 = %q, 期望渲染后的标题", got)
	}
}
