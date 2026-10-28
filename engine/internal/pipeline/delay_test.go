package pipeline

import (
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// delayStep 构造一个 delay 步骤。
func delayStep(id string, cfg map[string]any) domain.Step {
	return domain.Step{ID: id, Type: domain.ChannelDelay, Name: "延迟", Config: cfg}
}

// delayWorkflow 构造「站内信 → 延迟 → 邮件」的互动触达工作流。
func delayWorkflow(cfg map[string]any) domain.Workflow {
	return domain.Workflow{
		ID:      "wf_post_liked",
		Name:    "post_liked",
		Version: 1,
		Active:  true,
		Steps: []domain.Step{
			{
				ID:   "s1",
				Type: domain.ChannelInApp,
				Name: "点赞站内信",
				Config: map[string]any{
					"template": "{{ payload.actor }} 赞了你的帖子 {{ payload.post_id }}",
				},
			},
			delayStep("s2", cfg),
			{
				ID:   "s3",
				Type: domain.ChannelEmail,
				Name: "点赞汇总邮件",
				Config: map[string]any{
					"subject":  "帖子 {{ payload.post_id }} 有了新的点赞",
					"template": "你的帖子 {{ payload.post_id }} 收到新点赞",
				},
			},
		},
	}
}

// likedMessage 构造一条点赞事件队列消息。
func likedMessage() queue.QueuedMessage {
	return queue.QueuedMessage{
		TriggerID:    "t_delay_1",
		Name:         "post_liked",
		SubscriberID: "creator_1",
		Email:        "creator@example.com",
		Payload: map[string]any{
			"post_id": "P2001",
			"actor":   "阿岚",
		},
	}
}

func TestParseDelaySpec(t *testing.T) {
	cases := []struct {
		name    string
		cfg     map[string]any
		want    time.Duration
		wantErr bool
	}{
		{name: "秒", cfg: map[string]any{"seconds": 30}, want: 30 * time.Second},
		{name: "分钟", cfg: map[string]any{"minutes": 5}, want: 5 * time.Minute},
		{name: "小时", cfg: map[string]any{"hours": 2}, want: 2 * time.Hour},
		{name: "单位相加", cfg: map[string]any{"minutes": 1, "seconds": 30}, want: 90 * time.Second},
		{name: "浮点天数", cfg: map[string]any{"days": 0.5}, want: 12 * time.Hour},
		{name: "duration 字符串", cfg: map[string]any{"delay": "90s"}, want: 90 * time.Second},
		{name: "字符串数字", cfg: map[string]any{"seconds": "45"}, want: 45 * time.Second},
		{name: "缺配置", cfg: nil, wantErr: true},
		{name: "空配置", cfg: map[string]any{"unrelated": 1}, wantErr: true},
		{name: "零值", cfg: map[string]any{"seconds": 0}, wantErr: true},
		{name: "负值", cfg: map[string]any{"seconds": -5}, wantErr: true},
		{name: "超上限", cfg: map[string]any{"days": 31}, wantErr: true},
		{name: "非法字符串", cfg: map[string]any{"delay": "半分钟"}, wantErr: true},
		{name: "delay 与单位冲突", cfg: map[string]any{"delay": "30s", "seconds": 30}, wantErr: true},
		{name: "非数字类型", cfg: map[string]any{"seconds": true}, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := ParseDelaySpec(delayStep("d1", tc.cfg))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("期望报错，实际得到 %+v", spec)
				}
				return
			}
			if err != nil {
				t.Fatalf("期望成功，实际报错：%v", err)
			}
			if spec.Duration != tc.want {
				t.Errorf("Duration = %s, 期望 %s", spec.Duration, tc.want)
			}
		})
	}
}

func TestDelayDefersRemainingSteps(t *testing.T) {
	fixed := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	var got []Deferral

	p := newMockPipeline().
		WithClock(func() time.Time { return fixed }).
		WithDeferrer(DeferrerFunc(func(d Deferral) error {
			got = append(got, d)
			return nil
		}))

	ctx := NewContext(likedMessage())
	results := p.Run(ctx, delayWorkflow(map[string]any{"seconds": 30}))

	// 本轮只执行到 delay 步骤：站内信 + delay，共 2 条结果。
	if len(results) != 2 {
		t.Fatalf("本轮结果数 = %d, 期望 2", len(results))
	}
	if results[0].Rendered != "阿岚 赞了你的帖子 P2001" {
		t.Errorf("站内信渲染 = %q", results[0].Rendered)
	}

	delayRes := results[1]
	if delayRes.StepType != domain.StepTypeDelay {
		t.Fatalf("第二步类型 = %q, 期望 delay", delayRes.StepType)
	}
	if !delayRes.Deferred {
		t.Errorf("delay 步骤应标记已登记续跑任务")
	}
	if want := fixed.Add(30 * time.Second); !delayRes.DeferredUntil.Equal(want) {
		t.Errorf("到期时刻 = %s, 期望 %s", delayRes.DeferredUntil, want)
	}

	// 续跑任务内容：剩余步骤只有邮件，消息原样携带。
	if len(got) != 1 {
		t.Fatalf("续跑任务数 = %d, 期望 1", len(got))
	}
	if got[0].StepID != "s2" || got[0].WorkflowName != "post_liked" {
		t.Errorf("续跑任务定位错误：%+v", got[0])
	}
	if len(got[0].Remaining) != 1 || got[0].Remaining[0].ID != "s3" {
		t.Errorf("剩余步骤 = %+v, 期望仅 s3", got[0].Remaining)
	}
	if got[0].Message.SubscriberID != "creator_1" {
		t.Errorf("续跑任务丢失原始消息：%+v", got[0].Message)
	}

	// 延迟后的邮件步骤本轮不应执行。
	if _, executed := ctx.Results["s3"]; executed {
		t.Errorf("延迟后的步骤不应在本轮执行")
	}
}

func TestDelayResumeRunsRemainingSteps(t *testing.T) {
	p := newMockPipeline()
	ctx := NewContext(likedMessage())

	var deferral Deferral
	p.WithDeferrer(DeferrerFunc(func(d Deferral) error {
		deferral = d
		return nil
	}))
	p.WithClock(func() time.Time { return time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC) })
	p.Run(ctx, delayWorkflow(map[string]any{"minutes": 1}))

	payload, err := deferral.Encode()
	if err != nil {
		t.Fatalf("编码失败：%v", err)
	}

	resumed, results, err := p.Resume(payload)
	if err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	if resumed.WorkflowName != "post_liked" {
		t.Errorf("续跑工作流 = %q", resumed.WorkflowName)
	}
	if len(results) != 1 {
		t.Fatalf("续跑结果数 = %d, 期望 1", len(results))
	}
	if results[0].StepID != "s3" {
		t.Errorf("续跑步骤 = %q, 期望 s3", results[0].StepID)
	}
	if results[0].Rendered != "你的帖子 P2001 收到新点赞" {
		t.Errorf("续跑渲染 = %q（payload 变量未还原？）", results[0].Rendered)
	}
}

func TestDelayInvalidConfigShortCircuits(t *testing.T) {
	var called int
	p := newMockPipeline().WithDeferrer(DeferrerFunc(func(d Deferral) error {
		called++
		return nil
	}))

	results := p.Run(NewContext(likedMessage()), delayWorkflow(nil))

	if called != 0 {
		t.Errorf("非法配置不应登记续跑任务")
	}
	if len(results) != 3 {
		t.Fatalf("结果数 = %d, 期望 3（站内信 + delay + 短路邮件）", len(results))
	}
	if results[1].Error == "" {
		t.Errorf("delay 步骤应报错")
	}
	if !results[2].ShortCircuited {
		t.Errorf("delay 失败后，后续步骤应被短路")
	}
}

func TestDelayWithoutDeferrerFails(t *testing.T) {
	results := newMockPipeline().Run(NewContext(likedMessage()),
		delayWorkflow(map[string]any{"seconds": 10}))

	if len(results) != 3 {
		t.Fatalf("结果数 = %d, 期望 3（站内信 + delay + 短路邮件）", len(results))
	}
	if results[1].Error == "" {
		t.Errorf("未配置调度器时 delay 步骤应报错，而不是静默跳过")
	}
	if !results[2].ShortCircuited {
		t.Errorf("delay 失败后，后续邮件步骤应被短路")
	}
}

func TestDelayAsLastStepNeedsNoJob(t *testing.T) {
	var called int
	p := newMockPipeline().WithDeferrer(DeferrerFunc(func(d Deferral) error {
		called++
		return nil
	}))

	wf := domain.Workflow{
		Name: "post_mentioned",
		Steps: []domain.Step{
			{ID: "s1", Type: domain.ChannelInApp, Config: map[string]any{"template": "$ 被 @ 了"}},
			delayStep("s2", map[string]any{"delay": "15m"}),
		},
	}
	results := p.Run(NewContext(likedMessage()), wf)

	if called != 0 {
		t.Errorf("末尾 delay 不应产生续跑任务")
	}
	if len(results) != 2 || results[1].Error != "" {
		t.Fatalf("末尾 delay 应正常记账：%+v", results)
	}
	if results[1].DeferredUntil.IsZero() {
		t.Errorf("末尾 delay 仍应算出到期时刻")
	}
}

func TestDelayEnqueueDeferrer(t *testing.T) {
	type enqueued struct {
		dueAt   time.Time
		payload []byte
	}
	var got enqueued

	enq := enqueuerFunc(func(dueAt time.Time, payload []byte) (string, error) {
		got = enqueued{dueAt: dueAt, payload: payload}
		return "task-1", nil
	})

	dueAt := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	d := Deferral{
		StepID:       "s2",
		WorkflowName: "post_commented",
		DueAt:        dueAt,
		Message:      likedMessage(),
		Remaining:    []domain.Step{{ID: "s3", Type: domain.ChannelEmail}},
	}

	if err := NewEnqueueDeferrer(enq).Defer(d); err != nil {
		t.Fatalf("登记失败：%v", err)
	}
	if !got.dueAt.Equal(dueAt) {
		t.Errorf("登记到期时刻 = %s, 期望 %s", got.dueAt, dueAt)
	}

	decoded, err := DecodeDeferral(got.payload)
	if err != nil {
		t.Fatalf("解码失败：%v", err)
	}
	if decoded.StepID != "s2" || len(decoded.Remaining) != 1 || decoded.Remaining[0].ID != "s3" {
		t.Errorf("解码结果不符：%+v", decoded)
	}
	if decoded.Message.Payload["post_id"] != "P2001" {
		t.Errorf("消息 payload 未保留：%+v", decoded.Message.Payload)
	}
}

// enqueuerFunc 把函数适配为 DelayEnqueuer 接口。
type enqueuerFunc func(dueAt time.Time, payload []byte) (string, error)

func (f enqueuerFunc) EnqueueDelay(dueAt time.Time, payload []byte) (string, error) {
	return f(dueAt, payload)
}
