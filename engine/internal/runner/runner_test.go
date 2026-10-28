package runner

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// rig 把执行器接到真实管线与内存收件箱上，便于断言「投递到了哪里」。
type rig struct {
	pipe   *pipeline.Pipeline
	inbox  *pipeline.MemoryInboxStore
	runner *Runner

	mu        sync.Mutex
	deferrals []pipeline.Deferral
}

func newRig(t *testing.T, source Source) *rig {
	t.Helper()
	store := pipeline.NewMemoryInboxStore()
	registry := pipeline.NewProviderRegistry()
	registry.Register(pipeline.NewInAppProvider(testLogger(), store))

	r := &rig{inbox: store}
	r.pipe = pipeline.NewWithRegistry(testLogger(), registry).
		WithDeferrer(pipeline.DeferrerFunc(func(d pipeline.Deferral) error {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.deferrals = append(r.deferrals, d)
			return nil
		}))
	r.runner = New(r.pipe, source, testLogger())
	return r
}

func (r *rig) deferred() []pipeline.Deferral {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]pipeline.Deferral(nil), r.deferrals...)
}

// likedMessage 构造一条点赞消息，steps 决定它带哪些步骤快照。
func likedMessage(trigger string, steps ...queue.StepSpec) queue.QueuedMessage {
	return queue.QueuedMessage{
		TriggerID:     trigger,
		TransactionID: "tx-" + trigger,
		Name:          "post_liked",
		SubscriberID:  "creator_1",
		Channels:      []string{"in_app"},
		Payload: map[string]any{
			"post_id":    "P-2026-1001",
			"post_title": "我的第一支 Vlog",
			"actor":      "小林",
		},
		Workflow: &queue.WorkflowSnapshot{
			Name:    "post_liked",
			Version: 3,
			Steps:   steps,
		},
	}
}

func inAppStep(id, tmpl string) queue.StepSpec {
	return queue.StepSpec{
		ID:     id,
		Type:   string(domain.ChannelInApp),
		Name:   "站内信",
		Config: map[string]any{"template": tmpl},
	}
}

// 消息自带步骤快照时，执行器不应再去查任何外部来源。
func TestRunExecutesInlineSnapshot(t *testing.T) {
	r := newRig(t, nil) // source 为nil：只能用消息自带的快照

	rep, err := r.runner.Run(likedMessage("t-1", inAppStep("step-1", "{{ payload.actor }} 赞了你的帖子")))
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if !rep.FromSnapshot {
		t.Errorf("步骤应来自消息快照")
	}
	if rep.Sent != 1 || rep.Failed != 0 {
		t.Errorf("期望 1 条成功，实际 sent=%d failed=%d", rep.Sent, rep.Failed)
	}
	entries, _ := r.inbox.List("creator_1")
	if len(entries) != 1 {
		t.Fatalf("收件箱应有 1 条，实际 %d", len(entries))
	}
	if !strings.Contains(entries[0].Body, "小林") {
		t.Errorf("模板变量未渲染: %q", entries[0].Body)
	}
	if entries[0].Workflow != "post_liked" || entries[0].StepID != "step-1" {
		t.Errorf("收件箱条目缺少工作流/步骤归属: %+v", entries[0])
	}
}

// 没有快照的消息回退到定义表按名称查找。
func TestRunFallsBackToSource(t *testing.T) {
	source := NewStaticSource(domain.Workflow{
		Name:  "post_liked",
		Steps: []domain.Step{{ID: "step-1", Type: domain.ChannelInApp, Config: map[string]any{"template": "{{ payload.post_title }} 收到新点赞"}}},
	})
	msg := likedMessage("t-2") // 不带步骤
	r := newRig(t, source)

	rep, err := r.runner.Run(msg)
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if rep.FromSnapshot {
		t.Errorf("步骤应来自定义表而非消息")
	}
	if rep.Sent != 1 {
		t.Errorf("期望 1 条成功，实际 %d", rep.Sent)
	}
	entries, _ := r.inbox.List("creator_1")
	if len(entries) != 1 || !strings.Contains(entries[0].Body, "我的第一支 Vlog") {
		t.Errorf("回退来源的步骤未执行: %+v", entries)
	}
}

// 既没快照也没定义表命中：消息不可投递，且不产生任何投递。
func TestRunUnknownWorkflowIsUndeliverable(t *testing.T) {
	r := newRig(t, NewStaticSource())

	rep, err := r.runner.Run(likedMessage("t-3"))
	if !errors.Is(err, queue.ErrUndeliverable) {
		t.Fatalf("期望不可投递错误，实际 %v", err)
	}
	if rep.Sent != 0 || rep.Steps != 0 {
		t.Errorf("不可投递的消息不应执行任何步骤: %+v", rep)
	}
	if entries, _ := r.inbox.List("creator_1"); len(entries) != 0 {
		t.Errorf("收件箱不应有写入，实际 %d 条", len(entries))
	}
}

// 步骤结构非法（依赖成环）：整条工作流被拒绝，不是「部分执行」。
func TestRunRejectsInvalidWorkflow(t *testing.T) {
	r := newRig(t, nil)
	msg := likedMessage("t-4",
		queue.StepSpec{ID: "a", Type: string(domain.ChannelInApp), Config: map[string]any{
			"template": "a", "depends_on": []any{"b"},
		}},
		queue.StepSpec{ID: "b", Type: string(domain.ChannelInApp), Config: map[string]any{
			"template": "b", "depends_on": []any{"a"},
		}},
	)

	rep, err := r.runner.Run(msg)
	if !errors.Is(err, queue.ErrUndeliverable) {
		t.Fatalf("期望不可投递错误，实际 %v", err)
	}
	if rep.Rejected == "" {
		t.Errorf("报告应记录拒绝原因")
	}
	if rep.Sent != 0 {
		t.Errorf("校验失败不应投递，实际 sent=%d", rep.Sent)
	}
	if entries, _ := r.inbox.List("creator_1"); len(entries) != 0 {
		t.Errorf("收件箱不应有写入，实际 %d 条", len(entries))
	}
}

// delay 步骤把后续步骤交给调度器，本轮不投递剩余步骤。
func TestRunDefersRemainingSteps(t *testing.T) {
	r := newRig(t, nil)
	msg := likedMessage("t-5",
		queue.StepSpec{ID: "wait", Type: string(domain.ChannelDelay), Config: map[string]any{"delay": "30s"}},
		inAppStep("step-2", "{{ payload.actor }} 赞了你的帖子"),
	)

	rep, err := r.runner.Run(msg)
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if !rep.Deferred {
		t.Errorf("报告应标记延迟")
	}
	if entries, _ := r.inbox.List("creator_1"); len(entries) != 0 {
		t.Errorf("延迟期间不应投递，实际 %d 条", len(entries))
	}
	deferred := r.deferred()
	if len(deferred) != 1 || len(deferred[0].Remaining) != 1 {
		t.Fatalf("应登记 1 个续跑任务且剩余 1 步，实际 %+v", deferred)
	}
}

// 同一条消息重复投递：digest 卡片原地累加，收件箱不会多出一条。
func TestRunDigestCardAccumulates(t *testing.T) {
	r := newRig(t, nil)
	digestStep := queue.StepSpec{
		ID:   "step-1",
		Type: string(domain.ChannelDigest),
		Config: map[string]any{
			"key":      "post_id",
			"window":   "10m",
			"template": "{{ digest.count }} 人赞了你的帖子",
		},
	}
	first, err := r.runner.Run(likedMessage("t-6", digestStep))
	if err != nil || first.Sent != 1 {
		t.Fatalf("首次执行应写入卡片: rep=%+v err=%v", first, err)
	}
	// 同一 trigger 重投（至少一次语义下的重放）不应把计数顶成 2。
	if _, err := r.runner.Run(likedMessage("t-6", digestStep)); err != nil {
		t.Fatalf("重放执行失败: %v", err)
	}
	// 新的互动：同一条卡片累加计数。
	if _, err := r.runner.Run(likedMessage("t-7", digestStep)); err != nil {
		t.Fatalf("第二次互动执行失败: %v", err)
	}

	entries, _ := r.inbox.List("creator_1")
	if len(entries) != 1 {
		t.Fatalf("同一篇帖子的互动应合并成 1 条卡片，实际 %d", len(entries))
	}
	if !strings.Contains(entries[0].Body, "2") {
		t.Errorf("卡片计数应累加为 2，实际正文 %q", entries[0].Body)
	}
}

// Execute 是消费侧入口：不可投递的错误要能被 errors.Is 识别出来。
func TestExecuteWrapsUndeliverable(t *testing.T) {
	r := newRig(t, nil)
	err := r.runner.Execute(likedMessage("t-8"))
	if !errors.Is(err, queue.ErrUndeliverable) {
		t.Fatalf("期望包装 ErrUndeliverable，实际 %v", err)
	}
}
