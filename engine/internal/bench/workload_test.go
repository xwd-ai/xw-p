package bench

import (
	"context"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

func TestWorkflowShape(t *testing.T) {
	wf := Workflow()
	if wf.Name != "post_liked" {
		t.Errorf("工作流名 = %q, 期望 post_liked", wf.Name)
	}
	if len(wf.Steps) != 3 {
		t.Fatalf("步骤数 = %d, 期望 3", len(wf.Steps))
	}
	if wf.Steps[0].Type != "digest" {
		t.Errorf("首个步骤应为 digest, 得到 %s", wf.Steps[0].Type)
	}
	if key, _ := wf.Steps[0].Config["key"].(string); key != "post_id" {
		t.Errorf("聚合键 = %q, 期望 post_id", key)
	}
}

func TestSnapshotDecodesBackToWorkflow(t *testing.T) {
	snap := Snapshot()
	if snap == nil || !snap.HasSteps() {
		t.Fatal("快照应包含步骤")
	}
	wf := snap.Workflow()
	if wf.Name != "post_liked" || len(wf.Steps) != 3 {
		t.Fatalf("还原的工作流不符: %+v", wf)
	}
	if !wf.Active {
		t.Error("快照还原的工作流应视为启用")
	}
}

func TestPipelineHandlerRunsSteps(t *testing.T) {
	logger := QuietLogger()
	inbox := pipeline.NewMemoryInboxStore()
	registry := pipeline.NewProviderRegistry()
	registry.Register(pipeline.NewInAppProvider(logger, inbox))
	registry.Register(pipeline.NewConsolePushProvider(logger))
	pipe := pipeline.NewWithRegistry(logger, registry).WithClock(time.Now)

	handler := PipelineHandler(pipe, Workflow())
	raw, err := BuildEnvelope(1, time.Now(), Snapshot()).Encode()
	if err != nil {
		t.Fatalf("编码失败: %v", err)
	}
	if err := handler(context.Background(), raw); err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	// 一条消息落两条：digest 步骤写的聚合卡片，以及 in_app 步骤写的站内信。
	if inbox.Len() != 2 {
		t.Fatalf("收件箱条目 = %d, 期望 2", inbox.Len())
	}

	// 重复投递同一条消息：卡片按聚合键原地更新、站内信按投递 ID 去重，
	// 条目数不应增长。
	if err := handler(context.Background(), raw); err != nil {
		t.Fatalf("第二次执行失败: %v", err)
	}
	if inbox.Len() != 2 {
		t.Errorf("重复投递不应新增条目, 实际 %d", inbox.Len())
	}
}

func TestPipelineHandlerRejectsInvalidJSON(t *testing.T) {
	handler := PipelineHandler(NewPipeline(), Workflow())
	if err := handler(context.Background(), []byte("not json")); err == nil {
		t.Fatal("非法消息体应返回错误")
	}
}

func TestDecodeHandlerHonoursWork(t *testing.T) {
	raw, _ := BuildEnvelope(0, time.Now(), nil).Encode()

	start := time.Now()
	if err := DecodeHandler(20*time.Millisecond)(context.Background(), raw); err != nil {
		t.Fatalf("解码失败: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Errorf("模拟耗时未生效, 实际 %s", elapsed)
	}

	if err := DecodeHandler(0)(context.Background(), []byte("oops")); err == nil {
		t.Error("非法消息体应返回错误")
	}
}

func TestDecodeHandlerStopsOnCancel(t *testing.T) {
	raw, _ := BuildEnvelope(0, time.Now(), nil).Encode()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := DecodeHandler(time.Second)(ctx, raw)
	if err == nil {
		t.Fatal("取消后应返回错误")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("取消未打断模拟耗时, 实际 %s", elapsed)
	}
}

func TestEnvelopeIsValidQueueMessage(t *testing.T) {
	raw, err := BuildEnvelope(3, time.Now(), Snapshot()).Encode()
	if err != nil {
		t.Fatalf("编码失败: %v", err)
	}
	msg, err := queue.Decode(raw)
	if err != nil {
		t.Fatalf("压测消息应能被队列解码: %v", err)
	}
	if msg.Name != "post_liked" || msg.SubscriberID == "" {
		t.Errorf("消息字段不符: %+v", msg)
	}
	if wf, ok := msg.InlineWorkflow(); !ok || len(wf.Steps) != 3 {
		t.Errorf("消息应携带三步快照, ok=%v", ok)
	}
}
