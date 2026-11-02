package runner

import (
	"errors"
	"strings"
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/execution"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/provider"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// stubProvider 是结果可控的渠道实现，只用于观察回写出来的明细。
type stubProvider struct {
	id  string
	ch  domain.Channel
	res pipeline.SendResult
}

func (s stubProvider) Channel() domain.Channel { return s.ch }

func (s stubProvider) ID() string { return s.id }

func (s stubProvider) Send(pipeline.Delivery) pipeline.SendResult { return s.res }

func register(t *testing.T, reg *pipeline.ProviderRegistry, p pipeline.Provider, id string, priority int) {
	t.Helper()
	if err := reg.RegisterEntry(provider.Entry{
		ID:       id,
		Channel:  p.Channel(),
		Priority: priority,
		Enabled:  true,
		Sender:   p,
	}); err != nil {
		t.Fatalf("登记 %s 失败：%v", id, err)
	}
}

// writebackRig 组装「真实管线 + 内存执行明细」，返回执行器与存储。
func writebackRig(t *testing.T, build func(*pipeline.ProviderRegistry)) (*Runner, *execution.MemoryStore, *pipeline.MemoryInboxStore) {
	t.Helper()
	inbox := pipeline.NewMemoryInboxStore()
	registry := pipeline.NewProviderRegistry()
	registry.Register(pipeline.NewInAppProvider(testLogger(), inbox))
	if build != nil {
		build(registry)
	}
	store := execution.NewMemoryStore(nil)
	pipe := pipeline.NewWithRegistry(testLogger(), registry)
	runner := New(pipe, nil, testLogger()).WithRecorder(execution.NewRecorder(store, testLogger()))
	return runner, store, inbox
}

func emailMessage(trigger string, steps ...queue.StepSpec) queue.QueuedMessage {
	msg := likedMessage(trigger, steps...)
	msg.Email = "creator@example.com"
	msg.Channels = append(msg.Channels, "email")
	return msg
}

// 每个走到分发阶段的渠道都要留一条明细：成功记 sent、失败记 failed 加原因。
func TestRecordResultsWritesSentAndFailed(t *testing.T) {
	runner, store, _ := writebackRig(t, func(reg *pipeline.ProviderRegistry) {
		register(t, reg, stubProvider{
			id:  "console-email",
			ch:  domain.ChannelEmail,
			res: pipeline.SendResult{Sent: false, Reason: "SMTP 收件人 creator@example.com 失败（550）：no such user"},
		}, "console-email", 100)
	})

	msg := emailMessage("t-100",
		inAppStep("notify", "{{ payload.actor }} 赞了你的帖子"),
		queue.StepSpec{ID: "mail", Type: string(domain.ChannelEmail), Config: map[string]any{"template": "有新互动"}},
	)
	rep, err := runner.Run(msg)
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if rep.Sent != 1 || rep.Failed != 1 {
		t.Fatalf("期望 1 成功 1 失败，实际 %+v", rep)
	}
	if rep.Recorded != 2 {
		t.Fatalf("应回写 2 条明细，实际 %d", rep.Recorded)
	}

	records := store.Records()
	if len(records) != 2 {
		t.Fatalf("应有 2 条明细，实际 %d", len(records))
	}

	sent := records[0]
	if sent.Channel != "in_app" || sent.Status != execution.StatusSent {
		t.Errorf("站内信应记为 sent: %+v", sent)
	}
	if sent.Reason != nil {
		t.Errorf("成功记录不应带失败原因: %v", *sent.Reason)
	}
	if sent.TriggerID != "t-100" || sent.Workflow != "post_liked" || sent.SubscriberID != "creator_1" || sent.StepID != "notify" {
		t.Errorf("明细缺少归属信息: %+v", sent)
	}
	if sent.Provider != "console-in-app" {
		t.Errorf("成功记录应带实际实现标识，实际 %q", sent.Provider)
	}

	failed := records[1]
	if failed.Channel != "email" || failed.Status != execution.StatusFailed {
		t.Errorf("邮件应记为 failed: %+v", failed)
	}
	if failed.Reason == nil || !strings.Contains(*failed.Reason, "550") {
		t.Errorf("失败原因应原样落库，实际 %v", failed.Reason)
	}
	if failed.DurationMS < 0 {
		t.Errorf("耗时不能为负: %d", failed.DurationMS)
	}
	if failed.ID == "" || failed.CreatedAt.IsZero() {
		t.Errorf("记录应补齐标识与时间: %+v", failed)
	}
}

// 回退成功时，明细里要能看出「用了哪个实现、试过哪些、是否回退」。
func TestRecordResultsCapturesFallbackChain(t *testing.T) {
	runner, store, _ := writebackRig(t, func(reg *pipeline.ProviderRegistry) {
		register(t, reg, stubProvider{
			id:  "smtp-email",
			ch:  domain.ChannelEmail,
			res: pipeline.SendResult{Sent: false, Reason: "SMTP 连接 smtp.example.com:587 失败：i/o timeout"},
		}, "smtp-email", 10)
		register(t, reg, stubProvider{
			id:  "console-email",
			ch:  domain.ChannelEmail,
			res: pipeline.SendResult{Sent: true},
		}, "console-email", 100)
	})

	msg := emailMessage("t-101", queue.StepSpec{
		ID: "mail", Type: string(domain.ChannelEmail), Config: map[string]any{"template": "有新互动"},
	})
	if _, err := runner.Run(msg); err != nil {
		t.Fatalf("执行失败: %v", err)
	}

	records := store.Records()
	if len(records) != 1 {
		t.Fatalf("应有 1 条明细，实际 %d", len(records))
	}
	record := records[0]
	if record.Status != execution.StatusSent {
		t.Errorf("回退成功应记为 sent: %+v", record)
	}
	if record.Provider != "console-email" {
		t.Errorf("应记录完成投递的实现，实际 %q", record.Provider)
	}
	if !record.Fallback {
		t.Errorf("应标记发生了回退")
	}
	if len(record.Attempts) != 2 || record.Attempts[0] != "smtp-email" || record.Attempts[1] != "console-email" {
		t.Errorf("尝试序列应按顺序落库，实际 %v", record.Attempts)
	}
}

// 没有投递动作的步骤不产生明细：被过滤、被短路、已登记延迟的都不算一次投递。
func TestRecordResultsSkipsStepsWithoutDelivery(t *testing.T) {
	runner, store, inbox := writebackRig(t, func(reg *pipeline.ProviderRegistry) {
		register(t, reg, stubProvider{
			id:  "console-email",
			ch:  domain.ChannelEmail,
			res: pipeline.SendResult{Sent: false, Reason: "SMTP 连接失败"},
		}, "console-email", 100)
	})

	msg := emailMessage("t-102",
		queue.StepSpec{ID: "filtered", Type: string(domain.ChannelInApp), Config: map[string]any{
			"template": "不该发出",
			"if":       map[string]any{"field": "payload.actor", "operator": "eq", "value": "不存在的互动者"},
		}},
		queue.StepSpec{ID: "mail", Type: string(domain.ChannelEmail), Config: map[string]any{"template": "有新互动"}},
		// 邮件失败后这一步被短路，没有投递动作。
		inAppStep("never", "{{ payload.actor }} 赞了你的帖子"),
	)
	rep, err := runner.Run(msg)
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if rep.Skipped != 1 || rep.ShortCircuited != 1 || rep.Failed != 1 {
		t.Fatalf("步骤计数不符: %+v", rep)
	}

	records := store.Records()
	if len(records) != 1 {
		t.Fatalf("只应回写真正投递过的 1 条明细，实际 %d: %+v", len(records), records)
	}
	if records[0].StepID != "mail" || records[0].Status != execution.StatusFailed {
		t.Errorf("明细应只对应失败的邮件步骤: %+v", records[0])
	}
	if entries, _ := inbox.List("creator_1"); len(entries) != 0 {
		t.Errorf("被过滤与被短路的站内信都不应写入收件箱，实际 %d 条", len(entries))
	}
}

// 工作流校验不通过时一条投递都没发生，执行明细也不该凭空出现记录。
func TestRecordResultsEmptyOnRejectedWorkflow(t *testing.T) {
	runner, store, _ := writebackRig(t, nil)
	msg := likedMessage("t-103",
		queue.StepSpec{ID: "a", Type: string(domain.ChannelInApp), Config: map[string]any{
			"template": "a", "depends_on": []any{"b"},
		}},
		queue.StepSpec{ID: "b", Type: string(domain.ChannelInApp), Config: map[string]any{
			"template": "b", "depends_on": []any{"a"},
		}},
	)

	rep, err := runner.Run(msg)
	if err == nil || rep.Rejected == "" {
		t.Fatalf("校验不通过应返回拒绝原因: rep=%+v err=%v", rep, err)
	}
	if rep.Recorded != 0 {
		t.Errorf("被拒绝的工作流不应回写明细，实际 %d", rep.Recorded)
	}
	if records := store.Records(); len(records) != 0 {
		t.Errorf("执行明细应为空，实际 %+v", records)
	}
}

// 没接记录器时照常执行，只是不记账。
func TestRunWithoutRecorder(t *testing.T) {
	runner, _, inbox := writebackRig(t, nil)
	runner.WithRecorder(nil)

	rep, err := runner.Run(likedMessage("t-104", inAppStep("notify", "{{ payload.actor }} 赞了你的帖子")))
	if err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if rep.Sent != 1 || rep.Recorded != 0 {
		t.Errorf("未接记录器时只执行不记账: %+v", rep)
	}
	if entries, _ := inbox.List("creator_1"); len(entries) != 1 {
		t.Errorf("投递本身不受影响，收件箱应有 1 条，实际 %d", len(entries))
	}
}

// 明细写不进去不影响投递结果：消息已经发出去了，重投一次反而会再发一遍。
func TestRecordResultsStoreFailureDoesNotFailDelivery(t *testing.T) {
	inbox := pipeline.NewMemoryInboxStore()
	registry := pipeline.NewProviderRegistry()
	registry.Register(pipeline.NewInAppProvider(testLogger(), inbox))
	store := execution.NewMemoryStore(errors.New("磁盘只读"))
	runner := New(pipeline.NewWithRegistry(testLogger(), registry), nil, testLogger()).
		WithRecorder(execution.NewRecorder(store, testLogger()))

	rep, err := runner.Run(likedMessage("t-105", inAppStep("notify", "{{ payload.actor }} 赞了你的帖子")))
	if err != nil {
		t.Fatalf("明细写入失败不应影响执行结果: %v", err)
	}
	if rep.Sent != 1 || rep.Recorded != 0 {
		t.Errorf("投递应成功但不记账: %+v", rep)
	}
	if entries, _ := inbox.List("creator_1"); len(entries) != 1 {
		t.Errorf("收件箱应有 1 条，实际 %d", len(entries))
	}
}

// delay 续跑的步骤同样要回写：否则触发后过一段时间才发出的渠道在后台查不到。
func TestRecordResultsForResumedSteps(t *testing.T) {
	inbox := pipeline.NewMemoryInboxStore()
	registry := pipeline.NewProviderRegistry()
	registry.Register(pipeline.NewInAppProvider(testLogger(), inbox))
	store := execution.NewMemoryStore(nil)
	recorder := execution.NewRecorder(store, testLogger())
	pipe := pipeline.NewWithRegistry(testLogger(), registry)

	var pending pipeline.Deferral
	pipe.WithDeferrer(pipeline.DeferrerFunc(func(d pipeline.Deferral) error {
		pending = d
		return nil
	}))

	runner := New(pipe, nil, testLogger()).WithRecorder(recorder)
	if _, err := runner.Run(likedMessage("t-106",
		queue.StepSpec{ID: "wait", Type: string(domain.ChannelDelay), Config: map[string]any{"delay": "1m"}},
		inAppStep("later", "{{ payload.actor }} 赞了你的帖子"),
	)); err != nil {
		t.Fatalf("执行失败: %v", err)
	}
	if len(store.Records()) != 0 {
		t.Fatalf("延迟期间不应有投递明细，实际 %+v", store.Records())
	}

	payload, err := pending.Encode()
	if err != nil {
		t.Fatalf("编码续跑任务失败: %v", err)
	}
	_, results, err := pipe.Resume(payload)
	if err != nil {
		t.Fatalf("续跑失败: %v", err)
	}
	if written := RecordResults(recorder, testLogger(), pending.Message, results); written != 1 {
		t.Fatalf("续跑应回写 1 条明细，实际 %d", written)
	}
	records := store.Records()
	if len(records) != 1 || records[0].StepID != "later" || records[0].Status != execution.StatusSent {
		t.Fatalf("续跑明细不符: %+v", records)
	}
	if records[0].TriggerID != "t-106" {
		t.Errorf("续跑明细应归属原触发: %+v", records[0])
	}
}
