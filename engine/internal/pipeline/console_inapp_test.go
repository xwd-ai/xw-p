package pipeline

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

// newTestInAppProvider 构造挂在内存收件箱上的站内信 provider，并返回收件箱供断言。
func newTestInAppProvider() (Provider, *MemoryInboxStore) {
	store := NewMemoryInboxStore()
	return NewInAppProvider(discardLogger(), store), store
}

func TestInAppProviderWritesInboxEntry(t *testing.T) {
	p, store := newTestInAppProvider()
	if p.Channel() != domain.ChannelInApp {
		t.Errorf("渠道 = %s, 期望 in_app", p.Channel())
	}

	res := p.Send(Delivery{
		Channel:      domain.ChannelInApp,
		Subject:      "你的帖子有新评论",
		Body:         "小林 评论了你的帖子 P1001",
		StepID:       "s1",
		TriggerID:    "t_100",
		WorkflowName: "post_commented",
		SubscriberID: "sub_1",
	})
	if !res.Sent {
		t.Fatalf("站内信应投递成功, 结果 = %+v", res)
	}

	entries, err := store.List("sub_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("收件箱条目数 = %d, 期望 1", len(entries))
	}
	e := entries[0]
	if e.Title != "你的帖子有新评论" {
		t.Errorf("标题 = %q", e.Title)
	}
	if !strings.Contains(e.Body, "P1001") {
		t.Errorf("正文 = %q, 期望包含帖子 ID", e.Body)
	}
	if e.Workflow != "post_commented" || e.StepID != "s1" || e.TriggerID != "t_100" {
		t.Errorf("条目溯源字段不正确: %+v", e)
	}
	if e.ID != "t_100:s1" {
		t.Errorf("条目 ID = %q, 期望 t_100:s1", e.ID)
	}
	if e.Read {
		t.Error("引擎写入的条目应为未读")
	}
	if e.CreatedAt.IsZero() {
		t.Error("条目应带有写入时间")
	}
}

func TestInAppProviderIsIdempotent(t *testing.T) {
	p, store := newTestInAppProvider()
	d := Delivery{
		Body:         "有人赞了你的帖子",
		StepID:       "s1",
		TriggerID:    "t_200",
		WorkflowName: "post_liked",
		SubscriberID: "sub_1",
	}

	first := p.Send(d)
	second := p.Send(d)
	if !first.Sent || !second.Sent {
		t.Fatalf("重复投递均应视为成功: %+v / %+v", first, second)
	}
	if !strings.Contains(second.Reason, "已存在") {
		t.Errorf("第二次投递应说明幂等去重, 实际 %q", second.Reason)
	}
	if n := store.Len(); n != 1 {
		t.Errorf("收件箱条目数 = %d, 期望 1（幂等不新增）", n)
	}
}

func TestInAppProviderRequiresSubscriber(t *testing.T) {
	p, store := newTestInAppProvider()
	res := p.Send(Delivery{Body: "内容", StepID: "s1", TriggerID: "t_1"})
	if res.Sent {
		t.Error("缺少订阅者 ID 应投递失败")
	}
	if res.Reason == "" {
		t.Error("失败结果应携带原因说明")
	}
	if store.Len() != 0 {
		t.Error("失败时不应写入收件箱")
	}
}

func TestInAppProviderRejectsEmptyBody(t *testing.T) {
	p, store := newTestInAppProvider()
	res := p.Send(Delivery{Body: "   ", StepID: "s1", TriggerID: "t_1", SubscriberID: "sub_1"})
	if res.Sent {
		t.Error("正文为空应投递失败")
	}
	if store.Len() != 0 {
		t.Error("失败时不应写入收件箱")
	}
}

func TestInAppTitleFallback(t *testing.T) {
	cases := []struct {
		name string
		d    Delivery
		want string
	}{
		{"优先 subject", Delivery{Subject: "新评论", WorkflowName: "post_commented"}, "新评论"},
		{"回退工作流名", Delivery{WorkflowName: "post_liked"}, "post_liked"},
		{"最终兜底", Delivery{}, "新通知"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := inAppTitle(tc.d); got != tc.want {
				t.Errorf("inAppTitle = %q, 期望 %q", got, tc.want)
			}
		})
	}
}

func TestJSONLInboxStorePersistsAndDedupes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.jsonl")
	store, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("打开收件箱文件失败: %v", err)
	}

	entry := InboxEntry{
		ID:           "t_300:s1",
		TriggerID:    "t_300",
		SubscriberID: "sub_1",
		Workflow:     "post_mentioned",
		StepID:       "s1",
		Title:        "有人 @ 了你",
		Body:         "小林 在帖子 P1001 中提到了你",
		CreatedAt:    time.Now().UTC(),
	}
	if inserted, err := store.Save(entry); err != nil || !inserted {
		t.Fatalf("首次写入应成功: inserted=%v err=%v", inserted, err)
	}
	if inserted, err := store.Save(entry); err != nil || inserted {
		t.Fatalf("重复写入应被去重: inserted=%v err=%v", inserted, err)
	}

	// 重新打开同一文件：应能从文件回放出去重索引。
	reopened, err := NewJSONLInboxStore(path)
	if err != nil {
		t.Fatalf("重新打开收件箱文件失败: %v", err)
	}
	if inserted, _ := reopened.Save(entry); inserted {
		t.Error("重启后仍应识别重复条目")
	}

	entries, err := reopened.List("sub_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("收件箱条目数 = %d, 期望 1", len(entries))
	}
	if entries[0].Body != entry.Body {
		t.Errorf("回放正文 = %q", entries[0].Body)
	}
	if other, _ := reopened.List("sub_2"); len(other) != 0 {
		t.Errorf("其他订阅者不应看到该条目, 实际 %d 条", len(other))
	}
}

func TestDispatchInAppViaProvider(t *testing.T) {
	store := NewMemoryInboxStore()
	registry := NewProviderRegistry()
	registry.Register(NewInAppProvider(discardLogger(), store))
	p := NewWithRegistry(nil, registry)

	wf := domain.Workflow{
		ID:     "wf_liked",
		Name:   "post_liked",
		Active: true,
		Steps: []domain.Step{
			{
				ID:   "s1",
				Type: domain.ChannelInApp,
				Name: "点赞站内信",
				Config: map[string]any{
					"title":    "你的帖子被点赞了",
					"template": "{{ payload.actor }} 赞了你的帖子 {{ payload.post_id }}",
				},
			},
		},
	}
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t_400",
		Name:         "post_liked",
		SubscriberID: "sub_1",
		Payload:      map[string]any{"post_id": "P1001", "actor": "小林"},
	})

	results := p.Run(ctx, wf)
	if len(results) != 1 {
		t.Fatalf("投递结果数 = %d, 期望 1", len(results))
	}
	if results[0].Error != "" {
		t.Fatalf("站内信步骤出错: %s", results[0].Error)
	}
	if !results[0].Sent {
		t.Error("站内信步骤应标记为已发送")
	}

	entries, _ := store.List("sub_1")
	if len(entries) != 1 {
		t.Fatalf("收件箱条目数 = %d, 期望 1", len(entries))
	}
	if want := "小林 赞了你的帖子 P1001"; entries[0].Body != want {
		t.Errorf("收件箱正文 = %q, 期望 %q", entries[0].Body, want)
	}
	if entries[0].Title != "你的帖子被点赞了" {
		t.Errorf("收件箱标题 = %q", entries[0].Title)
	}
}
