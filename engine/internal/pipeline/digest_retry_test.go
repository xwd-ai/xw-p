package pipeline

import (
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// 聚合卡片走的是同一个渠道层：普通消息能重试，卡片也该能重试。
func TestDigestDeliveryRetriesAndRecordsProvider(t *testing.T) {
	flaky := newFlaky("smtp-email", domain.ChannelEmail,
		SendResult{Sent: false, Reason: "连接被重置"},
		SendResult{Sent: false, Reason: "连接被重置"},
		SendResult{Sent: true, Reason: "已接收"},
	)
	reg := NewProviderRegistry()
	registerStub(t, reg, flaky, "smtp-email", 10)

	pipe := NewWithRegistry(discardLogger(), reg).
		WithProviderRetries(fastRetries(t, domain.ChannelEmail, 3))

	wf := likedDigestWorkflow(map[string]any{
		"key":      "post_id",
		"window":   "10m",
		"channel":  "email",
		"title":    "你的帖子有新互动",
		"template": "{{ digest.actor }} 等 {{ digest.count }} 人赞了你的帖子",
	})
	results := pipe.Run(NewContext(digestMessage("t_dg_retry", "P2001", "小林")), wf)

	if len(results) != 1 {
		t.Fatalf("步骤结果数 = %d, 期望 1", len(results))
	}
	res := results[0]
	if res.Error != "" {
		t.Fatalf("重试后应投递成功：%s", res.Error)
	}
	if got := flaky.count(); got != 3 {
		t.Errorf("卡片投递被调用 %d 次, 期望 3", got)
	}
	if res.ProviderRetries != 2 {
		t.Errorf("ProviderRetries = %d, 期望 2", res.ProviderRetries)
	}
	if res.UsedProvider != "smtp-email" {
		t.Errorf("实际实现 = %q, 期望 smtp-email", res.UsedProvider)
	}
	if res.AggregateCount != 1 {
		t.Errorf("聚合计数 = %d, 期望 1", res.AggregateCount)
	}
}
