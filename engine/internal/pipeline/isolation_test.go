package pipeline

import (
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// 隔离维度的缺省值是「老消息」的兼容层：升级前入队的消息没有 tenant /
// environment，投递与收件箱不能因此写出空字段——API 侧按字段过滤，空值
// 会让默认租户的视图出现不该有的条目。

func TestTenantOfFallsBackToDefault(t *testing.T) {
	cases := []struct {
		name string
		msg  queue.QueuedMessage
		want string
	}{
		{"未设置", queue.QueuedMessage{}, "default"},
		{"只有空白", queue.QueuedMessage{Tenant: "   "}, "default"},
		{"显式租户", queue.QueuedMessage{Tenant: "acme"}, "acme"},
		{"带空格的租户", queue.QueuedMessage{Tenant: " acme "}, "acme"},
	}
	for _, c := range cases {
		if got := tenantOf(c.msg); got != c.want {
			t.Errorf("%s: tenantOf = %q, 期望 %q", c.name, got, c.want)
		}
	}
}

func TestEnvironmentOfFallsBackToDevelopment(t *testing.T) {
	cases := []struct {
		name string
		msg  queue.QueuedMessage
		want string
	}{
		{"未设置", queue.QueuedMessage{}, "development"},
		{"只有空白", queue.QueuedMessage{Environment: "\t"}, "development"},
		{"生产环境", queue.QueuedMessage{Environment: "production"}, "production"},
	}
	for _, c := range cases {
		if got := environmentOf(c.msg); got != c.want {
			t.Errorf("%s: environmentOf = %q, 期望 %q", c.name, got, c.want)
		}
	}
}

// isolationWorkflow 复用 email 工作流跑一条消息，返回投递实现收到的请求。
func runEmailDispatch(t *testing.T, msg queue.QueuedMessage) Delivery {
	t.Helper()
	email := &stubProvider{
		id: "smtp-email", ch: domain.ChannelEmail,
		res: SendResult{Sent: true, Reason: "已发送"},
	}
	reg := NewProviderRegistry()
	registerStub(t, reg, email, "smtp-email", 10)

	results := NewWithRegistry(discardLogger(), reg).Run(NewContext(msg), emailWorkflow())
	if len(results) != 1 || !results[0].Sent {
		t.Fatalf("投递未成功：%+v", results)
	}
	return email.lastDelivery(t)
}

func TestDispatchCarriesTenantAndEnvironment(t *testing.T) {
	msg := queueMessageWithEmail()
	msg.Tenant = "acme"
	msg.Environment = "production"

	got := runEmailDispatch(t, msg)
	if got.Tenant != "acme" {
		t.Errorf("投递请求的租户 = %q, 期望 acme", got.Tenant)
	}
	if got.Environment != "production" {
		t.Errorf("投递请求的环境 = %q, 期望 production", got.Environment)
	}
}

func TestDispatchDefaultsIsolationDimensions(t *testing.T) {
	// 升级前入队的消息没有这两个字段：投递请求要带上缺省值，
	// 而不是空字符串。
	got := runEmailDispatch(t, queueMessageWithEmail())
	if got.Tenant != "default" {
		t.Errorf("投递请求的租户 = %q, 期望 default", got.Tenant)
	}
	if got.Environment != "development" {
		t.Errorf("投递请求的环境 = %q, 期望 development", got.Environment)
	}
}

func TestInAppEntryCarriesIsolationDimensions(t *testing.T) {
	p, store := newTestInAppProvider()
	res := p.Send(Delivery{
		Channel:      domain.ChannelInApp,
		Body:         "小林 评论了你的帖子",
		StepID:       "s1",
		TriggerID:    "t_iso",
		WorkflowName: "post_commented",
		SubscriberID: "sub_1",
		Tenant:       "acme",
		Environment:  "production",
	})
	if !res.Sent {
		t.Fatalf("站内信应投递成功：%+v", res)
	}

	entries, err := store.List("sub_1")
	if err != nil {
		t.Fatalf("读取收件箱失败：%v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("收件箱条目数 = %d, 期望 1", len(entries))
	}
	if entries[0].Tenant != "acme" || entries[0].Environment != "production" {
		t.Errorf("收件箱条目的隔离维度不正确：%+v", entries[0])
	}
}
