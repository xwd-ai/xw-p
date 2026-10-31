package pipeline

import (
	"context"
	"encoding/base64"
	"mime"
	"strings"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/mailer"
	"github.com/xwd-ai/xw-p/engine/internal/mailer/mailertest"
	"github.com/xwd-ai/xw-p/engine/internal/provider"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// TestSMTPEmailProviderDelivers 验证管道层把渲染结果交给 SMTP 客户端后，
// 服务器收到的信封、主题与正文与步骤产出一致。
func TestSMTPEmailProviderDelivers(t *testing.T) {
	srv, err := mailertest.New(mailertest.Config{})
	if err != nil {
		t.Fatalf("启动测试服务器失败：%v", err)
	}
	defer srv.Close()

	emailProvider, err := NewSMTPEmailProvider(discardLogger(), mailer.Config{
		Host:    srv.Host(),
		Port:    srv.Port(),
		From:    "互动提醒 <notify@example.com>",
		TLS:     mailer.TLSDisabled,
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("构造 smtp provider 失败：%v", err)
	}
	if emailProvider.Channel() != domain.ChannelEmail {
		t.Errorf("渠道 = %s，期望 email", emailProvider.Channel())
	}
	if id, ok := emailProvider.(provider.Identifier); !ok || id.ID() != "smtp-email" {
		t.Errorf("实现标识不正确：%v", emailProvider)
	}

	res := emailProvider.Send(Delivery{
		Channel:      domain.ChannelEmail,
		To:           "creator@example.com",
		Subject:      "阿岚 评论了你的帖子《周末骑行》",
		Body:         "「这段路线我也骑过，风景很好。」",
		StepID:       "email-step",
		WorkflowName: "post_commented",
		SubscriberID: "creator-1",
	})
	if !res.Sent {
		t.Fatalf("投递应成功，实际失败：%s", res.Reason)
	}
	if !strings.Contains(res.Reason, "creator@example.com") {
		t.Errorf("成功原因应写明收件人：%q", res.Reason)
	}

	mails := srv.Mails()
	if len(mails) != 1 {
		t.Fatalf("服务器应收到 1 封，实际 %d", len(mails))
	}
	if mails[0].From != "notify@example.com" || len(mails[0].To) != 1 || mails[0].To[0] != "creator@example.com" {
		t.Errorf("信封不正确：%+v", mails[0])
	}
}

// TestSMTPEmailProviderRequiresRecipient 缺收件邮箱时直接判失败，不连网。
func TestSMTPEmailProviderRequiresRecipient(t *testing.T) {
	srv, err := mailertest.New(mailertest.Config{})
	if err != nil {
		t.Fatalf("启动测试服务器失败：%v", err)
	}
	defer srv.Close()

	emailProvider, err := NewSMTPEmailProvider(discardLogger(), mailer.Config{
		Host:    srv.Host(),
		Port:    srv.Port(),
		From:    "notify@example.com",
		TLS:     mailer.TLSDisabled,
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatalf("构造 smtp provider 失败：%v", err)
	}

	res := emailProvider.Send(Delivery{Channel: domain.ChannelEmail, To: "  "})
	if res.Sent {
		t.Fatal("缺少收件邮箱时应失败")
	}
	if !strings.Contains(res.Reason, "收件邮箱") {
		t.Errorf("失败原因应指出缺少收件邮箱：%q", res.Reason)
	}
	if srv.Accepted() != 0 {
		t.Error("缺地址时不应连到服务器")
	}
}

// TestSMTPEmailProviderReportsDeliveryFailure 中继不可达时把原因带回执行结果。
func TestSMTPEmailProviderReportsDeliveryFailure(t *testing.T) {
	srv, err := mailertest.New(mailertest.Config{})
	if err != nil {
		t.Fatalf("启动测试服务器失败：%v", err)
	}
	port := srv.Port()
	_ = srv.Close() // 关掉监听，制造「连不上」的情况

	emailProvider, err := NewSMTPEmailProvider(discardLogger(), mailer.Config{
		Host:    "127.0.0.1",
		Port:    port,
		From:    "notify@example.com",
		TLS:     mailer.TLSDisabled,
		Timeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("构造 smtp provider 失败：%v", err)
	}

	// dispatch 在步骤超时时要能中断等待，因此实现必须支持 ContextSender。
	ctxSender, ok := emailProvider.(ContextSender)
	if !ok {
		t.Fatalf("smtp provider 应实现 ContextSender")
	}
	res := ctxSender.SendContext(context.Background(), Delivery{
		Channel: domain.ChannelEmail,
		To:      "creator@example.com",
		Subject: "新点赞",
		Body:    "正文",
	})
	if res.Sent {
		t.Fatal("中继不可达时应失败")
	}
	if !strings.Contains(res.Reason, "连接") {
		t.Errorf("失败原因应指出连接阶段：%q", res.Reason)
	}
}

// TestProviderFactoryBuildsSMTP 验证工厂按 kind=smtp 构造真实实现，
// 并在凭据写错时拒绝装配。
func TestProviderFactoryBuildsSMTP(t *testing.T) {
	factory := ProviderFactory(discardLogger())
	sender, err := factory.Build(provider.Config{
		ID:      "email-smtp",
		Channel: domain.ChannelEmail,
		Kind:    provider.KindSMTP,
		Credentials: provider.Credentials{
			"host": "smtp.example.com",
			"from": "notify@example.com",
		},
	})
	if err != nil {
		t.Fatalf("构造失败：%v", err)
	}
	if sender.Channel() != domain.ChannelEmail {
		t.Errorf("渠道 = %s，期望 email", sender.Channel())
	}

	if _, err := factory.Build(provider.Config{
		ID:      "email-smtp",
		Channel: domain.ChannelSMS,
		Kind:    provider.KindSMTP,
		Credentials: provider.Credentials{
			"host": "smtp.example.com", "from": "notify@example.com",
		},
	}); err == nil {
		t.Error("smtp 用于 sms 渠道时应失败")
	}

	if _, err := factory.Build(provider.Config{
		ID:      "email-smtp",
		Channel: domain.ChannelEmail,
		Kind:    provider.KindSMTP,
		Credentials: provider.Credentials{
			"host": "smtp.example.com", "from": "notify@example.com", "port": "abc",
		},
	}); err == nil || !strings.Contains(err.Error(), "端口") {
		t.Errorf("端口非数字时应报错，实际 %v", err)
	}

	if _, err := ProviderFactory(discardLogger()).Build(provider.Config{
		ID: "sms-twilio", Channel: domain.ChannelSMS, Kind: provider.KindTwilio,
	}); err == nil {
		t.Error("尚未接入的种类应返回错误")
	}
}

// TestDefaultRegistryUsesConsoleEmail 确认默认装配仍是 console：没有配置
// 真实中继时不发网络请求。
func TestDefaultRegistryUsesConsoleEmail(t *testing.T) {
	registry := DefaultProviderRegistry(discardLogger())
	entry, ok := registry.Select(domain.ChannelEmail)
	if !ok {
		t.Fatal("email 渠道应有默认实现")
	}
	if entry.ID != "console-email" {
		t.Errorf("默认 email 实现 = %q，期望 console-email", entry.ID)
	}
}

// TestDispatchDeliversEmailThroughSMTP 覆盖「管线 → 分发阶段 → SMTP」整条路径：
// 渲染后的主题与正文要真的落到中继上，而不是停在日志里。
func TestDispatchDeliversEmailThroughSMTP(t *testing.T) {
	srv, err := mailertest.New(mailertest.Config{})
	if err != nil {
		t.Fatalf("启动测试服务器失败：%v", err)
	}
	defer srv.Close()

	smtpSender, err := NewSMTPEmailProvider(discardLogger(), mailer.Config{
		Host:    srv.Host(),
		Port:    srv.Port(),
		From:    "互动提醒 <notify@example.com>",
		TLS:     mailer.TLSDisabled,
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("构造 smtp provider 失败：%v", err)
	}

	registry := NewProviderRegistry()
	if err := registry.RegisterEntry(provider.Entry{
		ID:       "email-smtp",
		Channel:  domain.ChannelEmail,
		Priority: 10,
		Enabled:  true,
		Sender:   smtpSender,
	}); err != nil {
		t.Fatalf("登记 provider 失败：%v", err)
	}
	pipe := NewWithRegistry(discardLogger(), registry)

	msg := queue.QueuedMessage{
		TriggerID:    "trig-smtp-1",
		Name:         "post_commented",
		SubscriberID: "creator_1001",
		Email:        "creator@example.com",
		Payload: map[string]any{
			"actor":      "阿岚",
			"post_title": "周末骑行",
			"comment":    "这条路线我也骑过",
		},
	}
	wf := domain.Workflow{
		Name: "post_commented",
		Steps: []domain.Step{{
			ID:   "comment_email",
			Type: domain.ChannelEmail,
			Config: map[string]any{
				"subject":  "《{{ payload.post_title }}》有新评论",
				"template": "{{ payload.actor }} 说：{{ payload.comment }}",
			},
		}},
	}

	results := pipe.Run(NewContext(msg), wf)
	if len(results) != 1 || !results[0].Sent {
		t.Fatalf("邮件步骤应投递成功，实际 %+v", results)
	}

	mails := srv.Mails()
	if len(mails) != 1 {
		t.Fatalf("中继应收到 1 封，实际 %d", len(mails))
	}
	if mails[0].From != "notify@example.com" || mails[0].To[0] != "creator@example.com" {
		t.Errorf("信封不正确：%+v", mails[0])
	}
	subject, body := decodeMail(t, mails[0].Data)
	if subject != "《周末骑行》有新评论" {
		t.Errorf("主题 = %q，期望渲染后的标题", subject)
	}
	if !strings.Contains(body, "阿岚 说：这条路线我也骑过") {
		t.Errorf("正文 = %q，期望渲染后的内容", body)
	}
}

// decodeMail 解开 RFC 2047 主题与 base64 正文，用于断言渲染结果。
func decodeMail(t *testing.T, raw string) (string, string) {
	t.Helper()
	head, bodyRaw, ok := strings.Cut(raw, "\r\n\r\n")
	if !ok {
		t.Fatalf("报文缺少头与正文的分隔：%q", raw)
	}
	var subjectLine string
	for _, line := range strings.Split(head, "\r\n") {
		if value, found := strings.CutPrefix(line, "Subject: "); found {
			subjectLine = value
		}
	}
	subject, err := new(mime.WordDecoder).DecodeHeader(subjectLine)
	if err != nil {
		t.Fatalf("主题解码失败：%v", err)
	}
	compact := strings.Join(strings.Fields(bodyRaw), "")
	decoded, err := base64.StdEncoding.DecodeString(compact)
	if err != nil {
		t.Fatalf("正文不是合法 base64：%v", err)
	}
	return subject, string(decoded)
}
