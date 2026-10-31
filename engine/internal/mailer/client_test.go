package mailer

import (
	"context"
	"encoding/base64"
	"errors"
	"mime"
	"strings"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/mailer/mailertest"
)

// 测试里固定使用的收发双方。
const (
	testFrom = "通知 <notify@example.com>"
	testTo   = "creator@example.com"
)

// startFake 启动测试服务器并在用例结束时关闭。
func startFake(t *testing.T, cfg mailertest.Config) *mailertest.Server {
	t.Helper()
	srv, err := mailertest.New(cfg)
	if err != nil {
		t.Fatalf("启动测试服务器失败：%v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

// newTestClient 构造指向测试服务器的客户端。
func newTestClient(t *testing.T, srv *mailertest.Server, mutate func(*Config)) *Client {
	t.Helper()
	cfg := Config{
		Host:    srv.Host(),
		Port:    srv.Port(),
		From:    testFrom,
		TLS:     TLSDisabled,
		Timeout: 2 * time.Second,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	client, err := New(cfg)
	if err != nil {
		t.Fatalf("构造客户端失败：%v", err)
	}
	return client
}

// splitMail 把报文拆成头字段与解码后的正文。
func splitMail(t *testing.T, raw string) (map[string]string, string) {
	t.Helper()
	head, body, ok := strings.Cut(raw, "\r\n\r\n")
	if !ok {
		t.Fatalf("报文缺少头与正文的分隔：%q", raw)
	}
	headers := make(map[string]string)
	for _, line := range strings.Split(head, "\r\n") {
		name, value, ok := strings.Cut(line, ": ")
		if !ok {
			t.Fatalf("头字段格式不正确：%q", line)
		}
		headers[name] = value
	}
	compact := strings.Join(strings.Fields(body), "")
	decoded, err := base64.StdEncoding.DecodeString(compact)
	if err != nil {
		t.Fatalf("正文不是合法 base64：%v", err)
	}
	return headers, string(decoded)
}

func TestSendPlainDelivery(t *testing.T) {
	srv := startFake(t, mailertest.Config{})
	client := newTestClient(t, srv, nil)

	subject := "你的帖子《周末骑行》有 3 个新点赞"
	body := "阿岚 赞了你的帖子\n.行首带点，不能当成结束标记\n阿岚 还留了言"
	receipt, err := client.Send(context.Background(), Message{
		From:    testFrom,
		To:      []string{testTo, testTo}, // 重复收件人只投一次
		Subject: subject,
		Text:    body,
	})
	if err != nil {
		t.Fatalf("投递失败：%v", err)
	}
	if !strings.HasPrefix(receipt.MessageID, "<") || !strings.HasSuffix(receipt.MessageID, "@example.com>") {
		t.Errorf("Message-ID 格式不正确：%q", receipt.MessageID)
	}
	if !strings.Contains(receipt.Reply, "queued as") {
		t.Errorf("回执应带服务器回复，实际 %q", receipt.Reply)
	}

	mails := srv.Mails()
	if len(mails) != 1 {
		t.Fatalf("服务器应收到 1 封，实际 %d", len(mails))
	}
	if mails[0].From != "notify@example.com" {
		t.Errorf("信封发件人 = %q，期望 notify@example.com", mails[0].From)
	}
	if len(mails[0].To) != 1 || mails[0].To[0] != testTo {
		t.Errorf("信封收件人 = %v，期望去重后的单个地址", mails[0].To)
	}

	headers, text := splitMail(t, mails[0].Data)
	decoder := new(mime.WordDecoder)
	decodedSubject, err := decoder.DecodeHeader(headers["Subject"])
	if err != nil {
		t.Fatalf("主题解码失败：%v", err)
	}
	if decodedSubject != subject {
		t.Errorf("主题 = %q，期望 %q", decodedSubject, subject)
	}
	if !strings.Contains(headers["From"], "<notify@example.com>") {
		t.Errorf("From 头未保留发件人地址：%q", headers["From"])
	}
	if headers["To"] != testTo {
		t.Errorf("To 头 = %q，期望 %q", headers["To"], testTo)
	}
	if headers["Message-ID"] != receipt.MessageID {
		t.Errorf("报文里的 Message-ID 与回执不一致：%q / %q", headers["Message-ID"], receipt.MessageID)
	}
	if headers["Content-Type"] != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type = %q", headers["Content-Type"])
	}
	if text != body {
		t.Errorf("正文 = %q，期望 %q", text, body)
	}
}

func TestSendUpgradesStartTLSAndAuthenticates(t *testing.T) {
	srv := startFake(t, mailertest.Config{StartTLS: true, AuthUser: "notify", AuthPass: "s3cret"})
	client := newTestClient(t, srv, func(cfg *Config) {
		cfg.TLS = TLSStartTLS
		cfg.Username = "notify"
		cfg.Password = "s3cret"
		cfg.SkipVerify = true // 自签证书
	})

	if _, err := client.Send(context.Background(), Message{
		From:    testFrom,
		To:      []string{testTo},
		Subject: "新评论",
		Text:    "有人评论了你的帖子",
	}); err != nil {
		t.Fatalf("投递失败：%v", err)
	}
	if got := srv.Upgrades(); got != 1 {
		t.Errorf("STARTTLS 升级次数 = %d，期望 1", got)
	}
	mails := srv.Mails()
	if len(mails) != 1 {
		t.Fatalf("服务器应收到 1 封，实际 %d", len(mails))
	}
	if mails[0].AuthUser != "notify" {
		t.Errorf("认证用户 = %q，期望 notify", mails[0].AuthUser)
	}
}

func TestSendFailsWhenStartTLSNotAdvertised(t *testing.T) {
	srv := startFake(t, mailertest.Config{})
	client := newTestClient(t, srv, func(cfg *Config) {
		cfg.TLS = TLSStartTLS
	})

	_, err := client.Send(context.Background(), Message{
		From:    testFrom,
		To:      []string{testTo},
		Subject: "新点赞",
		Text:    "正文",
	})
	if err == nil {
		t.Fatal("服务器未宣告 STARTTLS 时应失败")
	}
	if !strings.Contains(err.Error(), "STARTTLS") {
		t.Errorf("错误应指出 STARTTLS：%v", err)
	}
	if got := srv.Upgrades(); got != 0 {
		t.Errorf("不应发生 TLS 升级，实际 %d 次", got)
	}
	if len(srv.Mails()) != 0 {
		t.Error("未加密连接不应投递成功")
	}
}

func TestSendAuthRejected(t *testing.T) {
	srv := startFake(t, mailertest.Config{StartTLS: true, AuthUser: "notify", AuthPass: "right"})
	client := newTestClient(t, srv, func(cfg *Config) {
		cfg.TLS = TLSStartTLS
		cfg.Username = "notify"
		cfg.Password = "wrong"
		cfg.SkipVerify = true
	})

	_, err := client.Send(context.Background(), Message{
		From:    testFrom,
		To:      []string{testTo},
		Subject: "新提及",
		Text:    "正文",
	})
	if err == nil {
		t.Fatal("认证失败时应返回错误")
	}
	var smtpErr *Error
	if !errors.As(err, &smtpErr) {
		t.Fatalf("错误类型应为 *Error，实际 %T", err)
	}
	if smtpErr.Code != 535 {
		t.Errorf("回复码 = %d，期望 535", smtpErr.Code)
	}
	if !strings.Contains(err.Error(), "认证") {
		t.Errorf("错误应指出认证阶段：%v", err)
	}
	if strings.Contains(err.Error(), "wrong") {
		t.Errorf("错误信息不应包含口令：%v", err)
	}
	if len(srv.Mails()) != 0 {
		t.Error("认证失败后不应有邮件进入队列")
	}
}

func TestSendRecipientRejected(t *testing.T) {
	srv := startFake(t, mailertest.Config{RejectRecipient: "gone@example.com"})
	client := newTestClient(t, srv, nil)

	_, err := client.Send(context.Background(), Message{
		From:    testFrom,
		To:      []string{testTo, "gone@example.com"},
		Subject: "新点赞",
		Text:    "正文",
	})
	if err == nil {
		t.Fatal("收件人被拒时应返回错误")
	}
	if !strings.Contains(err.Error(), "550") || !strings.Contains(err.Error(), "gone@example.com") {
		t.Errorf("错误应同时带上回复码与被拒地址：%v", err)
	}
}

func TestSendRejectsHeaderInjection(t *testing.T) {
	srv := startFake(t, mailertest.Config{})
	client := newTestClient(t, srv, nil)

	_, err := client.Send(context.Background(), Message{
		From:    testFrom,
		To:      []string{testTo},
		Subject: "新点赞\r\nBcc: victim@example.com",
		Text:    "正文",
	})
	if err == nil {
		t.Fatal("主题里带换行时应失败")
	}
	if !strings.Contains(err.Error(), "换行") {
		t.Errorf("错误应指出换行符：%v", err)
	}
	if srv.Accepted() != 0 {
		t.Error("非法报文不应连到服务器")
	}
}

func TestSendRespectsContextDeadline(t *testing.T) {
	srv := startFake(t, mailertest.Config{Silent: true})
	client := newTestClient(t, srv, func(cfg *Config) {
		cfg.Timeout = 5 * time.Second
	})

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	started := time.Now()
	_, err := client.Send(ctx, Message{
		From:    testFrom,
		To:      []string{testTo},
		Subject: "新点赞",
		Text:    "正文",
	})
	if err == nil {
		t.Fatal("服务器不说话时应因超时失败")
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Errorf("取消耗时过长：%s", elapsed)
	}
}

func TestSendRequiresRecipient(t *testing.T) {
	srv := startFake(t, mailertest.Config{})
	client := newTestClient(t, srv, nil)

	if _, err := client.Send(context.Background(), Message{From: testFrom, Subject: "x"}); err == nil {
		t.Fatal("没有收件人时应失败")
	}
	if srv.Accepted() != 0 {
		t.Error("没有收件人时不应连网投递")
	}
}

func TestFormatAddressWithDisplayName(t *testing.T) {
	out := formatAddress("通知 <notify@example.com>")
	if !strings.Contains(out, "<notify@example.com>") {
		t.Fatalf("应保留地址：%q", out)
	}
	decoder := new(mime.WordDecoder)
	name, err := decoder.DecodeHeader(strings.TrimSuffix(out, " <notify@example.com>"))
	if err != nil {
		t.Fatalf("显示名解码失败：%v", err)
	}
	if name != "通知" {
		t.Errorf("显示名 = %q，期望 通知", name)
	}
}
