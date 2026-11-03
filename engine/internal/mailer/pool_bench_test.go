package mailer

import (
	"context"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/mailer/mailertest"
)

// benchClient 准备一台进程内中继与一个客户端，供各组基准共用。
//
// secure 为真时走 STARTTLS + AUTH PLAIN，与真实中继（587 端口）的形态一致：
// 这种情况下每条消息的固定成本多了两次 TLS 握手与一次认证往返。
func benchClient(b *testing.B, poolSize int, secure bool) *Client {
	b.Helper()
	serverCfg := mailertest.Config{}
	clientCfg := Config{
		From:     "notify@example.com",
		TLS:      TLSDisabled,
		Timeout:  5 * time.Second,
		PoolSize: poolSize,
	}
	if secure {
		serverCfg = mailertest.Config{StartTLS: true, AuthUser: "notify", AuthPass: "s3cret"}
		clientCfg.TLS = TLSStartTLS
		clientCfg.Username = "notify"
		clientCfg.Password = "s3cret"
		clientCfg.SkipVerify = true // 自签证书
	}

	srv, err := mailertest.New(serverCfg)
	if err != nil {
		b.Fatalf("启动测试服务器失败：%v", err)
	}
	b.Cleanup(func() { _ = srv.Close() })
	clientCfg.Host = srv.Host()
	clientCfg.Port = srv.Port()

	client, err := New(clientCfg)
	if err != nil {
		b.Fatalf("构造客户端失败：%v", err)
	}
	b.Cleanup(func() { _ = client.Close() })
	return client
}

// benchMessage 是基准里固定发送的通知邮件。
func benchMessage() Message {
	return Message{
		From:    "notify@example.com",
		To:      []string{"creator@example.com"},
		Subject: "《周末骑行》有 3 个新点赞",
		Text:    "阿岚、小林、老周 赞了你的帖子。",
	}
}

// runSendBench 循环投递，出错即中止（基准里的失败没有统计意义）。
func runSendBench(b *testing.B, client *Client) {
	ctx := context.Background()
	msg := benchMessage()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.Send(ctx, msg); err != nil {
			b.Fatalf("投递失败：%v", err)
		}
	}
}

// BenchmarkSendReused 明文链路上复用连接的投递成本。
func BenchmarkSendReused(b *testing.B) {
	runSendBench(b, benchClient(b, DefaultPoolSize, false))
}

// BenchmarkSendPerMessage 明文链路上每条消息新建连接，作为复用前的基线。
func BenchmarkSendPerMessage(b *testing.B) {
	runSendBench(b, benchClient(b, -1, false))
}

// BenchmarkSendReusedTLS 加密链路（STARTTLS + 认证）上复用连接的成本。
func BenchmarkSendReusedTLS(b *testing.B) {
	runSendBench(b, benchClient(b, DefaultPoolSize, true))
}

// BenchmarkSendPerMessageTLS 加密链路上每条消息重新握手与认证。
func BenchmarkSendPerMessageTLS(b *testing.B) {
	runSendBench(b, benchClient(b, -1, true))
}
