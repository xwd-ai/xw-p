package mailer

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/mailer/mailertest"
)

// sendNote 发一封内容固定的通知邮件，用于只看连接行为的用例。
func sendNote(ctx context.Context, t *testing.T, client *Client, subject string) error {
	t.Helper()
	_, err := client.Send(ctx, Message{
		From:    testFrom,
		To:      []string{testTo},
		Subject: subject,
		Text:    "阿岚 赞了你的帖子《周末骑行》",
	})
	return err
}

// TestClientReusesConnection 连续投递复用同一条已登录连接。
//
// 这是「批量发送」省下来的固定成本所在：握手、EHLO、认证每条消息都要走一遍，
// 复用后只有第一封付这笔钱。
func TestClientReusesConnection(t *testing.T) {
	srv := startFake(t, mailertest.Config{})
	client := newTestClient(t, srv, nil)

	const rounds = 5
	for i := 0; i < rounds; i++ {
		if err := sendNote(context.Background(), t, client, "新点赞"); err != nil {
			t.Fatalf("第 %d 封投递失败：%v", i+1, err)
		}
	}

	if got := srv.Accepted(); got != 1 {
		t.Errorf("连接数 = %d，期望 5 封共用 1 条连接", got)
	}
	if got := len(srv.Mails()); got != rounds {
		t.Errorf("服务器收到 %d 封，期望 %d 封", got, rounds)
	}
	stats := client.PoolStats()
	if stats.Opened != 1 || stats.Reused != rounds-1 {
		t.Errorf("池统计 opened=%d reused=%d，期望 opened=1 reused=%d", stats.Opened, stats.Reused, rounds-1)
	}
	if stats.Capacity != DefaultPoolSize {
		t.Errorf("池容量 = %d，期望默认值 %d", stats.Capacity, DefaultPoolSize)
	}
}

// TestClientReuseKeepsAuthentication 复用的连接仍然是已认证状态。
//
// 服务器按连接记录认证结果，第二封不再走 AUTH 也必须能投递成功；否则复用
// 会把「认证成功」变成一次性优惠。
func TestClientReuseKeepsAuthentication(t *testing.T) {
	srv := startFake(t, mailertest.Config{StartTLS: true, AuthUser: "notify", AuthPass: "s3cret"})
	client := newTestClient(t, srv, func(cfg *Config) {
		cfg.TLS = TLSStartTLS
		cfg.Username = "notify"
		cfg.Password = "s3cret"
		cfg.SkipVerify = true // 自签证书
	})

	for i := 0; i < 3; i++ {
		if err := sendNote(context.Background(), t, client, "新评论"); err != nil {
			t.Fatalf("第 %d 封投递失败：%v", i+1, err)
		}
	}
	if got := srv.Accepted(); got != 1 {
		t.Errorf("连接数 = %d，期望 1（复用同一条已认证连接）", got)
	}
	if got := srv.Upgrades(); got != 1 {
		t.Errorf("STARTTLS 升级次数 = %d，期望 1", got)
	}
	mails := srv.Mails()
	if len(mails) != 3 {
		t.Fatalf("服务器收到 %d 封，期望 3 封", len(mails))
	}
	for i, mail := range mails {
		if mail.AuthUser != "notify" {
			t.Errorf("第 %d 封的认证用户 = %q，期望 notify", i+1, mail.AuthUser)
		}
	}
}

// TestClientDropsConnectionAfterFailure 投递失败后不复用这条连接。
//
// 收件人被拒时协议停在事务中间，继续用它发下一条只会把问题带过去；
// 客户端应当关掉并重连。
func TestClientDropsConnectionAfterFailure(t *testing.T) {
	srv := startFake(t, mailertest.Config{RejectRecipient: "blocked@example.com"})
	client := newTestClient(t, srv, nil)

	_, err := client.Send(context.Background(), Message{
		From:    testFrom,
		To:      []string{"blocked@example.com"},
		Subject: "新评论",
		Text:    "这条会被拒收",
	})
	if err == nil {
		t.Fatal("收件人被拒时应返回错误")
	}
	if !strings.Contains(err.Error(), "550") {
		t.Errorf("错误信息应带服务器回复码：%v", err)
	}

	if err := sendNote(context.Background(), t, client, "新评论"); err != nil {
		t.Fatalf("失败后重投应成功：%v", err)
	}
	if got := srv.Accepted(); got != 2 {
		t.Errorf("连接数 = %d，期望 2（失败连接不复用）", got)
	}
	stats := client.PoolStats()
	if stats.Reused != 0 {
		t.Errorf("复用次数 = %d，期望 0", stats.Reused)
	}
	if stats.Discarded != 1 {
		t.Errorf("丢弃次数 = %d，期望 1", stats.Discarded)
	}
}

// TestClientDiscardsExpiredIdleConnection 空闲超时的连接不再复用。
func TestClientDiscardsExpiredIdleConnection(t *testing.T) {
	srv := startFake(t, mailertest.Config{})
	client := newTestClient(t, srv, func(cfg *Config) {
		cfg.IdleTimeout = 20 * time.Millisecond
	})

	if err := sendNote(context.Background(), t, client, "新点赞"); err != nil {
		t.Fatalf("首封投递失败：%v", err)
	}
	time.Sleep(60 * time.Millisecond)
	if err := sendNote(context.Background(), t, client, "新点赞"); err != nil {
		t.Fatalf("过期后重投失败：%v", err)
	}

	if got := srv.Accepted(); got != 2 {
		t.Errorf("连接数 = %d，期望 2（空转过期后重连）", got)
	}
	if stats := client.PoolStats(); stats.Reused != 0 {
		t.Errorf("复用次数 = %d，期望 0", stats.Reused)
	}
}

// closeIdleInPlace 关掉池里的空闲连接但保留记录，模拟对端先断开、本地尚未
// 察觉的状态。
func closeIdleInPlace(t *testing.T, client *Client) {
	t.Helper()
	pool := client.pool
	if pool == nil {
		t.Fatal("客户端未启用连接池")
	}
	pool.mu.Lock()
	if len(pool.idle) == 0 {
		pool.mu.Unlock()
		t.Fatal("池里应有一条空闲连接")
	}
	sess := pool.idle[0].sess
	pool.mu.Unlock()
	sess.close()
}

// TestClientReplacesDeadSession 池里留着一条对端已断开的连接时，取用前的
// 存活确认能发现并换成新连接，投递照常成功。
func TestClientReplacesDeadSession(t *testing.T) {
	srv := startFake(t, mailertest.Config{})
	client := newTestClient(t, srv, nil)

	if err := sendNote(context.Background(), t, client, "新点赞"); err != nil {
		t.Fatalf("首封投递失败：%v", err)
	}
	// 模拟中继侧按会话超时把连接关掉，本地池还不知道。
	closeIdleInPlace(t, client)

	if err := sendNote(context.Background(), t, client, "新点赞"); err != nil {
		t.Fatalf("重连后投递应成功：%v", err)
	}
	if got := srv.Accepted(); got != 2 {
		t.Errorf("连接数 = %d，期望 2", got)
	}
	if stats := client.PoolStats(); stats.Discarded != 1 {
		t.Errorf("丢弃次数 = %d，期望 1（存活确认失败的那条）", stats.Discarded)
	}
}

// TestClientPoolCapacityBounded 并发投递时池只保留容量上限的空闲连接，
// 峰值并发不受容量限制（容量约束的是闲置资源，不是并发度）。
func TestClientPoolCapacityBounded(t *testing.T) {
	srv := startFake(t, mailertest.Config{})
	client := newTestClient(t, srv, func(cfg *Config) { cfg.PoolSize = 2 })

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- sendNote(context.Background(), t, client, "新评论")
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("并发投递失败：%v", err)
		}
	}

	if got := len(srv.Mails()); got != workers {
		t.Errorf("服务器收到 %d 封，期望 %d 封", got, workers)
	}
	stats := client.PoolStats()
	if stats.Idle > 2 {
		t.Errorf("空闲连接 %d 条，超过容量 2", stats.Idle)
	}
	// 8 条并发至少要 8 条连接（池不会让投递排队等连接），复用只能发生在
	// 并发退去之后的回收阶段，因此这里只约束上界。
	if accepted := srv.Accepted(); accepted > workers {
		t.Errorf("连接数 %d 超过并发数 %d", accepted, workers)
	}
}

// TestClientCloseReleasesIdle 关闭后空闲连接被释放，客户端仍可用于后续投递
// （重新建连），且重复关闭不报错。
func TestClientCloseReleasesIdle(t *testing.T) {
	srv := startFake(t, mailertest.Config{})
	client := newTestClient(t, srv, nil)

	if err := sendNote(context.Background(), t, client, "新点赞"); err != nil {
		t.Fatalf("首封投递失败：%v", err)
	}
	if stats := client.PoolStats(); stats.Idle != 1 {
		t.Fatalf("关闭前应有 1 条空闲连接，实际 %d", stats.Idle)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("关闭失败：%v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("重复关闭不应报错：%v", err)
	}
	if stats := client.PoolStats(); stats.Idle != 0 {
		t.Errorf("关闭后空闲连接应为 0，实际 %d", stats.Idle)
	}

	if err := sendNote(context.Background(), t, client, "新点赞"); err != nil {
		t.Fatalf("关闭后仍应能重新建连投递：%v", err)
	}
	if got := srv.Accepted(); got != 2 {
		t.Errorf("连接数 = %d，期望 2", got)
	}
}

// TestClientPoolDisabled 配置为不复用时，每条消息各建一条连接。
func TestClientPoolDisabled(t *testing.T) {
	srv := startFake(t, mailertest.Config{})
	client := newTestClient(t, srv, func(cfg *Config) { cfg.PoolSize = -1 })

	for i := 0; i < 3; i++ {
		if err := sendNote(context.Background(), t, client, "新提及"); err != nil {
			t.Fatalf("第 %d 封投递失败：%v", i+1, err)
		}
	}
	if got := srv.Accepted(); got != 3 {
		t.Errorf("连接数 = %d，期望 3（每条消息一条连接）", got)
	}
	stats := client.PoolStats()
	if stats.Opened != 0 || stats.Reused != 0 {
		t.Errorf("未启用池时不应计数，实际 %+v", stats)
	}
}

// TestClientCanceledSendLeavesNoIdle 上下文取消（步骤超时）的连接不回池，
// 避免把半死连接留给下一条消息。
func TestClientCanceledSendLeavesNoIdle(t *testing.T) {
	srv := startFake(t, mailertest.Config{Silent: true})
	client := newTestClient(t, srv, func(cfg *Config) { cfg.Timeout = 5 * time.Second })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := sendNote(ctx, t, client, "新评论"); err == nil {
		t.Fatal("服务器不应答时投递应失败")
	}
	if stats := client.PoolStats(); stats.Idle != 0 {
		t.Errorf("取消后不应留下空闲连接，实际 %d", stats.Idle)
	}
}
