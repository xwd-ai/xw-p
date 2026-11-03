package pipeline

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/chat"
	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/mailer"
	"github.com/xwd-ai/xw-p/engine/internal/provider"
)

// 本文件把「每个渠道实现都必须满足什么」写成可执行的约定。
//
// 各实现自己的测试（console_*_test.go、smtp_email_test.go、slack_chat_test.go）
// 验的是细节规则；这里只验所有实现共有的那部分：标识与渠道对得上、本地能判定
// 的失败一致地报成「终态失败 + 原因」、零值投递不 panic、探测不依赖外部网络时
// 如实返回、并发调用不出数据竞争。新增一个实现时把它加进 providerCases，
// 这些约定就自动覆盖到它。

// providerCase 描述一个可离线构造的渠道实现及其本地校验口径。
type providerCase struct {
	// id 是期望的实现标识。
	id      string
	channel domain.Channel
	sender  Provider
	// reject 是一条能在本地判定失败的投递（缺地址、地址非法、正文为空、
	// 载荷超限），reason 是期望出现在失败原因里的片段。
	reject Delivery
	reason string
	// deliver 是一条能通过本地校验的投递；offline 表示按下 Send 不会连网。
	deliver Delivery
	offline bool
}

// providerCases 构造全部实现。
//
// smtp 与 slack 指向不可达 / 不可写的本地地址：本轮只走本地校验路径，
// 真发链路由各自的测试与冒烟覆盖。
func providerCases(t *testing.T) []providerCase {
	t.Helper()

	// 取一个确定空闲的端口再关掉，让 SMTP 探测与真实发送都停在「连不上」。
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("申请临时端口失败：%v", err)
	}
	closedAddr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("释放临时端口失败：%v", err)
	}
	host, portText, err := net.SplitHostPort(closedAddr)
	if err != nil {
		t.Fatalf("拆分临时地址失败：%v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portText, "%d", &port); err != nil {
		t.Fatalf("解析临时端口失败：%v", err)
	}

	smtp, err := NewSMTPEmailProvider(discardLogger(), mailer.Config{
		Host:    host,
		Port:    port,
		From:    "notify@example.com",
		Timeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("构造 SMTP 实现失败：%v", err)
	}
	slack, err := NewSlackChatProvider(discardLogger(), chat.Config{
		WebhookURL: "https://hooks.example.com/services/T000/B000/XXXX",
		Timeout:    300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("构造 Slack 实现失败：%v", err)
	}

	base := Delivery{
		Subject:      "你的帖子有新评论",
		Body:         "帖子 P1001 收到林小满的评论：写得很清楚。",
		StepID:       "s1",
		TriggerID:    "t1",
		WorkflowName: "post_commented",
		SubscriberID: "sub_1",
	}

	return []providerCase{
		{
			id:      "console-in-app",
			channel: domain.ChannelInApp,
			sender:  NewInAppProvider(discardLogger(), NewMemoryInboxStore()),
			reject:  withDelivery(base, func(d *Delivery) { d.SubscriberID = "  " }),
			reason:  "缺少订阅者 ID",
			deliver: withDelivery(base, func(d *Delivery) {}),
			offline: true,
		},
		{
			id:      "console-email",
			channel: domain.ChannelEmail,
			sender:  NewConsoleEmailProvider(discardLogger()),
			reject:  withDelivery(base, func(d *Delivery) { d.To = "" }),
			reason:  "收件邮箱为空",
			deliver: withDelivery(base, func(d *Delivery) { d.To = "creator@example.com" }),
			offline: true,
		},
		{
			id:      "console-sms",
			channel: domain.ChannelSMS,
			sender:  NewConsoleSMSProvider(discardLogger()),
			reject:  withDelivery(base, func(d *Delivery) { d.To = "12ab34" }),
			reason:  "手机号为空或格式非法",
			deliver: withDelivery(base, func(d *Delivery) { d.To = "+8613800138000" }),
			offline: true,
		},
		{
			id:      "console-push",
			channel: domain.ChannelPush,
			sender:  NewConsolePushProvider(discardLogger()),
			reject:  withDelivery(base, func(d *Delivery) { d.To = " " }),
			reason:  "缺少 device token",
			deliver: withDelivery(base, func(d *Delivery) { d.To = strings.Repeat("a1b2", 16) }),
			offline: true,
		},
		{
			id:      "console-chat",
			channel: domain.ChannelChat,
			sender:  NewConsoleChatProvider(discardLogger()),
			reject:  withDelivery(base, func(d *Delivery) { d.To = "hooks.example.com/services" }),
			reason:  "webhook",
			deliver: withDelivery(base, func(d *Delivery) {
				d.To = "https://hooks.example.com/services/T000/B000/XXXX"
			}),
			offline: true,
		},
		{
			// SMTP 只在收件邮箱为空时不做网络调用，其余情况一律去连中继。
			id:      "smtp-email",
			channel: domain.ChannelEmail,
			sender:  smtp,
			reject:  withDelivery(base, func(d *Delivery) { d.To = "" }),
			reason:  "收件邮箱为空",
			deliver: withDelivery(base, func(d *Delivery) { d.To = "creator@example.com" }),
		},
		{
			// Slack 的空地址会回落到渠道默认 webhook，因此本地拦截点在正文上。
			id:      "slack-chat",
			channel: domain.ChannelChat,
			sender:  slack,
			reject:  withDelivery(base, func(d *Delivery) { d.Body = "\n\t " }),
			reason:  "正文为空",
			deliver: withDelivery(base, func(d *Delivery) {
				d.To = "https://hooks.example.com/services/T000/B000/XXXX"
			}),
		},
	}
}

// withDelivery 复制一份投递请求再改写，避免表里各行共用同一个底层结构。
func withDelivery(src Delivery, mutate func(*Delivery)) Delivery {
	out := src
	mutate(&out)
	return out
}

// TestChannelProvidersRejectLocally 验证本地能判定的失败口径一致：
// 不发送、标终态、给出非空原因。
//
// 终态这一项尤其重要：回退链看到 Terminal 就停止，否则一条「收件邮箱为空」
// 的消息会把渠道里所有备选实现都试一遍。
func TestChannelProvidersRejectLocally(t *testing.T) {
	for _, c := range providerCases(t) {
		t.Run(c.id, func(t *testing.T) {
			res := sendWithin(t, c.sender, c.reject, time.Second)
			if res.Sent {
				t.Fatalf("本地校验失败却报成投递成功：%+v", res)
			}
			if !res.Terminal {
				t.Errorf("本地校验失败应标为终态（不回退）：%+v", res)
			}
			if !strings.Contains(res.Reason, c.reason) {
				t.Errorf("失败原因应包含 %q，实际 %q", c.reason, res.Reason)
			}
			if strings.TrimSpace(res.Reason) == "" {
				t.Error("失败必须给出原因，否则执行明细里只有一行空状态")
			}
		})
	}
}

// TestChannelProvidersDeliverWellFormedMessage 验证通过本地校验的消息在离线
// 实现上确实落成成功结果：Sent 为真、不标终态、原因非空。
func TestChannelProvidersDeliverWellFormedMessage(t *testing.T) {
	for _, c := range providerCases(t) {
		if !c.offline {
			continue
		}
		t.Run(c.id, func(t *testing.T) {
			res := sendWithin(t, c.sender, c.deliver, time.Second)
			if !res.Sent {
				t.Fatalf("合法消息应投递成功：%+v", res)
			}
			if res.Terminal {
				t.Errorf("成功结果不该带终态标记：%+v", res)
			}
			if strings.TrimSpace(res.Reason) == "" {
				t.Error("成功也要留下可追溯的说明")
			}
		})
	}
}

// TestChannelProvidersHandleEmptyDelivery 验证零值投递不会 panic 也不会被
// 当成成功——上游字段缺失时应当得到一条可读的失败原因。
func TestChannelProvidersHandleEmptyDelivery(t *testing.T) {
	for _, c := range providerCases(t) {
		t.Run(c.id, func(t *testing.T) {
			res := sendWithin(t, c.sender, Delivery{}, time.Second)
			if res.Sent {
				t.Fatalf("空投递不应成功：%+v", res)
			}
			if strings.TrimSpace(res.Reason) == "" {
				t.Error("空投递的失败也要说明原因")
			}
		})
	}
}

// TestChannelProviderIdentity 验证实现自带标识：非空、互不重复、与服务渠道一致，
// 且能按渠道登记进同一张注册表后被逐个寻址到。
//
// 标识是启停与摘除的寻址键，重复会让运维接口改到另一个实现上。
func TestChannelProviderIdentity(t *testing.T) {
	cases := providerCases(t)
	reg := NewProviderRegistry()
	seen := make(map[string]domain.Channel, len(cases))

	for _, c := range cases {
		if strings.TrimSpace(c.id) == "" {
			t.Fatal("实现标识不能为空")
		}
		if _, dup := seen[c.id]; dup {
			t.Fatalf("实现标识重复：%s", c.id)
		}
		seen[c.id] = c.channel

		if got := c.sender.Channel(); got != c.channel {
			t.Errorf("%s 声明的渠道是 %s，期望 %s", c.id, got, c.channel)
		}
		if !c.channel.IsDelivery() {
			t.Errorf("%s 绑定到编排步骤渠道 %s", c.id, c.channel)
		}
		if err := reg.RegisterEntry(provider.Entry{
			ID: c.id, Channel: c.sender.Channel(), Priority: 100, Enabled: true, Sender: c.sender,
		}); err != nil {
			t.Fatalf("登记 %s 失败：%v", c.id, err)
		}
	}

	if reg.Len() != len(cases) {
		t.Fatalf("注册表条目数 %d，期望 %d", reg.Len(), len(cases))
	}
	for id, ch := range seen {
		entry, ok := reg.LookupByID(id)
		if !ok {
			t.Errorf("注册表里找不到 %s", id)
			continue
		}
		if entry.Channel != ch {
			t.Errorf("%s 在注册表里的渠道是 %s，期望 %s", id, entry.Channel, ch)
		}
	}
	// 每个投递渠道都应有可寻址的实现，缺一个就意味着该渠道只能 mock 打印。
	for _, ch := range domain.DeliveryChannels() {
		if _, ok := reg.Select(ch); !ok {
			t.Errorf("渠道 %s 没有登记任何实现", ch)
		}
	}
}

// TestConsoleProviderBinding 验证 console 种类按渠道选到对应实现，
// 编排步骤与未知渠道如实报「不支持」而不是返回一个投错的实现。
func TestConsoleProviderBinding(t *testing.T) {
	t.Setenv("XW_INBOX_FILE", filepath.Join(t.TempDir(), "inbox.jsonl"))

	want := map[domain.Channel]string{
		domain.ChannelInApp: "console-in-app",
		domain.ChannelEmail: "console-email",
		domain.ChannelSMS:   "console-sms",
		domain.ChannelPush:  "console-push",
		domain.ChannelChat:  "console-chat",
	}
	for ch, id := range want {
		sender, err := newConsoleProvider(discardLogger(), ch)
		if err != nil {
			t.Fatalf("构造 %s 的 console 实现失败：%v", ch, err)
		}
		if sender.Channel() != ch {
			t.Errorf("console 实现绑定到 %s，期望 %s", sender.Channel(), ch)
		}
		got, ok := sender.(provider.Identifier)
		if !ok {
			t.Fatalf("%s 的 console 实现应带稳定标识", ch)
		}
		if got.ID() != id {
			t.Errorf("%s 的标识是 %s，期望 %s", ch, got.ID(), id)
		}
	}

	for _, ch := range []domain.Channel{domain.ChannelDelay, domain.ChannelDigest, ""} {
		if _, err := newConsoleProvider(discardLogger(), ch); !errors.Is(err, provider.ErrUnsupportedKind) {
			t.Errorf("渠道 %q 应报不支持，实际 %v", ch, err)
		}
	}
}

// TestProviderFactoryRejectsChannelMismatch 验证工厂不会把实现装配到它服务
// 不了的渠道上：种类与渠道不匹配属于配置错误，应在启动期暴露。
func TestProviderFactoryRejectsChannelMismatch(t *testing.T) {
	factory := ProviderFactory(discardLogger())
	cases := []struct {
		name string
		cfg  provider.Config
	}{
		{"inapp 配到 email", provider.Config{ID: "x", Channel: domain.ChannelEmail, Kind: provider.KindInApp}},
		{"smtp 配到 chat", provider.Config{ID: "x", Channel: domain.ChannelChat, Kind: provider.KindSMTP,
			Credentials: provider.Credentials{"host": "127.0.0.1", "from": "a@example.com"}}},
		{"slack 配到 sms", provider.Config{ID: "x", Channel: domain.ChannelSMS, Kind: provider.KindSlack,
			Credentials: provider.Credentials{"webhook_url": "https://hooks.example.com/services/T/B/X"}}},
	}
	for _, c := range cases {
		if _, err := factory.Build(c.cfg); !errors.Is(err, provider.ErrUnsupportedKind) {
			t.Errorf("%s 应报不支持，实际 %v", c.name, err)
		}
	}

	// 前置条件：同一份配置配到正确的渠道上应当构造得出来。
	if _, err := factory.Build(provider.Config{
		ID: "ok", Channel: domain.ChannelChat, Kind: provider.KindSlack,
		Credentials: provider.Credentials{"webhook_url": "https://hooks.example.com/services/T/B/X"},
	}); err != nil {
		t.Fatalf("slack 配到 chat 应当可用，实际 %v", err)
	}
}

// TestProviderConstructorsAcceptZeroArgs 验证构造函数的零值容错：装配路径上
// 可能不传 logger，站内信也可能没有现成存储。两者都应回落到可用默认值，
// 而不是在第一次投递时才 panic。
func TestProviderConstructorsAcceptZeroArgs(t *testing.T) {
	builders := []struct {
		id    string
		build func() Provider
	}{
		{"console-email", func() Provider { return NewConsoleEmailProvider(nil) }},
		{"console-sms", func() Provider { return NewConsoleSMSProvider(nil) }},
		{"console-push", func() Provider { return NewConsolePushProvider(nil) }},
		{"console-chat", func() Provider { return NewConsoleChatProvider(nil) }},
	}
	for _, b := range builders {
		t.Run(b.id, func(t *testing.T) {
			p := b.build()
			// 空投递会走本地校验分支并打一行告警：logger 为 nil 时这正是要验的路径。
			if res := p.Send(Delivery{}); res.Sent {
				t.Fatalf("缺地址时应判失败：%+v", res)
			}
		})
	}

	// logger 与存储都为 nil：退回内存收件箱，投递仍然可见。
	p := NewInAppProvider(nil, nil)
	d := Delivery{
		TriggerID:    "t-9",
		StepID:       "s1",
		SubscriberID: "sub_9",
		WorkflowName: "post_liked",
		Body:         "帖子 P1001 收到 3 个赞。",
	}
	if res := p.Send(d); !res.Sent {
		t.Fatalf("缺省存储应可写入：%+v", res)
	}
	entries, err := p.(InboxStoreProvider).InboxStore().List("sub_9")
	if err != nil {
		t.Fatalf("读取收件箱失败：%v", err)
	}
	if len(entries) != 1 || entries[0].TriggerID != "t-9" {
		t.Fatalf("收件箱应有一条来自 t-9 的记录，实际 %+v", entries)
	}
	if err := p.(probeable).Probe(context.Background()); err != nil {
		t.Errorf("内存存储的探测应通过，实际 %v", err)
	}
}

// probingStore 是带探测能力的收件箱存储替身。
type probingStore struct {
	*MemoryInboxStore
	err   error
	calls int
	mu    sync.Mutex
}

func (s *probingStore) Probe() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.err
}

// TestProviderProbeContract 验证探测的两种口径：没有外部依赖的 console 实现
// 恒为可用；需要落盘 / 连网的实现如实报错，且探测不产生副作用。
func TestProviderProbeContract(t *testing.T) {
	consoles := []struct {
		id    string
		probe func(context.Context) error
	}{
		{"console-email", NewConsoleEmailProvider(discardLogger()).(probeable).Probe},
		{"console-sms", NewConsoleSMSProvider(discardLogger()).(probeable).Probe},
		{"console-push", NewConsolePushProvider(discardLogger()).(probeable).Probe},
		{"console-chat", NewConsoleChatProvider(discardLogger()).(probeable).Probe},
	}
	for _, c := range consoles {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // 已取消的 ctx 也不该让 console 实现报错：它没有可取消的动作。
		if err := c.probe(ctx); err != nil {
			t.Errorf("%s 的探测应恒为可用，实际 %v", c.id, err)
		}
	}

	t.Run("站内信委托给存储", func(t *testing.T) {
		store := &probingStore{MemoryInboxStore: NewMemoryInboxStore(), err: errors.New("磁盘只读")}
		p := NewInAppProvider(discardLogger(), store)
		err := p.(probeable).Probe(context.Background())
		if err == nil || !strings.Contains(err.Error(), "磁盘只读") {
			t.Errorf("存储报错应原样带出，实际 %v", err)
		}
		if store.calls != 1 {
			t.Errorf("应恰好探测一次，实际 %d", store.calls)
		}

		// 不带 Probe 的存储（内存实现）视为可用。
		if err := NewInAppProvider(discardLogger(), NewMemoryInboxStore()).(probeable).Probe(context.Background()); err != nil {
			t.Errorf("无外部依赖的存储应视为可用，实际 %v", err)
		}
	})

	t.Run("收件箱文件探测", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "nested", "inbox.jsonl")
		store, err := NewJSONLInboxStore(path)
		if err != nil {
			t.Fatalf("构造收件箱存储失败：%v", err)
		}
		if err := store.Probe(); err != nil {
			t.Fatalf("可写目录应探测通过，实际 %v", err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Errorf("探测应顺手建出可写的文件，实际 %v", err)
		}
		if entries, err := store.List("sub_1"); err != nil || len(entries) != 0 {
			t.Errorf("探测不该往里塞测试消息：entries=%v err=%v", entries, err)
		}

		// 目录在运行期变成同名文件（挂载丢失、误删重建）时如实报不可用：
		// 存储是装配期建好的，故障发生在之后，探测得当场发现。
		dir := filepath.Join(t.TempDir(), "vanish")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("准备目录失败：%v", err)
		}
		broken, err := NewJSONLInboxStore(filepath.Join(dir, "inbox.jsonl"))
		if err != nil {
			t.Fatalf("构造收件箱存储失败：%v", err)
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Fatalf("清理目录失败：%v", err)
		}
		if err := os.WriteFile(dir, []byte("x"), 0o644); err != nil {
			t.Fatalf("准备占位文件失败：%v", err)
		}
		if err := broken.Probe(); err == nil {
			t.Error("目录不可用时探测应报错")
		}
	})

	t.Run("SMTP 探测不握手", func(t *testing.T) {
		var smtp Provider
		for _, c := range providerCases(t) {
			if c.id == "smtp-email" {
				smtp = c.sender
			}
		}
		if smtp == nil {
			t.Fatal("测试数据里应有 smtp 实现")
		}
		// 端口已释放：探测应停在连接层，而不是等完整的 SMTP 握手。
		start := time.Now()
		if err := smtp.(probeable).Probe(context.Background()); err == nil {
			t.Error("中继不可达时探测应报错")
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("探测耗时 %s，不应等待握手超时", elapsed)
		}
	})
}

// probeable 是健康检查使用的探测接口（与 provider.Prober 同形）。
type probeable interface {
	Probe(context.Context) error
}

// TestChannelProvidersAreConcurrencySafe 在 -race 下并发调用同一批实现，
// 确认实现本身没有共享可变状态。
//
// 站内信每条用不同触发标识，因此不应互相覆盖；最终条目数要与调用次数一致。
func TestChannelProvidersAreConcurrencySafe(t *testing.T) {
	const workers, perWorker = 8, 25

	var offline []providerCase
	for _, c := range providerCases(t) {
		if c.offline {
			offline = append(offline, c)
		}
	}

	for _, c := range offline {
		t.Run(c.id, func(t *testing.T) {
			var wg sync.WaitGroup
			errs := make(chan string, workers*perWorker)
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < perWorker; i++ {
						d := c.deliver
						d.TriggerID = fmt.Sprintf("t-%d-%d", w, i)
						if res := c.sender.Send(d); !res.Sent {
							errs <- res.Reason
						}
					}
				}(w)
			}
			wg.Wait()
			close(errs)
			for reason := range errs {
				t.Errorf("并发投递失败：%s", reason)
			}
		})
	}

	// 站内信实现按触发标识去重，条目数应精确等于投递次数。
	for _, c := range offline {
		if c.channel != domain.ChannelInApp {
			continue
		}
		store := c.sender.(InboxStoreProvider).InboxStore()
		entries, err := store.List("sub_1")
		if err != nil {
			t.Fatalf("读取收件箱失败：%v", err)
		}
		if len(entries) != workers*perWorker {
			t.Errorf("收件箱条目数 %d，期望 %d", len(entries), workers*perWorker)
		}
	}
}

// sendWithin 在限定时间内拿到投递结果；超时说明实现走进了网络等待，
// 与「本地就能判定」的前提不符。
func sendWithin(t *testing.T, sender Provider, d Delivery, limit time.Duration) SendResult {
	t.Helper()
	done := make(chan SendResult, 1)
	go func() { done <- sender.Send(d) }()
	select {
	case res := <-done:
		return res
	case <-time.After(limit):
		t.Fatalf("投递在 %s 内没有返回，本地校验不应等待网络", limit)
		return SendResult{}
	}
}
