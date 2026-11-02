package chat

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

// recorded 是一次到达 webhook 的请求。
type recorded struct {
	method      string
	contentType string
	userAgent   string
	body        []byte
}

// fakeWebhook 是进程内的 webhook 端点：记录请求并按预设返回响应。
type fakeWebhook struct {
	mu       sync.Mutex
	requests []recorded

	status     int
	body       string
	retryAfter string
	delay      time.Duration

	server *httptest.Server
}

func newFakeWebhook(t *testing.T) *fakeWebhook {
	t.Helper()
	hook := &fakeWebhook{status: http.StatusOK, body: "ok"}
	hook.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		hook.mu.Lock()
		hook.requests = append(hook.requests, recorded{
			method:      r.Method,
			contentType: r.Header.Get("Content-Type"),
			userAgent:   r.Header.Get("User-Agent"),
			body:        raw,
		})
		status, body, retry := hook.status, hook.body, hook.retryAfter
		delay := hook.delay
		hook.mu.Unlock()

		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		if retry != "" {
			w.Header().Set("Retry-After", retry)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(hook.server.Close)
	return hook
}

func (f *fakeWebhook) url() string { return f.server.URL + "/services/T000/B000/XXXXSECRET" }

func (f *fakeWebhook) reply(status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status, f.body = status, body
}

func (f *fakeWebhook) last(t *testing.T) recorded {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) == 0 {
		t.Fatal("webhook 未收到请求")
	}
	return f.requests[len(f.requests)-1]
}

func TestConfigFromCredentials(t *testing.T) {
	base := map[string]string{"webhook_url": "https://hooks.slack.com/services/T000/B000/XXXX"}

	t.Run("缺省值", func(t *testing.T) {
		cfg, err := ConfigFromCredentials(base)
		if err != nil {
			t.Fatalf("配置应通过：%v", err)
		}
		if cfg.Timeout != DefaultTimeout {
			t.Errorf("超时 = %s, 期望 %s", cfg.Timeout, DefaultTimeout)
		}
		if cfg.Channel != "" || cfg.Username != "" || cfg.IconEmoji != "" {
			t.Errorf("外观字段应留空，实际 %+v", cfg)
		}
	})

	t.Run("完整配置", func(t *testing.T) {
		creds := map[string]string{
			"webhook_url": base["webhook_url"],
			"channel":     "#ops",
			"username":    "互动播报",
			"icon_emoji":  ":bell:",
			"timeout":     "2.5",
		}
		cfg, err := ConfigFromCredentials(creds)
		if err != nil {
			t.Fatalf("配置应通过：%v", err)
		}
		if cfg.Channel != "#ops" || cfg.Username != "互动播报" || cfg.IconEmoji != ":bell:" {
			t.Errorf("外观字段未生效：%+v", cfg)
		}
		if cfg.Timeout != 2500*time.Millisecond {
			t.Errorf("超时 = %s, 期望 2.5s", cfg.Timeout)
		}
	})

	bad := []struct {
		name  string
		creds map[string]string
		want  string
	}{
		{"缺少 webhook", map[string]string{}, "webhook_url"},
		{"远程 http", map[string]string{"webhook_url": "http://hooks.slack.com/services/T"}, "https"},
		{"非 http", map[string]string{"webhook_url": "ftp://hooks.slack.com/services/T"}, "http/https"},
		{"缺少 host", map[string]string{"webhook_url": "https:///services/T"}, "主机名"},
		{"频道缺少前缀", map[string]string{"webhook_url": base["webhook_url"], "channel": "ops"}, "#"},
		{"表情缺冒号", map[string]string{"webhook_url": base["webhook_url"], "icon_emoji": "bell"}, ":bell:"},
		{"超时非数字", map[string]string{"webhook_url": base["webhook_url"], "timeout": "2s"}, "秒数"},
		{"超时为负", map[string]string{"webhook_url": base["webhook_url"], "timeout": "-1"}, "大于 0"},
		{"超时过长", map[string]string{"webhook_url": base["webhook_url"], "timeout": "3600"}, "不能超过"},
		{"显示名含换行", map[string]string{"webhook_url": base["webhook_url"], "username": "a\nb"}, "换行"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ConfigFromCredentials(tc.creds); err == nil {
				t.Fatalf("配置应被拒绝：%v", tc.creds)
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("错误信息 %q 应包含 %q", err.Error(), tc.want)
			}
		})
	}

	t.Run("回环地址允许 http", func(t *testing.T) {
		if _, err := ConfigFromCredentials(map[string]string{"webhook_url": "http://127.0.0.1:9000/services/T"}); err != nil {
			t.Errorf("回环地址应被接受：%v", err)
		}
	})
}

func TestBuildMessage(t *testing.T) {
	msg := BuildMessage("《周末露营装备清单》有新评论", "小林：这个帐篷我有一顶，抗风一般。", BlockTextLimit)

	if len(msg.Blocks) != 2 {
		t.Fatalf("块数 = %d, 期望 2（标题 + 正文）", len(msg.Blocks))
	}
	if got := msg.Blocks[0].Text.Text; got != "*《周末露营装备清单》有新评论*" {
		t.Errorf("标题块 = %q", got)
	}
	if got := msg.Blocks[1].Text.Text; !strings.Contains(got, "抗风一般") {
		t.Errorf("正文块 = %q", got)
	}
	if !strings.Contains(msg.Text, "露营装备") || !strings.Contains(msg.Text, "抗风一般") {
		t.Errorf("通知预览应同时包含标题与正文，实际 %q", msg.Text)
	}
	if len([]rune(msg.Text)) > NotificationLimit {
		t.Errorf("通知预览超过上限：%d", utf8.RuneCountInString(msg.Text))
	}
}

func TestBuildMessageWithoutSubject(t *testing.T) {
	msg := BuildMessage("  ", "帖子 P1001 今日互动 128 次", BlockTextLimit)
	if len(msg.Blocks) != 1 {
		t.Fatalf("块数 = %d, 期望 1（只有正文）", len(msg.Blocks))
	}
	if strings.HasPrefix(msg.Text, "\n") {
		t.Errorf("预览不应以换行开头：%q", msg.Text)
	}
}

func TestBuildMessageFallsBackOnEmptyBody(t *testing.T) {
	msg := BuildMessage("互动播报", "   ", BlockTextLimit)
	if !strings.Contains(msg.Text, DefaultText) {
		t.Errorf("空正文应使用兜底文案，实际 %q", msg.Text)
	}
	if len(msg.Blocks) != 2 {
		t.Fatalf("块数 = %d, 期望 2", len(msg.Blocks))
	}
	if msg.Blocks[1].Text.Text != DefaultText {
		t.Errorf("正文块 = %q, 期望兜底文案", msg.Blocks[1].Text.Text)
	}
}

func TestSplitSectionsKeepsEveryRune(t *testing.T) {
	// 3 行各 1200 字：拼接后超过 2900，应在换行处断开而不是硬切在行中。
	line := strings.Repeat("赞", 1200)
	body := line + "\n" + line + "\n" + line

	parts := SplitSections(body, BlockTextLimit)
	if len(parts) < 2 {
		t.Fatalf("段数 = %d, 期望至少 2", len(parts))
	}
	for i, part := range parts {
		if n := utf8.RuneCountInString(part); n > BlockTextLimit {
			t.Errorf("第 %d 段超长：%d", i, n)
		}
	}
	if joined := strings.Join(parts, "\n"); joined != body {
		t.Error("切分后拼接应与原文一致（不应丢字符或改换行）")
	}
}

func TestSplitSectionsHardSplitsLongLine(t *testing.T) {
	body := strings.Repeat("评", BlockTextLimit*2+50)
	parts := SplitSections(body, BlockTextLimit)
	if len(parts) != 3 {
		t.Fatalf("段数 = %d, 期望 3", len(parts))
	}
	for i, part := range parts[:2] {
		if n := utf8.RuneCountInString(part); n != BlockTextLimit {
			t.Errorf("第 %d 段 = %d 字, 期望 %d", i, n, BlockTextLimit)
		}
	}
	if joined := strings.Join(parts, ""); joined != body {
		t.Error("硬切后拼接应与原文一致")
	}
}

func TestSplitSectionsEmpty(t *testing.T) {
	if parts := SplitSections("   ", BlockTextLimit); parts != nil {
		t.Errorf("空白正文应返回空切片，实际 %v", parts)
	}
}

func TestClientSendSuccess(t *testing.T) {
	hook := newFakeWebhook(t)
	client, err := NewClient(Config{
		WebhookURL: hook.url(),
		Channel:    "#ops",
		Username:   "互动播报",
		IconEmoji:  ":bell:",
	})
	if err != nil {
		t.Fatalf("构造客户端失败：%v", err)
	}

	receipt, err := client.Send(context.Background(), hook.url(),
		BuildMessage("帖子 P1001 有新评论", "小林：这个帐篷我有一顶", BlockTextLimit))
	if err != nil {
		t.Fatalf("提交失败：%v", err)
	}
	if receipt.Status != http.StatusOK || receipt.Body != "ok" {
		t.Errorf("回执 = %+v", receipt)
	}

	got := hook.last(t)
	if got.method != http.MethodPost {
		t.Errorf("方法 = %s, 期望 POST", got.method)
	}
	if !strings.Contains(got.contentType, "application/json") {
		t.Errorf("Content-Type = %q", got.contentType)
	}
	if got.userAgent != "xw-notify-chat/1.0" {
		t.Errorf("User-Agent = %q", got.userAgent)
	}

	var payload struct {
		Text      string  `json:"text"`
		Blocks    []Block `json:"blocks"`
		Channel   string  `json:"channel"`
		Username  string  `json:"username"`
		IconEmoji string  `json:"icon_emoji"`
	}
	if err := json.Unmarshal(got.body, &payload); err != nil {
		t.Fatalf("解析请求体失败：%v（%s）", err, got.body)
	}
	if !strings.Contains(payload.Text, "这个帐篷") {
		t.Errorf("text = %q", payload.Text)
	}
	if len(payload.Blocks) != 2 {
		t.Errorf("块数 = %d, 期望 2", len(payload.Blocks))
	}
	if payload.Channel != "#ops" || payload.Username != "互动播报" || payload.IconEmoji != ":bell:" {
		t.Errorf("外观覆盖未写进报文：%+v", payload)
	}
}

func TestClientSendRejectsEmptyMessage(t *testing.T) {
	hook := newFakeWebhook(t)
	client, err := NewClient(Config{WebhookURL: hook.url()})
	if err != nil {
		t.Fatalf("构造客户端失败：%v", err)
	}
	if _, err := client.Send(context.Background(), hook.url(), Message{}); err == nil {
		t.Fatal("空消息应被拒绝")
	}
}

func TestClientSendRejections(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		body       string
		retryAfter string
		wantParts  []string
		retryable  bool
	}{
		{"报文格式", 400, "invalid_payload", "", []string{"HTTP 400", "invalid_payload", "blocks"}, false},
		{"频道不存在", 404, "channel_not_found", "", []string{"HTTP 404", "撤销"}, false},
		{"频道已归档", 410, "channel_is_archived", "", []string{"HTTP 410", "归档"}, false},
		{"消息过长", 413, "msg_too_long", "", []string{"HTTP 413", "长度上限"}, false},
		{"限流", 429, "rate_limited", "3", []string{"HTTP 429", "3s", "限流"}, true},
		{"服务错误", 503, "service_unavailable", "", []string{"HTTP 503", "暂时不可用"}, true},
		{"未知错误码", 400, "something_new", "", []string{"HTTP 400", "something_new"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hook := newFakeWebhook(t)
			hook.reply(tc.status, tc.body)
			hook.retryAfter = tc.retryAfter
			client, err := NewClient(Config{WebhookURL: hook.url()})
			if err != nil {
				t.Fatalf("构造客户端失败：%v", err)
			}

			_, sendErr := client.Send(context.Background(), hook.url(), BuildMessage("标题", "正文", 0))
			if sendErr == nil {
				t.Fatal("应返回失败")
			}
			for _, part := range tc.wantParts {
				if !strings.Contains(sendErr.Error(), part) {
					t.Errorf("原因 %q 应包含 %q", sendErr.Error(), part)
				}
			}

			var chatErr *Error
			if !errors.As(sendErr, &chatErr) {
				t.Fatalf("错误类型 = %T, 期望 *chat.Error", sendErr)
			}
			if chatErr.Status != tc.status {
				t.Errorf("状态码 = %d, 期望 %d", chatErr.Status, tc.status)
			}
			if chatErr.Retryable() != tc.retryable {
				t.Errorf("Retryable = %v, 期望 %v", chatErr.Retryable(), tc.retryable)
			}
			if tc.retryAfter != "" && chatErr.RetryAfter != 3*time.Second {
				t.Errorf("RetryAfter = %s, 期望 3s", chatErr.RetryAfter)
			}
		})
	}
}

func TestClientSendTransportError(t *testing.T) {
	hook := newFakeWebhook(t)
	url := hook.url()
	hook.server.Close() // 端口关闭后再提交，模拟连不上

	client, err := NewClient(Config{WebhookURL: "https://hooks.slack.com/services/T000/B000/XXXX"})
	if err != nil {
		t.Fatalf("构造客户端失败：%v", err)
	}
	_, sendErr := client.Send(context.Background(), url, BuildMessage("标题", "正文", 0))
	if sendErr == nil {
		t.Fatal("连接失败应返回错误")
	}
	var chatErr *Error
	if !errors.As(sendErr, &chatErr) {
		t.Fatalf("错误类型 = %T, 期望 *chat.Error", sendErr)
	}
	if chatErr.Status != 0 {
		t.Errorf("Status = %d, 期望 0（请求未到达）", chatErr.Status)
	}
	if !strings.Contains(chatErr.Reason, "连接 Slack 失败") {
		t.Errorf("原因 = %q", chatErr.Reason)
	}
}

func TestClientSendCancelled(t *testing.T) {
	hook := newFakeWebhook(t)
	hook.delay = 2 * time.Second
	client, err := NewClient(Config{WebhookURL: hook.url(), Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("构造客户端失败：%v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	started := time.Now()
	_, sendErr := client.Send(ctx, hook.url(), BuildMessage("标题", "正文", 0))
	if sendErr == nil {
		t.Fatal("超时应返回错误")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Errorf("取消应尽快生效，实际耗时 %s", elapsed)
	}
	var chatErr *Error
	if !errors.As(sendErr, &chatErr) {
		t.Fatalf("错误类型 = %T, 期望 *chat.Error", sendErr)
	}
	if !strings.Contains(chatErr.Reason, "超时") && !strings.Contains(chatErr.Reason, "取消") {
		t.Errorf("原因 = %q", chatErr.Reason)
	}
	if !errors.Is(sendErr, context.DeadlineExceeded) {
		t.Error("应能通过 errors.Is 识别为超时")
	}
}

func TestMaskWebhookHidesToken(t *testing.T) {
	masked := MaskWebhook("https://hooks.slack.com/services/T000/B000/XXXXSECRET")
	if masked != "https://hooks.slack.com/…" {
		t.Errorf("脱敏结果 = %q", masked)
	}
	if strings.Contains(masked, "XXXXSECRET") || strings.Contains(masked, "B000") {
		t.Error("脱敏结果不应保留路径片段")
	}
	if MaskWebhook("::::") != "<无法解析的 webhook>" {
		t.Errorf("非法地址应给出占位串，实际 %q", MaskWebhook("::::"))
	}
}
