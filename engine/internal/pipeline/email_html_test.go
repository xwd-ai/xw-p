package pipeline

import (
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

// mailPart 是解析出来后的一段 MIME 正文。
type mailPart struct {
	contentType string
	body        string
}

// parseMailParts 解析 SMTP 报文：单段邮件返回一段，多段邮件返回各部分
// （按报文中出现的顺序）。
func parseMailParts(t *testing.T, raw string) []mailPart {
	t.Helper()
	head, body, ok := strings.Cut(raw, "\r\n\r\n")
	if !ok {
		t.Fatalf("报文缺少头与正文的分隔：%q", raw)
	}
	var contentType string
	for _, line := range strings.Split(head, "\r\n") {
		if value, found := strings.CutPrefix(line, "Content-Type: "); found {
			contentType = value
		}
	}
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		t.Fatalf("Content-Type 解析失败：%v", err)
	}
	decode := func(encoded string) string {
		decoded, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(encoded), ""))
		if err != nil {
			t.Fatalf("正文不是合法 base64：%v", err)
		}
		return string(decoded)
	}

	if !strings.HasPrefix(mediaType, "multipart/") {
		return []mailPart{{contentType: contentType, body: decode(body)}}
	}

	boundary := params["boundary"]
	if boundary == "" {
		t.Fatalf("多段邮件缺少 boundary：%q", contentType)
	}
	var parts []mailPart
	for _, chunk := range strings.Split(body, "--"+boundary) {
		chunk = strings.TrimPrefix(chunk, "\r\n")
		if chunk == "" || strings.HasPrefix(chunk, "--") {
			continue // 序言与结束分隔行
		}
		partHead, partBody, ok := strings.Cut(chunk, "\r\n\r\n")
		if !ok {
			t.Fatalf("MIME 段缺少头与正文的分隔：%q", chunk)
		}
		partType := ""
		for _, line := range strings.Split(partHead, "\r\n") {
			if value, found := strings.CutPrefix(line, "Content-Type: "); found {
				partType = value
			}
		}
		parts = append(parts, mailPart{contentType: partType, body: decode(strings.TrimRight(partBody, "\r\n"))})
	}
	return parts
}

// partOf 取出指定媒体类型的正文段。
func partOf(t *testing.T, parts []mailPart, mediaType string) string {
	t.Helper()
	for _, p := range parts {
		if strings.HasPrefix(p.contentType, mediaType) {
			return p.body
		}
	}
	t.Fatalf("报文中没有 %s 段：%+v", mediaType, parts)
	return ""
}

// startMailServer 起一个进程内 SMTP 服务器并返回装配好的发件实现。
func startMailServer(t *testing.T) (*mailertest.Server, Provider) {
	t.Helper()
	srv, err := mailertest.New(mailertest.Config{})
	if err != nil {
		t.Fatalf("启动测试服务器失败：%v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	sender, err := NewSMTPEmailProvider(discardLogger(), mailer.Config{
		Host:    srv.Host(),
		Port:    srv.Port(),
		From:    "互动提醒 <notify@example.com>",
		TLS:     mailer.TLSDisabled,
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("构造 smtp provider 失败：%v", err)
	}
	return srv, sender
}

// TestRenderEmailHTMLTemplateEscapesVariables 验证 HTML 模板只转义变量值，
// 模板自身的标签原样保留——否则评论里的 <script> 会变成页面代码。
func TestRenderEmailHTMLTemplateEscapesVariables(t *testing.T) {
	p := newMockPipeline()
	ctx := NewContext(queue.QueuedMessage{
		TriggerID: "trig-html-1",
		Name:      "post_commented",
		Email:     "creator@example.com",
		Payload: map[string]any{
			"actor":   "阿岚<b>",
			"post_id": "P1001",
			"comment": `<script>alert('x')</script> & "引号"`,
		},
	})
	wf := domain.Workflow{
		Name: "post_commented",
		Steps: []domain.Step{{
			ID:   "comment_email",
			Type: domain.ChannelEmail,
			Config: map[string]any{
				"subject":  "《{{ payload.post_id }}》有新评论",
				"template": "{{ payload.actor }} 说：{{ payload.comment }}",
				"html":     `<p style="margin:0">{{ payload.actor }} 说：{{ payload.comment }}</p>`,
			},
		}},
	}

	results := p.Run(ctx, wf)
	if len(results) != 1 {
		t.Fatalf("应有 1 条结果，实际 %d", len(results))
	}
	html := results[0].RenderedHTML
	if html == "" {
		t.Fatal("配置了 html 模板时应产出 HTML 正文")
	}
	if !strings.Contains(html, `<p style="margin:0">`) {
		t.Errorf("模板自身的标签应保留：%q", html)
	}
	if strings.Contains(html, "<script>") {
		t.Errorf("变量值里的脚本标签必须被转义：%q", html)
	}
	for _, want := range []string{"&lt;script&gt;", "&amp;", "&lt;b&gt;"} {
		if !strings.Contains(html, want) {
			t.Errorf("HTML 正文应包含转义片段 %q，实际 %q", want, html)
		}
	}
	// 纯文本形态不受影响：转义只作用于 HTML。
	if !strings.Contains(results[0].Rendered, "<script>alert('x')</script>") {
		t.Errorf("纯文本正文不应被转义：%q", results[0].Rendered)
	}
}

// TestRenderEmailHTMLLayoutFromText 验证 format=html 时把纯文本套进内置版式，
// 标题与正文都转义、换行转成 <br />。
func TestRenderEmailHTMLLayoutFromText(t *testing.T) {
	p := newMockPipeline()
	ctx := NewContext(queue.QueuedMessage{
		TriggerID: "trig-html-2",
		Name:      "post_liked",
		Email:     "creator@example.com",
		Payload: map[string]any{
			"actor":      "阿岚",
			"post_title": "周末骑行 <精选>",
		},
	})
	wf := domain.Workflow{
		Name: "post_liked",
		Steps: []domain.Step{{
			ID:   "like_email",
			Type: domain.ChannelEmail,
			Config: map[string]any{
				"subject":  "《{{ payload.post_title }}》有新点赞",
				"template": "{{ payload.actor }} 赞了你的帖子\n去看看评论区吧",
				"format":   "html",
			},
		}},
	}

	results := p.Run(ctx, wf)
	html := results[0].RenderedHTML
	if html == "" {
		t.Fatal("format=html 时应生成内置版式")
	}
	if !strings.HasPrefix(html, "<!DOCTYPE html>") {
		t.Errorf("应输出完整 HTML 文档：%q", html)
	}
	if !strings.Contains(html, "《周末骑行 &lt;精选&gt;》有新点赞") {
		t.Errorf("标题应转义后写进 HTML：%q", html)
	}
	if !strings.Contains(html, "阿岚 赞了你的帖子<br />去看看评论区吧") {
		t.Errorf("换行应转成 <br />：%q", html)
	}
}

// TestRenderEmailTextFormatOptOut 显式 format=text 时不产出 HTML，
// 即便同时配了 html 模板。
func TestRenderEmailTextFormatOptOut(t *testing.T) {
	p := newMockPipeline()
	ctx := NewContext(queue.QueuedMessage{
		TriggerID: "trig-html-3",
		Name:      "post_mentioned",
		Email:     "creator@example.com",
		Payload:   map[string]any{"actor": "小林"},
	})
	wf := domain.Workflow{
		Name: "post_mentioned",
		Steps: []domain.Step{{
			ID:   "mention_email",
			Type: domain.ChannelEmail,
			Config: map[string]any{
				"template": "{{ payload.actor }} 提到了你",
				"html":     "<p>{{ payload.actor }} 提到了你</p>",
				"format":   "text",
			},
		}},
	}

	results := p.Run(ctx, wf)
	if results[0].RenderedHTML != "" {
		t.Errorf("format=text 时不应产出 HTML：%q", results[0].RenderedHTML)
	}
	if results[0].Rendered != "小林 提到了你" {
		t.Errorf("纯文本正文 = %q", results[0].Rendered)
	}
}

// TestRenderNonEmailStepIgnoresHTML 非 email 步骤即便配了 html 也不产出 HTML。
func TestRenderNonEmailStepIgnoresHTML(t *testing.T) {
	p := newMockPipeline()
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "trig-html-4",
		Name:         "post_liked",
		SubscriberID: "creator_1001",
		Payload:      map[string]any{"actor": "阿岚"},
	})
	wf := domain.Workflow{
		Name: "post_liked",
		Steps: []domain.Step{{
			ID:   "like_inapp",
			Type: domain.ChannelInApp,
			Config: map[string]any{
				"template": "{{ payload.actor }} 赞了你的帖子",
				"html":     "<p>{{ payload.actor }}</p>",
				"format":   "html",
			},
		}},
	}

	results := p.Run(ctx, wf)
	if results[0].RenderedHTML != "" {
		t.Errorf("站内信步骤不应产出 HTML：%q", results[0].RenderedHTML)
	}
}

// TestDispatchSendsMultipartEmail 覆盖「管线 → 分发 → SMTP」：配置了 HTML
// 模板的邮件在报文里应同时有纯文本段与 HTML 段。
func TestDispatchSendsMultipartEmail(t *testing.T) {
	srv, sender := startMailServer(t)
	registry := NewProviderRegistry()
	if err := registry.RegisterEntry(provider.Entry{
		ID:       "email-smtp",
		Channel:  domain.ChannelEmail,
		Priority: 10,
		Enabled:  true,
		Sender:   sender,
	}); err != nil {
		t.Fatalf("登记 provider 失败：%v", err)
	}

	msg := queue.QueuedMessage{
		TriggerID:    "trig-html-5",
		Name:         "post_commented",
		SubscriberID: "creator_1001",
		Email:        "creator@example.com",
		Payload: map[string]any{
			"actor":      "阿岚",
			"post_title": "周末骑行",
			"comment":    "<b>这段路线我也骑过</b>",
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
				"html":     "<p><strong>{{ payload.actor }}</strong> 说：{{ payload.comment }}</p>",
			},
		}},
	}

	results := NewWithRegistry(discardLogger(), registry).Run(NewContext(msg), wf)
	if len(results) != 1 || !results[0].Sent {
		t.Fatalf("邮件步骤应投递成功，实际 %+v", results)
	}

	mails := srv.Mails()
	if len(mails) != 1 {
		t.Fatalf("中继应收到 1 封，实际 %d", len(mails))
	}
	parts := parseMailParts(t, mails[0].Data)
	if len(parts) != 2 {
		t.Fatalf("应为两段正文，实际 %d 段：%+v", len(parts), parts)
	}
	text := partOf(t, parts, "text/plain")
	html := partOf(t, parts, "text/html")
	if !strings.Contains(text, "阿岚 说：<b>这段路线我也骑过</b>") {
		t.Errorf("纯文本段 = %q", text)
	}
	if !strings.Contains(html, "<strong>阿岚</strong> 说：&lt;b&gt;这段路线我也骑过&lt;/b&gt;") {
		t.Errorf("HTML 段应转义评论内容：%q", html)
	}
}

// TestDispatchEmailHTMLOnlyFallsBackToText 只配 html 模板（没有纯文本模板）时，
// 纯文本段要有一句可读的降级文案，而不是空白。
func TestDispatchEmailHTMLOnlyFallsBackToText(t *testing.T) {
	srv, sender := startMailServer(t)
	registry := NewProviderRegistry()
	if err := registry.RegisterEntry(provider.Entry{
		ID:       "email-smtp",
		Channel:  domain.ChannelEmail,
		Priority: 10,
		Enabled:  true,
		Sender:   sender,
	}); err != nil {
		t.Fatalf("登记 provider 失败：%v", err)
	}

	msg := queue.QueuedMessage{
		TriggerID: "trig-html-6",
		Name:      "post_liked",
		Email:     "creator@example.com",
		Payload:   map[string]any{"actor": "阿岚"},
	}
	wf := domain.Workflow{
		Name: "post_liked",
		Steps: []domain.Step{{
			ID:   "like_email",
			Type: domain.ChannelEmail,
			Config: map[string]any{
				"subject": "新点赞",
				"html":    "<p>{{ payload.actor }} 赞了你的帖子</p>",
			},
		}},
	}

	results := NewWithRegistry(discardLogger(), registry).Run(NewContext(msg), wf)
	if len(results) != 1 || !results[0].Sent {
		t.Fatalf("邮件步骤应投递成功，实际 %+v", results)
	}
	mails := srv.Mails()
	if len(mails) != 1 {
		t.Fatalf("中继应收到 1 封，实际 %d 封", len(mails))
	}
	parts := parseMailParts(t, mails[0].Data)
	if got := partOf(t, parts, "text/plain"); got != htmlOnlyFallback {
		t.Errorf("纯文本降级文案 = %q，期望 %q", got, htmlOnlyFallback)
	}
	if got := partOf(t, parts, "text/html"); !strings.Contains(got, "阿岚 赞了你的帖子") {
		t.Errorf("HTML 段 = %q", got)
	}
}

// TestEmailHTMLLayoutEscapesText 直接校验内置版式的转义与换行处理。
func TestEmailHTMLLayoutEscapesText(t *testing.T) {
	html := emailHTMLLayout(`《周末骑行 <精选>》`, "第一行 & 第二行\n第三行")
	if strings.Contains(html, "<精选>") {
		t.Errorf("标题应转义：%q", html)
	}
	if !strings.Contains(html, "第一行 &amp; 第二行<br />第三行") {
		t.Errorf("正文应转义并保留换行：%q", html)
	}
	if !strings.Contains(html, "<title>《周末骑行 &lt;精选&gt;》</title>") {
		t.Errorf("文档标题应转义：%q", html)
	}
}
