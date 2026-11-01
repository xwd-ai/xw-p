package mailer

import (
	"encoding/base64"
	"mime"
	"strings"
	"testing"
	"time"
)

// fixedClock 让报文的 Date / Message-ID 稳定，便于断言。
var fixedClock = time.Date(2026, 3, 14, 9, 26, 53, 0, time.FixedZone("CST", 8*3600))

// buildMail 拼装一封邮件并返回报文与信封收件人。
func buildMail(t *testing.T, msg Message) string {
	t.Helper()
	envelopeFrom, recipients, err := msg.normalize()
	if err != nil {
		t.Fatalf("消息校验失败：%v", err)
	}
	return msg.build(envelopeFrom, recipients, fixedClock)
}

// splitRawMail 把头字段与正文分开，头字段解析成 map（键统一小写）。
func splitRawMail(t *testing.T, raw string) (map[string]string, string) {
	t.Helper()
	head, body, ok := strings.Cut(raw, "\r\n\r\n")
	if !ok {
		t.Fatalf("报文缺少头与正文的分隔：%q", raw)
	}
	headers := map[string]string{}
	for _, line := range strings.Split(head, "\r\n") {
		name, value, ok := strings.Cut(line, ": ")
		if !ok {
			t.Fatalf("头字段格式不正确：%q", line)
		}
		headers[strings.ToLower(name)] = value
	}
	return headers, body
}

// decodeBase64Body 解出一段 base64（忽略折行产生的空白）。
func decodeBase64Body(t *testing.T, encoded string) string {
	t.Helper()
	decoded, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(encoded), ""))
	if err != nil {
		t.Fatalf("内容不是合法 base64：%v", err)
	}
	return string(decoded)
}

// TestBuildTextOnlyMail 确认没有 HTML 时仍是单段纯文本，报文结构与之前一致。
func TestBuildTextOnlyMail(t *testing.T) {
	raw := buildMail(t, Message{
		From:    "互动提醒 <notify@example.com>",
		To:      []string{"creator@example.com"},
		Subject: "《周末骑行》有新评论",
		Text:    "阿岚 说：这条路线我也骑过",
	})

	headers, body := splitRawMail(t, raw)
	if got := headers["content-type"]; got != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type = %q，期望纯文本", got)
	}
	if strings.Contains(headers["content-type"], "boundary") {
		t.Error("纯文本邮件不应带 boundary")
	}
	if got := decodeBase64Body(t, body); got != "阿岚 说：这条路线我也骑过" {
		t.Errorf("正文 = %q", got)
	}
}

// TestBuildMultipartAlternativeMail 确认有 HTML 时按 multipart/alternative
// 提交，纯文本在前、HTML 在后，两段内容都能原样解出来。
func TestBuildMultipartAlternativeMail(t *testing.T) {
	text := "阿岚 说：这条路线我也骑过"
	htmlBody := "<p>阿岚 说：<strong>这条路线我也骑过</strong></p>"
	raw := buildMail(t, Message{
		From:    "互动提醒 <notify@example.com>",
		To:      []string{"creator@example.com"},
		Subject: "《周末骑行》有新评论",
		Text:    text,
		HTML:    htmlBody,
	})

	headers, body := splitRawMail(t, raw)
	contentType := headers["content-type"]
	if !strings.HasPrefix(contentType, "multipart/alternative;") {
		t.Fatalf("Content-Type = %q，期望 multipart/alternative", contentType)
	}
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		t.Fatalf("Content-Type 解析失败：%v", err)
	}
	if mediaType != "multipart/alternative" {
		t.Errorf("媒体类型 = %q", mediaType)
	}
	boundary := params["boundary"]
	if boundary == "" {
		t.Fatal("multipart 邮件应带 boundary")
	}

	parts := strings.Split(body, "--"+boundary)
	// 形如：["", 第一段, 第二段, "--\r\n"]。
	if len(parts) != 4 {
		t.Fatalf("应有两段正文，实际分隔出 %d 段：%q", len(parts)-1, body)
	}
	if !strings.HasPrefix(parts[3], "--") {
		t.Errorf("报文应以结束分隔行收尾，实际 %q", parts[3])
	}

	wantParts := []struct {
		contentType string
		content     string
	}{
		{"text/plain; charset=utf-8", text},
		{"text/html; charset=utf-8", htmlBody},
	}
	for i, want := range wantParts {
		partHeaders, partBody := splitRawMail(t, strings.TrimPrefix(parts[i+1], "\r\n"))
		if got := partHeaders["content-type"]; got != want.contentType {
			t.Errorf("第 %d 段 Content-Type = %q，期望 %q", i+1, got, want.contentType)
		}
		if got := partHeaders["content-transfer-encoding"]; got != "base64" {
			t.Errorf("第 %d 段传输编码 = %q", i+1, got)
		}
		if got := decodeBase64Body(t, partBody); got != want.content {
			t.Errorf("第 %d 段正文 = %q，期望 %q", i+1, got, want.content)
		}
	}
}

// TestBuildMultipartBoundaryIsUnique 同一进程连发两封邮件时边界不能复用，
// 否则内嵌的转发内容可能被误当作本封邮件的分隔行。
func TestBuildMultipartBoundaryIsUnique(t *testing.T) {
	msg := Message{
		From:    "notify@example.com",
		To:      []string{"creator@example.com"},
		Subject: "新点赞",
		Text:    "纯文本",
		HTML:    "<p>HTML</p>",
	}
	first := buildMail(t, msg)
	second := buildMail(t, msg)
	if first == second {
		t.Fatal("两封邮件的报文不应完全相同")
	}
	boundaryOf := func(raw string) string {
		headers, _ := splitRawMail(t, raw)
		_, params, err := mime.ParseMediaType(headers["content-type"])
		if err != nil {
			t.Fatalf("Content-Type 解析失败：%v", err)
		}
		return params["boundary"]
	}
	if boundaryOf(first) == boundaryOf(second) {
		t.Error("两封邮件的 boundary 不应相同")
	}
}

// TestBuildIgnoresBlankHTML 只有空白字符的 HTML 视为没写，保持单段纯文本。
func TestBuildIgnoresBlankHTML(t *testing.T) {
	raw := buildMail(t, Message{
		From:    "notify@example.com",
		To:      []string{"creator@example.com"},
		Subject: "新点赞",
		Text:    "阿岚 赞了你的帖子",
		HTML:    "   \r\n\t ",
	})
	headers, body := splitRawMail(t, raw)
	if got := headers["content-type"]; got != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type = %q，期望纯文本", got)
	}
	if got := decodeBase64Body(t, body); got != "阿岚 赞了你的帖子" {
		t.Errorf("正文 = %q", got)
	}
}

// TestBuildKeepsLongBase64Wrapped 多段报文的每一段都要按 76 列折行，
// 否则超长行会被部分中继拒收。
func TestBuildKeepsLongBase64Wrapped(t *testing.T) {
	long := strings.Repeat("这条路线我也骑过，风景很好。", 20)
	raw := buildMail(t, Message{
		From:    "notify@example.com",
		To:      []string{"creator@example.com"},
		Subject: "长评论",
		Text:    long,
		HTML:    "<p>" + long + "</p>",
	})
	for _, line := range strings.Split(raw, "\r\n") {
		if len(line) > 76 {
			// 头字段（Content-Type 等）可以长一些，正文行必须折到 76 列内。
			if strings.HasPrefix(line, "--") || strings.Contains(line, ": ") {
				continue
			}
			t.Fatalf("正文行长度 %d 超过 76 列：%q", len(line), line)
		}
	}
}
