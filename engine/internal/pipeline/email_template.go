package pipeline

import (
	"html"
	"strings"
)

// 邮件 HTML 版式：把已渲染的纯文本正文包成一封可直接阅读的邮件。
//
// 邮件客户端对 CSS 的支持远不如浏览器：外部样式表、<style> 块在部分客户端里
// 会被丢掉，flex/grid 更是基本不可用。因此这里只用表格布局 + 内联样式，
// 颜色与字号全部写在元素上，保证在不同客户端里呈现一致。
//
// 变量在进入本函数之前已经过 HTML 转义（见 renderStage.RenderHTML），
// 但仍然对传入值再转义一次：本函数也可能被直接调用（例如测试或后续的
// digest 复用），不能把「调用方已经转义过」当成前提。

// emailHTMLTitle 是版式头部的小字标识，说明这封邮件的来源。
const emailHTMLTitle = "XW 互动提醒"

// emailHTMLFooter 是版式底部的说明文案。
const emailHTMLFooter = "你在内容社区里关注了这条互动，才会收到这封邮件。"

// emailHTMLLayout 用内置版式渲染一封邮件：标题 + 正文 + 页脚。
//
// subject 与 text 都会做 HTML 转义；正文里的换行转成 <br />，因为纯文本
// 模板的作者通常靠换行分段，不转成换行会让整段挤成一行。
func emailHTMLLayout(subject, text string) string {
	title := strings.TrimSpace(subject)
	if title == "" {
		title = emailHTMLTitle
	}
	var b strings.Builder
	b.WriteString("<!DOCTYPE html>\n")
	b.WriteString(`<html lang="zh-CN"><head><meta charset="utf-8">`)
	b.WriteString(`<meta name="viewport" content="width=device-width, initial-scale=1">`)
	b.WriteString("<title>")
	b.WriteString(html.EscapeString(title))
	b.WriteString("</title></head>\n")
	b.WriteString(`<body style="margin:0;padding:24px 12px;background:#f4f5f7;` +
		`font-family:-apple-system,BlinkMacSystemFont,'PingFang SC','Microsoft YaHei',Helvetica,Arial,sans-serif;` +
		`color:#1c1f23;">` + "\n")
	b.WriteString(`<table role="presentation" cellpadding="0" cellspacing="0" width="100%" ` +
		`style="max-width:560px;margin:0 auto;border-collapse:separate;background:#ffffff;` +
		`border:1px solid #e6e8eb;border-radius:12px;">` + "\n")
	b.WriteString(`<tr><td style="padding:24px 24px 0;">` + "\n")
	b.WriteString(`<div style="font-size:12px;letter-spacing:0.08em;color:#8a8f98;">` +
		emailHTMLTitle + `</div>` + "\n")
	b.WriteString(`<h1 style="margin:8px 0 0;font-size:18px;line-height:1.5;font-weight:600;">`)
	b.WriteString(html.EscapeString(title))
	b.WriteString("</h1>\n</td></tr>\n")
	b.WriteString(`<tr><td style="padding:16px 24px 24px;font-size:14px;line-height:1.75;color:#3c4149;">`)
	b.WriteString(textToHTML(text))
	b.WriteString("</td></tr>\n")
	b.WriteString(`<tr><td style="padding:16px 24px 24px;border-top:1px solid #eceef1;` +
		`font-size:12px;line-height:1.6;color:#8a8f98;">`)
	b.WriteString(emailHTMLFooter)
	b.WriteString("</td></tr>\n</table>\n</body></html>\n")
	return b.String()
}

// textToHTML 把纯文本正文转成 HTML 片段：先整体转义，再把换行转成 <br />。
//
// 顺序不能颠倒：先替换换行再转义会把 <br /> 一起转义掉。
func textToHTML(text string) string {
	escaped := html.EscapeString(strings.TrimRight(text, "\r\n"))
	escaped = strings.ReplaceAll(escaped, "\r\n", "\n")
	return strings.ReplaceAll(escaped, "\n", "<br />")
}
