package pipeline

// 模板变量注入防护的测试。
//
// 关注点不是「渲染结果对不对」（render_test.go 管这个），而是「用户可控的
// 内容进入渠道时会不会改变下游的报文结构」：邮件头、日志行、卡片正文、
// HTML 页面。这些都是变量值能碰到的地方，也是注入真正落地的地方。

import (
	"strings"
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

func TestSanitizeSingleLine(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"普通标题原样保留", "《周末骑行》有新评论", "《周末骑行》有新评论"},
		{"CRLF 折成空格", "《周末骑行》\r\n有新评论", "《周末骑行》 有新评论"},
		{"单独 CR 折成空格", "新评论\rBcc: evil@example.com", "新评论 Bcc: evil@example.com"},
		{"LF 折成空格", "第一行\n第二行", "第一行 第二行"},
		{"制表符与连续空白压成一个空格", "互动\t\t 汇总  ", "互动 汇总"},
		{"NUL 与 ANSI 序列被移除", "赞了你\x00\x1b[2J", "赞了你"},
		{"DEL 被移除", "赞了你\x7f", "赞了你"},
		{"C1 控制字符被移除", "赞了你\u0085了", "赞了你 了"},
		{"Unicode 行分隔符折成空格", "第一段\u2028第二段", "第一段 第二段"},
		{"全角空格压成半角", "互动　汇总", "互动 汇总"},
		{"空串", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := sanitizeSingleLine(c.in); got != c.want {
				t.Errorf("sanitizeSingleLine(%q) = %q, 期望 %q", c.in, got, c.want)
			}
			// 幂等：渲染阶段与分发阶段各调一次不应再改一遍。
			if again := sanitizeSingleLine(sanitizeSingleLine(c.in)); again != c.want {
				t.Errorf("二次清理改变了结果: %q", again)
			}
		})
	}
}

func TestSanitizeMultiLine(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"正文换行保留", "第一行\n第二行", "第一行\n第二行"},
		{"CRLF 归一成 LF", "第一行\r\n第二行", "第一行\n第二行"},
		{"孤立 CR 归一成 LF", "第一行\r第二行", "第一行\n第二行"},
		{"制表符保留", "列表\t项目", "列表\t项目"},
		{"ANSI 颜色序列被整段移除", "评论\x1b[31m红色\x1b[0m", "评论红色"},
		{"NUL 与 DEL 被移除", "评论\x00正文\x7f", "评论正文"},
		{"C1 控制字符被移除", "评论\u0085正文", "评论正文"},
		{"段落分隔符当成换行", "第一段\u2029第二段", "第一段\n第二段"},
		{"OSC 序列（含 BEL 终止）被移除", "评论\x1b]0;伪造的标题\x07正文", "评论正文"},
		{"未终止的 CSI 序列吃到串尾", "评论\x1b[31", "评论"},
		{"孤立 ESC 只丢自身", "评论\x1b正文", "评论正文"},
		{"空串", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := sanitizeMultiLine(c.in); got != c.want {
				t.Errorf("sanitizeMultiLine(%q) = %q, 期望 %q", c.in, got, c.want)
			}
			if again := sanitizeMultiLine(sanitizeMultiLine(c.in)); again != c.want {
				t.Errorf("二次清理改变了结果: %q", again)
			}
		})
	}
}

// TestRenderDoesNotReexpandValues 锁定「替换是单遍的」。
//
// 变量值里若含模板语法，拼进结果之后不能再被求值一次：否则终端用户只要在
// 评论里写 {{ ... }} 就能读到本该只属于工作流的变量，甚至调用内置函数。
func TestRenderDoesNotReexpandValues(t *testing.T) {
	renderer := newRenderStage()
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "post_commented",
		SubscriberID: "creator_1",
		Payload: map[string]any{
			"comment":   "试试 {{ payload.secret }} 和 {{ upper payload.secret }}",
			"secret":    "不该被读到的值",
			"post_id":   "P1001",
			"post_name": "周末骑行",
		},
	})

	body, err := renderer.Render(ctx, "{{ payload.comment }}")
	if err != nil {
		t.Fatalf("渲染失败: %v", err)
	}
	if body != "试试 {{ payload.secret }} 和 {{ upper payload.secret }}" {
		t.Fatalf("变量值被二次求值: %q", body)
	}
	if strings.Contains(body, "不该被读到的值") {
		t.Fatalf("变量值里的表达式被求值，泄露了其它字段: %q", body)
	}

	// 同一个值出现在多个位置时同样是字面量：替换按出现位置各做一次，
	// 但都没有再进求值器。
	both, err := renderer.Render(ctx, "{{ payload.comment }} / {{ payload.post_name }}")
	if err != nil {
		t.Fatalf("渲染失败: %v", err)
	}
	if strings.Count(both, "{{ payload.secret }}") != 1 {
		t.Fatalf("多次替换的结果不一致: %q", both)
	}
	if strings.Contains(both, "不该被读到的值") {
		t.Fatalf("变量值里的表达式被求值: %q", both)
	}
}

// TestRenderInlineFlattensSubject 验证标题里的换行不会带到投递请求上。
func TestRenderInlineFlattensSubject(t *testing.T) {
	step := domain.Step{
		ID:   "mail",
		Type: domain.ChannelEmail,
		Name: "评论邮件",
		Config: map[string]any{
			"subject":  "《{{ payload.post_title }}》有新评论",
			"template": "{{ payload.actor }}：{{ payload.comment }}",
		},
	}
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "post_commented",
		SubscriberID: "creator_1",
		Payload: map[string]any{
			// 帖子标题带 CRLF：不清理就能往邮件里加一行头字段。
			"post_title": "周末骑行\r\nBcc: attacker@example.com\r\nX-Spam: no",
			"actor":      "阿岚",
			"comment":    "这段路线不错",
		},
	})

	stage := newRenderStage()
	if err := stage.Apply(ctx, step); err != nil {
		t.Fatalf("渲染失败: %v", err)
	}
	res := ctx.Results[step.ID]
	if strings.ContainsAny(res.RenderedSubject, "\r\n") {
		t.Fatalf("标题仍含换行: %q", res.RenderedSubject)
	}
	if !strings.Contains(res.RenderedSubject, "Bcc: attacker@example.com") {
		t.Fatalf("标题内容丢失: %q", res.RenderedSubject)
	}
	// 正文是多行场景，换行保留，但控制字符仍要剥掉。
	if !strings.Contains(res.Rendered, "阿岚：这段路线不错") {
		t.Fatalf("正文渲染异常: %q", res.Rendered)
	}
}

// TestRenderInlineFlattensLiteralTemplate 验证模板自身的换行同样被折平：
// 多行 YAML 里写的标题带换行，和变量值带换行一样会破坏单行字段。
func TestRenderInlineFlattensLiteralTemplate(t *testing.T) {
	renderer := newRenderStage()
	ctx := NewContext(queue.QueuedMessage{TriggerID: "t1", Name: "post_liked"})

	got, err := renderer.RenderInline(ctx, "你的帖子有了\n新互动\r\n")
	if err != nil {
		t.Fatalf("渲染失败: %v", err)
	}
	if got != "你的帖子有了 新互动" {
		t.Fatalf("单行渲染结果 = %q", got)
	}
}

// TestRenderHTMLStripsControlsBeforeEscaping 验证 HTML 形态的处理顺序：
// 先清控制字符再转义，否则控制字符会以实体形式留在页面里。
func TestRenderHTMLStripsControlsBeforeEscaping(t *testing.T) {
	renderer := newRenderStage()
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "post_commented",
		SubscriberID: "creator_1",
		Payload: map[string]any{
			"comment": "<script>alert(1)</script>\x00\x1b[2J",
		},
	})

	got, err := renderer.RenderHTML(ctx, "<p>{{ payload.comment }}</p>")
	if err != nil {
		t.Fatalf("渲染失败: %v", err)
	}
	if strings.Contains(got, "<script>") {
		t.Fatalf("HTML 未被转义: %q", got)
	}
	if strings.Contains(got, "\x00") || strings.Contains(got, "\x1b") {
		t.Fatalf("控制字符未被清理: %q", got)
	}
	// 模板自身的标签保留，说明只清理了变量值。
	if !strings.HasPrefix(got, "<p>&lt;script&gt;") {
		t.Fatalf("模板标签被破坏: %q", got)
	}
}

// TestRenderRejectsOversizedOutput 验证放大效应被拦住。
//
// 模板本身只有几十字节，但表达式指向一个巨大的变量值；不设上限的话，
// 「表达式个数 × 单值大小」就能把渲染缓冲撑到任意大。
func TestRenderRejectsOversizedOutput(t *testing.T) {
	renderer := newRenderStage()
	big := strings.Repeat("赞", maxRenderedBytes) // 每个汉字 3 字节，远超上限
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "post_commented",
		SubscriberID: "creator_1",
		Payload:      map[string]any{"comment": big},
	})

	if _, err := renderer.Render(ctx, "{{ payload.comment }}"); err == nil {
		t.Fatal("超大变量值应导致渲染失败")
	}

	// 多个表达式累加同样受同一个上限约束。
	tmpl := strings.Repeat("{{ payload.comment }} ", 8)
	if _, err := renderer.Render(ctx, tmpl); err == nil {
		t.Fatal("多个大变量值累加后应导致渲染失败")
	}
}

// TestDispatchSendsCleanSubjectAndBody 走完整管线：投递请求上的标题与正文
// 都不带换行与控制字符，注入尝试只体现为文本内容。
func TestDispatchSendsCleanSubjectAndBody(t *testing.T) {
	email := &recordingProvider{ch: domain.ChannelEmail}
	registry := NewProviderRegistry()
	registry.Register(email)
	p := NewWithRegistry(discardLogger(), registry)

	wf := domain.Workflow{
		ID:      "wf_post_commented",
		Name:    "post_commented",
		Version: 1,
		Active:  true,
		Steps: []domain.Step{{
			ID:   "mail",
			Type: domain.ChannelEmail,
			Name: "评论邮件",
			Config: map[string]any{
				"subject":  "《{{ payload.post_title }}》有新评论",
				"template": "{{ payload.actor }} 评论了你的帖子\n\n{{ payload.comment }}",
			},
		}},
	}
	msg := queue.QueuedMessage{
		TriggerID:    "t-inject-1",
		Name:         "post_commented",
		SubscriberID: "creator_1",
		Email:        "creator@example.com",
		Payload: map[string]any{
			"post_id":    "P7001",
			"post_title": "周末骑行\r\nBcc: attacker@example.com",
			"actor":      "阿岚\x1b[31m",
			"comment":    "这条路线我骑过\x00\r\n下次一起",
		},
	}

	res := p.Run(NewContext(msg), wf)[0]
	if res.Error != "" {
		t.Fatalf("执行失败: %s", res.Error)
	}
	got := email.Last()
	if strings.ContainsAny(got.Subject, "\r\n") {
		t.Fatalf("投递标题含换行: %q", got.Subject)
	}
	if strings.Contains(got.Subject, "{{") {
		t.Fatalf("投递标题残留模板语法: %q", got.Subject)
	}
	if strings.ContainsAny(got.Body, "\x00\x1b") {
		t.Fatalf("投递正文含控制字符: %q", got.Body)
	}
	if !strings.Contains(got.Body, "\n\n") {
		t.Fatalf("正文段间空行被压掉: %q", got.Body)
	}
	// 注入片段仍在，只是变成了一行普通文本——被清理，而不是被丢弃。
	if !strings.Contains(got.Subject, "Bcc: attacker@example.com") {
		t.Fatalf("标题内容被丢弃: %q", got.Subject)
	}
}

// TestDispatchSubjectFallsBackWhenTemplateEmpty 验证渲染后标题为空时不会
// 把模板原文当标题发出去。
func TestDispatchSubjectFallsBackWhenTemplateEmpty(t *testing.T) {
	sms := &countingProvider{ch: domain.ChannelSMS}
	registry := NewProviderRegistry()
	registry.Register(sms)
	p := NewWithRegistry(discardLogger(), registry)

	wf := domain.Workflow{
		ID:      "wf_post_liked",
		Name:    "post_liked",
		Version: 1,
		Active:  true,
		Steps: []domain.Step{{
			ID:   "sms",
			Type: domain.ChannelSMS,
			Name: "点赞短信",
			Config: map[string]any{
				// 帖子标题为空：渲染后标题是空串。
				"title":    "{{ payload.post_title }}",
				"template": "你的帖子收到一个赞",
			},
		}},
	}
	msg := queue.QueuedMessage{
		TriggerID:    "t-inject-2",
		Name:         "post_liked",
		SubscriberID: "creator_1",
		Phone:        "13800000000",
		Payload:      map[string]any{"post_id": "P7002", "post_title": ""},
	}

	res := p.Run(NewContext(msg), wf)[0]
	if res.Error != "" {
		t.Fatalf("执行失败: %s", res.Error)
	}
	got := sms.last.Subject
	if hasTemplateExpr(got) {
		t.Fatalf("标题残留模板语法: %q", got)
	}
	if got != "post_liked" {
		t.Fatalf("标题回退结果 = %q, 期望工作流名", got)
	}
}

// TestDigestCardTextIsSanitized 验证聚合卡片的标题、正文与聚合键都过清理：
// 这些内容会被写进收件箱，最终由前端渲染，控制字符没有理由跟着一起存下来。
func TestDigestCardTextIsSanitized(t *testing.T) {
	p, store := newDigestPipeline(t)

	wf := likedDigestWorkflow(map[string]any{
		"key":      "post_id",
		"window":   "10m",
		"title":    "帖子 {{ payload.post_id }} 有新互动",
		"template": "{{ digest.actor }} 等 {{ digest.count }} 人赞了你",
	})
	msg := digestMessage("t-inject-3", "P7003\r\nX-Fake: 1", "阿岚\x1b[31m")

	res := p.Run(NewContext(msg), wf)[0]
	if res.Error != "" {
		t.Fatalf("聚合执行失败: %s", res.Error)
	}
	if strings.ContainsAny(res.AggregateKey, "\r\n") {
		t.Fatalf("聚合键含换行: %q", res.AggregateKey)
	}

	entries, err := store.List("creator_1")
	if err != nil {
		t.Fatalf("读取收件箱失败: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("收件箱条目数 = %d, 期望 1", len(entries))
	}
	card := entries[0]
	if strings.ContainsAny(card.Title, "\r\n\x00\x1b") {
		t.Fatalf("卡片标题含控制字符: %q", card.Title)
	}
	if strings.ContainsAny(card.Body, "\x00\x1b") {
		t.Fatalf("卡片正文含控制字符: %q", card.Body)
	}
	if strings.Contains(card.Title, "{{") {
		t.Fatalf("卡片标题残留模板语法: %q", card.Title)
	}
}
