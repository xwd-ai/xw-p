package pipeline

import (
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

func TestNormalizePhone(t *testing.T) {
	cases := []struct {
		name   string
		raw    string
		want   string
		wantOK bool
	}{
		{"纯数字", "13800000000", "13800000000", true},
		{"带国际区号", "+8613800000000", "+8613800000000", true},
		{"含分隔符", "+86 138-0000-0000", "+8613800000000", true},
		{"含括号与点", "(138) 0000.0000", "13800000000", true},
		{"空字符串", "", "", false},
		{"仅空白", "   ", "", false},
		{"位数不足", "12345", "", false},
		{"位数过多", "1234567890123456", "", false},
		{"含字母", "138abc00000", "", false},
		{"加号不在首位", "138+00000000", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := NormalizePhone(tc.raw)
			if ok != tc.wantOK {
				t.Fatalf("NormalizePhone(%q) ok = %v, 期望 %v", tc.raw, ok, tc.wantOK)
			}
			if ok && got != tc.want {
				t.Errorf("NormalizePhone(%q) = %q, 期望 %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestSMSSegments(t *testing.T) {
	long := func(n int, filler rune) string {
		out := make([]rune, n)
		for i := range out {
			out[i] = filler
		}
		return string(out)
	}

	cases := []struct {
		name string
		body string
		want int
	}{
		{"空正文", "", 0},
		{"ASCII 单段", long(160, 'a'), 1},
		{"ASCII 超一段", long(161, 'a'), 2},
		{"ASCII 两段边界", long(306, 'a'), 2},
		{"ASCII 三段", long(307, 'a'), 3},
		{"中文单段", long(70, '赞'), 1},
		{"中文超一段", long(71, '赞'), 2},
		{"中文 134 字两段", long(134, '赞'), 2},
		// 71 个字符纯 ASCII 只需 1 段，但只要含一个非 ASCII 字符，整条按 UCS-2
		// 计费（单段 70 字符），因此这里变成 2 段。
		{"中英混排转 UCS-2 计费", "post P1001 " + long(60, '赞'), 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SMSSegments(tc.body); got != tc.want {
				t.Errorf("SMSSegments(len=%d) = %d, 期望 %d", len([]rune(tc.body)), got, tc.want)
			}
		})
	}
}

func TestConsoleSMSProviderSends(t *testing.T) {
	p := NewConsoleSMSProvider(discardLogger())
	if p.Channel() != domain.ChannelSMS {
		t.Errorf("渠道 = %s, 期望 sms", p.Channel())
	}

	res := p.Send(Delivery{
		Channel:      domain.ChannelSMS,
		To:           "+86 138-0000-0000",
		Body:         "你的帖子 P1001 收到 1 条新评论",
		StepID:       "s2",
		WorkflowName: "post_commented",
		SubscriberID: "sub_1",
	})
	if !res.Sent {
		t.Fatalf("sms 应投递成功, 结果 = %+v", res)
	}
	if res.Reason == "" {
		t.Error("成功结果应携带原因说明")
	}
}

func TestConsoleSMSProviderFailsWithoutPhone(t *testing.T) {
	p := NewConsoleSMSProvider(discardLogger())

	for _, to := range []string{"", "   ", "+86 138abc"} {
		res := p.Send(Delivery{Channel: domain.ChannelSMS, To: to, Body: "内容"})
		if res.Sent {
			t.Errorf("号码 %q 应投递失败", to)
		}
		if res.Reason == "" {
			t.Errorf("号码 %q 的失败结果应携带原因", to)
		}
	}
}

func TestDispatchSMSViaProvider(t *testing.T) {
	registry := NewProviderRegistry()
	registry.Register(NewConsoleSMSProvider(discardLogger()))
	p := NewWithRegistry(nil, registry)

	wf := domain.Workflow{
		ID:     "wf_sms",
		Name:   "post_liked",
		Active: true,
		Steps: []domain.Step{
			{
				ID:   "s1",
				Type: domain.ChannelSMS,
				Name: "点赞短信",
				Config: map[string]any{
					"template": "帖子 {{ payload.post_id }} 获得 {{ payload.likes }} 个赞",
				},
			},
		},
	}

	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "post_liked",
		SubscriberID: "sub_1",
		Phone:        "13800000000",
		Channels:     []string{"sms"},
		Payload:      map[string]any{"post_id": "P1001", "likes": 3},
	})
	results := p.Run(ctx, wf)

	if len(results) != 1 {
		t.Fatalf("投递结果数 = %d, 期望 1", len(results))
	}
	res := results[0]
	if res.Error != "" {
		t.Errorf("sms 步骤出错: %s", res.Error)
	}
	if !res.Sent {
		t.Error("sms 步骤应标记为已发送")
	}
	want := "帖子 P1001 获得 3 个赞"
	if res.Rendered != want {
		t.Errorf("渲染结果 = %q, 期望 %q", res.Rendered, want)
	}
}

func TestDispatchSMSFailsWithoutPhone(t *testing.T) {
	registry := NewProviderRegistry()
	registry.Register(NewConsoleSMSProvider(discardLogger()))
	p := NewWithRegistry(nil, registry)

	wf := domain.Workflow{
		ID:     "wf_sms",
		Name:   "post_liked",
		Active: true,
		Steps:  []domain.Step{{ID: "s1", Type: domain.ChannelSMS}},
	}
	ctx := NewContext(queue.QueuedMessage{
		TriggerID:    "t1",
		Name:         "post_liked",
		SubscriberID: "sub_1",
		Payload:      map[string]any{"post_id": "P1001"},
	})
	results := p.Run(ctx, wf)

	if results[0].Error == "" {
		t.Error("缺少手机号应导致 sms 步骤失败")
	}
	if results[0].Sent {
		t.Error("失败步骤不应标记已发送")
	}
}
