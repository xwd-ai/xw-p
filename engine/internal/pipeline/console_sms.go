package pipeline

import (
	"fmt"
	"log/slog"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// SMS 计费分段参数。
//
// 短信按「段」计费：纯 ASCII（GSM-7）单段 160 字符，含非 ASCII 字符
// （如中文）时转为 UCS-2，单段 70 字符；超过单段上限后多段拼接，每段
// 再扣掉 7 字符的拼接头，因此多段容量分别为 153 / 67。
const (
	smsGSMSingleSegment  = 160
	smsGSMConcatSegment  = 153
	smsUCS2SingleSegment = 70
	smsUCS2ConcatSegment = 67
)

// consoleSMSProvider 是 sms 渠道的 console mock 实现。
//
// 它不真正对接短信网关，而是把号码、正文与计费分段数打印到日志，
// 模拟「已提交到网关」的过程。真实网关（国内云厂商 / Twilio 风格接口）
// 在 Phase 3 接入，届时作为另一实现注册进 ProviderRegistry。
type consoleSMSProvider struct {
	logger *slog.Logger
}

// NewConsoleSMSProvider 创建 sms 渠道的 console mock provider。
func NewConsoleSMSProvider(logger *slog.Logger) Provider {
	if logger == nil {
		logger = slog.Default()
	}
	return &consoleSMSProvider{logger: logger}
}

// Channel 返回 sms 渠道。
func (c *consoleSMSProvider) Channel() domain.Channel {
	return domain.ChannelSMS
}

// ID 返回该实现的稳定标识，供注册表寻址与日志溯源。
func (c *consoleSMSProvider) ID() string {
	return "console-sms"
}

// Send 校验手机号并打印短信内容，模拟提交成功。
//
// 号码为空或格式非法时投递失败——真实反映「创作者未绑定手机号」的边界，
// 而不是无条件成功。号码通过校验时按段计费规则计算段数并记录。
func (c *consoleSMSProvider) Send(d Delivery) SendResult {
	to, ok := NormalizePhone(d.To)
	if !ok {
		c.logger.Warn("[sms] 投递失败：手机号为空或格式非法",
			"raw", d.To,
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
			"subscriberId", d.SubscriberID,
		)
		return SendResult{Sent: false, Reason: "手机号为空或格式非法"}
	}

	segments := SMSSegments(d.Body)
	c.logger.Info("[sms] 投递成功（console mock）",
		"to", to,
		"body", d.Body,
		"segments", segments,
		"stepId", d.StepID,
		"workflow", d.WorkflowName,
		"subscriberId", d.SubscriberID,
	)
	return SendResult{
		Sent:   true,
		Reason: fmt.Sprintf("sms 已提交至 %s（%d 段）", to, segments),
	}
}

// NormalizePhone 规范化并校验手机号：去掉空格、连字符与括号，允许一个前导
// "+"，其余必须全为数字，数字位数 6–15（E.164 上限 15 位）。
// 返回规范化号码与是否通过校验。
func NormalizePhone(raw string) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", false
	}

	var b strings.Builder
	b.Grow(len(trimmed))
	digits := 0
	for i, r := range trimmed {
		switch {
		case r == '+' && i == 0:
			b.WriteRune(r)
		case r == ' ' || r == '-' || r == '(' || r == ')' || r == '.':
			// 常见分隔符：直接丢弃
		case unicode.IsDigit(r) && r <= unicode.MaxASCII:
			b.WriteRune(r)
			digits++
		default:
			return "", false
		}
	}

	if digits < 6 || digits > 15 {
		return "", false
	}
	return b.String(), true
}

// SMSSegments 按 GSM-7 / UCS-2 计费规则计算短信段数。
//
// 纯 ASCII 正文走 GSM-7（160/153），含任意非 ASCII 字符（中文、emoji 等）
// 整条转为 UCS-2（70/67）。空正文按 0 段返回。
func SMSSegments(body string) int {
	if body == "" {
		return 0
	}

	gsm := true
	for _, r := range body {
		if r > unicode.MaxASCII {
			gsm = false
			break
		}
	}

	length := utf8.RuneCountInString(body)
	if gsm {
		if length <= smsGSMSingleSegment {
			return 1
		}
		return ceilDiv(length, smsGSMConcatSegment)
	}
	if length <= smsUCS2SingleSegment {
		return 1
	}
	return ceilDiv(length, smsUCS2ConcatSegment)
}

// ceilDiv 返回 a/b 向上取整的结果（a、b 均为正数）。
func ceilDiv(a, b int) int {
	return (a + b - 1) / b
}
