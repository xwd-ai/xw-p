package pipeline

import (
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// chatMessageLimit 是单条 chat 消息的字符上限。
//
// Slack 的 text block 上限 3000 字符、Teams 消息体上限约 28KB，取更保守的
// 4000 字符作为统一上限：超限时直接在引擎侧失败，而不是等 webhook 返回 400。
const chatMessageLimit = 4000

// consoleChatProvider 是 chat 渠道的 console mock 实现（Slack / Teams webhook 风格）。
//
// 它不真正向 webhook 发 HTTP 请求，而是校验 webhook 地址与消息体后打印日志，
// 模拟「已推送到群机器人」的过程；真实 HTTP 投递在 Phase 3 接入，届时作为
// 另一实现注册进 ProviderRegistry，二者实现同一 Provider 接口。
//
// 社区场景下 chat 渠道用于把互动事件推到运营群（如「热门帖子互动播报」），
// 收件目标是群机器人 webhook，而非订阅者本人，故 Delivery.To 承载 webhook 地址。
type consoleChatProvider struct {
	logger *slog.Logger
}

// NewConsoleChatProvider 创建 chat 渠道的 console mock provider。
func NewConsoleChatProvider(logger *slog.Logger) Provider {
	if logger == nil {
		logger = slog.Default()
	}
	return &consoleChatProvider{logger: logger}
}

// Channel 返回 chat 渠道。
func (c *consoleChatProvider) Channel() domain.Channel {
	return domain.ChannelChat
}

// ID 返回该实现的稳定标识，供注册表寻址与日志溯源。
func (c *consoleChatProvider) ID() string {
	return "console-chat"
}

// Send 校验 webhook 地址与消息体后打印推送内容，模拟提交成功。
//
// 地址为空 / 非 http(s) / 缺少 host 时失败——真实 webhook 也会直接拒收，
// 提前在本地拦住可以避免把必然失败的请求发出去。日志中的 webhook 做脱敏，
// 只保留 scheme + host，避免群机器人 token 泄露。
func (c *consoleChatProvider) Send(d Delivery) SendResult {
	raw := strings.TrimSpace(d.To)
	if raw == "" {
		c.logger.Warn("[chat] 投递失败：缺少 webhook 地址",
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
			"subscriberId", d.SubscriberID,
		)
		return SendResult{Sent: false, Reason: "缺少 chat webhook 地址"}
	}

	hook, err := parseWebhook(raw)
	if err != nil {
		c.logger.Warn("[chat] 投递失败：webhook 地址非法",
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
			"reason", err.Error(),
		)
		return SendResult{Sent: false, Reason: err.Error()}
	}

	body := strings.TrimSpace(d.Body)
	if body == "" {
		c.logger.Warn("[chat] 投递失败：消息正文为空",
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
			"webhook", maskWebhook(hook),
		)
		return SendResult{Sent: false, Reason: "chat 消息正文为空"}
	}
	if n := utf8.RuneCountInString(body); n > chatMessageLimit {
		c.logger.Warn("[chat] 投递失败：消息超过字符上限",
			"chars", n,
			"limit", chatMessageLimit,
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
		)
		return SendResult{
			Sent:   false,
			Reason: fmt.Sprintf("chat 消息超过 %d 字符上限", chatMessageLimit),
		}
	}

	c.logger.Info("[chat] 投递成功（console mock）",
		"webhook", maskWebhook(hook),
		"body", truncateForLog(body, 80),
		"chars", utf8.RuneCountInString(body),
		"stepId", d.StepID,
		"workflow", d.WorkflowName,
		"subscriberId", d.SubscriberID,
	)
	return SendResult{
		Sent:   true,
		Reason: fmt.Sprintf("chat 已推送至 %s", maskWebhook(hook)),
	}
}

// parseWebhook 校验并解析 webhook 地址：必须是 http/https 且带 host。
func parseWebhook(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("chat webhook 地址解析失败")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("chat webhook 仅支持 http/https")
	}
	if u.Host == "" {
		return nil, fmt.Errorf("chat webhook 缺少 host")
	}
	return u, nil
}

// maskWebhook 对 webhook 地址脱敏：只保留 scheme + host，丢弃全部路径。
// 群机器人 token 通常位于路径末段（如 /services/T000/B000/XXXX），一并隐藏。
func maskWebhook(u *url.URL) string {
	return u.Scheme + "://" + u.Host + "/…"
}
