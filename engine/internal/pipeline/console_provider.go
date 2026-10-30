package pipeline

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// consoleEmailProvider 是 email 渠道的 console mock 实现。
//
// 它不真正连接 SMTP，而是把邮件的内容（收件人/主题/正文）通过 logger 输出，
// 模拟「已发送」的过程。真实 SMTP 发送在 Phase 3 接入，届时将作为另一实现
// 注册进 ProviderRegistry，二者实现同一 Provider 接口。
type consoleEmailProvider struct {
	logger *slog.Logger
}

// NewConsoleEmailProvider 创建 email 渠道的 console mock provider。
func NewConsoleEmailProvider(logger *slog.Logger) Provider {
	if logger == nil {
		logger = slog.Default()
	}
	return &consoleEmailProvider{logger: logger}
}

// Channel 返回 email 渠道。
func (c *consoleEmailProvider) Channel() domain.Channel {
	return domain.ChannelEmail
}

// ID 返回该实现的稳定标识，供注册表寻址与日志溯源。
func (c *consoleEmailProvider) ID() string {
	return "console-email"
}

// Send 把邮件内容打印到控制台，模拟发送成功。
//
// 收件地址为空时视为无收件人，投递失败——这样能真实反映「订阅者未绑定邮箱」
// 的边界情况，而不是无条件成功。
func (c *consoleEmailProvider) Send(d Delivery) SendResult {
	to := strings.TrimSpace(d.To)
	if to == "" {
		c.logger.Warn("[email] 投递失败：缺少收件邮箱",
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
			"subscriberId", d.SubscriberID,
		)
		return SendResult{Sent: false, Reason: "收件邮箱为空"}
	}

	c.logger.Info("[email] 投递成功（console mock）",
		"to", to,
		"subject", d.Subject,
		"body", d.Body,
		"stepId", d.StepID,
		"workflow", d.WorkflowName,
		"subscriberId", d.SubscriberID,
	)
	return SendResult{
		Sent:   true,
		Reason: fmt.Sprintf("email 已发送至 %s", to),
	}
}
