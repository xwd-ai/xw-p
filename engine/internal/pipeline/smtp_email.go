package pipeline

import (
	"context"
	"log/slog"
	"strings"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/mailer"
)

// defaultEmailSubject 是步骤没配标题时的兜底主题。
//
// 主题为空在多数邮件客户端里会显示成「(无主题)」，看不出是哪条互动触发的；
// 给一句通用文案比让接收方猜要好。
const defaultEmailSubject = "你有一条新的互动提醒"

// smtpEmailProvider 是 email 渠道的真实实现：把渲染好的内容经 SMTP 提交出去。
//
// 与 console 实现实现同一个 Provider 接口，区别只在最后一步是真连网还是打日志。
// 渠道配置里写 kind=smtp 时由 ProviderFactory 构造本实现；凭据不全或格式不对
// 在装配阶段就会失败，不会带着半套配置跑起来。
type smtpEmailProvider struct {
	logger *slog.Logger
	client *mailer.Client
	from   string
}

// NewSMTPEmailProvider 按 SMTP 配置构造 email 渠道实现。
func NewSMTPEmailProvider(logger *slog.Logger, cfg mailer.Config) (Provider, error) {
	if logger == nil {
		logger = slog.Default()
	}
	client, err := mailer.New(cfg)
	if err != nil {
		return nil, err
	}
	return &smtpEmailProvider{
		logger: logger,
		client: client,
		from:   strings.TrimSpace(cfg.From),
	}, nil
}

// Channel 返回 email 渠道。
func (p *smtpEmailProvider) Channel() domain.Channel {
	return domain.ChannelEmail
}

// ID 返回该实现的稳定标识，供注册表寻址与日志溯源。
func (p *smtpEmailProvider) ID() string {
	return "smtp-email"
}

// Send 提交邮件；不感知取消，供只调用 Send 的调用方使用。
func (p *smtpEmailProvider) Send(d Delivery) SendResult {
	return p.SendContext(context.Background(), d)
}

// SendContext 提交邮件，ctx 取消或步骤超时时中断连接。
func (p *smtpEmailProvider) SendContext(ctx context.Context, d Delivery) SendResult {
	to := strings.TrimSpace(d.To)
	if to == "" {
		// 与 console 实现保持同一口径：没有收件邮箱就是失败，不假装发出去。
		p.logger.Warn("[email] SMTP 投递失败：缺少收件邮箱",
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
			"subscriberId", d.SubscriberID,
		)
		return SendResult{Sent: false, Reason: "收件邮箱为空"}
	}

	subject := strings.TrimSpace(d.Subject)
	if subject == "" {
		subject = defaultEmailSubject
	}
	receipt, err := p.client.Send(ctx, mailer.Message{
		From:    p.from,
		To:      []string{to},
		Subject: subject,
		Text:    d.Body,
	})
	if err != nil {
		// 失败原因写进执行明细：运维据此区分「收件人被拒」与「连不上中继」。
		p.logger.Warn("[email] SMTP 投递失败",
			"to", to,
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
			"subscriberId", d.SubscriberID,
			"error", err.Error(),
		)
		return SendResult{Sent: false, Reason: err.Error()}
	}

	p.logger.Info("[email] SMTP 投递成功",
		"to", to,
		"subject", subject,
		"messageId", receipt.MessageID,
		"stepId", d.StepID,
		"workflow", d.WorkflowName,
		"subscriberId", d.SubscriberID,
	)
	return SendResult{
		Sent:   true,
		Reason: "email 已提交至 " + to + "（" + receipt.MessageID + "）",
	}
}
