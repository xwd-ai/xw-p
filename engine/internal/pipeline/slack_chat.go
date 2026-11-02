package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"unicode/utf8"

	"github.com/xwd-ai/xw-p/engine/internal/chat"
	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// slackChatProvider 是 chat 渠道的真实实现：把消息提交到群机器人的
// incoming webhook。
//
// 与 console 实现实现同一个 Provider 接口，区别只在最后一步是真发 HTTP
// 还是打日志；校验（地址、正文、长度）两边保持一致，因此把 console 换成
// slack 不会改变哪些消息会被拒。
//
// 收件地址按「步骤 → Provider 默认」解析：步骤配了 webhook 就用它（同一份
// 工作流可以把不同类型的互动播报到不同群），没配则回退到渠道配置里的
// webhook_url；两者都没有时如实判失败，而不是打印成功日志。
type slackChatProvider struct {
	logger   *slog.Logger
	client   *chat.Client
	fallback string
}

// NewSlackChatProvider 按渠道配置构造 chat 渠道实现。
func NewSlackChatProvider(logger *slog.Logger, cfg chat.Config) (Provider, error) {
	if logger == nil {
		logger = slog.Default()
	}
	client, err := chat.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &slackChatProvider{
		logger:   logger,
		client:   client,
		fallback: client.Config().WebhookURL,
	}, nil
}

// Channel 返回 chat 渠道。
func (p *slackChatProvider) Channel() domain.Channel {
	return domain.ChannelChat
}

// ID 返回该实现的稳定标识，供注册表寻址与日志溯源。
func (p *slackChatProvider) ID() string {
	return "slack-chat"
}

// Send 提交消息；不感知取消，供只调用 Send 的调用方使用。
func (p *slackChatProvider) Send(d Delivery) SendResult {
	return p.SendContext(context.Background(), d)
}

// SendContext 提交消息，ctx 取消或步骤超时时中断请求。
func (p *slackChatProvider) SendContext(ctx context.Context, d Delivery) SendResult {
	webhook := strings.TrimSpace(d.To)
	if webhook == "" {
		webhook = p.fallback
	}
	if webhook == "" {
		p.logger.Warn("[chat] 投递失败：缺少 webhook 地址",
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
			"subscriberId", d.SubscriberID,
		)
		return SendResult{Sent: false, Reason: "缺少 chat webhook 地址"}
	}
	if _, err := chat.NormalizeWebhook(webhook); err != nil {
		p.logger.Warn("[chat] 投递失败：webhook 地址非法",
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
			"reason", err.Error(),
		)
		return SendResult{Sent: false, Reason: err.Error()}
	}

	body := strings.TrimSpace(d.Body)
	if body == "" {
		p.logger.Warn("[chat] 投递失败：消息正文为空",
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
			"webhook", chat.MaskWebhook(webhook),
		)
		return SendResult{Sent: false, Reason: "chat 消息正文为空"}
	}
	if n := utf8.RuneCountInString(body); n > chatMessageLimit {
		p.logger.Warn("[chat] 投递失败：消息超过字符上限",
			"chars", n,
			"limit", chatMessageLimit,
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
		)
		return SendResult{
			Sent: false,
			Reason: fmt.Sprintf("chat 消息超过 %d 字符上限（当前 %d）",
				chatMessageLimit, n),
		}
	}

	// 标题来自 render 阶段（没有则为工作流名）：作为卡片的首段，让人一眼
	// 看出是哪类互动，而不是把标题塞在正文第一行。
	msg := chat.BuildMessage(d.Subject, body, chat.BlockTextLimit)
	receipt, err := p.client.Send(ctx, webhook, msg)
	if err != nil {
		reason := err.Error()
		var chatErr *chat.Error
		if errors.As(err, &chatErr) && chatErr.Retryable() {
			// 限流与 5xx 值得重试；4xx 里的地址或报文问题重试多少次都一样。
			reason += "（可稍后重试）"
		}
		p.logger.Warn("[chat] 投递失败",
			"webhook", chat.MaskWebhook(webhook),
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
			"subscriberId", d.SubscriberID,
			"error", reason,
		)
		return SendResult{Sent: false, Reason: reason}
	}

	p.logger.Info("[chat] 投递成功",
		"webhook", chat.MaskWebhook(webhook),
		"status", receipt.Status,
		"chars", utf8.RuneCountInString(body),
		"stepId", d.StepID,
		"workflow", d.WorkflowName,
		"subscriberId", d.SubscriberID,
	)
	return SendResult{
		Sent:   true,
		Reason: "chat 已推送至 " + chat.MaskWebhook(webhook),
	}
}
