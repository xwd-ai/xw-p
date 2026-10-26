package pipeline

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// pushPayloadLimit 是单条推送 payload 的字节上限。
//
// APNs 与 FCM 对 payload 都限制在 4KB 以内，超出会被网关直接拒收；
// console mock 在本地就做这道校验，避免把必然失败的请求交给网关。
const pushPayloadLimit = 4096

// consolePushProvider 是 push 渠道的 console mock 实现。
//
// 它不真正对接 APNs / FCM，而是把 device token、标题与正文打印到日志，
// 模拟「已提交到推送服务」的过程。真实推送在 Phase 3 接入，届时作为
// 另一实现注册进 ProviderRegistry，二者实现同一 Provider 接口。
type consolePushProvider struct {
	logger *slog.Logger
}

// NewConsolePushProvider 创建 push 渠道的 console mock provider。
func NewConsolePushProvider(logger *slog.Logger) Provider {
	if logger == nil {
		logger = slog.Default()
	}
	return &consolePushProvider{logger: logger}
}

// Channel 返回 push 渠道。
func (c *consolePushProvider) Channel() domain.Channel {
	return domain.ChannelPush
}

// Send 校验 device token 与 payload 体积后打印推送内容，模拟提交成功。
//
// 创作者未在设备上登录（无 device token）时投递失败，而不是静默成功；
// payload 超过 4KB 同样失败，与真实网关行为一致。
func (c *consolePushProvider) Send(d Delivery) SendResult {
	token := strings.TrimSpace(d.To)
	if token == "" {
		c.logger.Warn("[push] 投递失败：缺少 device token",
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
			"subscriberId", d.SubscriberID,
		)
		return SendResult{Sent: false, Reason: "缺少 device token"}
	}

	title := pushTitle(d)
	body := d.Body
	if size := len(title) + len(body); size > pushPayloadLimit {
		c.logger.Warn("[push] 投递失败：payload 超过上限",
			"size", size,
			"limit", pushPayloadLimit,
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
		)
		return SendResult{Sent: false, Reason: fmt.Sprintf("payload 超过 %d 字节上限", pushPayloadLimit)}
	}

	c.logger.Info("[push] 投递成功（console mock）",
		"token", maskToken(token),
		"title", title,
		"body", body,
		"stepId", d.StepID,
		"workflow", d.WorkflowName,
		"subscriberId", d.SubscriberID,
	)
	return SendResult{
		Sent:   true,
		Reason: fmt.Sprintf("push 已提交至 %s", maskToken(token)),
	}
}

// pushTitle 返回推送标题：优先步骤 config 的 subject，其次工作流名。
// 推送标题不宜空，因此最终兜底为渠道名「新通知」。
func pushTitle(d Delivery) string {
	if t := strings.TrimSpace(d.Subject); t != "" {
		return t
	}
	if t := strings.TrimSpace(d.WorkflowName); t != "" {
		return t
	}
	return "新通知"
}

// maskToken 对 device token 做脱敏，仅保留首尾各 4 位，避免日志泄露完整凭证。
func maskToken(token string) string {
	if len(token) <= 8 {
		return "****"
	}
	return token[:4] + "…" + token[len(token)-4:]
}
