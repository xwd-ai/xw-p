package pipeline

import (
	"log/slog"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// dispatchStage 是管线第四阶段：按步骤绑定渠道分发。
//
// 优先经 ProviderRegistry 查找该渠道的 provider 并真正投递；未注册 provider
// 的渠道回退为 mock 打印（保持管线在渠道逐批落地期间可运行）。
// 当前已落地 provider：email / sms / push（均为 console mock）。
// in-app / chat 在后续 Phase 接入。
type dispatchStage struct {
	logger   *slog.Logger
	registry *ProviderRegistry
}

// NewDispatchStage 创建分发阶段。
// logger 为 nil 时使用默认 logger；registry 为 nil 时新建空注册表。
func NewDispatchStage(logger *slog.Logger, registry *ProviderRegistry) Stage {
	if logger == nil {
		logger = slog.Default()
	}
	if registry == nil {
		registry = NewProviderRegistry()
	}
	return &dispatchStage{logger: logger, registry: registry}
}

// Apply 执行投递：有 provider 的渠道真实调用，其余 mock 打印。
func (d *dispatchStage) Apply(ctx *Context, step domain.Step) error {
	res := ctx.Results[step.ID]
	res.StepID = step.ID
	res.StepType = domain.StepType(step.Type)
	res.Channel = step.Type

	ch := domain.Channel(step.Type)
	provider, ok := d.registry.Lookup(ch)
	if !ok {
		// 未注册 provider：保持原有 mock 打印，视为投递成功（不阻断后续步骤）。
		d.logger.Info("[mock] 分发步骤（未注册 provider）",
			"stepId", step.ID,
			"channel", string(step.Type),
			"workflow", ctx.Msg.Name,
			"subscriberId", ctx.Msg.SubscriberID,
			"rendered", res.Rendered,
		)
		ctx.Results[step.ID] = res
		return nil
	}

	// 构造投递请求：收件人按渠道从消息契约取值，内容取渲染结果。
	delivery := Delivery{
		Channel:      ch,
		To:           d.recipientFor(ctx, ch),
		Subject:      d.subjectFor(ctx, step),
		Body:         res.Rendered,
		StepID:       step.ID,
		WorkflowName: ctx.Msg.Name,
		SubscriberID: ctx.Msg.SubscriberID,
	}

	send := provider.Send(delivery)
	if send.Sent {
		res.Sent = true
		ctx.Results[step.ID] = res
		return nil
	}

	// 投递失败：记录错误，向上传播以触发失败短路。
	res.Error = send.Reason
	ctx.Results[step.ID] = res
	return &deliveryError{channel: ch, reason: send.Reason}
}

// recipientFor 按渠道返回收件地址。
func (d *dispatchStage) recipientFor(ctx *Context, ch domain.Channel) string {
	// 订阅者已解析时优先取订阅者联系方式，否则回退消息契约字段。
	switch ch {
	case domain.ChannelEmail:
		if ctx.Subscriber != nil && ctx.Subscriber.Email != "" {
			return ctx.Subscriber.Email
		}
		return ctx.Msg.Email
	case domain.ChannelSMS:
		if ctx.Subscriber != nil && ctx.Subscriber.Phone != "" {
			return ctx.Subscriber.Phone
		}
		return ctx.Msg.Phone
	case domain.ChannelPush:
		if ctx.Subscriber != nil && ctx.Subscriber.DeviceToken != "" {
			return ctx.Subscriber.DeviceToken
		}
		return ctx.Msg.DeviceToken
	default:
		return ""
	}
}

// subjectFor 返回消息标题：email 用 subject、push 用 title，二者取其一。
// 优先步骤 config 的 subject/title 字段，否则回退工作流名。
func (d *dispatchStage) subjectFor(ctx *Context, step domain.Step) string {
	for _, key := range []string{"subject", "title"} {
		if s, ok := step.Config[key].(string); ok && s != "" {
			return s
		}
	}
	if ctx.Workflow != nil && ctx.Workflow.Name != "" {
		return ctx.Workflow.Name
	}
	return ctx.Msg.Name
}

// deliveryError 表示一次投递失败，携带渠道与原因。
type deliveryError struct {
	channel domain.Channel
	reason  string
}

// Error 实现 error 接口。
func (e *deliveryError) Error() string {
	return "投递失败（" + string(e.channel) + "）: " + e.reason
}
