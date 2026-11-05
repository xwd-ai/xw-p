package pipeline

import (
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/preference"
)

// dispatchStage 是管线第四阶段：按步骤绑定渠道分发。
//
// 优先经 ProviderRegistry 查找该渠道的 provider 并真正投递；未注册 provider
// 的渠道回退为 mock 打印（保持管线在渠道逐批落地期间可运行）。
// 同一渠道登记了多个实现时按优先级主备切换：默认实现失败且失败与实现相关
// 时，依次尝试后面的备选，实际用到的实现在结果里留痕。
type dispatchStage struct {
	logger   *slog.Logger
	failover *Failover
	// health 记录每次投递的成败，供运维接口观察渠道当前是否可用。
	health *HealthTracker
	// preferences 是投递前的偏好判定，为空表示不做判定。
	preferences *preference.Gate
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
	return &dispatchStage{
		logger:   logger,
		failover: NewFailover(registry),
		health:   NewHealthTracker(registry),
	}
}

// Apply 执行投递：有 provider 的渠道真实调用（失败时按优先级回退），其余 mock 打印。
func (d *dispatchStage) Apply(ctx *Context, step domain.Step) error {
	res := ctx.Results[step.ID]
	res.StepID = step.ID
	res.StepType = domain.StepType(step.Type)
	res.Channel = step.Type

	ch := domain.Channel(step.Type)

	// 偏好判定放在投递之前：被拦下的渠道既不取实现也不发请求，但仍要留一条
	// 「为什么没发」的结论——回写阶段据此写一条 skipped 明细，回答「这个人
	// 为什么没收到这封邮件」时不必再让人去翻偏好文件。
	if decision := d.gateDecision(ctx, ch); !decision.Allowed {
		res.Skipped = true
		res.SkipReason = SkipReasonPreference
		res.SkipMessage = decision.SkipMessage()
		ctx.Results[step.ID] = res
		d.logger.Info("[dispatch] 订阅者偏好跳过该渠道",
			"stepId", step.ID,
			"channel", string(ch),
			"workflow", workflowName(ctx),
			"subscriberId", ctx.Msg.SubscriberID,
			"reason", decision.Reason,
		)
		// 不是失败：后续步骤照常执行（关掉邮件不该连带跳过站内信）。
		return errSkipStep
	}

	// 构造投递请求：收件人按渠道从消息契约取值，内容取渲染结果。
	//
	// 标题与正文在这里再过一次清理：渲染阶段已经清过，但分发是内容离开引擎
	// 前的最后一道口子，聚合卡片、未走渲染阶段的步骤同样会经过这里。渠道层
	// 不该依赖上游是否记得清理，清理函数本身也是幂等的。
	delivery := Delivery{
		Channel:      ch,
		To:           d.recipientFor(ctx, step, ch),
		Subject:      sanitizeSingleLine(d.subjectFor(ctx, step)),
		Body:         sanitizeMultiLine(res.Rendered),
		HTML:         sanitizeMultiLine(res.RenderedHTML),
		StepID:       step.ID,
		TriggerID:    ctx.Msg.TriggerID,
		WorkflowName: ctx.Msg.Name,
		SubscriberID: ctx.Msg.SubscriberID,
		Tenant:       tenantOf(ctx.Msg),
		Environment:  environmentOf(ctx.Msg),
	}

	// 按优先级依次尝试该渠道的实现：首个是默认实现，其后是备选。
	started := time.Now()
	outcome := d.failover.Send(ctx.Context(), delivery)
	res.Duration = time.Since(started)
	// 投递结果是健康的被动观测来源：消息真的走通了没有，比探测更接近事实。
	d.recordHealth(outcome)
	if outcome.Candidates == 0 {
		if reg := d.failover.Registry(); reg != nil && reg.Registered(ch) {
			// 渠道配了实现但全被停用：这是运维的明确决定，不该退化成 mock
			// 打印并算作成功——那会让「停用后消息去哪了」变成一个查不出的问题。
			res.Error = "渠道 " + string(ch) + " 的实现均已停用"
			ctx.Results[step.ID] = res
			d.logger.Warn("[dispatch] 渠道实现均已停用，投递未执行",
				"stepId", step.ID,
				"channel", string(ch),
				"workflow", ctx.Msg.Name,
			)
			return &deliveryError{channel: ch, reason: res.Error}
		}
		// 未注册可用 provider：保持原有 mock 打印，视为投递成功（不阻断后续步骤）。
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

	res.ProviderAttempts = outcome.Attempted()
	res.ProviderRetries = outcome.Retries()
	res.UsedProvider = outcome.Provider.ID
	res.ProviderFallback = outcome.FellBack()

	if outcome.Sent {
		res.Sent = true
		ctx.Results[step.ID] = res
		logArgs := []any{
			"stepId", step.ID,
			"channel", string(ch),
			"provider", res.UsedProvider,
		}
		if res.ProviderRetries > 0 {
			// 重试成功说明对端抖了一下：记 warn 级，便于看出这个渠道在靠重试兜底。
			logArgs = append(logArgs,
				"attempts", res.ProviderAttempts,
				"retried", res.ProviderRetries,
			)
			d.logger.Warn("[dispatch] 投递经重试后成功", logArgs...)
			return nil
		}
		if res.ProviderFallback {
			// 回退成功说明默认实现当时不可用：记 warn 级，便于看出渠道在靠备选兜底。
			logArgs = append(logArgs,
				"attempts", res.ProviderAttempts,
				"skipped", skipSummary(outcome),
			)
			d.logger.Warn("[dispatch] 默认实现投递失败，已回退到备选", logArgs...)
			return nil
		}
		d.logger.Debug("[dispatch] 投递成功", logArgs...)
		return nil
	}

	// 投递失败：记录错误（含整条回退链的尝试），向上传播以触发失败短路。
	res.Error = outcome.Result.Reason
	ctx.Results[step.ID] = res
	return &deliveryError{channel: ch, reason: outcome.Result.Reason}
}

// gateDecision 在投递前问一次偏好。
//
// 未装配判定时直接放行；判定器自己负责「读不到偏好按接收处理」，这里不重复
// 兜底，否则两处兜底会掩盖读取失败这件事。
func (d *dispatchStage) gateDecision(ctx *Context, ch domain.Channel) preference.Decision {
	if d == nil || d.preferences == nil {
		return preference.Decision{Allowed: true, Reason: "未配置偏好判定，按默认接收"}
	}
	return d.preferences.Decide(ctx.Msg.SubscriberID, workflowName(ctx), string(ch))
}

// recordHealth 把一次投递的结果折算成健康观测。
//
// 只记「与实现相关」的失败：缺收件地址、正文为空这类 Terminal 失败换哪个
// 实现都是同样结论，算在渠道头上会让一条格式错误的消息把整个渠道标红。
// 成功记在实际完成投递的实现上——回退到备选时，主实现已经在失败的尝试里
// 各记过一次，这里只补那次成功。
func (d *dispatchStage) recordHealth(o Outcome) {
	if d.health == nil {
		return
	}
	for _, a := range o.Attempts {
		if a.Sent || a.Terminal {
			continue
		}
		d.health.Record(a.ProviderID, false, a.Reason)
	}
	if o.Sent {
		d.health.Record(o.Provider.ID, true, o.Result.Reason)
	}
}

// skipSummary 把失败的尝试拼成「标识: 原因」，用于回退成功的告警日志。
func skipSummary(outcome Outcome) []string {
	failed := outcome.FailedAttempts()
	if len(failed) == 0 {
		return nil
	}
	out := make([]string, 0, len(failed))
	for _, a := range failed {
		out = append(out, a.ProviderID+": "+a.Reason)
	}
	return out
}

// recipientFor 按渠道返回收件地址。
func (d *dispatchStage) recipientFor(ctx *Context, step domain.Step, ch domain.Channel) string {
	return deliveryRecipient(ctx, step, ch)
}

// subjectFor 返回消息标题：优先用 render 阶段渲染好的标题（标题里可能有
// 帖子名、互动者变量），没有则回退到步骤原始配置。
//
// 回退路径要挡住模板原文：渲染后标题为空（帖子标题本身为空）时，直接拿
// 步骤配置里的 `{{ payload.post_title }}` 去投递，收件人看到的会是一串
// 花括号。此时改用工作流名，仍然是可读的标题。
func (d *dispatchStage) subjectFor(ctx *Context, step domain.Step) string {
	if res, ok := ctx.Results[step.ID]; ok {
		if s := strings.TrimSpace(res.RenderedSubject); s != "" {
			return s
		}
	}
	fallback := deliverySubject(ctx, step)
	if hasTemplateExpr(fallback) {
		return workflowName(ctx)
	}
	return fallback
}

// deliveryRecipient 按渠道解析收件地址（dispatch 与 digest 步骤共用）。
//
// in_app 不需要地址（收件人是订阅者本人，由 SubscriberID 标识）；
// chat 的收件目标是群机器人 webhook，优先取步骤 config 的 webhook，
// 其次回退环境变量 XW_CHAT_WEBHOOK（便于本地 demo 统一配置）。
func deliveryRecipient(ctx *Context, step domain.Step, ch domain.Channel) string {
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
	case domain.ChannelChat:
		if hook, ok := step.Config["webhook"].(string); ok && strings.TrimSpace(hook) != "" {
			return strings.TrimSpace(hook)
		}
		return strings.TrimSpace(os.Getenv("XW_CHAT_WEBHOOK"))
	default:
		return ""
	}
}

// deliverySubject 返回消息标题：email 用 subject、push 用 title，二者取其一。
// 优先步骤 config 的 subject/title 字段，否则回退工作流名。
func deliverySubject(ctx *Context, step domain.Step) string {
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
