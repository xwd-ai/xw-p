package pipeline

import (
	"log/slog"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// dispatchStage 是管线第四阶段：按步骤绑定渠道分发。
//
// 骨架实现为 mock 投递：打印发送日志并记录成功结果，不真正调用任何
// provider。真实 provider（SMTP/短信/推送/webhook）在 Phase 3 接入，
// 届时 DispatchStage 将委托 ProviderRegistry 查找对应 provider。
type dispatchStage struct {
	logger *slog.Logger
}

// NewDispatchStage 创建分发阶段。
func NewDispatchStage(logger *slog.Logger) Stage {
	if logger == nil {
		logger = slog.Default()
	}
	return &dispatchStage{logger: logger}
}

// Apply 执行 mock 投递并记录结果。
func (d *dispatchStage) Apply(ctx *Context, step domain.Step) error {
	res := ctx.Results[step.ID]
	res.StepID = step.ID
	res.StepType = domain.StepType(step.Type)
	res.Channel = step.Type

	d.logger.Info("[mock] 分发步骤",
		"stepId", step.ID,
		"channel", string(step.Type),
		"workflow", ctx.Msg.Name,
		"subscriberId", ctx.Msg.SubscriberID,
		"rendered", res.Rendered,
	)

	ctx.Results[step.ID] = res
	return nil
}
