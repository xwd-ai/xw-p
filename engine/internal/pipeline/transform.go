package pipeline

import "github.com/cdgjhg/xw-p/engine/internal/domain"

// transformStage 是管线第二阶段：把 payload 展开为模板变量上下文。
//
// 骨架实现仅做「载荷展开」：将原始 payload 字段映射为 "payload.<key>" 变量，
// 并写入系统变量（订阅者、触发名、时间戳等）。更丰富的变量体系
// （内置函数、嵌套访问）在 Phase 2 的「变量系统」任务中扩展。
type transformStage struct{}

// NewTransformStage 创建变换阶段。
func NewTransformStage() Stage {
	return transformStage{}
}

// Apply 将 payload 展开进变量表并写入系统变量。
func (t transformStage) Apply(ctx *Context, step domain.Step) error {
	ctx.LoadPayload()

	// 系统变量：供模板使用的基础上下文。
	ctx.SetSystem("workflow", ctx.Msg.Name)
	ctx.SetSystem("subscriberId", ctx.Msg.SubscriberID)
	ctx.SetSystem("triggerId", ctx.Msg.TriggerID)
	ctx.SetSystem("stepId", step.ID)
	ctx.SetSystem("channel", string(step.Type))

	return nil
}
