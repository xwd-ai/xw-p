package pipeline

import (
	"time"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// transformStage 是管线第二阶段：把 payload 展开为模板变量上下文。
//
// 完整变量体系（Phase 2「变量系统」）：
//   - payload 变量：嵌套访问由 variableSystem.resolve 在 render 阶段按需解析，
//     此处仍将顶层字段展开为 payload.<key> 便于兼容。
//   - 系统变量：注入订阅者、触发、工作流、时间戳等上下文，供模板以 $ 前缀访问。
//   - 内置函数：由 variableSystem 注册，render 阶段调用。
type transformStage struct {
	vs *variableSystem
}

// NewTransformStage 创建变换阶段。
func NewTransformStage() Stage {
	return &transformStage{vs: newVariableSystem()}
}

// Apply 将 payload 展开进变量表并写入系统变量。
func (t *transformStage) Apply(ctx *Context, step domain.Step) error {
	ctx.LoadPayload()

	// 系统变量：供模板使用的基础上下文。
	ctx.SetSystem("workflow", ctx.Msg.Name)
	ctx.SetSystem("subscriber_id", ctx.Msg.SubscriberID)
	ctx.SetSystem("subscriber", ctx.Msg.SubscriberID)
	ctx.SetSystem("trigger_id", ctx.Msg.TriggerID)
	ctx.SetSystem("transaction_id", ctx.Msg.TransactionID)
	ctx.SetSystem("step_id", step.ID)
	ctx.SetSystem("step_name", step.Name)
	ctx.SetSystem("channel", string(step.Type))
	ctx.SetSystem("email", ctx.Msg.Email)
	ctx.SetSystem("phone", ctx.Msg.Phone)
	ctx.SetSystem("timestamp", t.vs.now().Unix())
	ctx.SetSystem("timestamp_iso", t.vs.now().Format(time.RFC3339))

	return nil
}
