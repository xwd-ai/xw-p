package pipeline

import "github.com/cdgjhg/xw-p/engine/internal/domain"

// StepResult 记录单个步骤的执行结果。
type StepResult struct {
	StepID   string
	StepType domain.StepType
	Channel  domain.Channel
	// Skipped 表示该步骤被跳过（filter 过滤或条件分支不满足）。
	Skipped bool
	// SkipReason 说明跳过原因：空=未跳过，"filter"=被过滤条件拦截，"condition"=条件分支不满足。
	SkipReason string
	// ShortCircuited 表示该步骤因上游步骤失败而未执行（失败短路）。
	ShortCircuited bool
	// Rendered 表示 render 阶段产出的内容（如模板渲染后的文本）。
	Rendered string
	// Error 表示该步骤执行失败的原因。
	Error string
}

// Stage 是管线中的一个阶段。每个阶段接收上下文并按需修改或返回结果。
// 返回 error 表示该步骤终止（失败短路）。
type Stage interface {
	// Apply 对上下文的当前步骤执行阶段逻辑。
	Apply(ctx *Context, step domain.Step) error
}

// StageFunc 将函数适配为 Stage 接口，便于组合与测试。
type StageFunc func(ctx *Context, step domain.Step) error

// Apply 调用底层函数。
func (f StageFunc) Apply(ctx *Context, step domain.Step) error {
	return f(ctx, step)
}
