// Package pipeline 实现工作流执行管线骨架（Phase 2）。
//
// 管线将一条触发消息按阶段推进：
//
//		filter → transform → render → dispatch
//
//	  - filter：根据步骤过滤条件决定是否跳过该步骤（支持 eq/ne/contains/regex/in/exists）。
//	  - transform：把原始 payload 变换为模板可用的变量上下文。
//	  - render：用变量渲染步骤模板（变量替换 + 内置函数，带安全沙箱）。
//	  - dispatch：按步骤绑定渠道调用 provider 投递（in_app/email/sms/push/chat）。
//
// 两个编排步骤不走上表四阶段：
//   - delay：把其后的步骤整体推迟到到期时刻，交由 Deferrer 唤醒续跑（delay.go）；
//   - digest：把同一聚合键的多次互动收敛成一张卡片，窗口内原地累加计数（digest.go）。
//
// 本包只依赖 domain 与 queue 的纯数据结构，不依赖 Redis / 数据库，便于测试。
package pipeline

import (
	"context"
	"time"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

// Context 是一条消息贯穿管线全程的上下文。
// Variables 在 transform 阶段由 payload 填充，供 render 阶段使用。
type Context struct {
	// Ctx 是贯穿本次执行的 context：引擎退出时取消，单步超时时按步骤派生。
	// 支持感知取消的 provider 通过它感知超时（见 ContextSender）。
	Ctx context.Context

	// Msg 是进入管线的原始队列消息。
	Msg queue.QueuedMessage

	// Subscriber 关联的订阅者（可为空，未解析时用 Msg 里的联系方式兜底）。
	Subscriber *domain.Subscriber

	// Workflow 触发的工作流（可为空，未知工作流时按消息自带渠道分发）。
	Workflow *domain.Workflow

	// Variables 是模板变量表：key 为变量名，value 为任意值。
	// 约定以 "payload." 前缀访问原始 payload 字段，系统变量以 "$" 前缀。
	Variables map[string]any

	// Results 记录每个投递步骤的 dispatch 结果，按步骤 ID 索引。
	Results map[string]StepResult

	// budget 是整条消息的执行预算（0 表示不限），deadline 是据此算出的截止时刻。
	// 由 RunSteps 在首次进入时设置，后续步骤共用一个截止时刻。
	budget   time.Duration
	deadline time.Time
}

// NewContext 由队列消息创建管线上下文，并初始化变量表与结果表。
func NewContext(msg queue.QueuedMessage) *Context {
	return &Context{
		Ctx:       context.Background(),
		Msg:       msg,
		Variables: make(map[string]any),
		Results:   make(map[string]StepResult),
	}
}

// WithContext 绑定一个外部 context（用于引擎退出时取消在途步骤），返回自身。
func (c *Context) WithContext(ctx context.Context) *Context {
	if ctx != nil {
		c.Ctx = ctx
	}
	return c
}

// Context 返回贯穿本次执行的 context；未设置时返回 background（避免调用方判空）。
func (c *Context) Context() context.Context {
	if c == nil || c.Ctx == nil {
		return context.Background()
	}
	return c.Ctx
}

// StartBudget 为本次执行设置整体预算，截止时刻只设置一次。
//
// 续跑（delay 到期）会新建上下文，因此每次续跑都重新获得一份完整预算——
// 预算约束的是「一次执行」而不是「一条消息的一生」，否则被延迟数小时的消息
// 会在续跑时立刻超时。
func (c *Context) StartBudget(total time.Duration, now time.Time) {
	if c.budget != 0 || !c.deadline.IsZero() {
		return
	}
	if total <= 0 {
		return
	}
	c.budget = total
	c.deadline = now.Add(total)
}

// RemainingBudget 返回整体预算的剩余时间；第二个返回值表示是否启用了整体预算。
// 返回值可能为负，调用方据此判断预算是否已耗尽。
func (c *Context) RemainingBudget(now time.Time) (time.Duration, bool) {
	if c == nil || c.deadline.IsZero() {
		return 0, false
	}
	return c.deadline.Sub(now), true
}

// Fork 派生一个用于单步执行的子上下文：替换 context，复制消息视图与当前结果。
//
// 复制是超时隔离的前提：步骤在子上下文里写入的结果只有在按时返回时才合并回
// 父上下文，超时后被放弃的子 goroutine 也就不会与父上下文产生数据竞争。
func (c *Context) Fork(runCtx context.Context) *Context {
	child := &Context{
		Ctx:        runCtx,
		Msg:        c.Msg,
		Subscriber: c.Subscriber,
		Workflow:   c.Workflow,
		Variables:  make(map[string]any, len(c.Variables)),
		Results:    make(map[string]StepResult, len(c.Results)),
		budget:     c.budget,
		deadline:   c.deadline,
	}
	for k, v := range c.Variables {
		child.Variables[k] = v
	}
	for k, v := range c.Results {
		child.Results[k] = v
	}
	return child
}

// MergeResult 把子上下文中指定步骤的结果合并回父上下文。
func (c *Context) MergeResult(child *Context, stepID string) {
	if child == nil {
		return
	}
	if res, ok := child.Results[stepID]; ok {
		c.Results[stepID] = res
	}
}

// SetSystem 写入一个系统变量（以 "$" 前缀约定）。
func (c *Context) SetSystem(name string, value any) {
	c.Variables["$"+name] = value
}

// LoadPayload 把原始 payload 字段展开为 "payload.<key>" 形式的变量。
func (c *Context) LoadPayload() {
	for k, v := range c.Msg.Payload {
		c.Variables["payload."+k] = v
	}
}

// Get 读取一个变量值。
func (c *Context) Get(name string) (any, bool) {
	v, ok := c.Variables[name]
	return v, ok
}
