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
	"github.com/cdgjhg/xw-p/engine/internal/domain"
	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

// Context 是一条消息贯穿管线全程的上下文。
// Variables 在 transform 阶段由 payload 填充，供 render 阶段使用。
type Context struct {
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
}

// NewContext 由队列消息创建管线上下文，并初始化变量表与结果表。
func NewContext(msg queue.QueuedMessage) *Context {
	return &Context{
		Msg:       msg,
		Variables: make(map[string]any),
		Results:   make(map[string]StepResult),
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
