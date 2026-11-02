// Package provider 定义投递渠道实现的统一抽象与注册表。
//
// 一个「渠道实现」负责把一条已渲染的消息真正送出去：console mock 打印日志、
// SMTP 提交邮件、短信网关下单、推送服务下发、webhook 推群。它们都实现 Sender，
// 只声明自己服务的渠道（Channel）和发送动作（Send），调用方按渠道取用，
// 不需要知道背后是哪种实现。
//
// 注册表（Registry）按渠道登记实现，同一渠道可以并存多个实现（主备切换、
// 灰度、不同区域），由优先级与注册顺序决定默认用哪一个；开关状态可在运行期
// 调整，便于临时摘掉某个不健康的实现而不重启进程。
package provider

import (
	"context"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// Delivery 是一条已完成渲染、准备投递的消息投递请求。
//
// 由投递阶段在渲染之后构造：收件人来自消息契约（Email/Phone/DeviceToken）
// 或步骤配置（chat 的 webhook），内容来自渲染结果，元信息（步骤/工作流/
// 订阅者）用于日志、审计与幂等去重。
type Delivery struct {
	// Channel 是目标投递渠道。
	Channel domain.Channel
	// To 是收件地址（email 为邮箱、sms 为手机号、push 为 device token、
	// chat 为群机器人 webhook）。
	To string
	// Subject 是消息主题（email 用）。
	Subject string
	// Body 是渲染后的消息正文。
	Body string
	// HTML 是渲染后的 HTML 正文，仅 email 渠道使用；为空表示只发纯文本。
	//
	// 它与 Body 是同一条消息的两种形态，不是替代关系：实现该发多段报文时
	// 应当两段都带上，让不支持 HTML 的客户端仍能读到 Body。
	HTML string
	// StepID 是触发该投递的步骤 ID。
	StepID string
	// TriggerID 是触发本次投递的事件回执 ID，用于幂等去重与收件箱溯源。
	TriggerID string
	// WorkflowName 是触发工作流名（如 post_commented）。
	WorkflowName string
	// SubscriberID 是订阅者 ID。
	SubscriberID string
}

// Result 是一次投递的执行结果。
type Result struct {
	// Sent 表示投递是否成功。
	Sent bool
	// Reason 说明结果（失败时为错误原因，成功时可为 provider 返回的凭证/说明）。
	Reason string
	// Terminal 表示这次失败与具体实现无关：缺收件地址、正文为空、内容超长
	// 这类校验问题换一个备选实现也是同样的结果。回退链遇到它会立即停止，
	// 不再把同一条内容重复投给渠道里的每个实现。
	//
	// 默认 false：未表态的失败按「可能是本实现的问题」处理，值得换备选再试。
	Terminal bool
}

// Sender 是投递渠道的抽象接口。
//
// 每个实现只关心自己渠道的投递细节：地址校验、内容限制（短信分段、
// 推送体积）、外网调用与返回码解释。投递失败用 Result.Sent=false +
// Reason 表达，不返回 error——失败原因需要随结果落库与回给调用方，
// 而不是当成程序异常向上抛。
type Sender interface {
	// Channel 返回该实现服务的投递渠道。
	Channel() domain.Channel
	// Send 投递一条已渲染的消息，返回投递结果。
	Send(d Delivery) Result
}

// ContextSender 是 Sender 的可选扩展：能够感知取消与超时的实现可实现它。
//
// 投递阶段优先调用 SendContext，ctx 在步骤超时或引擎退出时被取消，
// 实现据此中断 HTTP 请求等阻塞操作——这比「超时后放弃等待、请求仍在
// 后台跑」更干净。不实现本接口的实现回退到 Send，行为不变。
type ContextSender interface {
	SendContext(ctx context.Context, d Delivery) Result
}

// Identifier 是 Sender 的可选扩展：返回稳定的实现标识。
//
// 标识用于注册表内的寻址（启停、替换、日志溯源），因此应当在同一渠道内
// 唯一且不随进程重启变化，例如 "console-email"、"smtp-email"。
// 未实现本接口的实现由注册表按渠道自动编号。
type Identifier interface {
	ID() string
}
