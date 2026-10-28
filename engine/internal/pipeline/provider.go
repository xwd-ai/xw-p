package pipeline

import (
	"context"
	"fmt"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// Delivery 是一条已完成渲染、准备投递的消息投递请求。
//
// 由 dispatch 阶段在 render 之后构造：收件人来自消息契约（Email/Phone），
// 内容来自步骤渲染结果，元信息（步骤/工作流/订阅者）用于日志与审计。
type Delivery struct {
	// Channel 是目标投递渠道。
	Channel domain.Channel
	// To 是收件地址（email 为邮箱、sms 为手机号、push 为 device token）。
	To string
	// Subject 是消息主题（email 用）。
	Subject string
	// Body 是渲染后的消息正文。
	Body string
	// StepID 是触发该投递的步骤 ID。
	StepID string
	// TriggerID 是触发本次投递的事件回执 ID，用于幂等去重与收件箱溯源。
	TriggerID string
	// WorkflowName 是触发工作流名（如 post_commented）。
	WorkflowName string
	// SubscriberID 是订阅者 ID。
	SubscriberID string
}

// SendResult 是一次投递的执行结果。
type SendResult struct {
	// Sent 表示投递是否成功。
	Sent bool
	// Reason 说明结果（失败时为错误原因，成功时可为 provider 返回的凭证/说明）。
	Reason string
}

// Provider 是投递渠道的抽象接口（Phase 3 Provider 层的雏形）。
//
// 每种投递渠道（email/sms/push/in-app/chat）各有一个实现；真实 provider
// （SMTP/短信网关/推送服务）在 Phase 3 接入，当前仅提供 console mock 实现。
type Provider interface {
	// Channel 返回该 provider 服务的投递渠道。
	Channel() domain.Channel
	// Send 投递一条已渲染的消息，返回投递结果。
	Send(d Delivery) SendResult
}

// ContextSender 是 Provider 的可选扩展：能够感知取消与超时的实现可实现它。
//
// dispatch 阶段优先调用 SendContext，ctx 在步骤超时或引擎退出时被取消，
// provider 据此中断 HTTP 请求等阻塞操作——这比「超时后放弃等待、请求仍在
// 后台跑」更干净。不实现本接口的 provider 回退到 Send，行为不变。
type ContextSender interface {
	SendContext(ctx context.Context, d Delivery) SendResult
}

// ProviderRegistry 按渠道索引 Provider，供 dispatch 阶段查找并投递。
//
// 未注册渠道说明尚无对应 provider，dispatch 阶段回退为 mock 打印，
// 保证管线在渠道逐批落地期间始终可运行。
type ProviderRegistry struct {
	providers map[domain.Channel]Provider
}

// NewProviderRegistry 创建空注册表。
func NewProviderRegistry() *ProviderRegistry {
	return &ProviderRegistry{providers: make(map[domain.Channel]Provider)}
}

// Register 注册一个 provider，同渠道后者覆盖前者。
func (r *ProviderRegistry) Register(p Provider) {
	r.providers[p.Channel()] = p
}

// Lookup 按渠道查找 provider；未注册时返回 nil, false。
func (r *ProviderRegistry) Lookup(ch domain.Channel) (Provider, bool) {
	p, ok := r.providers[ch]
	return p, ok
}

// Has 判断某渠道是否已注册 provider。
func (r *ProviderRegistry) Has(ch domain.Channel) bool {
	_, ok := r.providers[ch]
	return ok
}

// Channels 返回已注册的渠道列表（无序）。
func (r *ProviderRegistry) Channels() []domain.Channel {
	out := make([]domain.Channel, 0, len(r.providers))
	for ch := range r.providers {
		out = append(out, ch)
	}
	return out
}

// String 返回注册表的人类可读描述（调试用）。
func (r *ProviderRegistry) String() string {
	return fmt.Sprintf("ProviderRegistry{channels=%v}", r.Channels())
}
