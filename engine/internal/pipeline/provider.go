package pipeline

import (
	"github.com/xwd-ai/xw-p/engine/internal/provider"
)

// Delivery 是一条已完成渲染、准备投递的消息投递请求。
//
// 类型定义在 provider 包，这里保留别名，让管线的各阶段无需直接依赖 provider
// 包的其余内容。
type Delivery = provider.Delivery

// SendResult 是一次投递的执行结果。
type SendResult = provider.Result

// Provider 是投递渠道的抽象接口：声明服务的渠道并投递一条消息。
//
// 每种投递渠道（email/sms/push/in-app/chat）各有一个实现；console mock 与
// 真实网关实现同一接口，由注册表按渠道与优先级决定用哪一个。
type Provider = provider.Sender

// ContextSender 是 Provider 的可选扩展：能够感知取消与超时的实现可实现它。
//
// dispatch 阶段优先调用 SendContext，ctx 在步骤超时或引擎退出时被取消，
// provider 据此中断 HTTP 请求等阻塞操作。不实现本接口的 provider 回退到
// Send，行为不变。
type ContextSender = provider.ContextSender

// ProviderRegistry 按渠道索引 Provider，供 dispatch 阶段查找并投递。
//
// 同一渠道可以登记多个实现（主备、灰度），由优先级与登记顺序决定默认取用
// 哪一个；条目可以临时停用。未注册可用实现的渠道回退为 mock 打印，保证
// 管线在渠道逐批落地期间始终可运行。
type ProviderRegistry = provider.Registry

// NewProviderRegistry 创建空注册表。
func NewProviderRegistry() *ProviderRegistry {
	return provider.NewRegistry()
}
