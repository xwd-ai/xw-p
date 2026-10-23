// Package domain 定义通知引擎的核心领域模型。
//
// 与 Python API 侧（api/app/models）保持概念一致：渠道、消息状态机、
// 环境隔离与工作流/消息/订阅者实体。本包不依赖任何基础设施，
// 便于单元测试与后续复用。
package domain

// Channel 是通知投递渠道，值即存储/传输标识（snake_case），
// 与 Python 侧 Channel 枚举对齐。
type Channel string

const (
	// ChannelInApp 应用内收件箱。
	ChannelInApp Channel = "in_app"
	// ChannelEmail 邮件。
	ChannelEmail Channel = "email"
	// ChannelSMS 短信。
	ChannelSMS Channel = "sms"
	// ChannelPush 移动推送。
	ChannelPush Channel = "push"
	// ChannelChat 聊天工具（webhook）。
	ChannelChat Channel = "chat"
	// ChannelDelay 定时延迟（编排步骤，非投递渠道）。
	ChannelDelay Channel = "delay"
	// ChannelDigest 摘要聚合（编排步骤，非投递渠道）。
	ChannelDigest Channel = "digest"
)

// AllChannels 返回全部渠道（含编排步骤）。
func AllChannels() []Channel {
	return []Channel{
		ChannelInApp, ChannelEmail, ChannelSMS, ChannelPush,
		ChannelChat, ChannelDelay, ChannelDigest,
	}
}

// DeliveryChannels 返回真正执行投递的渠道（排除 delay/digest 编排步骤）。
func DeliveryChannels() []Channel {
	var out []Channel
	for _, c := range AllChannels() {
		if c.IsDelivery() {
			out = append(out, c)
		}
	}
	return out
}

// IsDelivery 判断该渠道是否真正执行投递。
func (c Channel) IsDelivery() bool {
	return c != ChannelDelay && c != ChannelDigest
}

// ParseChannel 按存储标识解析渠道；未知值返回 false。
func ParseChannel(value string) (Channel, bool) {
	for _, c := range AllChannels() {
		if string(c) == value {
			return c, true
		}
	}
	return "", false
}
