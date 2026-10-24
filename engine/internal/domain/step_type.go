package domain

// StepType 是工作流步骤类型（ROADMAP Phase 2），
// 与 Python 侧 models/step_type.py 的 StepType 对齐。
//
// 投递步骤类型（in_app / email / sms / push / chat）绑定到一个确定的
// 投递渠道，由 pipeline 的 dispatch 阶段真正发出；编排步骤类型
// （delay / digest）不直接投递，而是影响消息时序与聚合方式。
type StepType string

const (
	// StepTypeInApp 应用内收件箱。
	StepTypeInApp StepType = "in_app"
	// StepTypeEmail 邮件。
	StepTypeEmail StepType = "email"
	// StepTypeSMS 短信。
	StepTypeSMS StepType = "sms"
	// StepTypePush 移动推送。
	StepTypePush StepType = "push"
	// StepTypeChat 聊天工具（webhook）。
	StepTypeChat StepType = "chat"
	// StepTypeDelay 定时延迟（编排）。
	StepTypeDelay StepType = "delay"
	// StepTypeDigest 摘要聚合（编排）。
	StepTypeDigest StepType = "digest"
)

// AllStepTypes 返回全部步骤类型。
func AllStepTypes() []StepType {
	return []StepType{
		StepTypeInApp, StepTypeEmail, StepTypeSMS, StepTypePush,
		StepTypeChat, StepTypeDelay, StepTypeDigest,
	}
}

// DeliveryStepTypes 返回真正执行投递的步骤类型（排除 delay/digest）。
func DeliveryStepTypes() []StepType {
	var out []StepType
	for _, t := range AllStepTypes() {
		if t.IsDelivery() {
			out = append(out, t)
		}
	}
	return out
}

// OrchestrationStepTypes 返回编排步骤类型（delay / digest）。
func OrchestrationStepTypes() []StepType {
	return []StepType{StepTypeDelay, StepTypeDigest}
}

// IsDelivery 判断是否为投递步骤类型。
func (t StepType) IsDelivery() bool {
	return t != StepTypeDelay && t != StepTypeDigest
}

// Channel 返回该步骤类型绑定的投递渠道；编排步骤返回 ok=false。
func (t StepType) Channel() (Channel, bool) {
	if !t.IsDelivery() {
		return "", false
	}
	return Channel(t), true
}

// FromChannel 由投递渠道反查对应的步骤类型；编排步骤无渠道，仅投递渠道可映射。
func FromChannel(c Channel) (StepType, bool) {
	t := StepType(c)
	if !t.IsDelivery() {
		return "", false
	}
	return t, true
}

// ParseStepType 按存储值解析步骤类型；未知值返回 false。
func ParseStepType(value string) (StepType, bool) {
	for _, t := range AllStepTypes() {
		if string(t) == value {
			return t, true
		}
	}
	return "", false
}
