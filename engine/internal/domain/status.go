package domain

// MessageStatus 是消息状态机状态，与 Python 侧 Message.Status 对齐。
//
// 合法转移路径：
//
//	created → queued → processing → sent
//	                              ↘ failed
//
// 此外允许 processing → queued（投递前回滚重试），
// 以及 queued/processing → failed（投递失败终态）。
type MessageStatus string

const (
	// StatusCreated 消息已创建。
	StatusCreated MessageStatus = "created"
	// StatusQueued 已入队。
	StatusQueued MessageStatus = "queued"
	// StatusProcessing 处理中。
	StatusProcessing MessageStatus = "processing"
	// StatusSent 投递成功（终态）。
	StatusSent MessageStatus = "sent"
	// StatusFailed 投递失败（终态）。
	StatusFailed MessageStatus = "failed"
)

// transitions 记录每个状态允许迁入的下一状态集合。
var transitions = map[MessageStatus]map[MessageStatus]bool{
	StatusCreated:    {StatusQueued: true, StatusFailed: true},
	StatusQueued:     {StatusProcessing: true, StatusFailed: true},
	StatusProcessing: {StatusSent: true, StatusFailed: true, StatusQueued: true},
	StatusSent:       {},
	StatusFailed:     {},
}

// AllStatuses 返回全部状态。
func AllStatuses() []MessageStatus {
	return []MessageStatus{StatusCreated, StatusQueued, StatusProcessing, StatusSent, StatusFailed}
}

// IsTerminal 判断是否为终态（sent / failed），终态不再允许任何转移。
func (s MessageStatus) IsTerminal() bool {
	return s == StatusSent || s == StatusFailed
}

// CanTransitionTo 判断从当前状态能否合法转移到 target。
func (s MessageStatus) CanTransitionTo(target MessageStatus) bool {
	return transitions[s][target]
}

// ParseStatus 按存储值解析状态；未知值返回 false。
func ParseStatus(value string) (MessageStatus, bool) {
	for _, s := range AllStatuses() {
		if string(s) == value {
			return s, true
		}
	}
	return "", false
}
