package pipeline

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// inAppProvider 是 in_app 渠道的站内信实现。
//
// 站内信没有外部网关：投递动作就是把消息写进订阅者的收件箱，由客户端拉取。
// 因此 provider 的「发送」实际是一次收件箱落库（InboxStore.Save），
// 落库成功即视为投递成功；同一条触发消息重放时按条目 ID 幂等去重。
type inAppProvider struct {
	logger *slog.Logger
	store  InboxStore
	now    func() time.Time // 注入时间源，便于测试断言
}

// NewInAppProvider 创建站内信 provider。
// store 为 nil 时使用内存收件箱（测试与无持久化场景）。
func NewInAppProvider(logger *slog.Logger, store InboxStore) Provider {
	if logger == nil {
		logger = slog.Default()
	}
	if store == nil {
		store = NewMemoryInboxStore()
	}
	return &inAppProvider{logger: logger, store: store, now: time.Now}
}

// Channel 返回 in_app 渠道。
func (p *inAppProvider) Channel() domain.Channel {
	return domain.ChannelInApp
}

// InboxStore 暴露该 provider 使用的收件箱存储（实现 InboxStoreProvider），
// 供 digest 步骤在同一份存储上原地改写聚合卡片。
func (p *inAppProvider) InboxStore() InboxStore {
	return p.store
}

// Send 把一条站内信写入订阅者收件箱。
//
// 订阅者 ID 缺失或正文为空时投递失败——站内信没有收件人就没有意义，
// 空正文则会在收件箱里留下一条点不动的空卡片。重复投递同一
// trigger_id + step_id 时按幂等处理：不新增条目，但仍视为成功。
func (p *inAppProvider) Send(d Delivery) SendResult {
	subscriberID := strings.TrimSpace(d.SubscriberID)
	if subscriberID == "" {
		p.logger.Warn("[in_app] 投递失败：缺少订阅者 ID",
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
			"triggerId", d.TriggerID,
		)
		return SendResult{Sent: false, Reason: "缺少订阅者 ID"}
	}

	body := strings.TrimSpace(d.Body)
	if body == "" {
		p.logger.Warn("[in_app] 投递失败：站内信正文为空",
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
			"subscriberId", subscriberID,
		)
		return SendResult{Sent: false, Reason: "站内信正文为空"}
	}

	entry := InboxEntry{
		ID:           inboxEntryID(d, p.now()),
		TriggerID:    d.TriggerID,
		SubscriberID: subscriberID,
		Workflow:     d.WorkflowName,
		StepID:       d.StepID,
		Title:        inAppTitle(d),
		Body:         body,
		CreatedAt:    p.now().UTC(),
	}

	inserted, err := p.store.Save(entry)
	if err != nil {
		p.logger.Warn("[in_app] 投递失败：写入收件箱出错",
			"stepId", d.StepID,
			"workflow", d.WorkflowName,
			"subscriberId", subscriberID,
			"error", err,
		)
		return SendResult{Sent: false, Reason: "写入收件箱失败: " + err.Error()}
	}
	if !inserted {
		p.logger.Info("[in_app] 已存在同一条站内信（幂等去重）",
			"id", entry.ID,
			"subscriberId", subscriberID,
			"workflow", d.WorkflowName,
		)
		return SendResult{Sent: true, Reason: fmt.Sprintf("站内信已存在（id=%s）", entry.ID)}
	}

	p.logger.Info("[in_app] 投递成功（写入收件箱）",
		"id", entry.ID,
		"subscriberId", subscriberID,
		"title", entry.Title,
		"body", truncateForLog(body, 80),
		"workflow", d.WorkflowName,
		"stepId", d.StepID,
	)
	return SendResult{Sent: true, Reason: fmt.Sprintf("站内信已写入收件箱（id=%s）", entry.ID)}
}

// inboxEntryID 生成收件箱条目 ID：优先 "trigger_id:step_id"（天然幂等），
// 触发 ID 缺失时退化为带时间的唯一 ID（重放会新增条目，属可接受的降级）。
func inboxEntryID(d Delivery, now time.Time) string {
	trigger := strings.TrimSpace(d.TriggerID)
	step := strings.TrimSpace(d.StepID)
	if trigger != "" {
		return trigger + ":" + step
	}
	return fmt.Sprintf("inapp-%d-%s", now.UTC().UnixNano(), step)
}

// inAppTitle 返回站内信标题：优先步骤 config 的 title/subject，其次工作流名。
func inAppTitle(d Delivery) string {
	if t := strings.TrimSpace(d.Subject); t != "" {
		return t
	}
	if t := strings.TrimSpace(d.WorkflowName); t != "" {
		return t
	}
	return "新通知"
}

// truncateForLog 截断过长正文，避免日志被单条消息刷屏（按字符计）。
func truncateForLog(s string, limit int) string {
	runes := []rune(s)
	if len(runes) <= limit {
		return s
	}
	return string(runes[:limit]) + "…"
}
