package runner

import (
	"log/slog"
	"strings"

	"github.com/xwd-ai/xw-p/engine/internal/execution"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// RecordResults 把一次执行的步骤结果回写成执行明细，返回写入条数。
//
// 只记录真正走到了分发阶段的步骤：被过滤、条件不满足、上游失败短路、
// 已登记延迟续跑的步骤都没有投递动作，把它们也写成 failed 会让执行明细
// 里的失败原因变成噪音（「没发」和「发失败」是两件事）。
//
// 一个例外是按订阅者偏好跳过的渠道：它的结论同样需要落账，写 “skipped“
// 并带上命中的层级。被过滤的步骤没有投递意图，偏好跳过的步骤有——区别在于
// 「本来要发，因为用户设置没发」，这个结论事后无法从别处推出来。
func RecordResults(recorder *execution.Recorder, logger *slog.Logger, msg queue.QueuedMessage, results []pipeline.StepResult) int {
	if recorder == nil {
		return 0
	}
	if logger == nil {
		logger = slog.Default()
	}
	written := 0
	for _, res := range results {
		record, ok := buildRecord(msg, res)
		if !ok {
			continue
		}
		if _, err := recorder.Write(record); err != nil {
			// 记录器已经打过 Warn，这里不再重复；继续写下一条，
			// 一条记录写不进去不该让同一次执行的其他渠道也丢掉明细。
			continue
		}
		written++
	}
	return written
}

// deliveryChannel 判断一个步骤结果是否对应真实的投递动作，返回其渠道。
//
// 明细回写与指标采集共用这一处判定，否则两边对「什么算发过」的口径迟早分叉。
func deliveryChannel(res pipeline.StepResult) (string, bool) {
	channel := strings.TrimSpace(string(res.Channel))
	if channel == "" {
		return "", false
	}
	if res.Skipped || res.ShortCircuited || res.Deferred {
		return "", false
	}
	if !res.Sent && res.Error == "" {
		// 既没有成功也没有失败原因：聚合卡片在窗口内命中、其他渠道按设计
		// 不重复投递等情形。这类步骤没有「这次投递的结果」。
		return "", false
	}
	return channel, true
}

// buildRecord 把单个步骤结果折算成一条执行明细。
//
// 第二个返回值为 false 表示这个步骤没有投递动作，不该产生明细。
func buildRecord(msg queue.QueuedMessage, res pipeline.StepResult) (execution.Record, bool) {
	// 偏好跳过单独一条路径：它没走到渠道层，因此不带 provider / attempts /
	// 耗时，但需要 reason 说明是哪一层拦下的。
	if res.Skipped && res.SkipReason == pipeline.SkipReasonPreference {
		channel := strings.TrimSpace(string(res.Channel))
		if channel == "" {
			return execution.Record{}, false
		}
		reason := strings.TrimSpace(res.SkipMessage)
		if reason == "" {
			reason = "偏好跳过"
		}
		return execution.Record{
			TriggerID:    msg.TriggerID,
			Tenant:       tenantOr(msg.Tenant),
			Environment:  environmentOr(msg.Environment),
			Workflow:     msg.Name,
			SubscriberID: msg.SubscriberID,
			StepID:       res.StepID,
			Channel:      channel,
			Status:       execution.StatusSkipped,
			Reason:       &reason,
		}, true
	}

	channel, ok := deliveryChannel(res)
	if !ok || res.TimedOut {
		// 超时的步骤不落明细：投递可能已经在放弃等待之后真的发出去了，
		// 写成 failed 会把执行记录变成误报。超时另有指标计数（见 metrics.go）。
		return execution.Record{}, false
	}

	record := execution.Record{
		TriggerID:    msg.TriggerID,
		Tenant:       tenantOr(msg.Tenant),
		Environment:  environmentOr(msg.Environment),
		Workflow:     msg.Name,
		SubscriberID: msg.SubscriberID,
		StepID:       res.StepID,
		Channel:      channel,
		Provider:     res.UsedProvider,
		Attempts:     res.ProviderAttempts,
		Fallback:     res.ProviderFallback,
		DurationMS:   res.Duration.Milliseconds(),
	}

	switch {
	case res.Sent:
		record.Status = execution.StatusSent
	case res.Error != "":
		record.Status = execution.StatusFailed
		reason := res.Error
		record.Reason = &reason
	default:
		return execution.Record{}, false
	}
	return record, true
}

// tenantOr / environmentOr 把空值折算成缺省隔离维度。
//
// 升级前入队的消息没有这两个字段；写进明细时补默认值，API 默认租户的
// 视图才能继续看到这些历史投递。
func tenantOr(value string) string {
	if strings.TrimSpace(value) == "" {
		return "default"
	}
	return value
}

func environmentOr(value string) string {
	if strings.TrimSpace(value) == "" {
		return "development"
	}
	return value
}
