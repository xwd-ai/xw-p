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

// buildRecord 把单个步骤结果折算成一条执行明细。
//
// 第二个返回值为 false 表示这个步骤没有投递动作，不该产生明细。
func buildRecord(msg queue.QueuedMessage, res pipeline.StepResult) (execution.Record, bool) {
	channel := strings.TrimSpace(string(res.Channel))
	if channel == "" {
		return execution.Record{}, false
	}
	if res.Skipped || res.ShortCircuited || res.Deferred || res.TimedOut {
		return execution.Record{}, false
	}

	record := execution.Record{
		TriggerID:    msg.TriggerID,
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
		// 既没有成功也没有失败原因：聚合卡片在窗口内命中、其他渠道按设计
		// 不重复投递等情形。这类步骤没有「这次投递的结果」，不落明细。
		return execution.Record{}, false
	}
	return record, true
}
