package runner

import (
	"github.com/xwd-ai/xw-p/engine/internal/metrics"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
)

// RecordMetrics 把一次执行的步骤结果折算成投递指标，返回记录条数。
//
// 与执行明细共用「是否发生投递」的判定（deliveryChannel），差别只在超时：
// 明细不记超时步骤（投递可能在被放弃等待之后才发出，记成 failed 是误报），
// 而指标要记——超时说明这个渠道正在拖住链路，正是最该被看到的信号，按失败
// 计并单独给出 timeouts 计数。
//
// 延迟样本含失败的尝试：失败同样占用了消费者时间，只看成功的分布会把
// 「慢且发不出去」的渠道藏起来。
func RecordMetrics(reg *metrics.Registry, results []pipeline.StepResult) int {
	if reg == nil {
		return 0
	}
	recorded := 0
	for _, res := range results {
		channel, ok := deliveryChannel(res)
		if !ok {
			continue
		}
		status := metrics.StatusFailed
		if res.Sent {
			status = metrics.StatusSent
		}
		reg.Record(metrics.Sample{
			Channel:  channel,
			Provider: res.UsedProvider,
			Status:   status,
			Duration: res.Duration,
			Retries:  res.ProviderRetries,
			Fallback: res.ProviderFallback,
			TimedOut: res.TimedOut,
		})
		recorded++
	}
	return recorded
}
