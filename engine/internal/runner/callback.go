package runner

import (
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
	"github.com/xwd-ai/xw-p/engine/internal/webhook"
)

// Notifier 接收一次执行的结果事件，返回是否成功入队。
//
// 声明成「只投递、不等待」的接口：回调是旁路，投递方（webhook.Dispatcher）
// 内部自己排队与发送，执行路径不关心对方什么时候发出、成不成功。
type Notifier interface {
	Notify(ev webhook.Event) bool
}

// NotifyResults 把步骤结果汇总成回调事件投给通知方，返回是否入队。
//
// 正常消费与 delay 续跑两个出口共用这一处折算：回调描述的是「这条触发的
// 每个渠道最后怎样」，两个出口各写一遍必然会有口径差异（比如一边算上
// 被过滤的步骤、另一边不算）。没接通知方或没有投递动作时返回 false。
func NotifyResults(notifier Notifier, msg queue.QueuedMessage, results []pipeline.StepResult) bool {
	if notifier == nil {
		return false
	}
	ev, ok := webhook.NewEvent(msg, results)
	if !ok {
		return false
	}
	return notifier.Notify(ev)
}
