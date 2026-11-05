package runner

import (
	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
	"github.com/xwd-ai/xw-p/engine/internal/webhook"
)

// Notifier 接收一次执行的事件，返回是否成功入队。
//
// 声明成「只投递、不等待」的接口：回调是旁路，投递方（webhook.Dispatcher）
// 内部自己排队与发送，执行路径不关心对方什么时候发出、成不成功。
type Notifier interface {
	Notify(ev webhook.Event) bool
}

// NotifyTrigger 投递「触发已受理」事件，返回是否入队。
//
// 在真正执行之前调用：接入方据此可以先把状态改成「处理中」，而结果事件
// 可能晚到几秒，中间这段空白在评论区里会被当成「点了没反应」。
func NotifyTrigger(notifier Notifier, msg queue.QueuedMessage, wf domain.Workflow, fromSnapshot bool) bool {
	if notifier == nil {
		return false
	}
	ev, ok := webhook.NewTriggerEvent(msg, wf, fromSnapshot)
	if !ok {
		return false
	}
	return notifier.Notify(ev)
}

// NotifyResults 把步骤结果折算成事件族投给通知方，返回入队条数。
//
// 正常消费与 delay 续跑两个出口共用这一处折算：回调描述的是「这条触发的
// 每个渠道最后怎样」，两个出口各写一遍必然会有口径差异（比如一边算上被
// 过滤的步骤、另一边不算）。
//
// 一次执行会产出多条事件：逐渠道的成功/失败、聚合卡片的状态，以及一条
// 汇总。订阅集合由分发器筛，这里只管把该发的都发出去——在折算层做过滤，
// 换一个投递实现（例如以后接事件总线）就得把同一套规则再写一遍。
func NotifyResults(notifier Notifier, msg queue.QueuedMessage, results []pipeline.StepResult) int {
	if notifier == nil {
		return 0
	}

	accepted := 0
	for _, ev := range webhook.NewChannelEvents(msg, results) {
		if notifier.Notify(ev) {
			accepted++
		}
	}
	for _, ev := range webhook.NewDigestEvents(msg, results) {
		if notifier.Notify(ev) {
			accepted++
		}
	}
	if ev, ok := webhook.NewEvent(msg, results); ok && notifier.Notify(ev) {
		accepted++
	}
	return accepted
}
