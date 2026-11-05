package webhook

import (
	"strings"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/execution"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// NewTriggerEvent 构造「触发已受理」事件。
//
// 它只描述动手之前能确定的事：这次跑哪份步骤定义、涉及哪些渠道、步骤取自
// 快照还是定义表。实际发没发出去由后面的事件回答——把两者混在一条报文里
// 会让接入方在「已受理」时就以为「已投递」。
func NewTriggerEvent(msg queue.QueuedMessage, wf domain.Workflow, fromSnapshot bool) (Event, bool) {
	if strings.TrimSpace(msg.Name) == "" && strings.TrimSpace(wf.Name) == "" {
		return Event{}, false
	}

	ev := newEnvelope(EventTriggerReceived, msg)
	if strings.TrimSpace(ev.Workflow) == "" {
		ev.Workflow = wf.Name
	}
	ev.Trigger = &Trigger{
		Steps:        len(wf.Steps),
		Channels:     declaredChannels(wf),
		Version:      wf.Version,
		FromSnapshot: fromSnapshot,
	}
	return ev, true
}

// declaredChannels 取工作流会投递到的渠道，去重并保留步骤出现顺序。
//
// 顺序有含义：它反映工作流里先发哪个渠道，接入方按它排序比按字母序更接近
// 实际执行；去重是因为同一渠道可能配了多个步骤（例如站内信分「先提醒」
// 与「补一条」），报文里没必要重复列出。
func declaredChannels(wf domain.Workflow) []string {
	seen := make(map[domain.Channel]struct{}, len(wf.Steps))
	out := make([]string, 0, len(wf.Steps))
	for _, step := range wf.Steps {
		channel, ok := stepChannel(step)
		if !ok {
			continue
		}
		if _, ok := seen[channel]; ok {
			continue
		}
		seen[channel] = struct{}{}
		out = append(out, string(channel))
	}
	return out
}

// stepChannel 取步骤实际会投递到的渠道。
//
// 投递步骤直接看类型；聚合步骤的卡片渠道写在 config.channel 里（默认站内信），
// 只看类型会把「这张卡片会进收件箱」漏掉，受理事件给出的渠道清单就会少一项。
// 配置写坏时按「不投递」处理：那条路径本来就会在步骤执行时报错，受理事件
// 不必替它编一个渠道。
func stepChannel(step domain.Step) (domain.Channel, bool) {
	if step.IsDelivery() {
		return step.Type, true
	}
	if step.Type == domain.ChannelDigest {
		if spec, err := pipeline.ParseDigestSpec(step); err == nil {
			return spec.Channel, true
		}
	}
	return "", false
}

// NewChannelEvents 按渠道拆出投递结果事件。
//
// 一条明细一个事件，而不是把多个渠道塞进一条：接收方订阅 delivery.failed
// 就是为了按渠道做告警，一条报文里同时有成功与失败反而还要再判断一次。
func NewChannelEvents(msg queue.QueuedMessage, results []pipeline.StepResult) []Event {
	events := make([]Event, 0, len(results))
	for _, res := range results {
		item, ok := buildDelivery(res)
		if !ok {
			continue
		}
		ev := newEnvelope(EventDeliverySent, msg)
		if item.Status == StatusFailed {
			ev.Event = EventDeliveryFailed
		}
		ev.Status = item.Status
		// 每个事件持有自己的副本：切片里的元素被后续步骤复用或改写时，
		// 已入队的事件不应跟着变。
		delivery := item
		delivery.Attempts = append([]string(nil), item.Attempts...)
		ev.Delivery = &delivery
		events = append(events, ev)
	}
	return events
}

// NewDigestEvents 按聚合步骤拆出卡片状态事件。
//
// 只有真正落到卡片上的步骤才有这一条：聚合键缺失、订阅者缺失、渲染失败时
// 没有卡片可言，报一个「累计 0 条」只会让接收方以为卡片被清零。重放要报，
// 因为它是接入方判断「这条互动已经计过数、不要再显示一次」的唯一依据。
func NewDigestEvents(msg queue.QueuedMessage, results []pipeline.StepResult) []Event {
	events := make([]Event, 0, len(results))
	for _, res := range results {
		if res.StepType != domain.StepTypeDigest {
			continue
		}
		if res.ShortCircuited || res.TimedOut {
			continue
		}
		if strings.TrimSpace(res.AggregateKey) == "" || res.AggregateCount < 1 {
			continue
		}
		ev := newEnvelope(EventDigestUpdated, msg)
		digest := &Digest{
			StepID:   res.StepID,
			Key:      res.AggregateKey,
			Count:    res.AggregateCount,
			Opened:   res.AggregateOpened,
			Replayed: res.AggregateReplayed,
		}
		if !res.AggregateExpiresAt.IsZero() {
			stamp := execution.NewTimestamp(res.AggregateExpiresAt)
			digest.ExpiresAt = &stamp
		}
		ev.Digest = digest
		events = append(events, ev)
	}
	return events
}
