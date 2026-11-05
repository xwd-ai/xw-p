package webhook

import (
	"fmt"
	"strings"
)

// Selection 是回调订阅的事件类型集合。
//
// 一个部署通常只关心其中几类：接进群里做告警的端点只要失败事件，做站内状态
// 同步的端点要汇总事件，按帖子渲染卡片的前端要聚合事件。全发的话，一次触发
// 会变成四五条 HTTP 请求，接收方还得自己丢掉不要的那些。
//
// 零值表示「未配置」，按 DefaultSelection 处理：Options 是结构体字面量构造的，
// 零值若表示「一条都不发」，忘写这个字段就会静默变成没有回调。
type Selection struct {
	initialized bool
	all         bool
	types       map[string]struct{}
}

// DefaultSelection 只订阅汇总事件。
//
// 与新增逐渠道事件之前的行为一致：升级引擎不会让已有接入方的回调量翻几倍，
// 想收更细的事件再显式打开。
func DefaultSelection() Selection {
	return Selection{
		initialized: true,
		types:       map[string]struct{}{EventDeliveryResult: {}},
	}
}

// AllEvents 订阅全部类型。
func AllEvents() Selection {
	return Selection{initialized: true, all: true}
}

// ParseSelection 解析逗号分隔的事件类型配置。
//
// 支持三种写法：空（用默认集合）、all / *（全部）、事件名列表。名称写错时
// 报错并列出可选值——悄悄忽略一个拼错的类型，表现是「订阅了却收不到」，
// 比起启动时报错难查得多。
func ParseSelection(raw string) (Selection, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return DefaultSelection(), nil
	}
	if strings.EqualFold(trimmed, "all") || trimmed == "*" {
		return AllEvents(), nil
	}

	types := make(map[string]struct{}, 4)
	for _, part := range strings.Split(trimmed, ",") {
		name := strings.ToLower(strings.TrimSpace(part))
		if name == "" {
			continue
		}
		if !IsKnownEvent(name) {
			return Selection{}, fmt.Errorf("未知的回调事件类型 %q，可选：%s（或用 all 订阅全部）",
				name, strings.Join(eventTypes, " / "))
		}
		types[name] = struct{}{}
	}
	if len(types) == 0 {
		return Selection{}, fmt.Errorf("回调事件配置里没有有效的事件类型：%q", raw)
	}
	return Selection{initialized: true, types: types}, nil
}

// Normalized 返回可直接使用的集合：零值收敛到默认集合。
func (s Selection) Normalized() Selection {
	if !s.initialized {
		return DefaultSelection()
	}
	return s
}

// Allows 判断是否订阅了某个事件类型。
func (s Selection) Allows(name string) bool {
	s = s.Normalized()
	if s.all {
		return true
	}
	_, ok := s.types[name]
	return ok
}

// All 判断是否订阅了全部类型。
func (s Selection) All() bool { return s.Normalized().all }

// Names 按固定顺序返回订阅的事件类型，供启动日志与文档展示。
func (s Selection) Names() []string {
	s = s.Normalized()
	if s.all {
		return EventTypes()
	}
	out := make([]string, 0, len(s.types))
	for _, name := range eventTypes {
		if _, ok := s.types[name]; ok {
			out = append(out, name)
		}
	}
	return out
}

// String 输出配置的可读形态。
func (s Selection) String() string {
	if s.All() {
		return "all"
	}
	names := s.Names()
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ",")
}
