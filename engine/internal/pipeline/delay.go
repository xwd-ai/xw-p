package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// maxDelayDuration 是单个 delay 步骤允许的最大延迟时长（30 天）。
// 目的是拦住上游误配置（例如把毫秒当成秒、把天当成秒），避免消息被排到
// 遥遥无期的时刻而永久滞留。
const maxDelayDuration = 30 * 24 * time.Hour

// delayUnitFactors 定义 delay 步骤 config 中可直接写的时长字段及其换算。
// 允许同时出现多个字段（如 {"minutes": 5, "seconds": 30}），按和计算。
var delayUnitFactors = []struct {
	key string
	// factor 为「1 个该单位 = 多少个 time.Duration 纳秒单位」。
	factor time.Duration
}{
	{"days", 24 * time.Hour},
	{"hours", time.Hour},
	{"minutes", time.Minute},
	{"seconds", time.Second},
}

// DelaySpec 是一个 delay 步骤解析后的延迟时长。
type DelaySpec struct {
	// Duration 为延迟时长，恒为正且不超过 maxDelayDuration。
	Duration time.Duration
}

// DueAt 返回从 now 起算的到期时刻。
func (s DelaySpec) DueAt(now time.Time) time.Time {
	return now.Add(s.Duration)
}

// ParseDelaySpec 解析 delay 步骤的时长配置，支持两种写法：
//
//	{"seconds": 30}                              // 单位字段，可多字段相加
//	{"minutes": 5, "seconds": 30}
//	{"delay": "90s"}                             // Go duration 字符串（90s / 15m / 2h）
//
// 校验：时长必须为正且不超过 30 天；两种写法都不存在时报错。
// 注意 "delay" 是保留键，不与单位字段混用（混用时以 "delay" 为准并报错冲突）。
func ParseDelaySpec(step domain.Step) (DelaySpec, error) {
	cfg := step.Config
	if len(cfg) == 0 {
		return DelaySpec{}, errors.New("delay 步骤缺少时长配置（config.seconds 或 config.delay）")
	}

	// 写法一：duration 字符串。
	if raw, ok := cfg["delay"]; ok {
		for _, unit := range delayUnitFactors {
			if _, dup := cfg[unit.key]; dup {
				return DelaySpec{}, fmt.Errorf(
					"delay 步骤的 delay 与 %s 不能同时配置", unit.key)
			}
		}
		text, ok := raw.(string)
		if !ok {
			return DelaySpec{}, fmt.Errorf("delay 步骤的 delay 必须是字符串（如 \"90s\"），实际为 %T", raw)
		}
		d, err := time.ParseDuration(strings.TrimSpace(text))
		if err != nil {
			return DelaySpec{}, fmt.Errorf("delay 步骤的 delay 无法解析：%q", text)
		}
		return newDelaySpec(d)
	}

	// 写法二：单位字段相加。
	var total time.Duration
	var configured bool
	for _, unit := range delayUnitFactors {
		raw, ok := cfg[unit.key]
		if !ok {
			continue
		}
		configured = true
		n, err := durationCount(raw)
		if err != nil {
			return DelaySpec{}, fmt.Errorf("delay 步骤的 %s 非法：%w", unit.key, err)
		}
		// 先按浮点折算再转 Duration，支持 0.5 天这类写法；
		// 上限校验放在最后，这里的溢出风险由 maxDelayDuration 提前拦住。
		if n > float64(maxDelayDuration/unit.factor) {
			return DelaySpec{}, fmt.Errorf("delay 步骤的 %s 超出上限（最多 30 天）", unit.key)
		}
		total += time.Duration(n * float64(unit.factor))
	}
	if !configured {
		return DelaySpec{}, errors.New("delay 步骤缺少时长配置（config.seconds 或 config.delay）")
	}
	return newDelaySpec(total)
}

// newDelaySpec 校验时长范围并构造 DelaySpec。
func newDelaySpec(d time.Duration) (DelaySpec, error) {
	if d <= 0 {
		return DelaySpec{}, fmt.Errorf("delay 步骤的时长必须为正，实际为 %s", d)
	}
	if d > maxDelayDuration {
		return DelaySpec{}, fmt.Errorf("delay 步骤的时长超出上限 30 天，实际为 %s", d)
	}
	return DelaySpec{Duration: d}, nil
}

// durationCount 把 config 里的数值（JSON 解出的 float64、或测试里的 int）转为 float64。
func durationCount(raw any) (float64, error) {
	switch v := raw.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case json.Number:
		return v.Float64()
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, fmt.Errorf("不是数字：%q", v)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("不是数字（%T）", raw)
	}
}

// Deferral 是一次「延迟续跑」任务：delay 步骤之后尚未执行的步骤，
// 连同原始队列消息一起，等到 DueAt 时刻再由调度器唤醒执行。
//
// 字段全部导出并带 JSON tag，便于编码后投递给引擎外部的延迟调度器
// （Redis ZSET / 消息队列的延迟投递能力），实现进程重启后不丢任务。
type Deferral struct {
	// StepID 是触发本次延迟的 delay 步骤 id，便于排查「卡在哪个延迟步骤」。
	StepID string `json:"step_id"`
	// WorkflowName 是工作流名，续跑时用于日志与上下文还原。
	WorkflowName string `json:"workflow_name,omitempty"`
	// DueAt 是到期时刻（UTC），由调度器按此排序。
	DueAt time.Time `json:"due_at"`
	// Message 是原始队列消息，续跑时原样传入管线。
	Message queue.QueuedMessage `json:"message"`
	// Remaining 是 delay 步骤之后待执行的步骤（按原顺序，可能再次包含 delay）。
	Remaining []domain.Step `json:"remaining,omitempty"`
}

// Encode 把续跑任务序列化为 JSON。
func (d Deferral) Encode() ([]byte, error) {
	return json.Marshal(d)
}

// DecodeDeferral 反序列化续跑任务。
func DecodeDeferral(data []byte) (Deferral, error) {
	var d Deferral
	if err := json.Unmarshal(data, &d); err != nil {
		return Deferral{}, err
	}
	return d, nil
}

// Deferrer 是管线用于提交延迟续跑任务的出口。
//
// 管线本身不持有定时能力，只负责「算好到期时刻 + 剩余步骤」交给实现方；
// 实现方可以是内存调度器（测试 / 单机 demo），也可以是 Redis ZSET 或
// 消息队列的延迟投递（生产）。
type Deferrer interface {
	// Defer 提交一次延迟续跑任务；返回 error 表示本次 delay 步骤失败。
	Defer(d Deferral) error
}

// DeferrerFunc 把函数适配为 Deferrer 接口，便于测试与轻量装配。
type DeferrerFunc func(d Deferral) error

// Defer 调用底层函数。
func (f DeferrerFunc) Defer(d Deferral) error {
	return f(d)
}

// DelayEnqueuer 是延迟调度器需要具备的最小能力：按到期时刻登记一个编码后的
// 续跑任务，并返回任务 id。
type DelayEnqueuer interface {
	// EnqueueDelay 登记续跑任务，返回可追踪的任务 id。
	EnqueueDelay(dueAt time.Time, payload []byte) (string, error)
}

// EnqueueDeferrer 是基于 DelayEnqueuer 的 Deferrer 实现：
// 把 Deferral 编码为 JSON 后交给延迟调度器，管线与调度器因此互不依赖。
type EnqueueDeferrer struct {
	enq DelayEnqueuer
}

// NewEnqueueDeferrer 创建基于调度器的 Deferrer。
func NewEnqueueDeferrer(enq DelayEnqueuer) *EnqueueDeferrer {
	return &EnqueueDeferrer{enq: enq}
}

// Defer 编码并登记延迟续跑任务。
func (e *EnqueueDeferrer) Defer(d Deferral) error {
	if e == nil || e.enq == nil {
		return errors.New("delay 调度器未初始化")
	}
	payload, err := d.Encode()
	if err != nil {
		return fmt.Errorf("延迟续跑任务编码失败：%w", err)
	}
	if _, err := e.enq.EnqueueDelay(d.DueAt, payload); err != nil {
		return fmt.Errorf("延迟续跑任务登记失败：%w", err)
	}
	return nil
}
