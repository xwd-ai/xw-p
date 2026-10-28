package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// 超时预算是执行链路的兜底：投递渠道一旦卡住（连接不返回、webhook 不应答），
// 一条消息就会长期占住消费者，后续消息全部排队。两道预算分别约束
// 「单个步骤」与「一条消息的一次执行」。
//
// 取值偏保守：本地 mock 投递在毫秒级完成，10s 已经意味着下游异常。
const (
	// DefaultStepTimeout 是单步默认超时。
	DefaultStepTimeout = 10 * time.Second
	// DefaultWorkflowTimeout 是一条消息一次执行的默认总预算。
	DefaultWorkflowTimeout = 60 * time.Second
)

var (
	// ErrStepTimeout 表示单个步骤超过预算未返回。
	ErrStepTimeout = errors.New("步骤执行超时")
	// ErrWorkflowTimeout 表示整条消息的执行预算耗尽。
	ErrWorkflowTimeout = errors.New("整体执行超时")
)

// TimeoutPolicy 是超时策略。零值表示不限时（便于测试与离线批处理）。
type TimeoutPolicy struct {
	// Step 是单步默认超时，可被步骤 config 覆盖。
	Step time.Duration
	// Workflow 是一条消息一次执行的总预算。
	Workflow time.Duration
}

// DefaultTimeoutPolicy 返回默认超时策略。
func DefaultTimeoutPolicy() TimeoutPolicy {
	return TimeoutPolicy{Step: DefaultStepTimeout, Workflow: DefaultWorkflowTimeout}
}

// TimeoutError 描述一次超时，errors.Is 可分别判定为 ErrStepTimeout
// 或 ErrWorkflowTimeout（Overdue 为 true 表示被整体预算收紧）。
type TimeoutError struct {
	// StepID 是超时步骤的 ID。
	StepID string
	// Limit 是该步骤实际获批的时间预算。
	Limit time.Duration
	// Overall 表示该预算来自整体预算的剩余量，而非步骤自身的超时设置。
	Overall bool
}

// Error 实现 error，区分「步骤自身超时」与「整体预算耗尽」两种语义。
func (e *TimeoutError) Error() string {
	if e.Overall {
		return fmt.Sprintf("%s：剩余预算 %s 内未完成", ErrWorkflowTimeout.Error(), e.Limit)
	}
	return fmt.Sprintf("%s：超过 %s 未返回", ErrStepTimeout.Error(), e.Limit)
}

// Unwrap 让 errors.Is 能按超时类别判定。
func (e *TimeoutError) Unwrap() error {
	if e.Overall {
		return ErrWorkflowTimeout
	}
	return ErrStepTimeout
}

// WithTimeouts 设置超时策略，返回管线自身便于链式装配。负值按「不限时」处理。
func (p *Pipeline) WithTimeouts(policy TimeoutPolicy) *Pipeline {
	if policy.Step < 0 {
		policy.Step = 0
	}
	if policy.Workflow < 0 {
		policy.Workflow = 0
	}
	p.timeouts = policy
	return p
}

// Timeouts 返回当前超时策略。
func (p *Pipeline) Timeouts() TimeoutPolicy {
	return p.timeouts
}

// ParseStepTimeout 解析步骤 config 上的超时覆盖，支持三种写法：
//
//	{"timeout": "1500ms"}   // Go duration 字符串
//	{"timeout": 2}          // 数字，单位秒（可为小数）
//	{"timeout_ms": 800}     // 数字，单位毫秒
//
// 返回 (limit, configured, err)：configured 为 false 表示步骤未覆盖，沿用策略
// 默认值；limit 为 0 表示该步骤显式关闭超时。
func ParseStepTimeout(step domain.Step) (time.Duration, bool, error) {
	if len(step.Config) == 0 {
		return 0, false, nil
	}

	raw, hasTimeout := step.Config["timeout"]
	rawMs, hasTimeoutMs := step.Config["timeout_ms"]
	if hasTimeout && hasTimeoutMs {
		return 0, false, errors.New("timeout 与 timeout_ms 只能二选一")
	}
	if !hasTimeout && !hasTimeoutMs {
		return 0, false, nil
	}

	if hasTimeoutMs {
		ms, err := configNumber(rawMs)
		if err != nil {
			return 0, false, fmt.Errorf("timeout_ms 必须是数字（毫秒）：%w", err)
		}
		return nonNegativeDuration(time.Duration(ms*float64(time.Millisecond)), "timeout_ms")
	}

	// timeout：字符串按 duration 解析，数字按秒解析。
	if text, ok := raw.(string); ok {
		d, err := time.ParseDuration(strings.TrimSpace(text))
		if err != nil {
			return 0, false, fmt.Errorf("timeout 无法解析：%q", text)
		}
		return nonNegativeDuration(d, "timeout")
	}

	seconds, err := configNumber(raw)
	if err != nil {
		return 0, false, fmt.Errorf("timeout 必须是 duration 字符串或数字（秒）：%w", err)
	}
	return nonNegativeDuration(time.Duration(seconds*float64(time.Second)), "timeout")
}

// nonNegativeDuration 校验解析结果：负值属于配置错误，不能被当作「不限时」静默吞掉。
func nonNegativeDuration(d time.Duration, field string) (time.Duration, bool, error) {
	if d < 0 {
		return 0, false, fmt.Errorf("%s 不能为负值（%s）", field, d)
	}
	return d, true, nil
}

// configNumber 把 config 中的数字（JSON 反序列化为 float64，手写代码多为 int）转成 float64。
func configNumber(raw any) (float64, error) {
	switch v := raw.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return 0, fmt.Errorf("无法解析数字 %q", v.String())
		}
		return f, nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, fmt.Errorf("无法解析数字 %q", v)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("实际类型为 %T", raw)
	}
}

// stepBudget 计算某步骤本次可用的时间预算与超时语义。
//
// 优先级：步骤 config 覆盖 > 策略默认；再与整体预算的剩余量取较小值，
// 第二个返回值说明预算是否被整体预算收紧（用于区分两种超时原因）。
func (p *Pipeline) stepBudget(ctx *Context, step domain.Step) (time.Duration, bool, error) {
	limit := p.timeouts.Step
	override, configured, err := ParseStepTimeout(step)
	if err != nil {
		return 0, false, fmt.Errorf("步骤 %s 超时配置非法：%w", step.ID, err)
	}
	if configured {
		limit = override
	}

	remaining, limited := ctx.RemainingBudget(p.now())
	if !limited {
		return limit, false, nil
	}
	if remaining <= 0 {
		return 0, true, fmt.Errorf("%w：执行预算已耗尽", ErrWorkflowTimeout)
	}
	if limit <= 0 || remaining < limit {
		return remaining, true, nil
	}
	return limit, false, nil
}

// execStep 在限定预算内执行一个步骤。
//
// 不限时（limit<=0）时直接在父上下文执行，不引入额外 goroutine；限时时把步骤
// 放到 fork 出的子上下文里并发执行，超时即不再等待并返回 *TimeoutError。
//
// 超时的语义边界要说清楚：Go 无法抢占 goroutine，超时只保证「管线不再等待」，
// 已经发出的投递无法撤回，子 goroutine 也可能在超时之后才结束。因此步骤超时
// 应大于下游 P99 延迟，它是防止链路被单条消息拖死的兜底，不是精确的取消机制。
// 需要真正可中断的渠道，实现 ContextSender 即可在超时瞬间收到 ctx.Done。
func (p *Pipeline) execStep(
	ctx *Context,
	step domain.Step,
	limit time.Duration,
	overall bool,
	fn func(*Context) error,
) error {
	if limit <= 0 {
		return fn(ctx)
	}

	runCtx, cancel := context.WithTimeout(ctx.Context(), limit)
	defer cancel()

	child := ctx.Fork(runCtx)
	// 缓冲区为 1：超时后子 goroutine 结束时也能写入并退出，不会泄漏。
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("步骤 %s 执行 panic：%v", step.ID, r)
			}
		}()
		done <- fn(child)
	}()

	select {
	case err := <-done:
		ctx.MergeResult(child, step.ID)
		return err

	case <-runCtx.Done():
		// 引擎退出导致的取消不同于超时：如实回报取消，避免把停机写成「超时」。
		if cause := ctx.Context().Err(); cause != nil {
			return fmt.Errorf("步骤 %s 执行被取消：%w", step.ID, cause)
		}
		p.logger.Warn("[pipeline] 步骤超时，放弃等待",
			"stepId", step.ID,
			"channel", string(step.Type),
			"limit", limit,
			"overallBudget", overall,
			"triggerId", ctx.Msg.TriggerID,
		)
		return &TimeoutError{StepID: step.ID, Limit: limit, Overall: overall}
	}
}
