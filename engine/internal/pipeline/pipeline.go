package pipeline

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// errSkipStep 是内部哨兵错误，表示该步骤被 filter 阶段或条件分支跳过。
var errSkipStep = errors.New("pipeline: step skipped by filter or condition")

// Pipeline 将 filter → transform → render → dispatch 四个阶段串成一条管线，
// 对工作流的每个投递步骤依次执行。
//
// 两个编排步骤不走这四个阶段：
//   - delay 在步骤序列中把「之后的步骤」整体推迟到到期时刻，交由 Deferrer 唤醒续跑（见 delay.go）；
//   - digest 把同一聚合键的多次互动收敛成一张卡片，窗口内原地累加计数（见 digest.go）。
type Pipeline struct {
	stages   []Stage
	deferrer Deferrer
	// now 提供当前时刻，测试可注入固定时钟。
	now func() time.Time

	// 以下为编排步骤（digest）复用的阶段与依赖，单独持有引用避免重复构造。
	logger    *slog.Logger
	registry  *ProviderRegistry
	filter    *filterStage
	transform *transformStage
	renderer  *renderStage
	digests   DigestStore
}

// New 创建默认管线（filter → transform → render → dispatch）。
// logger 为 nil 时使用默认 logger。
//
// 默认注册全部五个投递渠道：in_app（写收件箱）、email / sms / push（console
// mock）、chat（webhook mock）。真实网关在 Phase 3 接入时替换对应实现即可。
func New(logger *slog.Logger) *Pipeline {
	registry := DefaultProviderRegistry(logger)
	return NewWithRegistry(logger, registry)
}

// DefaultProviderRegistry 构造默认 provider 注册表：五个投递渠道各自注册实现。
//
// in_app 使用默认收件箱存储（JSONL，可由 XW_INBOX_FILE 指定）；
// 测试或无持久化需求时可改用 NewInAppProvider + MemoryInboxStore 自行组装。
func DefaultProviderRegistry(logger *slog.Logger) *ProviderRegistry {
	registry := NewProviderRegistry()
	registry.Register(NewInAppProvider(logger, DefaultInboxStore(logger)))
	registry.Register(NewConsoleEmailProvider(logger))
	registry.Register(NewConsoleSMSProvider(logger))
	registry.Register(NewConsolePushProvider(logger))
	registry.Register(NewConsoleChatProvider(logger))
	return registry
}

// NewWithRegistry 使用指定 provider 注册表创建管线，便于测试与自定义渠道集合。
func NewWithRegistry(logger *slog.Logger, registry *ProviderRegistry) *Pipeline {
	if logger == nil {
		logger = slog.Default()
	}
	if registry == nil {
		registry = NewProviderRegistry()
	}
	filter := newFilterStage()
	transform := newTransformStage()
	renderer := newRenderStage()
	return &Pipeline{
		stages: []Stage{
			filter,
			NewBranchStage(),
			transform,
			renderer,
			NewDispatchStage(logger, registry),
		},
		logger:    logger,
		registry:  registry,
		filter:    filter,
		transform: transform,
		renderer:  renderer,
		digests:   NewMemoryDigestStore(),
		now:       time.Now,
	}
}

// WithDigestStore 注入聚合卡片存储（如多引擎实例共享的 Redis 实现），
// 返回管线自身便于链式装配。传 nil 表示恢复默认内存存储。
func (p *Pipeline) WithDigestStore(store DigestStore) *Pipeline {
	if store == nil {
		store = NewMemoryDigestStore()
	}
	p.digests = store
	return p
}

// DigestStore 返回当前聚合卡片存储（供外部观测或测试读取卡片状态）。
func (p *Pipeline) DigestStore() DigestStore {
	return p.digests
}

// WithDeferrer 注入延迟续跑任务的提交出口，返回管线自身便于链式装配。
//
// 未注入时，工作流中的 delay 步骤会被判为失败（配置缺失而非静默跳过），
// 避免「以为延迟生效、实际立刻发送」的隐蔽错误。
func (p *Pipeline) WithDeferrer(d Deferrer) *Pipeline {
	p.deferrer = d
	return p
}

// WithClock 注入时钟，返回管线自身。传 nil 表示恢复默认 time.Now。
func (p *Pipeline) WithClock(now func() time.Time) *Pipeline {
	if now == nil {
		now = time.Now
	}
	p.now = now
	return p
}

// Run 按工作流步骤顺序执行管线；遇到 delay 步骤时提交延迟续跑并返回，
// 其后的步骤留到到期后由调度器通过 RunSteps 续跑。
//
// 返回每个已执行步骤的结果，顺序与执行顺序一致。
func (p *Pipeline) Run(ctx *Context, wf domain.Workflow) []StepResult {
	return p.RunSteps(ctx, wf.Steps)
}

// RunSteps 是 Run 的可续跑核心：对给定的步骤序列执行，遇到 delay 步骤即
// 停止并把剩余步骤登记为延迟续跑任务。
//
// 失败短路（fail-fast）：某个投递步骤真实失败（非 filter/条件分支跳过）时，
// 后续步骤不再执行，而是标记 ShortCircuited 并记录上游失败原因；
// delay 步骤自身失败（时长非法 / 调度器不可用）同样短路后续步骤。
func (p *Pipeline) RunSteps(ctx *Context, steps []domain.Step) []StepResult {
	var out []StepResult
	var shortCircuit *StepResult // 记录首个真实失败，用于短路后续步骤
	for i, step := range steps {
		// 上游已失败：当前步骤直接标记短路，不再进入管线。
		if shortCircuit != nil {
			out = append(out, p.markShortCircuited(ctx, step, shortCircuit.Error))
			continue
		}

		// delay 步骤：登记剩余步骤的延迟续跑，并结束本轮执行。
		if step.Type == domain.ChannelDelay {
			res := p.handleDelay(ctx, step, steps[i+1:])
			out = append(out, res)
			if res.Error != "" {
				// 调度失败：后续步骤由下一轮迭代标记为短路。
				shortCircuit = &res
				continue
			}
			// 成功：剩余步骤已交给调度器，本轮到此为止，避免「以为延迟生效、
			// 实际立刻发送」。
			break
		}

		// digest 步骤：把互动收敛进聚合卡片（窗口内原地累加计数），不进入
		// filter→transform→render→dispatch 常规四阶段（那会导致同一卡片被
		// 反复当成新消息投递）。
		if step.Type == domain.ChannelDigest {
			res := p.handleDigest(ctx, step)
			out = append(out, res)
			if res.Error != "" {
				shortCircuit = &res
			}
			continue
		}

		if err := p.runStep(ctx, step); err != nil {
			if errors.Is(err, errSkipStep) {
				// 被 filter/分支跳过：结果已由对应阶段写入，不触发短路。
			} else {
				// 真实失败：记录失败结果并作为短路源。
				res := ctx.Results[step.ID]
				res.StepID = step.ID
				res.StepType = domain.StepType(step.Type)
				res.Channel = step.Type
				res.Error = err.Error()
				ctx.Results[step.ID] = res
				shortCircuit = &res
			}
		}
		out = append(out, ctx.Results[step.ID])
	}
	return out
}

// Resume 从延迟续跑任务（编码后的 Deferral）还原上下文并执行剩余步骤。
//
// 调度器在 delay 步骤到期后调用本方法即可继续这条消息的执行：
// 原始队列消息原样还原，剩余步骤按原顺序重新走管线（可再次遇到 delay）。
func (p *Pipeline) Resume(payload []byte) (Deferral, []StepResult, error) {
	d, err := DecodeDeferral(payload)
	if err != nil {
		return Deferral{}, nil, fmt.Errorf("延迟续跑任务解析失败：%w", err)
	}
	ctx := NewContext(d.Message)
	return d, p.RunSteps(ctx, d.Remaining), nil
}

// handleDelay 处理一个 delay 步骤：解析时长、算出到期时刻、登记剩余步骤。
func (p *Pipeline) handleDelay(ctx *Context, step domain.Step, remaining []domain.Step) StepResult {
	res := StepResult{
		StepID:   step.ID,
		StepType: domain.StepTypeDelay,
	}

	spec, err := ParseDelaySpec(step)
	if err != nil {
		res.Error = err.Error()
		ctx.Results[step.ID] = res
		return res
	}

	now := time.Now
	if p.now != nil {
		now = p.now
	}
	dueAt := spec.DueAt(now())
	res.DeferredUntil = dueAt

	// delay 是最后一步：没有需要推迟的步骤，只记录到期时刻，不产生空任务。
	if len(remaining) == 0 {
		ctx.Results[step.ID] = res
		return res
	}

	if p.deferrer == nil {
		res.Error = "delay 步骤未配置延迟调度器（pipeline.WithDeferrer）"
		ctx.Results[step.ID] = res
		return res
	}

	d := Deferral{
		StepID:       step.ID,
		WorkflowName: workflowName(ctx),
		DueAt:        dueAt,
		Message:      ctx.Msg,
		Remaining:    remaining,
	}
	if err := p.deferrer.Defer(d); err != nil {
		res.Error = err.Error()
		ctx.Results[step.ID] = res
		return res
	}

	res.Deferred = true
	ctx.Results[step.ID] = res
	return res
}

// markShortCircuited 记录一个因上游失败而未执行的步骤。
func (p *Pipeline) markShortCircuited(ctx *Context, step domain.Step, reason string) StepResult {
	res := StepResult{
		StepID:         step.ID,
		StepType:       domain.StepType(step.Type),
		Channel:        step.Type,
		ShortCircuited: true,
		Error:          reason,
	}
	ctx.Results[step.ID] = res
	return res
}

// workflowName 返回上下文里的工作流名，缺省回退消息名。
func workflowName(ctx *Context) string {
	if ctx.Workflow != nil && ctx.Workflow.Name != "" {
		return ctx.Workflow.Name
	}
	return ctx.Msg.Name
}

// runStep 对单个步骤依次执行四个阶段，返回首个错误。
func (p *Pipeline) runStep(ctx *Context, step domain.Step) error {
	for _, s := range p.stages {
		if err := s.Apply(ctx, step); err != nil {
			return err
		}
	}
	return nil
}
