package pipeline

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
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
	// timeouts 是单步与整体执行的超时策略。
	timeouts TimeoutPolicy

	// 以下为编排步骤（digest）复用的阶段与依赖，单独持有引用避免重复构造。
	logger    *slog.Logger
	registry  *ProviderRegistry
	dispatch  *dispatchStage
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
	dispatch := NewDispatchStage(logger, registry).(*dispatchStage)
	return &Pipeline{
		stages: []Stage{
			filter,
			NewBranchStage(),
			transform,
			renderer,
			dispatch,
		},
		logger:    logger,
		registry:  registry,
		dispatch:  dispatch,
		filter:    filter,
		transform: transform,
		renderer:  renderer,
		digests:   NewMemoryDigestStore(),
		now:       time.Now,
		timeouts:  DefaultTimeoutPolicy(),
	}
}

// WithProviderRetries 注入渠道级重试策略，返回管线自身便于链式装配。
//
// 策略按渠道生效，作用于回退链上「同一个实现的失败重试」，与队列层的
// 消息重试互不影响：前者是「对端抖了一下」，后者是「这条消息没处理成」。
func (p *Pipeline) WithProviderRetries(t RetryTable) *Pipeline {
	if p.dispatch != nil {
		p.dispatch.failover.WithRetries(t)
	}
	return p
}

// ProviderRetries 返回分发阶段当前生效的重试策略表。
func (p *Pipeline) ProviderRetries() RetryTable {
	if p.dispatch == nil {
		return DefaultRetryTable()
	}
	return p.dispatch.failover.Retries()
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

// Registry 返回管线使用的渠道实现注册表。
//
// 启动装配处据此打印「哪些渠道用了哪个实现」，运行期也可以通过它对单个
// 实现启停——投递阶段每次取用都读注册表，切换无需重建管线。
func (p *Pipeline) Registry() *ProviderRegistry {
	return p.registry
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

// ValidationStepID 是工作流校验失败时占位的结果步骤 id：
// 校验问题属于整个工作流而非某个步骤（例如环），没有可归属的步骤。
// 调用方（如执行汇总）据此区分「某个步骤失败」与「整条工作流被拒绝」。
const ValidationStepID = "__workflow__"

// Run 校验工作流步骤的依赖图，按依赖先后排序后执行管线。
//
// 步骤顺序由编排顺序与 depends_on 共同决定：声明了依赖的步骤排在其依赖之后，
// 未声明依赖的步骤保持原有顺序。校验失败（重复 id、悬空依赖、依赖成环等）时
// 不执行任何投递，直接返回一条带错误的结果，避免把畸形工作流发出去。
//
// 遇到 delay 步骤时提交延迟续跑并返回，其后的步骤留到到期后由调度器续跑。
// 返回每个已执行步骤的结果，顺序与执行顺序一致。
func (p *Pipeline) Run(ctx *Context, wf domain.Workflow) []StepResult {
	steps, err := domain.OrderSteps(wf.Steps)
	if err != nil {
		p.logger.Warn("[pipeline] 工作流校验失败，放弃本次执行",
			"workflow", wf.Name,
			"triggerId", ctx.Msg.TriggerID,
			"error", err,
		)
		return []StepResult{{
			StepID: ValidationStepID,
			Error:  err.Error(),
		}}
	}
	return p.RunSteps(ctx, steps)
}

// RunSteps 是 Run 的可续跑核心：对给定的步骤序列执行，遇到 delay 步骤即
// 停止并把剩余步骤登记为延迟续跑任务。
//
// 传入的步骤需已通过 domain.ValidateSteps 校验并完成依赖排序（Run 会做这件事）；
// 续跑路径沿用的是登记时已排好序的剩余步骤，因此这里不再重复校验。
//
// 失败短路（fail-fast）：某个投递步骤真实失败（非 filter/条件分支跳过）时，
// 后续步骤不再执行，而是标记 ShortCircuited 并记录上游失败原因；
// delay 步骤自身失败（时长非法 / 调度器不可用）同样短路后续步骤。
//
// 超时：首次进入时按策略设定本次执行的整体预算，每个步骤在「自身超时」与
// 「整体剩余预算」中较小的那个内执行；超时同失败一样短路后续步骤，后续步骤
// 标记为短路而不是继续尝试——预算已经用完，再试只会拖长整条链路的占用时间。
func (p *Pipeline) RunSteps(ctx *Context, steps []domain.Step) []StepResult {
	ctx.StartBudget(p.timeouts.Workflow, p.now())

	var out []StepResult
	var blockedReason string // 非空表示后续步骤不再执行：上游失败或整体预算耗尽
	for i, step := range steps {
		if blockedReason != "" {
			out = append(out, p.markShortCircuited(ctx, step, blockedReason))
			continue
		}

		// 整体预算已耗尽：剩余步骤一律不再尝试，避免在超时之后继续投递。
		if remaining, limited := ctx.RemainingBudget(p.now()); limited && remaining <= 0 {
			blockedReason = fmt.Sprintf("%s：%s 的执行预算已耗尽，剩余步骤未执行",
				ErrWorkflowTimeout.Error(), p.timeouts.Workflow)
			out = append(out, p.markShortCircuited(ctx, step, blockedReason))
			continue
		}

		// delay 步骤：登记剩余步骤的延迟续跑，并结束本轮执行。
		if step.Type == domain.ChannelDelay {
			res := p.handleDelay(ctx, step, steps[i+1:])
			out = append(out, res)
			if res.Error != "" {
				// 调度失败：后续步骤由下一轮迭代标记为短路。
				blockedReason = res.Error
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
			res, err := p.executeStep(ctx, step, p.digestExec)
			out = append(out, res)
			if err != nil {
				blockedReason = res.Error
			}
			continue
		}

		res, err := p.executeStep(ctx, step, p.runStep)
		out = append(out, res)
		if err != nil && !errors.Is(err, errSkipStep) {
			// 真实失败（含超时）：记录失败结果并作为短路源。
			blockedReason = res.Error
		}
	}
	return out
}

// executeStep 在超时预算内执行一个投递步骤，并把失败/超时写进结果。
//
// fn 决定走常规四阶段还是 digest 编排；两路共用同一套预算、fork 与结果合并逻辑。
// 返回的 error 用于调用方判断是否需要短路后续步骤（errSkipStep 不算失败）。
func (p *Pipeline) executeStep(
	ctx *Context,
	step domain.Step,
	fn func(*Context, domain.Step) error,
) (StepResult, error) {
	limit, overall, budgetErr := p.stepBudget(ctx, step)
	if budgetErr != nil {
		res := StepResult{
			StepID:   step.ID,
			StepType: domain.StepType(step.Type),
			Channel:  step.Type,
			Error:    budgetErr.Error(),
			TimedOut: errors.Is(budgetErr, ErrWorkflowTimeout),
		}
		ctx.Results[step.ID] = res
		return res, budgetErr
	}

	runErr := p.execStep(ctx, step, limit, overall, func(child *Context) error {
		return fn(child, step)
	})

	res, ok := ctx.Results[step.ID]
	if !ok {
		// 阶段未留下结果（例如超时、panic）：补一条以步骤自身信息为准的结果。
		res = StepResult{StepID: step.ID, StepType: domain.StepType(step.Type), Channel: step.Type}
	}
	res.StepID = step.ID
	if res.StepType == "" {
		res.StepType = domain.StepType(step.Type)
	}
	if res.Channel == "" {
		res.Channel = step.Type
	}

	if runErr != nil && !errors.Is(runErr, errSkipStep) {
		res.Error = runErr.Error()
		res.TimedOut = errors.Is(runErr, ErrStepTimeout) || errors.Is(runErr, ErrWorkflowTimeout)
	}
	ctx.Results[step.ID] = res
	return res, runErr
}

// digestExec 把 digest 编排的结果适配为 error：超时与失败都需要短路后续步骤。
func (p *Pipeline) digestExec(ctx *Context, step domain.Step) error {
	if res := p.handleDigest(ctx, step); res.Error != "" {
		return errors.New(res.Error)
	}
	return nil
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
