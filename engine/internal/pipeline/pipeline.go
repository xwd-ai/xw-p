package pipeline

import (
	"errors"
	"log/slog"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// errSkipStep 是内部哨兵错误，表示该步骤被 filter 阶段或条件分支跳过。
var errSkipStep = errors.New("pipeline: step skipped by filter or condition")

// Pipeline 将 filter → transform → render → dispatch 四个阶段串成一条管线，
// 对工作流的每个投递步骤依次执行。
type Pipeline struct {
	stages []Stage
}

// New 创建默认管线（filter → transform → render → dispatch）。
// logger 为 nil 时使用默认 logger。
//
// 默认注册 email / sms / push 三个渠道的 console mock provider；其余渠道
// （in-app/chat）尚未落地 provider，dispatch 阶段对它们保持 mock 打印。
// 后续渠道落地时在 NewWithRegistry 中追加注册即可。
func New(logger *slog.Logger) *Pipeline {
	registry := DefaultProviderRegistry(logger)
	return NewWithRegistry(logger, registry)
}

// DefaultProviderRegistry 构造默认 provider 注册表：email / sms / push 三个
// 已落地渠道各自注册 console mock 实现。
func DefaultProviderRegistry(logger *slog.Logger) *ProviderRegistry {
	registry := NewProviderRegistry()
	registry.Register(NewConsoleEmailProvider(logger))
	registry.Register(NewConsoleSMSProvider(logger))
	registry.Register(NewConsolePushProvider(logger))
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
	return &Pipeline{
		stages: []Stage{
			NewFilterStage(),
			NewBranchStage(),
			NewTransformStage(),
			NewRenderStage(),
			NewDispatchStage(logger, registry),
		},
	}
}

// Run 对工作流的全部投递步骤执行管线；编排步骤（delay/digest）暂不处理。
// 返回每个步骤的结果（含被跳过与失败），结果顺序与投递步骤顺序一致。
//
// 失败短路（fail-fast）：某个投递步骤在管线中真实失败（非 filter/条件分支
// 跳过）时，后续步骤不再执行，而是标记 ShortCircuited 并记录上游失败原因。
func (p *Pipeline) Run(ctx *Context, wf domain.Workflow) []StepResult {
	var out []StepResult
	var shortCircuit *StepResult // 记录首个真实失败，用于短路后续步骤
	for _, step := range wf.DeliverySteps() {
		// 上游已失败：当前步骤直接标记短路，不再进入管线。
		if shortCircuit != nil {
			ctx.Results[step.ID] = StepResult{
				StepID:         step.ID,
				StepType:       domain.StepType(step.Type),
				Channel:        step.Type,
				ShortCircuited: true,
				Error:          shortCircuit.Error,
			}
			out = append(out, ctx.Results[step.ID])
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

// runStep 对单个步骤依次执行四个阶段，返回首个错误。
func (p *Pipeline) runStep(ctx *Context, step domain.Step) error {
	for _, s := range p.stages {
		if err := s.Apply(ctx, step); err != nil {
			return err
		}
	}
	return nil
}
