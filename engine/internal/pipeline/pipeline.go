package pipeline

import (
	"errors"
	"log/slog"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

// errSkipStep 是内部哨兵错误，表示该步骤被 filter 阶段跳过。
var errSkipStep = errors.New("pipeline: step skipped by filter")

// Pipeline 将 filter → transform → render → dispatch 四个阶段串成一条管线，
// 对工作流的每个投递步骤依次执行。
type Pipeline struct {
	stages []Stage
}

// New 创建默认管线（filter → transform → render → dispatch）。
// logger 为 nil 时使用默认 logger。
func New(logger *slog.Logger) *Pipeline {
	if logger == nil {
		logger = slog.Default()
	}
	return &Pipeline{
		stages: []Stage{
			NewFilterStage(),
			NewTransformStage(),
			NewRenderStage(),
			NewDispatchStage(logger),
		},
	}
}

// Run 对工作流的全部投递步骤执行管线；编排步骤（delay/digest）暂不处理。
// 返回每个步骤的结果（含被跳过与失败），结果顺序与投递步骤顺序一致。
func (p *Pipeline) Run(ctx *Context, wf domain.Workflow) []StepResult {
	var out []StepResult
	for _, step := range wf.DeliverySteps() {
		if err := p.runStep(ctx, step); err != nil {
			if !errors.Is(err, errSkipStep) {
				ctx.Results[step.ID] = StepResult{
					StepID:   step.ID,
					StepType: domain.StepType(step.Type),
					Channel:  step.Type,
					Error:    err.Error(),
				}
			}
			// errSkipStep 时结果已由 filter 阶段写入（标记 Skipped）。
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
