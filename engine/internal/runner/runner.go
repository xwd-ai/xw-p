// Package runner 把队列消息接到执行管线。
//
// 职责边界：
//   - 决定这条消息该执行哪份步骤定义（消息自带的快照优先，其次按名称回退查找）；
//   - 调用 pipeline 执行，并把结果汇总成一条可读的执行报告；
//   - 区分「不可修复」与「暂时性」失败：前者记日志后由消费方确认丢弃，后者
//     交由消费方保留在途记录等待重投。
//
// 管线本身不认识队列，队列也不认识管线；本包是两者之间的粘合层，
// 把「取步骤 → 执行 → 记账」的决策集中在一处，消费循环保持极薄。
package runner

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
	"github.com/cdgjhg/xw-p/engine/internal/pipeline"
	"github.com/cdgjhg/xw-p/engine/internal/queue"
)

// Source 按名称提供工作流定义，用于消息未携带步骤快照时的回退查找。
type Source interface {
	Lookup(name string) (domain.Workflow, bool)
}

// Report 是一次消息执行的汇总，供日志与测试观察。
type Report struct {
	// Workflow 是实际执行的工作流名。
	Workflow string
	// TriggerID / SubscriberID 定位这次投递。
	TriggerID    string
	SubscriberID string
	// Version 是步骤定义的版本（快照未带版本时为 0）。
	Version int
	// FromSnapshot 表示步骤取自消息自带快照（否则来自 Source 回退查找）。
	FromSnapshot bool
	// Steps / Sent / Failed / Skipped / ShortCircuited 是各步骤结果的计数。
	Steps          int
	Sent           int
	Failed         int
	Skipped        int
	ShortCircuited int
	// Deferred 表示本次执行停在 delay 步骤，剩余步骤已登记为延迟续跑。
	Deferred bool
	// Rejected 非空表示工作流步骤校验不通过，整条消息未执行任何投递。
	Rejected string
	// Duration 是这次执行耗时。
	Duration time.Duration
}

// Runner 按消息执行工作流步骤。
type Runner struct {
	pipe   *pipeline.Pipeline
	source Source
	logger *slog.Logger
}

// New 创建 Runner。source 可为 nil（只接受自带步骤快照的消息）；
// logger 为 nil 时使用默认 logger。
func New(pipe *pipeline.Pipeline, source Source, logger *slog.Logger) *Runner {
	if logger == nil {
		logger = slog.Default()
	}
	return &Runner{pipe: pipe, source: source, logger: logger}
}

// Execute 是消费侧的执行入口。
//
// 返回非空 error 表示消息应当保留在途记录、等待重投；不可修复的问题
// （工作流不存在、步骤校验不通过）在内部记日志后返回 nil —— 这类消息
// 重投多少次结果都一样，不该在队列里无限循环。
func (r *Runner) Execute(msg queue.QueuedMessage) error {
	_, err := r.Run(msg)
	return err
}

// Run 执行一条消息并返回执行报告。
func (r *Runner) Run(msg queue.QueuedMessage) (Report, error) {
	start := time.Now()
	rep := Report{
		Workflow:     msg.Name,
		TriggerID:    msg.TriggerID,
		SubscriberID: msg.SubscriberID,
	}

	wf, fromSnapshot, err := r.resolve(msg)
	if err != nil {
		r.logger.Error("[runner] 消息没有可执行的步骤定义，确认后丢弃",
			"workflow", msg.Name,
			"triggerId", msg.TriggerID,
			"subscriberId", msg.SubscriberID,
			"error", err,
		)
		rep.Duration = time.Since(start)
		return rep, err
	}
	rep.FromSnapshot = fromSnapshot
	rep.Version = wf.Version

	results := r.pipe.Run(pipeline.NewContext(msg), wf)
	rep.summarize(results)
	rep.Duration = time.Since(start)

	if rep.Rejected != "" {
		r.logger.Error("[runner] 工作流步骤校验不通过，本次未投递",
			"workflow", rep.Workflow,
			"triggerId", rep.TriggerID,
			"reason", rep.Rejected,
		)
		return rep, fmt.Errorf("%w: 工作流 %s 校验不通过：%s",
			queue.ErrUndeliverable, rep.Workflow, rep.Rejected)
	}

	r.logReport(&rep)
	return rep, nil
}

// resolve 选定本次要执行的步骤定义：自带快照优先，其次按名称回退查找。
//
// 快照优先是刻意的：消息入队那一刻的模板形态才是触发方期望的形态；
// 回退查找只服务于没有快照的消息（历史消息、手工构造的消息、集成测试）。
func (r *Runner) resolve(msg queue.QueuedMessage) (domain.Workflow, bool, error) {
	if wf, ok := msg.InlineWorkflow(); ok {
		return wf, true, nil
	}
	if r.source != nil {
		if wf, ok := r.source.Lookup(msg.Name); ok {
			return wf, false, nil
		}
	}
	return domain.Workflow{}, false, fmt.Errorf("%w: 工作流 %s 不存在，且消息未携带步骤定义",
		queue.ErrUndeliverable, msg.Name)
}

// summarize 把步骤结果折算进报告。
func (rep *Report) summarize(results []pipeline.StepResult) {
	rep.Steps = len(results)
	for _, res := range results {
		switch {
		case res.StepID == pipeline.ValidationStepID:
			// 整份工作流没通过校验：一条投递都没发出去。
			rep.Rejected = res.Error
		case res.ShortCircuited:
			rep.ShortCircuited++
		case res.Sent:
			rep.Sent++
		case res.Error != "":
			rep.Failed++
		default:
			rep.Skipped++
		}
		if res.Deferred {
			rep.Deferred = true
		}
	}
}

// logReport 输出一条执行汇总；有失败步骤时降级为 Warn，
// 便于按 triggerId 回查是哪个渠道出的问题。
func (r *Runner) logReport(rep *Report) {
	attrs := []any{
		"workflow", rep.Workflow,
		"triggerId", rep.TriggerID,
		"subscriberId", rep.SubscriberID,
		"source", sourceLabel(rep.FromSnapshot),
		"steps", rep.Steps,
		"sent", rep.Sent,
		"failed", rep.Failed,
		"skipped", rep.Skipped,
		"shortCircuited", rep.ShortCircuited,
		"deferred", rep.Deferred,
		"duration", rep.Duration,
	}
	if rep.Failed > 0 || rep.ShortCircuited > 0 {
		r.logger.Warn("[runner] 工作流执行完成，部分步骤未成功", attrs...)
		return
	}
	r.logger.Info("[runner] 工作流执行完成", attrs...)
}

// sourceLabel 把步骤来源转成日志里好认的短标签。
func sourceLabel(fromSnapshot bool) string {
	if fromSnapshot {
		return "message"
	}
	return "lookup"
}
