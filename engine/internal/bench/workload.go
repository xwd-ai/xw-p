package bench

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// workloadName 是压测使用的工作流名：与预置的社区互动工作流同名，
// 消息体因此和线上触发出来的消息长得一样。
const workloadName = "post_liked"

// Workflow 返回压测用的工作流：同一篇帖子的点赞先聚合成一张卡片，
// 再分发站内信与推送。选这条链路的理由是它覆盖了两类路径——
// 编排步骤（digest 的聚合与原地更新）和投递步骤（provider 分发）。
func Workflow() domain.Workflow {
	return domain.Workflow{
		Name:    workloadName,
		Version: 1,
		Active:  true,
		Steps: []domain.Step{
			{
				ID:   "step-1",
				Type: domain.ChannelDigest,
				Name: "点赞聚合",
				Config: map[string]any{
					"key":      "post_id",
					"window":   "10m",
					"title":    "你的帖子有新互动",
					"template": "{{ digest.actor }} 等 {{ digest.count }} 人赞了你的帖子",
				},
			},
			{
				ID:   "step-2",
				Type: domain.ChannelInApp,
				Name: "站内信",
				Config: map[string]any{
					"depends_on": []any{"step-1"},
					"template":   "{{ payload.actor }} 赞了你的帖子",
				},
			},
			{
				ID:   "step-3",
				Type: domain.ChannelPush,
				Name: "推送",
				Config: map[string]any{
					"depends_on": []any{"step-1"},
					"template":   "{{ payload.actor }} 赞了你的帖子",
				},
			},
		},
	}
}

// Snapshot 把压测工作流转成随消息下发的步骤快照。
func Snapshot() *queue.WorkflowSnapshot {
	wf := Workflow()
	steps := make([]queue.StepSpec, 0, len(wf.Steps))
	for _, s := range wf.Steps {
		steps = append(steps, queue.StepSpec{
			ID:     s.ID,
			Type:   string(s.Type),
			Name:   s.Name,
			Config: s.Config,
		})
	}
	return &queue.WorkflowSnapshot{Name: wf.Name, Version: wf.Version, Steps: steps}
}

// QuietLogger 返回丢弃输出的 logger：压测时每条消息都会经过 provider，
// 把 console mock 的日志真的打到终端会让测量结果被 I/O 主导。
func QuietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// DecodeHandler 只解码消息并可选地模拟一段处理耗时，用于单独观察队列机制的开销。
func DecodeHandler(work time.Duration) Handler {
	return func(ctx context.Context, raw []byte) error {
		if _, err := queue.Decode(raw); err != nil {
			return err
		}
		if work > 0 {
			timer := time.NewTimer(work)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
		return nil
	}
}

// PipelineHandler 把消息真正喂给执行管线：解码 → 还原工作流 → 跑步骤。
//
// 收件箱用内存实现，避免压测把磁盘写满、也避免 JSONL 的顺序写主导测量；
// 要观察真实持久化路径的开销，应当把这两个 mock 换成线上实现再跑。
func PipelineHandler(pipe *pipeline.Pipeline, wf domain.Workflow) Handler {
	return func(ctx context.Context, raw []byte) error {
		msg, err := queue.Decode(raw)
		if err != nil {
			return err
		}
		pc := pipeline.NewContext(msg)
		pc.Ctx = ctx
		for _, r := range pipe.Run(pc, wf) {
			if r.Error != "" {
				return errors.New(r.Error)
			}
		}
		return nil
	}
}

// NewPipeline 构造压测用管线：内存收件箱 + 静默 logger，其余与引擎默认一致。
func NewPipeline() *pipeline.Pipeline {
	logger := QuietLogger()
	registry := pipeline.NewProviderRegistry()
	registry.Register(pipeline.NewInAppProvider(logger, pipeline.NewMemoryInboxStore()))
	registry.Register(pipeline.NewConsoleEmailProvider(logger))
	registry.Register(pipeline.NewConsoleSMSProvider(logger))
	registry.Register(pipeline.NewConsolePushProvider(logger))
	registry.Register(pipeline.NewConsoleChatProvider(logger))
	return pipeline.NewWithRegistry(logger, registry)
}
