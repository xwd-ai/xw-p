// Command engine 是 xw-notify 的通知执行引擎入口。
//
// 职责：消费消息队列、执行工作流求值、digest 聚合与 provider 分发。
// 当前阶段：从 Redis 队列可靠消费（取出即登记在途、处理成功才确认，
// 租约过期未确认的消息由回收流程重新入队），并发度由 XW_WORKERS 控制，
// 退出时等待在途消息处理完毕；消费到的消息按自带步骤快照执行整条工作流
// （filter → transform → render → dispatch，delay 步骤交给调度器续跑），
// 同时运行延迟调度器，按时唤醒 delay 步骤之后的续跑步骤，未到期任务
// 存放在 Redis 里，重启或换实例都能接上；每个渠道的投递结果写入执行明细，
// 配置了回调地址时再异步推给接入方；投递的成功/失败/超时与延迟在进程内累计，
// 经运维接口 /admin/metrics 读取；
// 另起一个本地管理接口，供运维查询、重放与丢弃死信。
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/redis/go-redis/v9"

	"github.com/xwd-ai/xw-p/engine/internal/admin"
	"github.com/xwd-ai/xw-p/engine/internal/config"
	"github.com/xwd-ai/xw-p/engine/internal/execution"
	"github.com/xwd-ai/xw-p/engine/internal/metrics"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/provider"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
	"github.com/xwd-ai/xw-p/engine/internal/runner"
	"github.com/xwd-ai/xw-p/engine/internal/schedule"
	"github.com/xwd-ai/xw-p/engine/internal/webhook"
	"github.com/xwd-ai/xw-p/engine/internal/worker"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumer := queue.NewReliableConsumer(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, cfg.QueueKey, cfg.ClaimLeaseOrDefault())
	defer consumer.Close()

	pingCtx, cancel := context.WithTimeout(ctx, cfg.PollTimeoutDuration())
	err := consumer.Ping(pingCtx)
	cancel()
	if err != nil {
		logger.Warn("[engine] Redis 连接检查失败，稍后在消费时重试", "redis", cfg.RedisAddr, "error", err)
	}

	logger.Info("[engine] 启动",
		"redis", cfg.RedisAddr,
		"queue", cfg.QueueKey,
		"pollTimeout", cfg.PollTimeoutDuration(),
		"claimLease", cfg.ClaimLeaseOrDefault(),
		"reclaimInterval", cfg.ReclaimInterval,
		"workers", cfg.WorkersOrDefault(),
		"prefetch", cfg.PrefetchOrDefault(),
		"shutdownGrace", cfg.ShutdownGraceOrDefault(),
		"delayStore", cfg.DelayStore,
		"delayPrefix", cfg.DelayPrefix,
		"delayLease", cfg.DelayLeaseOrDefault(),
		"adminAddr", cfg.AdminAddr,
	)

	// 聚合卡片存储：多实例共享走 Redis，Redis 不可用时回退进程内存
	// （单实例语义正确，多实例会各开一张卡，日志里明示这件事）。
	pipe, closeDigest, err := newPipeline(logger, cfg)
	if err != nil {
		// 渠道配置写错时直接退出：带着半套渠道继续跑，只会在投递那一刻
		// 才发现某个渠道没人接。
		logger.Error("[engine] 渠道实现装配失败", "error", err)
		os.Exit(1)
	}
	defer closeDigest()

	// 投递结果回写：引擎把每个渠道的成败与原因写进执行明细，API 的
	// GET /v1/executions 读同一份文件。写不了不至于让投递失败，故只告警。
	recorder, closeExecution := newExecutionRecorder(logger, cfg)
	defer closeExecution()

	// 投递结果回调：接入方不必轮询执行明细，每次执行结束后把各渠道结果
	// 推到它自己的端点。发送在独立 goroutine 里进行，不占用消费路径。
	callback, closeCallback := newCallbackDispatcher(logger, cfg)
	defer closeCallback()

	// 投递指标：进程内的成功/失败/超时计数与延迟分布，经运维接口读取。
	// 与执行明细同源，但口径不同——明细是逐条审计，指标是聚合观察。
	sink := metrics.New()

	// 延迟调度器：到期任务 → 管线续跑。delay 步骤到期后把剩余步骤跑完。
	// 未到期任务存在 Redis，重启或换实例都不会丢。
	scheduler, closeDelayStore := newDelayScheduler(logger, pipe, cfg, recorder, callback, sink)
	defer closeDelayStore()

	// 把调度器接成管线的延迟出口：没有这一句，delay 步骤会以「未配置调度器」
	// 失败——那正是「延迟看起来配了、实际直接失败」的隐蔽故障。
	pipe.WithDeferrer(pipeline.NewEnqueueDeferrer(scheduler))

	// 执行器：把队列消息还原成工作流并跑完步骤。工作流定义优先取消息自带
	// 的快照，快照缺失时回退到启动时加载的定义表。
	exec := runner.New(pipe, loadWorkflowSource(logger, cfg), logger).
		WithRecorder(recorder).
		WithNotifier(callback).
		WithMetrics(sink)

	var wg sync.WaitGroup

	// 渠道健康：投递结果自动记入健康记录器，周期探测补齐「这段时间没有消息
	// 经过」的实现。探测只建立连接、不真的投递，不会往群里或用户手机上发
	// 测试消息；关掉周期探测后，运维接口仍可手动触发一次。
	checker := provider.NewChecker(pipe.Registry(), pipe.Health(), logger).
		WithInterval(cfg.ProviderProbeIntervalOrDefault()).
		WithTimeout(cfg.ProviderProbeTimeoutOrDefault())
	if cfg.ProviderProbeEnabled() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			checker.Run(ctx)
		}()
		logger.Info("[engine] 渠道健康探测已启用",
			"interval", checker.Interval(),
			"timeout", checker.Timeout(),
			"downAfter", pipe.Health().DownAfter(),
		)
	} else {
		logger.Info("[engine] 渠道健康探测已关闭",
			"reason", "XW_PROVIDER_PROBE_INTERVAL<=0",
			"fallback", "仍按投递结果记录，可经运维接口手动探测",
		)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		scheduler.Run(ctx)
	}()

	// 在途消息回收：启动即扫描一轮，捡回上次崩溃时未确认的消息，
	// 之后周期扫描同集群其他实例异常退出留下的记录。
	if cfg.ReclaimInterval > 0 {
		wg.Add(1)
		reclaimer := worker.NewReclaimer(consumer, cfg.ReclaimInterval, logger)
		go func() {
			defer wg.Done()
			reclaimer.Run(ctx)
		}()
	} else {
		logger.Info("[engine] 在途消息回收已关闭", "reason", "XW_RECLAIM_INTERVAL<=0")
	}

	// 失败重试：处理失败的消息按指数退避重新投递，次数耗尽转死信。
	// 关闭时（XW_RETRY_MAX_ATTEMPTS<=0）退化为「留在在途记录等租约回收」。
	policy := cfg.RetryPolicyOrDefault()
	var retrier *queue.Retrier
	if policy.Enabled() {
		retrier = consumer.Retrier(policy)
		wg.Add(1)
		pumper := worker.NewRetryPumper(consumer, cfg.RetryInterval, logger)
		go func() {
			defer wg.Done()
			pumper.Run(ctx)
		}()
		logger.Info("[engine] 失败重试已启用",
			"maxAttempts", policy.MaxAttempts,
			"base", policy.Base,
			"max", policy.Max,
			"jitter", policy.Jitter,
			"stateTTL", policy.StateTTL(),
		)
	} else {
		logger.Info("[engine] 失败重试已关闭",
			"reason", "XW_RETRY_MAX_ATTEMPTS<=0",
			"fallback", "保留在途记录等待租约回收",
		)
	}

	// 运维接口：死信查询、重放与丢弃，以及渠道健康查询、启停与手动探测。
	// 死信的键布局与记录格式由队列包定义，这里只把队列方法挂到 HTTP 上，
	// 避免出现第二份「键名拼装」的实现。
	if cfg.AdminAddr != "" {
		wg.Add(1)
		adminServer := admin.New(consumer, logger).
			WithProviders(pipe.Registry(), pipe.Health(), checker).
			WithMetrics(sink)
		go func() {
			defer wg.Done()
			if err := adminServer.Run(ctx, cfg.AdminAddr); err != nil {
				logger.Error("[engine] 管理接口退出", "addr", cfg.AdminAddr, "error", err)
			}
		}()
	} else {
		logger.Info("[engine] 管理接口已关闭", "reason", "XW_ADMIN_ADDR=off")
	}

	// 并发消费：XW_WORKERS 个消费者共享同一队列，关闭时等手里那条跑完。
	pool := worker.NewPool(consumer, cfg.WorkersOrDefault(), cfg.PollTimeoutDuration(), logger).
		WithShutdownGrace(cfg.ShutdownGraceOrDefault()).
		WithPrefetch(cfg.PrefetchOrDefault()).
		WithExecutor(exec)
	if retrier != nil {
		pool.WithRetrier(retrier)
	}
	pool.Run(ctx)

	// 池子退出（ctx 取消）后等待调度器收尾，避免任务被中断在日志中途。
	wg.Wait()

	logMetricsSummary(logger, sink)
}

// logMetricsSummary 在退出时打一条指标汇总。
//
// 进程退出后内存计数随之消失，最后一条日志是这批数字唯一的落点：排查
// 「今天下午那波失败是从哪个渠道开始的」时，它比翻上百条 runner 日志快得多。
func logMetricsSummary(logger *slog.Logger, sink *metrics.Registry) {
	snap := sink.Snapshot()
	if snap.Total.Attempted == 0 {
		logger.Info("[engine] 投递指标累计：本次运行没有真实投递")
		return
	}
	attrs := []any{
		"attempted", snap.Total.Attempted,
		"sent", snap.Total.Sent,
		"failed", snap.Total.Failed,
		"timeouts", snap.Total.Timeouts,
		"retried", snap.Total.Retried,
		"fallback", snap.Total.Fallback,
		"errorRate", snap.Total.ErrorRate,
		"p50ms", snap.Total.Latency.P50MS,
		"p99ms", snap.Total.Latency.P99MS,
		"uptimeSeconds", snap.UptimeSeconds,
	}
	perChannel := make(map[string]int64, len(snap.Channels))
	for _, ch := range snap.Channels {
		perChannel[ch.Channel] = ch.Attempted
	}
	attrs = append(attrs, "byChannel", perChannel)
	logger.Info("[engine] 投递指标累计", attrs...)
}

// newDelayScheduler 组装延迟调度器：到期任务 → 解码续跑任务 → 执行剩余步骤。
//
// in_app 渠道写入默认收件箱存储，其余渠道走 console mock，
// 与本进程的投递语义保持一致。返回值里的函数用于释放存储连接。
func newDelayScheduler(logger *slog.Logger, pipe *pipeline.Pipeline, cfg config.Config, recorder *execution.Recorder, notifier runner.Notifier, sink *metrics.Registry) (*schedule.Scheduler, func()) {
	handler := func(ctx context.Context, item schedule.Item) error {
		deferral, results, err := pipe.Resume(item.Payload)
		if err != nil {
			return err
		}
		sent := 0
		for _, r := range results {
			if r.Sent {
				sent++
			}
			if r.Error != "" {
				logger.Warn("[engine] 续跑步骤失败",
					"workflow", deferral.WorkflowName,
					"stepId", r.StepID,
					"channel", string(r.Channel),
					"error", r.Error,
				)
			}
		}
		// 续跑同样是一次投递：不写执行明细的话，delay 之后的渠道在后台
		// 就查不到结果（触发时刻只有前半程的明细）。
		recorded := runner.RecordResults(recorder, logger, deferral.Message, results)
		// 指标同理：续跑的这段投递同样是「这条渠道发了一次」，不计数会让
		// 带 delay 的工作流在指标里少一截成功数。
		runner.RecordMetrics(sink, results)
		// 回调同理：接入方先前只拿到前半程的结果，续跑的这一段要补推。
		notified := runner.NotifyResults(notifier, deferral.Message, results)
		logger.Info("[engine] 延迟任务已续跑",
			"workflow", deferral.WorkflowName,
			"delayStep", deferral.StepID,
			"subscriberId", deferral.Message.SubscriberID,
			"steps", len(results),
			"sent", sent,
			"recorded", recorded,
			"notified", notified,
		)
		return nil
	}
	store, closeStore := newDelayStore(logger, cfg)
	return schedule.New(store, handler, logger,
		schedule.WithClaimLease(cfg.DelayLeaseOrDefault())), closeStore
}

// newExecutionRecorder 装配投递结果的执行明细回写。
//
// 三种情况：显式关闭（XW_EXECUTION_FILE=off）、文件打不开（目录只读、路径被
// 目录占位）、正常打开。前两种都返回 nil 记录器，投递照常进行——审计链路
// 出问题不该拦住消息本身，但必须在启动日志里说清楚，否则「执行明细是空的」
// 会被误当成「没有投递过」。
func newExecutionRecorder(logger *slog.Logger, cfg config.Config) (*execution.Recorder, func()) {
	if cfg.ExecutionDisabled() {
		logger.Info("[engine] 执行明细回写已关闭", "reason", "XW_EXECUTION_FILE=off")
		return nil, func() {}
	}

	store, err := execution.NewJSONLStore(cfg.ExecutionFile)
	if err != nil {
		logger.Error("[engine] 执行明细文件不可用，本次不回写投递结果",
			"file", cfg.ExecutionFile,
			"error", err,
		)
		return nil, func() {}
	}

	logger.Info("[engine] 投递结果将回写执行明细", "file", store.Path())
	return execution.NewRecorder(store, logger), func() {
		if err := store.Close(); err != nil {
			logger.Warn("[engine] 关闭执行明细文件失败", "file", store.Path(), "error", err)
		}
	}
}

// newCallbackDispatcher 装配投递结果的回调。
//
// 三种情况：未配置（回调关闭）、地址不合法（记错误并关闭）、正常装配。
// 地址写错不让进程起不来——回调是旁路，投递本身仍应继续；但必须在启动
// 日志里说清楚，否则「接入方一直收不到回调」会被当成引擎没发结果。
func newCallbackDispatcher(logger *slog.Logger, cfg config.Config) (*webhook.Dispatcher, func()) {
	if cfg.CallbackDisabled() {
		logger.Info("[engine] 投递结果回调未启用", "reason", "XW_CALLBACK_URL 为空")
		return nil, func() {}
	}

	target, err := webhook.ParseTarget(cfg.CallbackURL)
	if err != nil {
		logger.Error("[engine] 回调地址不合法，本次不回调", "error", err)
		return nil, func() {}
	}

	// 密钥配错（过短、过长）时不悄悄降级成不签名：接收方会开始拒绝所有
	// 回调，而引擎这边只看得到「回调失败」，排查会绕远路。
	signer, err := webhook.NewSigner(cfg.CallbackSecret)
	if err != nil {
		logger.Error("[engine] 回调签名密钥不合法，本次不回调", "error", err)
		return nil, func() {}
	}

	opts := webhook.Options{
		Workers:   cfg.CallbackWorkers,
		QueueSize: cfg.CallbackQueue,
		Timeout:   cfg.CallbackTimeout,
		Retry:     cfg.CallbackRetry(),
		Logger:    logger,
	}.Normalized()

	dispatcher := webhook.NewDispatcher(target, webhook.NewClient(opts.Timeout).WithSigner(signer), opts)
	if dispatcher == nil {
		return nil, func() {}
	}

	logger.Info("[engine] 投递结果将回调给接入方",
		"target", target.String(),
		"timeout", opts.Timeout,
		"workers", opts.Workers,
		"queue", opts.QueueSize,
		"signed", signer != nil,
		"retry", opts.Retry.String(),
	)

	return dispatcher, func() {
		// 退出时把队列里剩下的事件发完：这些回执对应的消息已经确认，
		// 丢掉就再也没有第二次机会。最多等到退出宽限，避免被不响应的
		// 接收方拖住整个进程。
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGraceOrDefault())
		defer cancel()
		stats := dispatcher.Stats()
		if err := dispatcher.Close(ctx); err != nil {
			logger.Warn("[engine] 回调分发器未能在宽限内排空",
				"error", err,
				"delivered", stats.Delivered,
				"failed", stats.Failed,
			)
			return
		}
		logger.Info("[engine] 回调分发器已关闭",
			"enqueued", stats.Enqueued,
			"delivered", stats.Delivered,
			"failed", stats.Failed,
			"retried", stats.Retried,
			"dropped", stats.Dropped,
		)
	}
}

// newDelayStore 选定延迟任务存储：默认 Redis ZSET（重启与多实例都不丢），
// 显式配置 XW_DELAY_STORE=memory 或 Redis 不可用时回退进程内存储。
//
// 回退会丢未到期任务，所以这里必须把选择结果打进启动日志：运维能一眼看出
// 当前实例的 delay 任务是「活得比进程久」还是「跟着进程一起走」。
func newDelayStore(logger *slog.Logger, cfg config.Config) (schedule.Store, func()) {
	if strings.EqualFold(strings.TrimSpace(cfg.DelayStore), "memory") {
		logger.Info("[engine] 延迟任务存储：进程内存（重启后未到期任务丢失）",
			"reason", "XW_DELAY_STORE=memory")
		return schedule.NewMemoryStore(), func() {}
	}

	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	store := schedule.NewRedisStoreWithPrefix(client, cfg.DelayPrefix).WithLogger(logger)

	pingCtx, cancel := context.WithTimeout(context.Background(), cfg.PollTimeoutDuration())
	defer cancel()
	if err := store.Ping(pingCtx); err != nil {
		logger.Warn("[engine] 延迟任务存储连接 Redis 失败，本次回退进程内存；重启后未到期任务会丢失",
			"redis", cfg.RedisAddr, "error", err)
		_ = client.Close()
		return schedule.NewMemoryStore(), func() {}
	}

	logger.Info("[engine] 延迟任务存储：Redis ZSET",
		"redis", cfg.RedisAddr,
		"dueKey", store.DueKey(),
		"itemsKey", store.ItemsKey(),
		"inflightKey", store.InflightKey(),
	)
	return store, func() { _ = client.Close() }
}

// loadWorkflowSource 加载工作流定义表，作为消息未携带步骤快照时的回退来源。
//
// 正常路径上消息自带快照，这个文件只影响历史消息能否执行：没配置就返回空表，
// 配置了但读不动则记错误日志后继续启动——执行引擎不该因为一份可选的定义
// 快照文件而整体起不来。
func loadWorkflowSource(logger *slog.Logger, cfg config.Config) runner.Source {
	if cfg.WorkflowsFile == "" {
		logger.Info("[engine] 未配置工作流定义文件，仅执行消息自带的步骤快照")
		return runner.NewStaticSource()
	}

	source, found, err := runner.LoadWorkflowsFile(cfg.WorkflowsFile)
	if err != nil {
		logger.Error("[engine] 工作流定义文件加载失败，仅执行消息自带的步骤快照",
			"file", cfg.WorkflowsFile, "error", err)
		return runner.NewStaticSource()
	}
	if !found {
		logger.Warn("[engine] 工作流定义文件不存在，仅执行消息自带的步骤快照",
			"file", cfg.WorkflowsFile)
		return source
	}
	logger.Info("[engine] 工作流定义文件已加载",
		"file", cfg.WorkflowsFile, "workflows", source.Len())
	return source
}

// newPipeline 组装执行管线并选定聚合卡片存储，返回管线与资源释放函数。
//
// 渠道实现由 buildRegistry 决定：未配置 XW_PROVIDERS_FILE 时用内置的 console
// 实现，配置后按文件内容装配。
//
// 默认使用 Redis 存储：多个引擎实例共享同一张聚合卡片，计数走 HINCRBY，
// 窗口靠键的 TTL 收敛。显式配置 XW_DIGEST_STORE=memory 或 Redis 不可用时
// 回退进程内存储——那意味着多实例会各开一张卡，只在单实例部署下成立。
func newPipeline(logger *slog.Logger, cfg config.Config) (*pipeline.Pipeline, func(), error) {
	registry, retries, err := buildRegistry(logger, cfg)
	if err != nil {
		return nil, nil, err
	}
	pipe := pipeline.NewWithRegistry(logger, registry).
		WithTimeouts(timeoutPolicy(cfg)).
		WithProviderRetries(retries)

	entries := registry.Entries()
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		ids = append(ids, entry.ID)
	}
	logger.Info("[engine] 渠道实现注册表",
		"entries", len(ids),
		"providers", ids,
		"channels", registry.Channels(),
	)
	if chains := fallbackChains(registry); len(chains) > 0 {
		// 同一渠道登记了多个实现时，启动日志把取用顺序写清楚：
		// 第一个是默认实现，其后依次是失败时回退的备选。
		logger.Info("[engine] 渠道取用序列（主备）", "chains", chains)
	}
	if retries.Enabled() {
		// 重试是按渠道配的：把默认项与各渠道的实际参数都打出来，
		// 排查「为什么这里重试了三次那里只打一次」时不必回头翻配置文件。
		logger.Info("[engine] 渠道投递重试策略", "retries", retries.String())
	} else {
		logger.Info("[engine] 渠道投递重试未启用：失败即按主备切换")
	}
	logger.Info("[engine] 执行超时预算",
		"step", pipe.Timeouts().Step,
		"workflow", pipe.Timeouts().Workflow,
	)

	if strings.EqualFold(strings.TrimSpace(cfg.DigestStore), "memory") {
		logger.Info("[engine] 聚合卡片存储：进程内存（单实例语义）")
		return pipe, func() {}, nil
	}

	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	pingCtx, cancel := context.WithTimeout(context.Background(), cfg.PollTimeoutDuration())
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		logger.Warn("[engine] 聚合卡片存储连接 Redis 失败，本次回退进程内存；多实例下计数会各算各的",
			"redis", cfg.RedisAddr, "error", err)
		_ = client.Close()
		return pipe, func() {}, nil
	}

	store := pipeline.NewRedisDigestStoreWithPrefix(client, cfg.DigestPrefix)
	pipe.WithDigestStore(store)
	logger.Info("[engine] 聚合卡片存储：Redis", "redis", cfg.RedisAddr, "prefix", store.Prefix())
	return pipe, func() { _ = client.Close() }, nil
}

// fallbackChains 汇总每个渠道的取用序列（标识按优先级从高到低），
// 只保留存在备选的渠道——单实现渠道没有主备关系可看。
func fallbackChains(registry *pipeline.ProviderRegistry) map[string][]string {
	out := make(map[string][]string)
	for _, ch := range registry.Channels() {
		candidates := registry.Candidates(ch)
		if len(candidates) < 2 {
			continue
		}
		ids := make([]string, 0, len(candidates))
		for _, entry := range candidates {
			ids = append(ids, entry.ID)
		}
		out[string(ch)] = ids
	}
	return out
}

// buildRegistry 组装渠道实现注册表，并返回配置里的渠道级重试策略。
//
// 两种来源：未配置 XW_PROVIDERS_FILE 时使用内置的五个 console 实现（本地
// 跑通链路用，不重试）；配置了文件则严格按文件装配——文件解析失败、校验
// 不通过或实现构造失败都让进程起不来，避免「配置写错了但看起来在跑」。文件
// 里声明但实现尚未接入的种类跳过并逐条告警，装配结果里仍缺实现的渠道也一并
// 列出：那些渠道在运行期会退化为分发阶段的 mock 打印。
func buildRegistry(logger *slog.Logger, cfg config.Config) (*pipeline.ProviderRegistry, pipeline.RetryTable, error) {
	if cfg.ProvidersFile == "" {
		return pipeline.DefaultProviderRegistry(logger), pipeline.DefaultRetryTable(), nil
	}

	set, err := provider.LoadSet(cfg.ProvidersFile)
	if err != nil {
		return nil, pipeline.DefaultRetryTable(), err
	}

	registry := pipeline.NewProviderRegistry()
	report, err := set.Bind(registry, pipeline.ProviderFactory(logger))
	if err != nil {
		return nil, pipeline.DefaultRetryTable(), err
	}
	for _, skipped := range report.Skipped {
		logger.Warn("[engine] 渠道实现尚未接入，配置已跳过",
			"id", skipped.ID,
			"channel", skipped.Channel,
			"kind", skipped.Kind,
			"reason", skipped.Reason,
		)
	}
	if len(report.Missing) > 0 {
		logger.Warn("[engine] 以下渠道没有启用的实现，投递将回退为 mock 打印",
			"channels", report.Missing)
	}
	logger.Info("[engine] 渠道配置已加载",
		"file", set.Source(),
		"providers", set.Len(),
		"bound", len(report.Bound),
		"skipped", len(report.Skipped),
		"configured", set.String(),
	)
	return registry, set.Retries(), nil
}

// timeoutPolicy 组装超时策略：环境变量未配置时沿用管线默认值。
func timeoutPolicy(cfg config.Config) pipeline.TimeoutPolicy {
	policy := pipeline.DefaultTimeoutPolicy()
	if cfg.StepTimeout > 0 {
		policy.Step = cfg.StepTimeout
	}
	if cfg.WorkflowTimeout > 0 {
		policy.Workflow = cfg.WorkflowTimeout
	}
	return policy
}
