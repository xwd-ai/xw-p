// Package admin 提供引擎的运维接口：死信查询、重放与丢弃，渠道实现的
// 健康状态查询、启停与手动探测，以及投递指标的读取。
//
// 死信存在 Redis 里、由队列包写入，键布局与记录格式都由队列包定义，
// 所以这层只做「HTTP 入参 → 队列方法 → JSON 出参」，不自己拼键名。渠道
// 的启停与健康同理：状态存在注册表与健康记录器里，这里只做转发与入参校验。
// 指标是一次快照拷贝，读接口不会拖住投递路径。
// 接口默认关闭，需要时用 XW_ADMIN_ADDR 显式打开；性能采样端点（/debug/pprof/）
// 另由 XW_PPROF 打开，且默认只挂在回环地址上。
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/eventbus"
	"github.com/xwd-ai/xw-p/engine/internal/metrics"
	"github.com/xwd-ai/xw-p/engine/internal/provider"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
	"github.com/xwd-ai/xw-p/engine/internal/webhook"
)

// adminShutdownTimeout 是退出时等待在途管理请求收尾的上限。
// 管理接口只读多写少，给一两秒足够。
const adminShutdownTimeout = 2 * time.Second

// maxProviderIDs 是一次启停/探测请求里允许出现的标识数量。
//
// 注册表条目通常是个位数，上限给 64 已经足够宽松；再多的请求多半是把
// 别的接口的参数误传过来了，直接拒掉比逐个忽略更好定位问题。
const maxProviderIDs = 64

// maxRecentEvents 是一次事件流查询允许的最大条数，与环形缓冲的容量对齐。
const maxRecentEvents = eventbus.DefaultFeedCapacity

// DeadLetterStore 是管理接口依赖的存储能力，由 queue.ReliableConsumer 实现。
type DeadLetterStore interface {
	ListDeadLetters(ctx context.Context, q queue.DeadLetterQuery) (queue.DeadLetterPage, error)
	ReplayDeadLetters(ctx context.Context, ids []string) (queue.ReplayReport, error)
	DiscardDeadLetters(ctx context.Context, ids []string) (int, error)
}

// Probe 是就绪检查的一项依赖探测。
//
// 每项自带超时而不是统一给一个预算：Redis 一次 PING 的合理上限与一次外部
// 探测差着数量级，统一预算要么把快的拖慢、要么把慢的误判成失败。
type Probe struct {
	Name    string
	Timeout time.Duration
	Check   func(ctx context.Context) error
}

func (p Probe) timeout() time.Duration {
	if p.Timeout > 0 {
		return p.Timeout
	}
	return time.Second
}

// Server 承载管理接口的 HTTP 服务。
type Server struct {
	store  DeadLetterStore
	logger *slog.Logger
	// 渠道相关的三个依赖：注册表负责启停，健康记录器负责读状态，
	// 探测器负责手动探一次。未装配时相应的路由不注册（而非返回空列表），
	// 免得调用方把「没开这个能力」误读成「没有渠道」。
	providers *provider.Registry
	health    *provider.HealthTracker
	checker   *provider.Checker
	// metrics 是投递指标的读取入口，未装配时不注册路由。
	metrics *metrics.Registry
	// 回调审计与重放：审计表按事件标识回查每一次发送尝试，重放把留档的
	// 报文重新交给分发器。两者都没有时（未配置回调或审计关闭）不注册路由。
	webhookAudit    webhook.AuditStore
	webhookReplayer WebhookReplayer
	// feed 是事件总线上最近若干条事件的缓冲，供运维页面观察实时活动。
	feed *eventbus.Feed
	// probes 是就绪检查的依赖探测项；为空时就绪与存活等价。
	probes []Probe
	// profiling 是 /debug/pprof/ 的开关与采样率，默认全部关闭。
	profiling Profiling
}

// WebhookReplayer 是回调重放能力，由 webhook.Dispatcher 实现。
type WebhookReplayer interface {
	Replay(ids []string) (webhook.ReplayReport, error)
}

// New 创建管理接口服务。
func New(store DeadLetterStore, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{store: store, logger: logger}
}

// WithProviders 装配渠道启停与健康查询能力，返回自身便于链式调用。
func (s *Server) WithProviders(registry *provider.Registry, health *provider.HealthTracker, checker *provider.Checker) *Server {
	s.providers = registry
	s.health = health
	s.checker = checker
	return s
}

// WithMetrics 装配投递指标的读取与重置能力，返回自身便于链式调用。
func (s *Server) WithMetrics(reg *metrics.Registry) *Server {
	s.metrics = reg
	return s
}

// WithWebhookAudit 装配回调审计查询与重放能力。
//
// 两个依赖分开传：审计表可能独立于分发器存在（例如回调暂时停用，但历史
// 记录仍要能查），而重放必须有分发器。缺哪个就少注册哪条路由。
func (s *Server) WithWebhookAudit(store webhook.AuditStore, replayer WebhookReplayer) *Server {
	s.webhookAudit = store
	s.webhookReplayer = replayer
	return s
}

// WithEventFeed 装配最近事件流，返回自身便于链式调用。
func (s *Server) WithEventFeed(feed *eventbus.Feed) *Server {
	s.feed = feed
	return s
}

// WithProbes 装配就绪检查依赖，返回自身便于链式调用。
func (s *Server) WithProbes(probes ...Probe) *Server {
	for _, p := range probes {
		if p.Name == "" || p.Check == nil {
			continue
		}
		s.probes = append(s.probes, p)
	}
	return s
}

// WithProfiling 装配 /debug/pprof/ 采样端点，返回自身便于链式调用。
func (s *Server) WithProfiling(p Profiling) *Server {
	s.profiling = p
	return s
}

// Handler 返回路由。方法不匹配由路由层直接回 405。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// 存活与就绪不放在 /admin 前缀下：这两个是编排器与负载均衡打的，
	// 与「运维人员主动调用」的 /admin/* 不是同一类流量，分开更好配网络策略。
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	if s.metrics != nil {
		mux.HandleFunc("GET /metrics", s.handlePrometheus)
	}
	mux.HandleFunc("GET /admin/health", s.handleHealth)
	mux.HandleFunc("GET /admin/dead-letters", s.handleList)
	mux.HandleFunc("POST /admin/dead-letters/replay", s.handleReplay)
	mux.HandleFunc("POST /admin/dead-letters/discard", s.handleDiscard)

	if s.providers != nil && s.health != nil {
		mux.HandleFunc("GET /admin/providers", s.handleProviders)
		mux.HandleFunc("POST /admin/providers/enable", s.handleProviderToggle(true))
		mux.HandleFunc("POST /admin/providers/disable", s.handleProviderToggle(false))
	}
	if s.checker != nil {
		mux.HandleFunc("POST /admin/providers/probe", s.handleProbe)
	}
	if s.metrics != nil {
		mux.HandleFunc("GET /admin/metrics", s.handleMetrics)
		mux.HandleFunc("POST /admin/metrics/reset", s.handleMetricsReset)
	}
	if s.webhookAudit != nil {
		mux.HandleFunc("GET /admin/webhook/events", s.handleWebhookEvents)
	}
	if s.webhookReplayer != nil {
		mux.HandleFunc("POST /admin/webhook/replay", s.handleWebhookReplay)
	}
	if s.feed != nil {
		mux.HandleFunc("GET /admin/events/recent", s.handleRecentEvents)
	}
	if s.profiling.Enabled {
		registerProfiling(mux)
	}
	return mux
}

// Run 在 addr 上提供服务，ctx 取消时优雅关闭。
//
// 监听失败直接返回错误、由调用方记录：消费链路不该因为运维接口端口
// 被占用就整体退出。
func (s *Server) Run(ctx context.Context, addr string) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("管理接口监听失败: %w", err)
	}
	srv := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	// 采样率是进程级设置，在这里落地而不是在 Handler 里：Handler 会被测试
	// 反复调用，把全局状态改在构造路径上会让用例之间互相影响。
	applyProfilingRates(s.profiling)
	s.logger.Info("[engine] 管理接口已启动", "addr", ln.Addr().String())

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), adminShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
		s.logger.Info("[engine] 管理接口已关闭")
		return nil
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady 跑一遍依赖探测：全部通过 200，任一失败 503。
//
// 失败原因要写在响应里：503 本身只告诉编排器「别送流量」，运维还得知道
// 是 Redis 不通还是别的依赖没起来。错误文本来自驱动，可能包含地址，但管理
// 接口本身已经是受控入口。
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	type check struct {
		Name       string  `json:"name"`
		Status     string  `json:"status"`
		Error      string  `json:"error,omitempty"`
		DurationMS float64 `json:"durationMs"`
	}
	results := make([]check, 0, len(s.probes))
	healthy := true
	for _, probe := range s.probes {
		ctx, cancel := context.WithTimeout(r.Context(), probe.timeout())
		started := time.Now()
		err := probe.Check(ctx)
		cancel()
		item := check{
			Name:       probe.Name,
			Status:     "ok",
			DurationMS: roundMillis(time.Since(started)),
		}
		if err != nil {
			item.Status = "fail"
			item.Error = err.Error()
			healthy = false
		}
		results = append(results, item)
	}
	status := http.StatusOK
	summary := "ok"
	if !healthy {
		status = http.StatusServiceUnavailable
		summary = "degraded"
	}
	writeJSON(w, status, map[string]interface{}{"status": summary, "checks": results})
}

// handlePrometheus 输出 Prometheus 文本格式。
//
// 与 /admin/metrics 的 JSON 并存：页面与脚本读结构化数据，抓取端读文本，
// 两边共用同一份快照，不存在两个口径。
func (s *Server) handlePrometheus(w http.ResponseWriter, _ *http.Request) {
	body := s.metrics.PrometheusText()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, body)
}

func roundMillis(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	offset, err := intParam(params.Get("offset"), 0)
	if err != nil || offset < 0 {
		writeError(w, http.StatusBadRequest, "offset 必须是非负整数")
		return
	}
	limit, err := intParam(params.Get("limit"), queue.DefaultDeadLetterLimit)
	if err != nil || limit <= 0 {
		writeError(w, http.StatusBadRequest, "limit 必须是正整数")
		return
	}

	page, err := s.store.ListDeadLetters(r.Context(), queue.DeadLetterQuery{Offset: offset, Limit: limit})
	if err != nil {
		s.logger.Error("[engine] 读取死信失败", "error", err)
		writeError(w, http.StatusInternalServerError, "读取死信失败")
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request) {
	ids, ok := readIDs(w, r)
	if !ok {
		return
	}
	report, err := s.store.ReplayDeadLetters(r.Context(), ids)
	if err != nil {
		s.logger.Error("[engine] 重放死信失败", "error", err)
		writeError(w, http.StatusInternalServerError, "重放死信失败")
		return
	}
	s.logger.Info("[engine] 死信已重放",
		"requested", len(ids),
		"replayed", len(report.Replayed),
		"missing", len(report.Missing),
	)
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleDiscard(w http.ResponseWriter, r *http.Request) {
	ids, ok := readIDs(w, r)
	if !ok {
		return
	}
	removed, err := s.store.DiscardDeadLetters(r.Context(), ids)
	if err != nil {
		s.logger.Error("[engine] 丢弃死信失败", "error", err)
		writeError(w, http.StatusInternalServerError, "丢弃死信失败")
		return
	}
	s.logger.Info("[engine] 死信已丢弃", "requested", len(ids), "discarded", removed)
	writeJSON(w, http.StatusOK, map[string]int{"discarded": removed})
}

// readIDs 解析 {"ids": [...]} 请求体，出错时已写好响应。
func readIDs(w http.ResponseWriter, r *http.Request) ([]string, bool) {
	var body struct {
		IDs []string `json:"ids"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "请求体需为 {\"ids\": [...]} 形式的 JSON")
		return nil, false
	}
	if len(body.IDs) == 0 {
		writeError(w, http.StatusBadRequest, "ids 不能为空")
		return nil, false
	}
	if len(body.IDs) > queue.MaxReplayIDs {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("一次最多处理 %d 条死信", queue.MaxReplayIDs))
		return nil, false
	}
	return body.IDs, true
}

// handleProviders 返回渠道实现的健康快照与汇总计数。
//
// 快照包含从未被观测过的实现（状态 unknown）与已停用的实现，运维因此能看到
// 「有哪些通道、当前各自是什么状态」，而不只是「响应过的那些」。
func (s *Server) handleProviders(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"providers": s.health.Snapshot(),
		"summary":   s.health.Summary(),
	})
}

// handleProviderToggle 返回启停处理器：enabled 决定放回取用序列还是临时摘掉。
//
// 启用时清掉旧的失败计数：一个实现可能正是因为连续失败被停用的，保留计数
// 会让它刚回到取用序列就显示 down，直到下一次探测才翻过来。停用则保留最后
// 一次状态，运维还能看到它是因为什么被摘掉的。
func (s *Server) handleProviderToggle(enabled bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ids, ok := s.readProviderIDs(w, r, false)
		if !ok {
			return
		}

		updated := make([]string, 0, len(ids))
		missing := make([]string, 0)
		for _, id := range ids {
			if err := s.providers.SetEnabled(id, enabled); err != nil {
				if errors.Is(err, provider.ErrNotFound) {
					missing = append(missing, id)
					continue
				}
				s.logger.Error("[engine] 渠道实现启停失败",
					"id", id, "enabled", enabled, "error", err)
				writeError(w, http.StatusInternalServerError, "渠道实现启停失败")
				return
			}
			if enabled {
				s.health.Reset(id)
			}
			updated = append(updated, id)
		}

		action := "停用"
		if enabled {
			action = "启用"
		}
		s.logger.Info("[engine] 渠道实现已"+action,
			"providers", updated,
			"missing", missing,
		)

		if len(updated) == 0 {
			// 一个都没命中说明标识写错了：返回 200 会让调用方以为已经生效。
			writeError(w, http.StatusNotFound,
				"没有匹配的渠道实现: "+strings.Join(missing, ", "))
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"updated":   updated,
			"missing":   missing,
			"providers": s.health.Snapshot(),
			"summary":   s.health.Summary(),
		})
	}
}

// handleProbe 立即探测一批实现（请求体为空表示全部启用中的实现）。
//
// 结果里同时给出失败清单与更新后的快照：调用方既能知道哪条没探通，也能
// 直接拿快照刷新界面，不必再补一次 GET。
func (s *Server) handleProbe(w http.ResponseWriter, r *http.Request) {
	ids, ok := s.readProviderIDs(w, r, true)
	if !ok {
		return
	}
	if len(ids) == 0 {
		for _, entry := range s.providers.Entries() {
			if entry.Enabled {
				ids = append(ids, entry.ID)
			}
		}
	}

	type probeFailure struct {
		ID    string `json:"id"`
		Error string `json:"error"`
	}
	failed := make([]probeFailure, 0)
	probed := 0
	for _, id := range ids {
		if _, err := s.checker.Probe(r.Context(), id); err != nil {
			failed = append(failed, probeFailure{ID: id, Error: err.Error()})
			continue
		}
		probed++
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"probed":    probed,
		"failed":    failed,
		"providers": s.health.Snapshot(),
		"summary":   s.health.Summary(),
	})
}

// handleMetrics 返回本实例自启动以来的投递指标。
//
// 口径是单实例：进程重启即归零，跨实例汇总由抓取端按实例聚合。做成 JSON
// 而不是文本格式，是因为消费方是运维脚本与后台页面，需要结构化的渠道/实现
// 维度；同一份快照另有 /metrics 的文本形态给抓取端。
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.metrics.Snapshot())
}

// handleMetricsReset 清空计数并重新计时，返回重置后的空快照。
//
// 换完渠道配置后想看「改动之后的表现」，累计数回答不了这个问题；重置比重启
// 进程代价小得多。清理前的累计值不做保留，调用方需要留档应先取一次快照。
func (s *Server) handleMetricsReset(w http.ResponseWriter, _ *http.Request) {
	s.metrics.Reset()
	s.logger.Info("[engine] 投递指标已重置")
	writeJSON(w, http.StatusOK, s.metrics.Snapshot())
}

// readProviderIDs 解析 {"ids": [...]} 形式的请求体。
//
// 与死信接口共用同一个字段名，运维可以用同一套脚本调用。allowEmpty 为真时
// 空请求体或空列表表示「全部」（探测用），为假时视为参数错误（启停必须
// 指明对象，否则一次手滑就会把整条渠道关掉）。
func (s *Server) readProviderIDs(w http.ResponseWriter, r *http.Request, allowEmpty bool) ([]string, bool) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "读取请求体失败")
		return nil, false
	}
	var body struct {
		IDs []string `json:"ids"`
	}
	if len(strings.TrimSpace(string(raw))) > 0 {
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "请求体需为 {\"ids\": [...]} 形式的 JSON")
			return nil, false
		}
	}
	if len(body.IDs) == 0 {
		if allowEmpty {
			return nil, true
		}
		writeError(w, http.StatusBadRequest, "ids 不能为空")
		return nil, false
	}
	if len(body.IDs) > maxProviderIDs {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("一次最多处理 %d 个渠道实现", maxProviderIDs))
		return nil, false
	}
	return body.IDs, true
}

// handleWebhookEvents 返回一页回调审计记录。
//
// 过滤维度都是排查时手里真有的信息：事件标识（接收方报过来的）、事件类型
// （「哪些 delivery.failed 没发出去」）、状态（只看最终失败的）、时间区间。
func (s *Server) handleWebhookEvents(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	query := webhook.AuditQuery{
		EventID: params.Get("eventId"),
		Type:    params.Get("type"),
		Status:  params.Get("status"),
	}
	if query.Type != "" && !webhook.IsKnownEvent(query.Type) {
		writeError(w, http.StatusBadRequest,
			"未知事件类型，可选："+strings.Join(webhook.EventTypes(), ", "))
		return
	}
	switch query.Status {
	case "", webhook.AuditDelivered, webhook.AuditRetrying, webhook.AuditFailed:
	default:
		writeError(w, http.StatusBadRequest,
			"status 可选：delivered / retrying / failed")
		return
	}

	limit, err := intParam(params.Get("limit"), webhook.DefaultAuditLimit)
	if err != nil || limit <= 0 || limit > webhook.MaxAuditLimit {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("limit 必须是 1 到 %d 之间的整数", webhook.MaxAuditLimit))
		return
	}
	offset, err := intParam(params.Get("offset"), 0)
	if err != nil || offset < 0 {
		writeError(w, http.StatusBadRequest, "offset 必须是非负整数")
		return
	}
	if since, ok := parseTimeParam(params.Get("since")); !ok {
		writeError(w, http.StatusBadRequest, "since 需为 RFC3339 时间")
		return
	} else {
		query.Since = since
	}
	if until, ok := parseTimeParam(params.Get("until")); !ok {
		writeError(w, http.StatusBadRequest, "until 需为 RFC3339 时间")
		return
	} else {
		query.Until = until
	}
	query.Limit = limit
	query.Offset = offset

	entries, err := s.webhookAudit.List(query)
	if err != nil {
		s.logger.Error("[engine] 读取回调审计失败", "error", err)
		writeError(w, http.StatusInternalServerError, "读取回调审计失败")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"events": entries,
		"count":  len(entries),
	})
}

// handleWebhookReplay 重放一批已留档的回调事件。
//
// 结果按「已入队 / 正文缺失 / 已取消订阅 / 队列已满」四类分开报：
// 只回一句「已重放 3 条」会让调用方以为全部发出去了。
func (s *Server) handleWebhookReplay(w http.ResponseWriter, r *http.Request) {
	ids, ok := readIDs(w, r)
	if !ok {
		return
	}
	if len(ids) > webhook.DefaultReplayLimit {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("一次最多重放 %d 条事件", webhook.DefaultReplayLimit))
		return
	}
	report, err := s.webhookReplayer.Replay(ids)
	if err != nil {
		s.logger.Error("[engine] 重放回调事件失败", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, report)
}

// handleRecentEvents 返回最近若干条事件，最新在前。
//
// 数据来自总线上的一个环形缓冲，只覆盖最近几百条：这是「刚才发生了什么」
// 的视图，历史回查应当走审计表或执行明细。
func (s *Server) handleRecentEvents(w http.ResponseWriter, r *http.Request) {
	limit, err := intParam(r.URL.Query().Get("limit"), 50)
	if err != nil || limit <= 0 || limit > maxRecentEvents {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("limit 必须是 1 到 %d 之间的整数", maxRecentEvents))
		return
	}

	type eventItem struct {
		ID      string      `json:"id"`
		Topic   string      `json:"topic"`
		At      time.Time   `json:"at"`
		Payload interface{} `json:"payload,omitempty"`
	}
	messages := s.feed.Recent(limit)
	items := make([]eventItem, 0, len(messages))
	for _, msg := range messages {
		items = append(items, eventItem{
			ID:      msg.ID,
			Topic:   msg.Topic,
			At:      msg.At,
			Payload: msg.Payload,
		})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"events": items,
		"count":  len(items),
		"total":  s.feed.Total(),
	})
}

// parseTimeParam 解析可选的 RFC3339 时间参数。
//
// 返回 (零值, true) 表示没传；(零值, false) 表示写错了。空字符串与解析
// 失败必须区分开，否则一个拼错的时间会被当成「不过滤」。
func parseTimeParam(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, true
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func intParam(raw string, def int) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return def, nil
	}
	return strconv.Atoi(raw)
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
