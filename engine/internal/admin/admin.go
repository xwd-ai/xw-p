// Package admin 提供引擎的运维接口：死信查询、重放与丢弃。
//
// 死信存在 Redis 里、由队列包写入，键布局与记录格式都由队列包定义，
// 所以这层只做「HTTP 入参 → 队列方法 → JSON 出参」，不自己拼键名。
// 接口默认关闭，需要时用 XW_ADMIN_ADDR 显式打开。
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// adminShutdownTimeout 是退出时等待在途管理请求收尾的上限。
// 管理接口只读多写少，给一两秒足够。
const adminShutdownTimeout = 2 * time.Second

// DeadLetterStore 是管理接口依赖的存储能力，由 queue.ReliableConsumer 实现。
type DeadLetterStore interface {
	ListDeadLetters(ctx context.Context, q queue.DeadLetterQuery) (queue.DeadLetterPage, error)
	ReplayDeadLetters(ctx context.Context, ids []string) (queue.ReplayReport, error)
	DiscardDeadLetters(ctx context.Context, ids []string) (int, error)
}

// Server 承载管理接口的 HTTP 服务。
type Server struct {
	store  DeadLetterStore
	logger *slog.Logger
}

// New 创建管理接口服务。
func New(store DeadLetterStore, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{store: store, logger: logger}
}

// Handler 返回路由。方法不匹配由路由层直接回 405。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/health", s.handleHealth)
	mux.HandleFunc("GET /admin/dead-letters", s.handleList)
	mux.HandleFunc("POST /admin/dead-letters/replay", s.handleReplay)
	mux.HandleFunc("POST /admin/dead-letters/discard", s.handleDiscard)
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
