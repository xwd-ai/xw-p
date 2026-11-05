package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/metrics"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func readyServer(probes ...Probe) http.Handler {
	return New(nil, discardLogger()).
		WithProbes(probes...).
		WithMetrics(metrics.New()).
		Handler()
}

// 就绪：全部通过 200，任一失败 503，且失败项要点名。
func TestReadyzReportsFailure(t *testing.T) {
	h := readyServer(
		Probe{Name: "redis", Check: func(context.Context) error { return nil }},
		Probe{Name: "file", Check: func(context.Context) error { return errors.New("磁盘只读") }},
	)

	rec := do(t, h, http.MethodGet, "/readyz", "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("状态码 = %d，期望 503", rec.Code)
	}
	var body struct {
		Status string `json:"status"`
		Checks []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
			Error  string `json:"error"`
		} `json:"checks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应不是合法 JSON：%v", err)
	}
	if body.Status != "degraded" || len(body.Checks) != 2 {
		t.Fatalf("响应结构不符：%+v", body)
	}
	if body.Checks[1].Error != "磁盘只读" {
		t.Errorf("失败原因未返回：%+v", body.Checks[1])
	}
}

func TestReadyzOK(t *testing.T) {
	h := readyServer(Probe{Name: "redis", Check: func(context.Context) error { return nil }})
	rec := do(t, h, http.MethodGet, "/readyz", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Fatalf("响应体不符：%s", rec.Body.String())
	}
}

// 没有配置探测项时就绪与存活等价：不该因为「没配」而永远 503。
func TestReadyzWithoutProbes(t *testing.T) {
	rec := do(t, readyServer(), http.MethodGet, "/readyz", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", rec.Code)
	}
}

// 慢依赖要被自己的超时切断，而不是把探针一起挂住。
func TestReadyzHonoursProbeTimeout(t *testing.T) {
	h := readyServer(Probe{
		Name:    "slow",
		Timeout: 20 * time.Millisecond,
		Check: func(ctx context.Context) error {
			select {
			case <-time.After(2 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})

	started := time.Now()
	rec := do(t, h, http.MethodGet, "/readyz", "")
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("探测未被超时切断，耗时 %v", elapsed)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("状态码 = %d，期望 503", rec.Code)
	}
}

func TestHealthzAlwaysOK(t *testing.T) {
	rec := do(t, readyServer(Probe{Name: "x", Check: func(context.Context) error { return errors.New("boom") }}),
		http.MethodGet, "/healthz", "")
	// 存活不因为依赖失败而失败：依赖问题归就绪管，重启解决不了它
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", rec.Code)
	}
}

func TestMetricsTextEndpoint(t *testing.T) {
	reg := metrics.New()
	reg.Record(metrics.Sample{Channel: "email", Provider: "smtp", Status: metrics.StatusSent, Duration: time.Millisecond})
	h := New(nil, discardLogger()).WithMetrics(reg).Handler()

	rec := do(t, h, http.MethodGet, "/metrics", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q，期望 text/plain", ct)
	}
	if !strings.Contains(rec.Body.String(), `xw_delivery_total{channel="email",status="sent"} 1`) {
		t.Fatalf("指标文本不符：%s", rec.Body.String())
	}
}

// 未装配指标时不该有 /metrics：返回空列表会被误读成「没有投递」。
func TestMetricsEndpointAbsentWithoutRegistry(t *testing.T) {
	rec := do(t, New(nil, discardLogger()).Handler(), http.MethodGet, "/metrics", "")
	if rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("未装配指标时状态码 = %d，期望 404/405", rec.Code)
	}
}
