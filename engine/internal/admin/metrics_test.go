package admin

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/metrics"
)

// metricsHandler 装配一个带指标读取能力的路由，其余依赖用死信假存储占位。
func metricsHandler(t *testing.T) (http.Handler, *metrics.Registry) {
	t.Helper()
	reg := metrics.New()
	h := New(&fakeStore{}, nil).
		WithMetrics(reg).
		Handler()
	return h, reg
}

// 未装配指标时不注册路由：调用方拿 404 就知道这个能力没开，
// 比拿到一个全零的快照更容易分辨。
func TestMetricsRouteAbsent(t *testing.T) {
	h := New(&fakeStore{}, nil).Handler()
	if rec := do(t, h, http.MethodGet, "/admin/metrics", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("未装配指标时应返回 404，实际 %d", rec.Code)
	}
}

// 读取接口给出合计、渠道、实现三个维度以及延迟分布。
func TestMetricsSnapshot(t *testing.T) {
	h, reg := metricsHandler(t)
	reg.Record(metrics.Sample{
		Channel:  "email",
		Provider: "smtp-email",
		Status:   metrics.StatusSent,
		Duration: 12 * time.Millisecond,
	})
	reg.Record(metrics.Sample{
		Channel:  "email",
		Provider: "smtp-email",
		Status:   metrics.StatusFailed,
		Duration: 30 * time.Millisecond,
	})
	reg.Record(metrics.Sample{
		Channel:  "in_app",
		Provider: "console-in-app",
		Status:   metrics.StatusSent,
		Duration: 2 * time.Millisecond,
	})

	rec := do(t, h, http.MethodGet, "/admin/metrics", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Fatalf("Content-Type = %q", ct)
	}

	var body metrics.Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应不是合法 JSON: %v", err)
	}
	if body.Total.Attempted != 3 || body.Total.Sent != 2 || body.Total.Failed != 1 {
		t.Fatalf("合计不符: %+v", body.Total)
	}
	if len(body.Channels) != 2 || body.Channels[0].Channel != "email" {
		t.Fatalf("渠道维度不符: %+v", body.Channels)
	}
	if body.Channels[0].ErrorRate != 0.5 {
		t.Fatalf("email 错误率 = %v，期望 0.5", body.Channels[0].ErrorRate)
	}
	if body.Channels[0].Latency.Count != 2 || len(body.Channels[0].Latency.Buckets) == 0 {
		t.Fatalf("延迟分布缺失: %+v", body.Channels[0].Latency)
	}
	if len(body.Providers) != 2 {
		t.Fatalf("实现维度不符: %+v", body.Providers)
	}
	if body.UptimeSeconds < 0 {
		t.Fatalf("运行时长不应为负: %v", body.UptimeSeconds)
	}
}

// 重置接口清空计数并返回重置后的空快照，省掉调用方再取一次。
func TestMetricsReset(t *testing.T) {
	h, reg := metricsHandler(t)
	reg.Record(metrics.Sample{Channel: "email", Provider: "smtp-email", Status: metrics.StatusSent})

	rec := do(t, h, http.MethodPost, "/admin/metrics/reset", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", rec.Code)
	}

	var body metrics.Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应不是合法 JSON: %v", err)
	}
	if body.Total.Attempted != 0 || len(body.Channels) != 0 {
		t.Fatalf("重置后应为空: %+v", body)
	}
	if again := reg.Snapshot(); again.Total.Attempted != 0 {
		t.Fatalf("收集器应被清空: %+v", again.Total)
	}
}

// 方法不匹配由路由层回 405，不会被当成「重置成功」。
func TestMetricsRoutesMethodNotAllowed(t *testing.T) {
	h, _ := metricsHandler(t)
	if rec := do(t, h, http.MethodPut, "/admin/metrics", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("PUT /admin/metrics 应返回 405，实际 %d", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/admin/metrics/reset", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /admin/metrics/reset 应返回 405，实际 %d", rec.Code)
	}
}
