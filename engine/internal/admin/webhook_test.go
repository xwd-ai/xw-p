package admin

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/execution"
	"github.com/xwd-ai/xw-p/engine/internal/webhook"
)

// fakeWebhookReplayer 记录重放请求，返回预置结果。
type fakeWebhookReplayer struct {
	report webhook.ReplayReport
	err    error
	last   []string
}

func (f *fakeWebhookReplayer) Replay(ids []string) (webhook.ReplayReport, error) {
	f.last = ids
	return f.report, f.err
}

func newWebhookAdmin() (*Server, *webhook.MemoryAuditStore, *fakeWebhookReplayer) {
	audit := webhook.NewMemoryAuditStore()
	replayer := &fakeWebhookReplayer{
		report: webhook.ReplayReport{Replayed: []string{"evt-1"}},
	}
	srv := New(&fakeStore{}, slog.New(slog.NewTextHandler(io.Discard, nil))).WithWebhookAudit(audit, replayer)
	return srv, audit, replayer
}

func TestWebhookEventsFilters(t *testing.T) {
	srv, audit, _ := newWebhookAdmin()
	at := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	for _, entry := range []webhook.AuditEntry{
		{EventID: "evt-1", Event: webhook.EventDeliveryResult, TriggerID: "trig-1",
			Status: webhook.AuditDelivered, At: execution.Timestamp{Time: at}},
		{EventID: "evt-2", Event: webhook.EventDeliveryFailed, TriggerID: "trig-2",
			Status: webhook.AuditFailed, At: execution.Timestamp{Time: at.Add(time.Minute)}},
	} {
		if err := audit.Append(entry); err != nil {
			t.Fatalf("写入审计失败: %v", err)
		}
	}

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/admin/webhook/events?status=failed", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "evt-2") || strings.Contains(body, "evt-1") {
		t.Errorf("按状态过滤结果不符: %s", body)
	}

	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/admin/webhook/events?type=delivery.sent", nil))
	if !strings.Contains(rec.Body.String(), `"count":0`) {
		t.Errorf("无命中时应返回空列表: %s", rec.Body.String())
	}
}

func TestWebhookEventsRejectsBadParams(t *testing.T) {
	srv, _, _ := newWebhookAdmin()
	cases := []string{
		"/admin/webhook/events?status=done",
		"/admin/webhook/events?type=order.created",
		"/admin/webhook/events?limit=0",
		"/admin/webhook/events?limit=999",
		"/admin/webhook/events?offset=-1",
		"/admin/webhook/events?since=yesterday",
	}
	for _, path := range cases {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s 应返回 400，实际 %d", path, rec.Code)
		}
	}
}

func TestWebhookReplayRoute(t *testing.T) {
	srv, _, replayer := newWebhookAdmin()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/webhook/replay",
		strings.NewReader(`{"ids":["evt-1","evt-2"]}`))
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，body = %s", rec.Code, rec.Body.String())
	}
	if len(replayer.last) != 2 || replayer.last[0] != "evt-1" {
		t.Errorf("重放入参不符: %+v", replayer.last)
	}

	replayer.err = errors.New("事件正文未留存，无法重放")
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/webhook/replay",
		strings.NewReader(`{"ids":["evt-1"]}`)))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("重放失败应返回 400，实际 %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "无法重放") {
		t.Errorf("应带回原因: %s", rec.Body.String())
	}
}

// 未装配审计能力时不注册路由：调用方应当看到 404，而不是一张空表。
func TestWebhookRoutesAbsentWithoutAudit(t *testing.T) {
	srv := New(&fakeStore{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/webhook/events", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("未装配时应 404，实际 %d", rec.Code)
	}
}
