package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// fakeStore 记录调用参数，用来验证路由与入参解析，不掺 Redis 逻辑。
type fakeStore struct {
	page     queue.DeadLetterPage
	report   queue.ReplayReport
	discard  int
	err      error
	lastPage queue.DeadLetterQuery
	lastIDs  []string
}

func (f *fakeStore) ListDeadLetters(_ context.Context, q queue.DeadLetterQuery) (queue.DeadLetterPage, error) {
	f.lastPage = q
	return f.page, f.err
}

func (f *fakeStore) ReplayDeadLetters(_ context.Context, ids []string) (queue.ReplayReport, error) {
	f.lastIDs = ids
	return f.report, f.err
}

func (f *fakeStore) DiscardDeadLetters(_ context.Context, ids []string) (int, error) {
	f.lastIDs = ids
	return f.discard, f.err
}

func newTestServer(store DeadLetterStore) http.Handler {
	return New(store, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler()
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestListDeadLettersEndpoint(t *testing.T) {
	store := &fakeStore{page: queue.DeadLetterPage{
		Items:  []queue.DeadLetterEntry{{ID: "abc", DeadLetter: queue.DeadLetter{Attempts: 5, LastError: "超时"}}},
		Total:  1,
		Offset: 0,
		Limit:  10,
	}}
	rec := do(t, newTestServer(store), http.MethodGet, "/admin/dead-letters?offset=0&limit=10", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("应返回 200，实际 %d: %s", rec.Code, rec.Body.String())
	}
	if store.lastPage.Offset != 0 || store.lastPage.Limit != 10 {
		t.Errorf("分页参数应透传，实际 %+v", store.lastPage)
	}
	var got queue.DeadLetterPage
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("响应应为 JSON: %v", err)
	}
	if len(got.Items) != 1 || got.Items[0].ID != "abc" || got.Items[0].Attempts != 5 {
		t.Errorf("响应内容不符: %+v", got)
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "application/json") {
		t.Errorf("Content-Type 应为 JSON，实际 %s", rec.Header().Get("Content-Type"))
	}
}

func TestListDeadLettersRejectsBadParams(t *testing.T) {
	store := &fakeStore{}
	cases := []string{
		"/admin/dead-letters?offset=-1",
		"/admin/dead-letters?offset=abc",
		"/admin/dead-letters?limit=0",
		"/admin/dead-letters?limit=x",
	}
	for _, path := range cases {
		rec := do(t, newTestServer(store), http.MethodGet, path, "")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s 应返回 400，实际 %d", path, rec.Code)
		}
	}
}

func TestReplayEndpoint(t *testing.T) {
	store := &fakeStore{report: queue.ReplayReport{Replayed: []string{"a"}, Missing: []string{"b"}}}
	rec := do(t, newTestServer(store), http.MethodPost, "/admin/dead-letters/replay", `{"ids":["a","b"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("应返回 200，实际 %d: %s", rec.Code, rec.Body.String())
	}
	if len(store.lastIDs) != 2 || store.lastIDs[0] != "a" || store.lastIDs[1] != "b" {
		t.Errorf("ID 应透传，实际 %v", store.lastIDs)
	}
	var got queue.ReplayReport
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("响应应为 JSON: %v", err)
	}
	if len(got.Replayed) != 1 || got.Replayed[0] != "a" || len(got.Missing) != 1 {
		t.Errorf("响应内容不符: %+v", got)
	}
}

func TestDiscardEndpoint(t *testing.T) {
	store := &fakeStore{discard: 2}
	rec := do(t, newTestServer(store), http.MethodPost, "/admin/dead-letters/discard", `{"ids":["a","b"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("应返回 200，实际 %d: %s", rec.Code, rec.Body.String())
	}
	var got map[string]int
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("响应应为 JSON: %v", err)
	}
	if got["discarded"] != 2 {
		t.Errorf("应报告丢弃 2 条，实际 %+v", got)
	}
}

func TestWriteEndpointsRejectBadBody(t *testing.T) {
	store := &fakeStore{}
	cases := []struct {
		body string
		want int
	}{
		{`不是 JSON`, http.StatusBadRequest},
		{`{}`, http.StatusBadRequest},
		{`{"ids":[]}`, http.StatusBadRequest},
		{`{"ids":["a"],"extra":1}`, http.StatusBadRequest},
	}
	for _, c := range cases {
		rec := do(t, newTestServer(store), http.MethodPost, "/admin/dead-letters/replay", c.body)
		if rec.Code != c.want {
			t.Errorf("请求体 %s 应返回 %d，实际 %d", c.body, c.want, rec.Code)
		}
	}
}

func TestWriteEndpointsRejectTooManyIDs(t *testing.T) {
	store := &fakeStore{}
	ids := make([]string, 0, queue.MaxReplayIDs+1)
	for i := 0; i <= queue.MaxReplayIDs; i++ {
		ids = append(ids, "id")
	}
	body, _ := json.Marshal(map[string][]string{"ids": ids})
	rec := do(t, newTestServer(store), http.MethodPost, "/admin/dead-letters/replay", string(body))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("超出单次上限应返回 400，实际 %d", rec.Code)
	}
	if store.lastIDs != nil {
		t.Error("校验失败时不应调用存储层")
	}
}

func TestStoreErrorBecomes500(t *testing.T) {
	store := &fakeStore{err: errors.New("redis 不可用")}
	cases := []struct{ method, path, body string }{
		{http.MethodGet, "/admin/dead-letters", ""},
		{http.MethodPost, "/admin/dead-letters/replay", `{"ids":["a"]}`},
		{http.MethodPost, "/admin/dead-letters/discard", `{"ids":["a"]}`},
	}
	for _, c := range cases {
		rec := do(t, newTestServer(store), c.method, c.path, c.body)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("%s %s 应返回 500，实际 %d", c.method, c.path, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "error") {
			t.Errorf("错误响应应带 error 字段，实际 %s", rec.Body.String())
		}
	}
}

func TestMethodNotAllowed(t *testing.T) {
	store := &fakeStore{}
	rec := do(t, newTestServer(store), http.MethodDelete, "/admin/dead-letters", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("不支持的方法应返回 405，实际 %d", rec.Code)
	}
}

func TestHealthEndpoint(t *testing.T) {
	rec := do(t, newTestServer(&fakeStore{}), http.MethodGet, "/admin/health", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "ok") {
		t.Errorf("健康检查应返回 ok，实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// 端到端：真实队列（进程内 Redis）写入死信 → 接口查询 → 接口重放 → 消息回到队列。
func TestDeadLetterRoundTrip(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	consumer := queue.NewReliableConsumer(mr.Addr(), "", 0, "xw:queue:events", 0)
	t.Cleanup(func() { _ = consumer.Close() })
	ctx := context.Background()

	payload := `{"name":"post_mentioned","payload":{"post_id":"P-7"}}`
	raw, err := json.Marshal(queue.DeadLetter{Payload: []byte(payload), Attempts: 5, LastError: "渠道超时", Queue: "xw:queue:events"})
	if err != nil {
		t.Fatalf("构造死信失败: %v", err)
	}
	if err := client.RPush(ctx, "xw:queue:events:dead", string(raw)).Err(); err != nil {
		t.Fatalf("写入死信失败: %v", err)
	}

	h := newTestServer(consumer)
	rec := do(t, h, http.MethodGet, "/admin/dead-letters", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("查询应返回 200，实际 %d", rec.Code)
	}
	var page queue.DeadLetterPage
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("响应应为 JSON: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 {
		t.Fatalf("应查到 1 条死信，实际 %+v", page)
	}

	body, _ := json.Marshal(map[string][]string{"ids": {page.Items[0].ID}})
	rec = do(t, h, http.MethodPost, "/admin/dead-letters/replay", string(body))
	if rec.Code != http.StatusOK {
		t.Fatalf("重放应返回 200，实际 %d: %s", rec.Code, rec.Body.String())
	}
	if n := client.LLen(ctx, "xw:queue:events").Val(); n != 1 {
		t.Errorf("重放后主队列应有 1 条消息，实际 %d", n)
	}
	queued, _ := client.LRange(ctx, "xw:queue:events", 0, -1).Result()
	if len(queued) != 1 || queued[0] != payload {
		t.Errorf("重放的消息体应与原始一致，实际 %v", queued)
	}
	if n := client.LLen(ctx, "xw:queue:events:dead").Val(); n != 0 {
		t.Errorf("重放后死信列表应清空，实际 %d", n)
	}
}
