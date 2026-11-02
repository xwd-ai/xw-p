package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/provider"
)

// adminSender 是管理接口测试用的渠道实现：可以实现探测，也可以只做投递。
type adminSender struct {
	id       string
	ch       domain.Channel
	probeErr error
	probed   int
}

func (s *adminSender) Channel() domain.Channel                { return s.ch }
func (s *adminSender) ID() string                             { return s.id }
func (s *adminSender) Send(provider.Delivery) provider.Result { return provider.Result{Sent: true} }

// proberSender 在 adminSender 之上实现 Probe。
type proberSender struct{ *adminSender }

func (s *proberSender) Probe(context.Context) error {
	s.probed++
	return s.probeErr
}

func newProviderRegistry(t *testing.T, entries ...provider.Entry) *provider.Registry {
	t.Helper()
	reg := provider.NewRegistry()
	for _, e := range entries {
		e.Enabled = true
		if err := reg.RegisterEntry(e); err != nil {
			t.Fatalf("登记 %s 失败：%v", e.ID, err)
		}
	}
	return reg
}

// newProviderHandler 装配带渠道能力的运维接口。
func newProviderHandler(t *testing.T, reg *provider.Registry, health *provider.HealthTracker, checker *provider.Checker) http.Handler {
	t.Helper()
	return New(&fakeStore{}, slog.New(slog.NewTextHandler(io.Discard, nil))).
		WithProviders(reg, health, checker).
		Handler()
}

// providerList 解析列表响应。
type providerList struct {
	Providers []provider.Status `json:"providers"`
	Summary   provider.Summary  `json:"summary"`
}

func decodeList(t *testing.T, body []byte) providerList {
	t.Helper()
	var got providerList
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("响应应为 JSON: %v（%s）", err, string(body))
	}
	return got
}

func TestProvidersEndpointListsRegistry(t *testing.T) {
	reg := newProviderRegistry(t,
		provider.Entry{ID: "console-email", Channel: domain.ChannelEmail, Priority: 100,
			Sender: &adminSender{id: "console-email", ch: domain.ChannelEmail}},
		provider.Entry{ID: "console-sms", Channel: domain.ChannelSMS, Priority: 100,
			Sender: &adminSender{id: "console-sms", ch: domain.ChannelSMS}},
	)
	health := provider.NewHealthTracker(reg)
	health.Record("console-email", false, "连接超时")

	rec := do(t, newProviderHandler(t, reg, health, provider.NewChecker(reg, health, nil)),
		http.MethodGet, "/admin/providers", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("应返回 200，实际 %d: %s", rec.Code, rec.Body.String())
	}
	got := decodeList(t, rec.Body.Bytes())
	if len(got.Providers) != 2 {
		t.Fatalf("应列出全部条目，实际 %d", len(got.Providers))
	}
	if got.Providers[0].ID != "console-email" || got.Providers[0].State != provider.StateDegraded {
		t.Errorf("应带出健康状态：%+v", got.Providers[0])
	}
	if got.Providers[1].State != provider.StateUnknown {
		t.Errorf("未观测条目应为 unknown：%+v", got.Providers[1])
	}
	if got.Summary.Total != 2 || got.Summary.Enabled != 2 || got.Summary.Degraded != 1 || got.Summary.Unknown != 1 {
		t.Errorf("汇总不符：%+v", got.Summary)
	}
}

func TestProvidersEndpointRequiresWiring(t *testing.T) {
	// 没有装配渠道能力时不注册路由，避免「没开这个功能」被读成「没有渠道」。
	rec := do(t, newTestServer(&fakeStore{}), http.MethodGet, "/admin/providers", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("未装配时应 404，实际 %d", rec.Code)
	}
}

func TestDisableProviderRemovesItFromRotation(t *testing.T) {
	primary := &adminSender{id: "smtp-email", ch: domain.ChannelEmail}
	standby := &adminSender{id: "console-email", ch: domain.ChannelEmail}
	reg := newProviderRegistry(t,
		provider.Entry{ID: "smtp-email", Channel: domain.ChannelEmail, Priority: 10, Sender: primary},
		provider.Entry{ID: "console-email", Channel: domain.ChannelEmail, Priority: 100, Sender: standby},
	)
	health := provider.NewHealthTracker(reg)
	handler := newProviderHandler(t, reg, health, provider.NewChecker(reg, health, nil))

	rec := do(t, handler, http.MethodPost, "/admin/providers/disable", `{"ids":["smtp-email"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("应返回 200，实际 %d: %s", rec.Code, rec.Body.String())
	}
	if _, ok := reg.Select(domain.ChannelEmail); !ok {
		t.Fatal("停用主实现后渠道仍应有备选")
	}
	if entry, _ := reg.Select(domain.ChannelEmail); entry.ID != "console-email" {
		t.Errorf("停用后应回退到备选，实际 %s", entry.ID)
	}
	got := decodeList(t, rec.Body.Bytes())
	if got.Providers[0].Enabled {
		t.Error("响应快照里应标记为停用")
	}
	if got.Summary.Disabled != 1 || got.Summary.Enabled != 1 {
		t.Errorf("汇总应把停用条目单独计数：%+v", got.Summary)
	}
}

func TestEnableProviderResetsHealth(t *testing.T) {
	reg := newProviderRegistry(t, provider.Entry{
		ID: "smtp-email", Channel: domain.ChannelEmail,
		Sender: &adminSender{id: "smtp-email", ch: domain.ChannelEmail},
	})
	health := provider.NewHealthTracker(reg)
	handler := newProviderHandler(t, reg, health, provider.NewChecker(reg, health, nil))

	if rec := do(t, handler, http.MethodPost, "/admin/providers/disable", `{"ids":["smtp-email"]}`); rec.Code != http.StatusOK {
		t.Fatalf("停用失败：%d", rec.Code)
	}
	for i := 0; i < 3; i++ {
		health.Record("smtp-email", false, "连接被拒")
	}
	if st, _ := health.Status("smtp-email"); st.State != provider.StateDown {
		t.Fatalf("三次失败后应为 down：%+v", st)
	}

	rec := do(t, handler, http.MethodPost, "/admin/providers/enable", `{"ids":["smtp-email"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("启用失败：%d: %s", rec.Code, rec.Body.String())
	}
	if st, _ := health.Status("smtp-email"); st.State != provider.StateUnknown || st.ConsecutiveFailures != 0 {
		t.Errorf("重新启用应清掉旧的失败计数：%+v", st)
	}
	if entry, ok := reg.Select(domain.ChannelEmail); !ok || entry.ID != "smtp-email" {
		t.Errorf("启用后应回到取用序列：%+v", entry)
	}
}

func TestProviderToggleRejectsUnknownIDs(t *testing.T) {
	reg := newProviderRegistry(t, provider.Entry{
		ID: "console-email", Channel: domain.ChannelEmail,
		Sender: &adminSender{id: "console-email", ch: domain.ChannelEmail},
	})
	health := provider.NewHealthTracker(reg)
	handler := newProviderHandler(t, reg, health, provider.NewChecker(reg, health, nil))

	rec := do(t, handler, http.MethodPost, "/admin/providers/disable", `{"ids":["nope"]}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("全是未知标识时应 404，实际 %d: %s", rec.Code, rec.Body.String())
	}
	if !reg.Has(domain.ChannelEmail) {
		t.Error("未命中任何条目时不应改动注册表")
	}
}

func TestProviderToggleValidatesBody(t *testing.T) {
	reg := newProviderRegistry(t, provider.Entry{
		ID: "console-email", Channel: domain.ChannelEmail,
		Sender: &adminSender{id: "console-email", ch: domain.ChannelEmail},
	})
	health := provider.NewHealthTracker(reg)
	handler := newProviderHandler(t, reg, health, provider.NewChecker(reg, health, nil))

	cases := []string{
		``,                        // 启停必须指明对象
		`{"ids":[]}`,              // 空列表同理
		`{"ids":"console-email"}`, // 类型不对
		`{"id":"console-email"}`,  // 字段名写错
		`not-json`,
	}
	for _, body := range cases {
		rec := do(t, handler, http.MethodPost, "/admin/providers/disable", body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("请求体 %q 应返回 400，实际 %d", body, rec.Code)
		}
	}
	if !reg.Has(domain.ChannelEmail) {
		t.Error("非法请求不应改动注册表")
	}
}

func TestProbeEndpoint(t *testing.T) {
	okProvider := &proberSender{&adminSender{id: "console-email", ch: domain.ChannelEmail}}
	badProvider := &proberSender{
		&adminSender{id: "smtp-email", ch: domain.ChannelEmail},
	}
	badProvider.probeErr = errors.New("中继 127.0.0.1:2525 不可达")
	reg := newProviderRegistry(t,
		provider.Entry{ID: "console-email", Channel: domain.ChannelEmail, Priority: 100, Sender: okProvider},
		provider.Entry{ID: "smtp-email", Channel: domain.ChannelEmail, Priority: 200, Sender: badProvider},
	)
	health := provider.NewHealthTracker(reg)
	handler := newProviderHandler(t, reg, health, provider.NewChecker(reg, health, nil))

	rec := do(t, handler, http.MethodPost, "/admin/providers/probe", `{"ids":["console-email","smtp-email"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("应返回 200，实际 %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Probed int `json:"probed"`
		Failed []struct {
			ID    string `json:"id"`
			Error string `json:"error"`
		} `json:"failed"`
		Providers []provider.Status `json:"providers"`
		Summary   provider.Summary  `json:"summary"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应应为 JSON: %v", err)
	}
	if body.Probed != 1 || len(body.Failed) != 1 || body.Failed[0].ID != "smtp-email" {
		t.Errorf("失败清单不符：%+v", body)
	}
	if body.Summary.Up != 1 || body.Summary.Degraded != 1 {
		t.Errorf("快照应反映探测结果：%+v", body.Summary)
	}
	if okProvider.probed != 1 || badProvider.probed != 1 {
		t.Errorf("每条应探一次：ok=%d bad=%d", okProvider.probed, badProvider.probed)
	}
}

func TestProbeEndpointDefaultsToAllEnabled(t *testing.T) {
	a := &proberSender{&adminSender{id: "console-email", ch: domain.ChannelEmail}}
	b := &proberSender{&adminSender{id: "console-sms", ch: domain.ChannelSMS}}
	reg := newProviderRegistry(t,
		provider.Entry{ID: "console-email", Channel: domain.ChannelEmail, Sender: a},
		provider.Entry{ID: "console-sms", Channel: domain.ChannelSMS, Sender: b},
	)
	if err := reg.SetEnabled("console-sms", false); err != nil {
		t.Fatalf("停用失败：%v", err)
	}
	health := provider.NewHealthTracker(reg)
	handler := newProviderHandler(t, reg, health, provider.NewChecker(reg, health, nil))

	rec := do(t, handler, http.MethodPost, "/admin/providers/probe", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("空请求体应表示全部，实际 %d: %s", rec.Code, rec.Body.String())
	}
	if a.probed != 1 || b.probed != 0 {
		t.Errorf("只应探测启用中的条目：a=%d b=%d", a.probed, b.probed)
	}
}
