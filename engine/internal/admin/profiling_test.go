package admin

import (
	"io"
	"log/slog"
	"net/http"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
)

func newProfilingServer(p Profiling) http.Handler {
	return New(nil, slog.New(slog.NewTextHandler(io.Discard, nil))).WithProfiling(p).Handler()
}

// 默认不挂采样端点：管理接口默认开着，端点没有鉴权，不能顺手带出来。
func TestProfilingRoutesHiddenByDefault(t *testing.T) {
	h := newProfilingServer(Profiling{})
	rec := do(t, h, http.MethodGet, "/debug/pprof/", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("未开启时 /debug/pprof/ 应返回 404，实际 %d", rec.Code)
	}
}

// 打开后索引页与命名 profile 都可用：命名 profile 由 pprof.Index 按路径分发，
// 这一条同时验证了「只注册子树也能拿到 heap / goroutine」。
func TestProfilingRoutesServeProfiles(t *testing.T) {
	h := newProfilingServer(Profiling{Enabled: true})

	index := do(t, h, http.MethodGet, "/debug/pprof/", "")
	if index.Code != http.StatusOK {
		t.Fatalf("索引页应返回 200，实际 %d: %s", index.Code, index.Body.String())
	}
	for _, name := range []string{"heap", "goroutine", "allocs", "block", "mutex"} {
		if !strings.Contains(index.Body.String(), name) {
			t.Errorf("索引页缺少 profile %q", name)
		}
	}

	goroutine := do(t, h, http.MethodGet, "/debug/pprof/goroutine?debug=1", "")
	if goroutine.Code != http.StatusOK || !strings.Contains(goroutine.Body.String(), "goroutine profile") {
		t.Fatalf("goroutine profile 不可用: %d %s", goroutine.Code, goroutine.Body.String())
	}

	cmdline := do(t, h, http.MethodGet, "/debug/pprof/cmdline", "")
	if cmdline.Code != http.StatusOK || cmdline.Body.Len() == 0 {
		t.Fatalf("cmdline 应返回非空内容，实际 %d", cmdline.Code)
	}
}

// 采样率落到运行时：锁采样可以用 SetMutexProfileFraction(-1) 读回，
// 阻塞采样没有读取接口，改用一次确定性阻塞后看记录数。
func TestApplyProfilingRates(t *testing.T) {
	applyProfilingRates(Profiling{Enabled: true, BlockRate: 1, MutexFraction: 1})
	t.Cleanup(func() {
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(0)
	})

	if got := runtime.SetMutexProfileFraction(-1); got != 1 {
		t.Fatalf("锁采样比例应为 1，实际 %d", got)
	}

	ch := make(chan struct{})
	go func() { ch <- struct{}{} }()
	<-ch
	if n := pprof.Lookup("block").Count(); n == 0 {
		t.Fatalf("block profile 没有记录到阻塞事件")
	}
}

// 关闭时不改运行时设置：别的进程级配置不该被一个关掉的开关顺手改掉。
func TestApplyProfilingRatesDisabled(t *testing.T) {
	runtime.SetMutexProfileFraction(0)
	t.Cleanup(func() { runtime.SetMutexProfileFraction(0) })

	applyProfilingRates(Profiling{Enabled: false, BlockRate: 1, MutexFraction: 1})
	if got := runtime.SetMutexProfileFraction(-1); got != 0 {
		t.Fatalf("未开启时不应改变采样比例，实际 %d", got)
	}
}
