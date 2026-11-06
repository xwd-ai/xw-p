package admin

import (
	"net/http"
	"net/http/pprof"
	"runtime"
)

// Profiling 是采样端点的开关与采样率。
//
// 三个 profile 不需要预热：CPU、堆、goroutine 随时可抓。block 与 mutex 必须在
// 启动时打开采样，否则抓到的是一份空文件——「没有数据」会被误读成「没有阻塞」，
// 这是这两类 profile 最常见的误用。
type Profiling struct {
	Enabled       bool
	BlockRate     int // runtime.SetBlockProfileRate 的纳秒阈值，<=0 表示不采集
	MutexFraction int // runtime.SetMutexProfileFraction 的采样比例，<=0 表示不采集
}

// registerProfiling 把标准库的采样端点挂到 /debug/pprof/。
//
// 命名 profile（heap / goroutine / allocs / block / mutex / threadcreate）不用
// 逐个注册：pprof.Index 会按路径后缀自己分发，只有需要单独处理查询参数的四个
// （cmdline / profile / symbol / trace）必须显式挂上。
//
// 这里显式引用处理函数，而不是依赖 net/http/pprof 的包级 init：那个 init 把端点
// 注册在 http.DefaultServeMux 上，而管理接口用的是自己的 mux——靠 init 生效，
// 等于把「有没有端点」寄托在一个 import 副作用上。
func registerProfiling(mux *http.ServeMux) {
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
}

// applyProfilingRates 把采样率交给运行时。
//
// 只在启动时调用一次：这两项是进程级全局设置，按请求切换会让同一份 profile
// 里混着不同采样率的数据，占比也就失去意义了。
func applyProfilingRates(p Profiling) {
	if !p.Enabled {
		return
	}
	if p.BlockRate > 0 {
		runtime.SetBlockProfileRate(p.BlockRate)
	}
	if p.MutexFraction > 0 {
		runtime.SetMutexProfileFraction(p.MutexFraction)
	}
}
