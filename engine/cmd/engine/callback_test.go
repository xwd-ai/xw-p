package main

import (
	"strings"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/config"
	"github.com/xwd-ai/xw-p/engine/internal/webhook"
)

// 未配置或显式关闭时不创建分发器，投递路径按「没有回调」继续跑。
func TestNewCallbackDispatcherDisabled(t *testing.T) {
	for _, value := range []string{"", "  ", "off", "OFF", "disabled", "none", "-"} {
		dispatcher, closeFn := newCallbackDispatcher(discardLogger(), config.Config{CallbackURL: value})
		if dispatcher != nil {
			t.Errorf("%q 应关闭回调", value)
		}
		closeFn() // 关闭函数必须可调用，defer 链上不能空指针
	}
}

// 地址写错时只告警不退出：回调是旁路，投递本身应继续。
func TestNewCallbackDispatcherInvalidURL(t *testing.T) {
	cases := []string{
		"http://community.example.com/hooks/xw",
		"https://user:pass@community.example.com/hooks/xw",
		"community.example.com/hooks/xw",
	}
	for _, raw := range cases {
		dispatcher, closeFn := newCallbackDispatcher(discardLogger(), config.Config{CallbackURL: raw})
		if dispatcher != nil {
			t.Errorf("%q 不合法，不应装配回调", raw)
		}
		closeFn()
	}
}

// 正常装配：地址被脱敏后再进日志，关闭时没有待发事件应立即返回。
func TestNewCallbackDispatcherWiring(t *testing.T) {
	cfg := config.Config{
		CallbackURL:     "https://community.example.com/hooks/s3cr3t-token",
		CallbackTimeout: 2 * time.Second,
		CallbackWorkers: 3,
		CallbackQueue:   32,
	}
	dispatcher, closeFn := newCallbackDispatcher(discardLogger(), cfg)
	if dispatcher == nil {
		t.Fatal("合法地址应装配回调")
	}

	masked := dispatcher.Target().String()
	if masked != "https://community.example.com" {
		t.Errorf("对外暴露的地址应脱敏，实际 %q", masked)
	}
	if strings.Contains(masked, "s3cr3t") {
		t.Errorf("脱敏值泄露了令牌: %q", masked)
	}

	if stats := dispatcher.Stats(); stats != (webhook.Stats{}) {
		t.Errorf("尚未投递时计数应为空: %+v", stats)
	}
	closeFn()
}

// 越界的并发与队列参数收敛到合法区间，装配不应因此失败。
func TestNewCallbackDispatcherNormalizesOptions(t *testing.T) {
	cfg := config.Config{
		CallbackURL:     "https://community.example.com/cb",
		CallbackTimeout: -time.Second,
		CallbackWorkers: 0,
		CallbackQueue:   -5,
	}
	dispatcher, closeFn := newCallbackDispatcher(discardLogger(), cfg)
	if dispatcher == nil {
		t.Fatal("参数越界也应能装配")
	}
	closeFn()
}
