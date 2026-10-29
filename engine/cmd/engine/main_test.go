package main

import (
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xwd-ai/xw-p/engine/internal/config"
	"github.com/xwd-ai/xw-p/engine/internal/schedule"
)

// 启动装配测试：newDelayStore 的选择结果决定 delay 步骤能不能活过进程重启。
// 这里校验三条路径（Redis / 显式内存 / Redis 不可用回退），并用「再装配一次」
// 模拟引擎重启，确认未到期任务留在了外部存储里。

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError}))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// 显式配置 memory 时使用进程内存储，且释放函数可安全调用。
func TestNewDelayStoreMemory(t *testing.T) {
	cfg := config.Config{DelayStore: "memory", DelayPrefix: "xw:delay:test", PollTimeout: 1}
	store, closeStore := newDelayStore(discardLogger(), cfg)
	defer closeStore()

	if _, ok := store.(*schedule.MemoryStore); !ok {
		t.Fatalf("XW_DELAY_STORE=memory 应使用进程内存储，实际 %T", store)
	}

	// 内存存储不连 Redis，登记与取出都要正常。
	now := time.Now().UTC()
	if _, err := store.Add(schedule.Item{DueAt: now, Payload: []byte("resume")}); err != nil {
		t.Fatalf("登记任务失败：%v", err)
	}
	if due, err := store.Due(now, 10); err != nil || len(due) != 1 {
		t.Fatalf("读取到期任务失败：n=%d err=%v", len(due), err)
	}
}

// 默认配置下用 Redis 存储，键前缀来自 XW_DELAY_PREFIX；重启（再装配一次）
// 之后任务仍在，说明 delay 任务没有跟着进程一起消失。
func TestNewDelayStoreRedisSurvivesRearm(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := config.Config{
		RedisAddr:   mr.Addr(),
		DelayStore:  "redis",
		DelayPrefix: "xw:delay:engine-test",
		PollTimeout: 1,
	}

	store, closeStore := newDelayStore(discardLogger(), cfg)
	redisStore, ok := store.(*schedule.RedisStore)
	if !ok {
		closeStore()
		t.Fatalf("默认应使用 Redis 存储，实际 %T", store)
	}
	if redisStore.Prefix() != cfg.DelayPrefix {
		t.Errorf("键前缀 = %s, 期望 %s", redisStore.Prefix(), cfg.DelayPrefix)
	}

	now := time.Now().UTC()
	if _, err := store.Add(schedule.Item{DueAt: now.Add(time.Hour), Payload: []byte("resume")}); err != nil {
		closeStore()
		t.Fatalf("登记任务失败：%v", err)
	}
	closeStore() // 相当于进程退出，释放连接

	// 重新装配一次：新的连接、新的存储对象，读取的是同一份 Redis 数据。
	store2, closeStore2 := newDelayStore(discardLogger(), cfg)
	defer closeStore2()
	if n := store2.Len(); n != 1 {
		t.Fatalf("重启后待执行任务数 = %d, 期望 1", n)
	}
	due, err := store2.Due(now.Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("重启后读取到期任务失败：%v", err)
	}
	if len(due) != 1 || string(due[0].Payload) != "resume" {
		t.Errorf("重启后任务内容不一致：%+v", due)
	}
}

// Redis 连不上时回退进程内存储，保证引擎仍能启动（代价是重启会丢未到期任务）。
func TestNewDelayStoreFallsBackWhenRedisDown(t *testing.T) {
	cfg := config.Config{
		RedisAddr:   "127.0.0.1:1", // 保留端口，连接会被立刻拒绝
		DelayStore:  "redis",
		DelayPrefix: "xw:delay:engine-test",
		PollTimeout: 1,
	}
	store, closeStore := newDelayStore(discardLogger(), cfg)
	defer closeStore()

	if _, ok := store.(*schedule.MemoryStore); !ok {
		t.Fatalf("Redis 不可用时应回退进程内存储，实际 %T", store)
	}
}
