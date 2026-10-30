package main

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestFlushQueueRemovesDerivedKeys(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	ctx := context.Background()
	const key = "xw:queue:bench"
	for _, suffix := range queueSuffixes {
		if err := client.LPush(ctx, key+suffix, "x").Err(); err != nil {
			t.Fatalf("准备数据失败: %v", err)
		}
	}
	if err := client.LPush(ctx, key+":other", "keep").Err(); err != nil {
		t.Fatalf("准备数据失败: %v", err)
	}

	if err := flushQueue(ctx, client, key); err != nil {
		t.Fatalf("清空失败: %v", err)
	}
	for _, suffix := range queueSuffixes {
		if n := client.LLen(ctx, key+suffix).Val(); n != 0 {
			t.Errorf("%s 未清空, 残留 %d", key+suffix, n)
		}
	}
	if n := client.LLen(ctx, key+":other").Val(); n != 1 {
		t.Errorf("不该动其它键, 实际 %d", n)
	}
}

func TestDefaultQueueKeyIsIsolated(t *testing.T) {
	if defaultQueueKey == "xw:queue:events" {
		t.Error("压测默认队列不应与触发链路共用")
	}
}

func TestEnvOr(t *testing.T) {
	const key = "XW_BENCH_TEST_ENV"
	t.Setenv(key, "")
	if got := envOr(key, "fallback"); got != "fallback" {
		t.Errorf("空值应回退默认, 得到 %q", got)
	}
	t.Setenv(key, " value ")
	if got := envOr(key, "fallback"); got != "value" {
		t.Errorf("应去掉首尾空白, 得到 %q", got)
	}
}
