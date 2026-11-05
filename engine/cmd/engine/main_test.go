package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xwd-ai/xw-p/engine/internal/config"
	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/provider"
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

// buildRegistry 的默认路径：未配置渠道文件时登记内置的五个 console 实现。
func TestBuildRegistryUsesBuiltinConsoleProviders(t *testing.T) {
	registry, retries, err := buildRegistry(discardLogger(), config.Config{})
	if err != nil {
		t.Fatalf("默认装配失败：%v", err)
	}
	if registry.Len() != 5 {
		t.Fatalf("登记条目数 = %d, 期望 5", registry.Len())
	}
	if retries.Enabled() {
		t.Error("默认装配不带渠道级重试")
	}
	for _, ch := range domain.DeliveryChannels() {
		if !registry.Has(ch) {
			t.Errorf("渠道 %s 缺少可用实现", ch)
		}
	}
}

// 配置了渠道文件时按文件装配：优先级决定默认实现，停用条目只登记不取用，
// smtp 这类已接入的种类按凭据构造真实实现。
func TestBuildRegistryFromConfigFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "providers.json")
	content := `{"providers": [
		{"id": "email-console", "channel": "email", "kind": "console", "priority": 100},
		{"id": "sms-console", "channel": "sms", "kind": "console", "enabled": false},
		{"id": "email-smtp", "channel": "email", "kind": "smtp", "priority": 10,
		 "credentials": {"host": "smtp.example.com", "from": "notify@example.com"}}
	]}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("写测试配置失败：%v", err)
	}

	registry, _, err := buildRegistry(discardLogger(), config.Config{ProvidersFile: path})
	if err != nil {
		t.Fatalf("装配失败：%v", err)
	}
	if registry.Len() != 3 {
		t.Fatalf("登记条目数 = %d, 期望 3（两条 email + 一条停用的 sms）", registry.Len())
	}
	entry, ok := registry.Select(domain.ChannelEmail)
	if !ok || entry.ID != "email-smtp" {
		t.Errorf("email 默认实现 = %q, 期望优先级更高的 email-smtp", entry.ID)
	}
	if smtp, ok := registry.LookupByID("email-smtp"); !ok || !smtp.Enabled {
		t.Error("smtp 实现应登记为启用条目")
	}
	if entry, ok := registry.LookupByID("sms-console"); !ok || entry.Enabled {
		t.Error("停用配置应登记为停用条目")
	}
	if registry.Has(domain.ChannelSMS) {
		t.Error("只有停用实现的渠道不应被判定为可用")
	}
}

// 配置写错时直接返回错误，让进程起不来，而不是带着半套渠道继续跑。
func TestBuildRegistryRejectsInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "providers.json")
	if err := os.WriteFile(path, []byte(`[{"id": "email", "channel": "email", "kind": "smtp"}]`), 0o600); err != nil {
		t.Fatalf("写测试配置失败：%v", err)
	}

	registry, _, err := buildRegistry(discardLogger(), config.Config{ProvidersFile: path})
	if err == nil {
		t.Fatal("缺少凭据的配置应装配失败")
	}
	if registry != nil {
		t.Error("失败时不应返回注册表")
	}
	if !strings.Contains(err.Error(), "credentials") {
		t.Errorf("错误信息应指出缺少凭据：%v", err)
	}
}

// 管线装配完成后必须带上一份健康记录器：运维接口读的就是它，装配漏了就
// 只能看到空列表。默认注册表的五个实现此刻都还没有观测，状态应为 unknown。
func TestNewPipelineWiresProviderHealth(t *testing.T) {
	cfg := config.Config{DigestStore: "memory", ProvidersFile: ""}
	pipe, closeFn, err := newPipeline(discardLogger(), cfg)
	if err != nil {
		t.Fatalf("装配管线失败：%v", err)
	}
	defer closeFn()

	health := pipe.Health()
	if health == nil {
		t.Fatal("管线应带有健康记录器")
	}
	snapshot := health.Snapshot()
	if len(snapshot) != len(pipe.Registry().Entries()) {
		t.Fatalf("快照应覆盖全部登记条目：%d vs %d", len(snapshot), len(pipe.Registry().Entries()))
	}
	for _, st := range snapshot {
		if st.State != provider.StateUnknown {
			t.Errorf("%s 尚未观测，状态应为 unknown，实际 %s", st.ID, st.State)
		}
	}
	if got := health.DownAfter(); got != provider.DefaultDownAfter {
		t.Errorf("默认判死阈值 = %d，期望 %d", got, provider.DefaultDownAfter)
	}
}

// closeTrackingSender 记录是否被关闭，用于验证退出路径会释放渠道资源。
type closeTrackingSender struct {
	channel domain.Channel
	closed  bool
	err     error
}

func (s *closeTrackingSender) Channel() domain.Channel { return s.channel }

func (s *closeTrackingSender) Send(provider.Delivery) provider.Result {
	return provider.Result{Sent: true}
}

func (s *closeTrackingSender) Close() error {
	s.closed = true
	return s.err
}

// 退出时关闭实现了 io.Closer 的渠道实现；没有该方法的保持原样，也不应出错。
func TestCloseProvidersReleasesClosers(t *testing.T) {
	registry := provider.NewRegistry()
	tracker := &closeTrackingSender{channel: domain.ChannelEmail}
	if err := registry.Register(tracker); err != nil {
		t.Fatalf("登记失败：%v", err)
	}
	// console 实现没有 Close，混在同一张表里不应让收尾失败。
	if err := registry.Register(pipeline.NewInAppProvider(discardLogger(), pipeline.DefaultInboxStore(discardLogger()))); err != nil {
		t.Fatalf("登记站内信实现失败：%v", err)
	}

	closeProviders(discardLogger(), registry)
	if !tracker.closed {
		t.Error("实现 io.Closer 的渠道应被关闭")
	}
}

// 日志格式切换：JSON 一行一条、字段可解析；text 保留人类可读的键值形态。
func TestNewLogHandlerJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := newLogHandler(config.Config{LogFormat: "json", LogLevel: "WARN"}, &buf)

	logger.Info("被级别过滤")
	if buf.Len() != 0 {
		t.Fatalf("低于级别的日志不应输出，实际：%s", buf.String())
	}

	logger.Warn("投递失败", "triggerId", "t-1", "channel", "email", "elapsed", 1500*time.Millisecond)
	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("输出不是合法 JSON：%v（%s）", err, buf.String())
	}
	if entry["level"] != "WARN" {
		t.Errorf("level 应为 WARN，实际 %v", entry["level"])
	}
	if entry["msg"] != "投递失败" {
		t.Errorf("msg 应保留原文，实际 %v", entry["msg"])
	}
	if entry["triggerId"] != "t-1" || entry["channel"] != "email" {
		t.Errorf("业务字段应平铺在同一层，实际 %v", entry)
	}
	if _, ok := entry["time"]; !ok {
		t.Errorf("缺少时间字段：%v", entry)
	}
	if entry["elapsed"] != "1.5s" {
		t.Errorf("时长应转成可读字符串，实际 %v", entry["elapsed"])
	}
}

func TestNewLogHandlerText(t *testing.T) {
	var buf bytes.Buffer
	logger := newLogHandler(config.Config{LogFormat: "text"}, &buf)
	logger.Info("hello", "k", "v")

	out := buf.String()
	if !strings.Contains(out, "msg=hello") || !strings.Contains(out, "k=v") {
		t.Fatalf("text 格式应输出键值对，实际：%s", out)
	}
	if json.Valid(bytes.TrimSpace(buf.Bytes())) {
		t.Fatalf("显式 text 格式不应输出 JSON：%s", out)
	}
}
