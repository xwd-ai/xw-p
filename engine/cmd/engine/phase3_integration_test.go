package main

// Phase 3 集成测试：渠道层装配好之后，一条互动事件在「投递、记账、观测」
// 三处是否给出同一个答案。
//
// 单项测试各自只验一层——回退链怎么选、重试怎么退避、明细怎么写、指标怎么
// 累计。这里把整层装到一起，用的就是启动路径上的装配函数 buildRegistry，
// 渠道配置来自一份真实的 providers 文件，邮件落到进程内的 SMTP 服务器，
// 群播报落到进程内的 HTTP 接收端，回调也走真实的 HTTP 提交并按接入方的做法
// 验签——签名不对接收端回 401，事件不会入库。
//
// 三个场景：
//   - post_commented 经四个渠道全部投出：收件箱、SMTP 信封、群机器人报文、
//     执行明细、指标、健康与回调结论一致；
//   - 主实现连不上时先重试再回退：四处记录的都是「最后由谁发出」；
//   - 本地校验失败不回退；运行期把实现全部停用后如实失败，而不是退化成
//     mock 成功。

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/backoff"
	"github.com/xwd-ai/xw-p/engine/internal/config"
	"github.com/xwd-ai/xw-p/engine/internal/execution"
	"github.com/xwd-ai/xw-p/engine/internal/mailer/mailertest"
	"github.com/xwd-ai/xw-p/engine/internal/metrics"
	"github.com/xwd-ai/xw-p/engine/internal/pipeline"
	"github.com/xwd-ai/xw-p/engine/internal/provider"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
	"github.com/xwd-ai/xw-p/engine/internal/runner"
	"github.com/xwd-ai/xw-p/engine/internal/webhook"
)

// callbackSecret 是回调签名用的密钥：接收端按接入方的做法验签，签名不对就
// 返回 401，让「报文没签」在链路上直接失败，而不是被当成一条正常回调。
const callbackSecret = "xw-phase3-integration-callback-secret"

// eventSink 是进程内的回调接收端：记录报文原文、解析后的事件，并逐条验签。
type eventSink struct {
	secret string

	mu       sync.Mutex
	events   []webhook.Event
	raw      []string
	problems []string
}

// signedAll 返回每条回调的验签结论（空切片表示全部通过）。
func (s *eventSink) signatureProblems() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.problems...)
}

func (s *eventSink) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "读取报文失败", http.StatusBadRequest)
		return
	}
	if s.secret != "" {
		verifier, err := webhook.NewVerifier(s.secret)
		if err != nil {
			http.Error(w, "验签器不可用", http.StatusInternalServerError)
			return
		}
		if err := verifier.Verify(r.Header, body); err != nil {
			s.mu.Lock()
			s.problems = append(s.problems, err.Error())
			s.mu.Unlock()
			http.Error(w, "签名校验失败", http.StatusUnauthorized)
			return
		}
	}
	var ev webhook.Event
	if err := json.Unmarshal(body, &ev); err != nil {
		http.Error(w, "报文不是合法 JSON", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.events = append(s.events, ev)
	s.raw = append(s.raw, string(body))
	s.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

// wait 等到至少 n 条事件；回调是异步提交的，直接读会读到空。
//
// 验签失败的请求不会入库，若只等超时，失败信息会停在「没收到」上；这里把
// 验签结论一并检查，签名不对就直接报出原因。
func (s *eventSink) wait(t *testing.T, n int) []webhook.Event {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		got := len(s.events)
		out := append([]webhook.Event(nil), s.events...)
		problems := append([]string(nil), s.problems...)
		s.mu.Unlock()
		if len(problems) != 0 {
			t.Fatalf("回调签名校验未通过：%v", problems)
		}
		if got >= n {
			return out
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.mu.Lock()
	got := len(s.events)
	s.mu.Unlock()
	t.Fatalf("等待回调事件超时：期望 %d 条，实际 %d 条", n, got)
	return nil
}

// phase3Rig 是一条渠道层的完整装配。
type phase3Rig struct {
	t        *testing.T
	pipe     *pipeline.Pipeline
	runner   *runner.Runner
	exec     *execution.JSONLStore
	metrics  *metrics.Registry
	registry *pipeline.ProviderRegistry
	inbox    string
	sink     *eventSink
}

// newPhase3Rig 按给定的渠道配置装配注册表、管线与执行器。
//
// 装配顺序与 cmd/engine 的启动路径一致：配置文件 → buildRegistry →
// pipeline.NewWithRegistry → runner（明细 + 指标 + 回调）。回调的重试在这里
// 关掉，验收点不是重复投递本身，而是回调内容与投递结果一致。
func newPhase3Rig(t *testing.T, providersPath string) *phase3Rig {
	t.Helper()

	dir := t.TempDir()
	inbox := filepath.Join(dir, "inbox.jsonl")
	// 站内信实现按环境变量定位收件箱文件，与引擎共用同一份配置口径。
	t.Setenv("XW_INBOX_FILE", inbox)

	registry, retries, err := buildRegistry(discardLogger(), config.Config{ProvidersFile: providersPath})
	if err != nil {
		t.Fatalf("渠道装配失败：%v", err)
	}

	pipe := pipeline.NewWithRegistry(discardLogger(), registry).WithProviderRetries(retries)

	execStore, err := execution.NewJSONLStore(filepath.Join(dir, "executions.jsonl"))
	if err != nil {
		t.Fatalf("打开执行明细文件失败：%v", err)
	}

	sink := &eventSink{secret: callbackSecret}
	recv := httptest.NewServer(sink)
	t.Cleanup(recv.Close)

	target, err := webhook.ParseTarget(recv.URL + "/callback")
	if err != nil {
		t.Fatalf("回调地址不合法：%v", err)
	}
	// 回调按接入方的约定签名：接收端不掌握密钥就验不出来，因此这里必须
	// 用与接收端同一份密钥构造签名器，否则整条链路根本走不通。
	signer, err := webhook.NewSigner(callbackSecret)
	if err != nil {
		t.Fatalf("构造回调签名器失败：%v", err)
	}
	dispatcher := webhook.NewDispatcher(target, webhook.NewClient(2*time.Second).WithSigner(signer), webhook.Options{
		Workers:   1,
		QueueSize: 32,
		Timeout:   2 * time.Second,
		Retry:     backoff.Policy{MaxAttempts: 1},
		Logger:    discardLogger(),
	})
	if dispatcher == nil {
		t.Fatal("回调分发器装配失败")
	}

	sinkMetrics := metrics.New()
	exec := runner.New(pipe, nil, discardLogger()).
		WithRecorder(execution.NewRecorder(execStore, discardLogger())).
		WithNotifier(dispatcher).
		WithMetrics(sinkMetrics)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := dispatcher.Close(ctx); err != nil {
			t.Logf("回调分发器未能在宽限内排空：%v", err)
		}
		if err := execStore.Close(); err != nil {
			t.Logf("关闭执行明细文件失败：%v", err)
		}
	})

	return &phase3Rig{
		t:        t,
		pipe:     pipe,
		runner:   exec,
		exec:     execStore,
		metrics:  sinkMetrics,
		registry: registry,
		inbox:    inbox,
		sink:     sink,
	}
}

// run 编码再解码后执行一条消息：队列契约的字段名漂移会在这里暴露。
func (r *phase3Rig) run(msg queue.QueuedMessage) runner.Report {
	r.t.Helper()
	raw, err := msg.Encode()
	if err != nil {
		r.t.Fatalf("编码队列消息失败：%v", err)
	}
	decoded, err := queue.Decode(raw)
	if err != nil {
		r.t.Fatalf("解码队列消息失败：%v", err)
	}
	rep, err := r.runner.Run(decoded)
	if err != nil {
		r.t.Fatalf("执行消息失败：%v", err)
	}
	return rep
}

// records 读回已写入的执行明细。
func (r *phase3Rig) records() []execution.Record {
	r.t.Helper()
	rows, err := r.exec.Records()
	if err != nil {
		r.t.Fatalf("读取执行明细失败：%v", err)
	}
	return rows
}

// assertCallbacksSigned 断言接收端收到的每一条回调都通过了验签。
//
// 签名不对时接收端回 401、事件不会入库，等待本身也会失败；这条断言是为了
// 让失败信息直接指出「签名没验过」，而不是停在「回调没到」上。
func (r *phase3Rig) assertCallbacksSigned() {
	r.t.Helper()
	if problems := r.sink.signatureProblems(); len(problems) != 0 {
		r.t.Errorf("回调签名校验未通过：%v", problems)
	}
}

// statsByChannel 按渠道索引指标快照。
func statsByChannel(snap metrics.Snapshot) map[string]metrics.Stat {
	out := make(map[string]metrics.Stat, len(snap.Channels))
	for _, stat := range snap.Channels {
		out[stat.Channel] = stat
	}
	return out
}

// statsByProvider 按实现标识索引指标快照。
func statsByProvider(snap metrics.Snapshot) map[string]metrics.Stat {
	out := make(map[string]metrics.Stat, len(snap.Providers))
	for _, stat := range snap.Providers {
		out[stat.Provider] = stat
	}
	return out
}

// healthOf 返回一条实现的健康快照。
func healthOf(t *testing.T, tracker *pipeline.HealthTracker, id string) pipeline.ProviderStatus {
	t.Helper()
	status, ok := tracker.Status(id)
	if !ok {
		t.Fatalf("健康快照里没有实现 %q", id)
	}
	return status
}

// contactEvent 构造一条社区互动事件的队列消息：消息自带步骤快照，
// 与触发 API 入队时的形态一致。
func contactEvent(trigger, workflow, subscriber string, payload map[string]any, steps []queue.StepSpec) queue.QueuedMessage {
	return queue.QueuedMessage{
		TriggerID:     trigger,
		TransactionID: "tx-" + trigger,
		Name:          workflow,
		SubscriberID:  subscriber,
		Email:         "creator@example.com",
		Phone:         "13800000000",
		DeviceToken:   "device-creator-1",
		Channels:      []string{"in_app", "email", "push", "chat"},
		Payload:       payload,
		Workflow: &queue.WorkflowSnapshot{
			Name:    workflow,
			Version: 6,
			Steps:   steps,
		},
	}
}

// writeProvidersConfig 把渠道声明与重试策略写进临时配置文件。
func writeProvidersConfig(t *testing.T, providers []map[string]any, retries map[string]any) string {
	t.Helper()
	body := map[string]any{"providers": providers}
	if retries != nil {
		body["retries"] = retries
	}
	raw, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		t.Fatalf("序列化渠道配置失败：%v", err)
	}
	return writeProvidersFile(t, string(raw))
}

// readInboxLines 逐行解析收件箱文件。
func readInboxLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("打开收件箱文件失败：%v", err)
	}
	defer file.Close()

	var out []map[string]any
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("收件箱行不是合法 JSON：%v", err)
		}
		out = append(out, row)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("读取收件箱文件失败：%v", err)
	}
	return out
}

// decodeMailBody 还原单段纯文本邮件的正文（base64 折行后传输）。
func decodeMailBody(t *testing.T, raw string) string {
	t.Helper()
	parts := strings.SplitN(raw, "\r\n\r\n", 2)
	if len(parts) != 2 {
		t.Fatalf("邮件报文缺少头与正文的分隔：%q", raw)
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(parts[1]), ""))
	if err != nil {
		t.Fatalf("邮件正文不是 base64：%v", err)
	}
	return string(decoded)
}

// TestPhase3IntegrationChannelsDeliverEverywhere 覆盖一次完整分发：
// 工作流同时落到站内信、邮件、推送与群播报，四个渠道的真实投递都成功，
// 执行明细、指标、健康与回调对「发了四条、四条都成功」给出同一个答案。
func TestPhase3IntegrationChannelsDeliverEverywhere(t *testing.T) {
	mailSrv, err := mailertest.New(mailertest.Config{})
	if err != nil {
		t.Fatalf("启动测试 SMTP 服务器失败：%v", err)
	}
	defer mailSrv.Close()

	var slackMu sync.Mutex
	var slackBodies []string
	slackSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		slackMu.Lock()
		slackBodies = append(slackBodies, string(body))
		slackMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer slackSrv.Close()

	providersPath := writeProvidersConfig(t, []map[string]any{
		{"id": "inapp", "channel": "in_app", "kind": "inapp", "priority": 10},
		{
			"id": "smtp-email", "channel": "email", "kind": "smtp", "priority": 10,
			"credentials": map[string]any{
				"host": "127.0.0.1",
				"port": fmt.Sprintf("%d", mailSrv.Port()),
				// 测试服务器不提供 STARTTLS，显式声明明文连接。
				"tls":  "none",
				"from": "notify@xw.test",
			},
		},
		{"id": "console-push", "channel": "push", "kind": "console", "priority": 10},
		{
			"id": "slack-chat", "channel": "chat", "kind": "slack", "priority": 10,
			"credentials": map[string]any{
				"webhook_url": slackSrv.URL + "/services/T000/B000/XXXX",
				"channel":     "#creator-ops",
			},
		},
	}, map[string]any{
		"default": map[string]any{"max_attempts": 1},
	})

	rig := newPhase3Rig(t, providersPath)

	msg := contactEvent("t-phase3-comment", "post_commented", "creator_1", map[string]any{
		"post_id":        "P-2026-3001",
		"post_title":     "给内容社区写一条通知管线",
		"actor":          "momo",
		"comment":        "这个聚合窗口的写法我得再看两遍",
		"comment_length": 16,
	}, []queue.StepSpec{
		{
			ID: "comment_inapp", Type: "in_app", Name: "评论站内信",
			Config: map[string]any{
				"title":    "{{ payload.actor }} 评论了你的帖子",
				"template": "《{{ payload.post_title }}》：{{ payload.comment }}",
			},
		},
		{
			ID: "comment_email", Type: "email", Name: "评论邮件",
			Config: map[string]any{
				"subject":  "《{{ payload.post_title }}》有新评论",
				"template": "{{ payload.actor }}：{{ payload.comment }}",
			},
		},
		{
			ID: "comment_push", Type: "push", Name: "评论推送",
			Config: map[string]any{
				"title":    "你的帖子有新互动",
				"template": "{{ payload.actor }} 在《{{ payload.post_title }}》下留言",
			},
		},
		{
			ID: "comment_chat", Type: "chat", Name: "社区运营群播报",
			Config: map[string]any{
				"template": "《{{ payload.post_title }}》收到来自 {{ payload.actor }} 的评论",
			},
		},
	})

	rep := rig.run(msg)
	if rep.Sent != 4 || rep.Failed != 0 {
		t.Fatalf("四个渠道都应投递成功，实际 report=%+v", rep)
	}
	if rep.Recorded != 4 {
		t.Errorf("执行明细应写入 4 条，实际 %d 条", rep.Recorded)
	}
	if rep.Notified == 0 {
		t.Error("有投递动作时应产生回调事件")
	}

	// 站内信落盘：标题与正文都经过模板渲染。
	rows := readInboxLines(t, rig.inbox)
	if len(rows) != 1 {
		t.Fatalf("收件箱应有 1 条站内信，实际 %d 条", len(rows))
	}
	if rows[0]["title"] != "momo 评论了你的帖子" {
		t.Errorf("站内信标题 = %v", rows[0]["title"])
	}
	if body, _ := rows[0]["body"].(string); !strings.Contains(body, "这个聚合窗口的写法我得再看两遍") {
		t.Errorf("站内信正文未渲染评论内容：%q", body)
	}

	// 邮件真的进了 SMTP 服务器：信封、主题与正文都对得上。
	mails := mailSrv.Mails()
	if len(mails) != 1 {
		t.Fatalf("SMTP 服务器应收到 1 封邮件，实际 %d 封", len(mails))
	}
	if len(mails[0].To) != 1 || mails[0].To[0] != "creator@example.com" {
		t.Errorf("邮件收件人 = %v", mails[0].To)
	}
	if mails[0].From != "notify@xw.test" {
		t.Errorf("邮件发件人 = %q", mails[0].From)
	}
	if body := decodeMailBody(t, mails[0].Data); !strings.Contains(body, "momo：这个聚合窗口的写法我得再看两遍") {
		t.Errorf("邮件正文未渲染：%q", body)
	}

	// 群播报落到 webhook 接收端，频道覆盖项生效。
	slackMu.Lock()
	bodies := append([]string(nil), slackBodies...)
	slackMu.Unlock()
	if len(bodies) != 1 {
		t.Fatalf("群机器人应收到 1 次提交，实际 %d 次", len(bodies))
	}
	if !strings.Contains(bodies[0], "#creator-ops") {
		t.Errorf("群播报未带频道覆盖：%s", bodies[0])
	}
	if !strings.Contains(bodies[0], "momo") {
		t.Errorf("群播报未包含互动者：%s", bodies[0])
	}

	// 执行明细：一条渠道一行，provider 是实际完成投递的实现标识。
	records := rig.records()
	if len(records) != 4 {
		t.Fatalf("执行明细应有 4 条，实际 %d 条", len(records))
	}
	wantProvider := map[string]string{
		"in_app": "inapp",
		"email":  "smtp-email",
		"push":   "console-push",
		"chat":   "slack-chat",
	}
	for _, rec := range records {
		if rec.Status != execution.StatusSent {
			t.Errorf("渠道 %s 的明细状态 = %s，原因 %v", rec.Channel, rec.Status, rec.Reason)
		}
		if want := wantProvider[rec.Channel]; rec.Provider != want {
			t.Errorf("渠道 %s 的明细 provider = %q, 期望 %q", rec.Channel, rec.Provider, want)
		}
		if rec.Fallback {
			t.Errorf("渠道 %s 没有备选可用，不应标记回退", rec.Channel)
		}
		if rec.TriggerID != "t-phase3-comment" || rec.Workflow != "post_commented" {
			t.Errorf("明细的溯源字段不正确：%+v", rec)
		}
	}

	// 指标：四个渠道各一次成功，无失败。
	snap := rig.metrics.Snapshot()
	if snap.Total.Attempted != 4 || snap.Total.Sent != 4 || snap.Total.Failed != 0 {
		t.Errorf("指标合计 = %+v，期望 4 次全成功", snap.Total)
	}
	byChannel := statsByChannel(snap)
	for _, ch := range []string{"in_app", "email", "push", "chat"} {
		stat, ok := byChannel[ch]
		if !ok {
			t.Errorf("指标里缺少渠道 %s", ch)
			continue
		}
		if stat.Sent != 1 || stat.Failed != 0 {
			t.Errorf("渠道 %s 的指标 = %+v", ch, stat)
		}
	}

	// 健康：投递结果就是最直接的观测来源，四个实现都应转为 up。
	for _, id := range []string{"inapp", "smtp-email", "console-push", "slack-chat"} {
		status := healthOf(t, rig.pipe.Health(), id)
		if status.State != provider.StateUp {
			t.Errorf("实现 %s 的健康状态 = %s，期望 up", id, status.State)
		}
		if status.Successes < 1 {
			t.Errorf("实现 %s 应有成功观测，实际 %+v", id, status)
		}
	}

	// 回调：一条事件、四条明细，整体状态 sent，且接收端验签通过。
	events := rig.sink.wait(t, 1)
	rig.assertCallbacksSigned()
	ev := events[0]
	if ev.Event != webhook.EventDeliveryResult || ev.Status != webhook.StatusSent {
		t.Errorf("回调事件 = %s / %s", ev.Event, ev.Status)
	}
	if ev.Summary == nil || ev.Summary.Total != 4 || ev.Summary.Sent != 4 || ev.Summary.Failed != 0 {
		t.Errorf("回调汇总 = %+v", ev.Summary)
	}
	if ev.TriggerID != "t-phase3-comment" || ev.Workflow != "post_commented" {
		t.Errorf("回调的溯源字段不正确：%+v", ev)
	}
	for _, d := range ev.Deliveries {
		if d.Status != webhook.StatusSent {
			t.Errorf("回调明细 %s 的状态 = %s", d.Channel, d.Status)
		}
	}
}

// TestPhase3IntegrationRetryThenFailover 覆盖主实现故障：SMTP 指向一个没有
// 监听的端口，按渠道策略重试一次后回退到 console 实现。四条记录链路——
// 步骤结果、执行明细、指标、回调——都应指向「最后由备选发出」，健康记录
// 则保留主实现的连续失败。
func TestPhase3IntegrationRetryThenFailover(t *testing.T) {
	providersPath := writeProvidersConfig(t, []map[string]any{
		{
			"id": "smtp-email", "channel": "email", "kind": "smtp", "priority": 10,
			"credentials": map[string]any{
				"host": "127.0.0.1",
				// 1 端口没有服务监听，连接会立刻被拒，不必等超时。
				"port": "1",
				"tls":  "none",
				"from": "notify@xw.test",
			},
		},
		{"id": "console-email", "channel": "email", "kind": "console", "priority": 20},
		{"id": "inapp", "channel": "in_app", "kind": "inapp", "priority": 10},
	}, map[string]any{
		"default": map[string]any{"max_attempts": 1},
		"email": map[string]any{
			"max_attempts": 2,
			"base_delay":   "1ms",
			"max_delay":    "5ms",
			"jitter":       0,
		},
	})

	rig := newPhase3Rig(t, providersPath)

	msg := contactEvent("t-phase3-like", "post_liked", "creator_1", map[string]any{
		"post_id":    "P-2026-3002",
		"post_title": "聚合卡片的窗口边界怎么定",
		"actor":      "老周",
		"like_count": 128,
	}, []queue.StepSpec{
		{
			ID: "like_email", Type: "email", Name: "点赞邮件",
			Config: map[string]any{
				"subject":  "《{{ payload.post_title }}》收到了新的点赞",
				"template": "{{ payload.actor }} 赞了你的帖子",
			},
		},
		{
			ID: "like_inapp", Type: "in_app", Name: "点赞站内信",
			Config: map[string]any{
				"title":    "{{ payload.actor }} 赞了你的帖子",
				"template": "《{{ payload.post_title }}》累计 {{ payload.like_count }} 个赞",
			},
		},
	})

	rep := rig.run(msg)
	if rep.Sent != 2 || rep.Failed != 0 {
		t.Fatalf("回退后两个步骤都应成功，实际 report=%+v", rep)
	}

	// 执行明细：邮件由备选完成，尝试序列保留主实现的两次失败。
	records := rig.records()
	if len(records) != 2 {
		t.Fatalf("执行明细应有 2 条，实际 %d 条", len(records))
	}
	var emailRec *execution.Record
	for i := range records {
		if records[i].Channel == "email" {
			emailRec = &records[i]
		}
	}
	if emailRec == nil {
		t.Fatal("执行明细里没有邮件渠道")
	}
	if emailRec.Status != execution.StatusSent || emailRec.Provider != "console-email" {
		t.Errorf("邮件明细应由备选完成，实际 %+v", *emailRec)
	}
	if !emailRec.Fallback {
		t.Error("邮件明细应标记回退")
	}
	wantAttempts := []string{"smtp-email", "smtp-email", "console-email"}
	if strings.Join(emailRec.Attempts, ",") != strings.Join(wantAttempts, ",") {
		t.Errorf("尝试序列 = %v, 期望 %v", emailRec.Attempts, wantAttempts)
	}

	// 指标：合计两次投递（邮件一次成功、站内信一次成功），重试与回退各计一次。
	// 实现维度按「最终完成投递的那一个」记账——失败的主实现记在健康里，
	// 指标回答的是「这条渠道最后由谁发出去」。
	snap := rig.metrics.Snapshot()
	if snap.Total.Attempted != 2 || snap.Total.Sent != 2 {
		t.Errorf("指标合计 = %+v", snap.Total)
	}
	if snap.Total.Retried < 1 {
		t.Errorf("指标未记录重试次数：%+v", snap.Total)
	}
	if snap.Total.Fallback != 1 {
		t.Errorf("指标未记录回退次数：%+v", snap.Total)
	}
	byProvider := statsByProvider(snap)
	if stat, ok := byProvider["smtp-email"]; ok && stat.Attempted != 0 {
		t.Errorf("主实现没有完成投递，不应出现在实现维度：%+v", stat)
	}
	if stat := byProvider["console-email"]; stat.Sent != 1 || stat.Failed != 0 {
		t.Errorf("备选的指标 = %+v, 期望一次成功", stat)
	}

	// 健康：主实现连续两次失败，状态不再是 up；备选因成功投递转为 up。
	smtpStatus := healthOf(t, rig.pipe.Health(), "smtp-email")
	if smtpStatus.Failures != 2 || smtpStatus.ConsecutiveFailures != 2 {
		t.Errorf("主实现的健康记录 = %+v, 期望两次失败", smtpStatus)
	}
	if smtpStatus.State == provider.StateUp {
		t.Errorf("连续失败后主实现不应仍是 up：%+v", smtpStatus)
	}
	if backup := healthOf(t, rig.pipe.Health(), "console-email"); backup.State != provider.StateUp {
		t.Errorf("备选健康状态 = %s, 期望 up", backup.State)
	}

	// 回调：整体成功，明细里给出实际完成投递的实现与尝试序列。
	events := rig.sink.wait(t, 1)
	rig.assertCallbacksSigned()
	ev := events[0]
	if ev.Status != webhook.StatusSent {
		t.Errorf("回调整体状态 = %s, 期望 sent", ev.Status)
	}
	var emailDelivery *webhook.Delivery
	for i := range ev.Deliveries {
		if ev.Deliveries[i].Channel == "email" {
			emailDelivery = &ev.Deliveries[i]
		}
	}
	if emailDelivery == nil {
		t.Fatal("回调明细里没有邮件渠道")
	}
	if emailDelivery.Provider != "console-email" || !emailDelivery.Fallback {
		t.Errorf("回调明细未指向备选：%+v", *emailDelivery)
	}
	if len(emailDelivery.Attempts) != 3 {
		t.Errorf("回调明细的尝试次数 = %d, 期望 3", len(emailDelivery.Attempts))
	}
}

// TestPhase3IntegrationTerminalFailureAndDisabled 覆盖两条「不该假装成功」的
// 路径：本地校验失败（号码非法）不触发回退；运行期停用渠道的全部实现后，
// 投递如实失败并写明原因，而不是退化成未注册实现时的 mock 打印。
func TestPhase3IntegrationTerminalFailureAndDisabled(t *testing.T) {
	providersPath := writeProvidersConfig(t, []map[string]any{
		{"id": "sms-primary", "channel": "sms", "kind": "console", "priority": 10},
		{"id": "sms-standby", "channel": "sms", "kind": "console", "priority": 20},
	}, map[string]any{
		"default": map[string]any{"max_attempts": 3, "base_delay": "1ms", "max_delay": "2ms", "jitter": 0},
	})

	rig := newPhase3Rig(t, providersPath)

	steps := []queue.StepSpec{{
		ID: "mention_sms", Type: "sms", Name: "提及短信",
		Config: map[string]any{
			"template": "{{ payload.actor }} 在评论里提到了你",
		},
	}}

	// 号码非法属于「换哪个实现都一样」的失败：不该重试，也不该换备选。
	badNumber := contactEvent("t-phase3-sms-bad", "post_mentioned", "creator_1", map[string]any{
		"post_id":    "P-2026-3003",
		"post_title": "社区互动的触达策略",
		"actor":      "小林",
	}, steps)
	badNumber.Phone = "creator-hotline"

	rep := rig.run(badNumber)
	if rep.Failed != 1 || rep.Sent != 0 {
		t.Fatalf("号码非法时应失败且不短路计数混在一起，实际 report=%+v", rep)
	}

	records := rig.records()
	if len(records) != 1 {
		t.Fatalf("执行明细应有 1 条，实际 %d 条", len(records))
	}
	if records[0].Status != execution.StatusFailed {
		t.Errorf("明细状态 = %s, 期望 failed", records[0].Status)
	}
	if records[0].Reason == nil || !strings.Contains(*records[0].Reason, "手机号") {
		t.Errorf("明细失败原因 = %v", records[0].Reason)
	}
	if len(records[0].Attempts) != 1 || records[0].Attempts[0] != "sms-primary" {
		t.Errorf("本地校验失败只应尝试主实现，实际 %v", records[0].Attempts)
	}
	if records[0].Fallback {
		t.Error("本地校验失败不应标记回退")
	}

	// 备选没有被调用：指标里只有主实现的一条失败记录。
	snap := rig.metrics.Snapshot()
	byProvider := statsByProvider(snap)
	if stat := byProvider["sms-primary"]; stat.Failed != 1 {
		t.Errorf("主实现的指标 = %+v, 期望一次失败", stat)
	}
	if _, ok := byProvider["sms-standby"]; ok {
		t.Error("本地校验失败不应尝试备选实现")
	}

	events := rig.sink.wait(t, 1)
	rig.assertCallbacksSigned()
	if events[0].Status != webhook.StatusFailed {
		t.Errorf("回调整体状态 = %s, 期望 failed", events[0].Status)
	}

	// 停用全部实现：投递必须失败并写明原因，不能退化成 mock 成功。
	for _, id := range []string{"sms-primary", "sms-standby"} {
		if err := rig.registry.SetEnabled(id, false); err != nil {
			t.Fatalf("停用实现 %s 失败：%v", id, err)
		}
	}

	validNumber := contactEvent("t-phase3-sms-off", "post_mentioned", "creator_1", map[string]any{
		"post_id":    "P-2026-3003",
		"post_title": "社区互动的触达策略",
		"actor":      "小林",
	}, steps)
	validNumber.Phone = "13800000000"

	rep = rig.run(validNumber)
	if rep.Failed != 1 || rep.Sent != 0 {
		t.Fatalf("实现全部停用时投递应失败，实际 report=%+v", rep)
	}

	records = rig.records()
	last := records[len(records)-1]
	if last.Status != execution.StatusFailed {
		t.Errorf("停用后的明细状态 = %s, 期望 failed", last.Status)
	}
	if last.Reason == nil || !strings.Contains(*last.Reason, "均已停用") {
		t.Errorf("停用后的失败原因 = %v", last.Reason)
	}
	if last.Provider != "" || len(last.Attempts) != 0 {
		t.Errorf("没有可用的实现时不该记录投递实现：%+v", last)
	}

	if status := healthOf(t, rig.pipe.Health(), "sms-standby"); status.Enabled {
		t.Error("健康快照应反映实现已停用")
	}

	events = rig.sink.wait(t, 2)
	rig.assertCallbacksSigned()
	if events[1].Status != webhook.StatusFailed {
		t.Errorf("停用后的回调状态 = %s, 期望 failed", events[1].Status)
	}
	if len(events[1].Deliveries) != 1 || events[1].Deliveries[0].Reason == "" {
		t.Errorf("停用后的回调明细应带失败原因：%+v", events[1].Deliveries)
	}
}
