package pipeline

// Phase 2 集成测试：把工作流的各项能力放进同一条互动链路里一起验证。
//
// 与单项能力的单元测试相比，这里关心的是「组合起来是否还成立」：
//   - 入口是 Python 侧写进队列的原始 JSON（用 queue.Decode 还原，字段名漂移会
//     直接被抓到），出口是落盘的收件箱 JSONL 与 provider 收到的真实投递请求；
//   - 一条 post_commented 工作流同时包含过滤、条件分支、模板渲染、delay 续跑
//     与多渠道分发，验证步骤之间不会互相覆盖上下文或结果；
//   - 渠道失败要能中断后续步骤，不能留下「邮件发不出去、推送却照发」的错位状态。
//
// 三个场景分别对应：短评论走「站内信 → 延迟 → 邮件」、长评论额外触发即时推送、
// 邮件渠道故障时的短路与恢复。

import (
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// recordingProvider 记录收到的投递请求，可配置为固定失败。
type recordingProvider struct {
	ch         domain.Channel
	failReason string // 非空时模拟渠道故障

	mu         sync.Mutex
	deliveries []Delivery
}

func (p *recordingProvider) Channel() domain.Channel { return p.ch }

func (p *recordingProvider) Send(d Delivery) SendResult {
	if p.failReason != "" {
		return SendResult{Sent: false, Reason: p.failReason}
	}
	p.mu.Lock()
	p.deliveries = append(p.deliveries, d)
	p.mu.Unlock()
	return SendResult{Sent: true, Reason: "测试渠道已接收"}
}

// Deliveries 返回已收到的投递请求副本。
func (p *recordingProvider) Deliveries() []Delivery {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Delivery, len(p.deliveries))
	copy(out, p.deliveries)
	return out
}

// Last 返回最后一次投递请求；没有投递时返回零值。
func (p *recordingProvider) Last() Delivery {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.deliveries) == 0 {
		return Delivery{}
	}
	return p.deliveries[len(p.deliveries)-1]
}

// phase2Rig 是一条完整链路的装配：真实收件箱文件 + 记录型外部渠道 + 延迟任务收集器。
type phase2Rig struct {
	pipe      *Pipeline
	inboxPath string
	now       time.Time
	providers map[domain.Channel]*recordingProvider

	mu        sync.Mutex
	deferrals []Deferral
}

func newPhase2Rig(t *testing.T) *phase2Rig {
	t.Helper()

	inboxPath := filepath.Join(t.TempDir(), "inbox.jsonl")
	store, err := NewJSONLInboxStore(inboxPath)
	if err != nil {
		t.Fatalf("打开收件箱文件失败: %v", err)
	}

	registry := NewProviderRegistry()
	registry.Register(NewInAppProvider(discardLogger(), store))

	rig := &phase2Rig{
		inboxPath: inboxPath,
		now:       time.Date(2026, 10, 28, 9, 0, 0, 0, time.UTC),
		providers: make(map[domain.Channel]*recordingProvider),
	}
	for _, ch := range []domain.Channel{
		domain.ChannelEmail, domain.ChannelSMS, domain.ChannelPush, domain.ChannelChat,
	} {
		p := &recordingProvider{ch: ch}
		rig.providers[ch] = p
		registry.Register(p)
	}

	rig.pipe = NewWithRegistry(discardLogger(), registry).
		WithClock(func() time.Time { return rig.now }).
		WithDeferrer(DeferrerFunc(func(d Deferral) error {
			rig.mu.Lock()
			defer rig.mu.Unlock()
			rig.deferrals = append(rig.deferrals, d)
			return nil
		}))
	return rig
}

func (r *phase2Rig) provider(ch domain.Channel) *recordingProvider {
	return r.providers[ch]
}

// deferred 返回已登记的延迟续跑任务副本。
func (r *phase2Rig) deferred() []Deferral {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Deferral, len(r.deferrals))
	copy(out, r.deferrals)
	return out
}

// commentEventJSON 构造一条评论事件的队列 JSON，字段与 Python 侧 QueuedMessage 一致。
// extra 用于按场景补充 payload 字段。
func commentEventJSON(t *testing.T, trigger, actor, comment string, extra map[string]any) string {
	t.Helper()

	payload := map[string]any{
		"post_id":          "P-2026-3001",
		"post_title":       "给内容社区写一条通知管线",
		"actor":            actor,
		"comment":          comment,
		"comment_length":   utf8.RuneCountInString(comment),
		"subscriber_id":    "creator_1",
		"actor_is_creator": actor == "creator_1",
	}
	for k, v := range extra {
		payload[k] = v
	}

	raw, err := json.Marshal(map[string]any{
		"trigger_id":     trigger,
		"transaction_id": "tx-" + trigger,
		"name":           "post_commented",
		"subscriber_id":  "creator_1",
		"email":          "creator@example.com",
		"phone":          "13800000000",
		"device_token":   "device-creator-1",
		"channels":       []string{"in_app", "push", "email"},
		"payload":        payload,
	})
	if err != nil {
		t.Fatalf("构造队列 JSON 失败: %v", err)
	}
	return string(raw)
}

// commentWorkflow 是评论触达工作流：站内信 → 长评论推送（条件）→ 延迟 → 邮件。
func commentWorkflow() domain.Workflow {
	return domain.Workflow{
		ID:      "wf_post_commented",
		Name:    "post_commented",
		Version: 4,
		Active:  true,
		Steps: []domain.Step{
			{
				ID:   "comment_inapp",
				Type: domain.ChannelInApp,
				Name: "评论站内信",
				Config: map[string]any{
					"title":    "{{ payload.actor }} 评论了你的帖子",
					"template": "《{{ payload.post_title }}》：{{ payload.comment }}",
					// 创作者自己的评论不打扰自己。
					"filters": []any{
						map[string]any{"field": "payload.actor", "operator": "ne", "value": "creator_1"},
					},
				},
			},
			{
				ID:   "comment_push",
				Type: domain.ChannelPush,
				Name: "长评论推送",
				Config: map[string]any{
					"title":    "你的帖子收到长评论",
					"template": "{{ payload.actor }} 在你的帖子下留了 {{ payload.comment_length }} 个字的评论",
					// 只有长评论才值得打断创作者。
					"if": map[string]any{
						"field": "payload.comment_length", "operator": "gte", "value": 80,
					},
				},
			},
			{
				ID:     "comment_delay",
				Type:   domain.ChannelDelay,
				Name:   "十分钟后补发邮件",
				Config: map[string]any{"minutes": 10},
			},
			{
				ID:   "comment_email",
				Type: domain.ChannelEmail,
				Name: "评论邮件",
				Config: map[string]any{
					"subject":  "《{{ payload.post_title }}》有新评论",
					"template": "{{ upper payload.actor }} 说：{{ payload.comment }}",
				},
			},
		},
	}
}

// TestPhase2IntegrationCommentChainResumesAfterDelay 覆盖短评论的完整链路：
// 队列 JSON → 站内信落盘 → 推送条件不满足被跳过 → delay 登记续跑 →
// 调度器续跑后发出邮件。断言每一步的结果、落盘内容与续跑任务的编码往返。
func TestPhase2IntegrationCommentChainResumesAfterDelay(t *testing.T) {
	rig := newPhase2Rig(t)
	wf := commentWorkflow()

	raw := commentEventJSON(t, "t-comment-1", "momo", "这个聚合窗口的写法我得再看两遍", nil)
	msg := decodeEvent(t, raw)
	if msg.Name != "post_commented" || msg.Email == "" || msg.DeviceToken == "" {
		t.Fatalf("队列消息还原不正确: %+v", msg)
	}

	results := rig.pipe.Run(NewContext(msg), wf)
	if len(results) != 3 {
		t.Fatalf("短评论应执行 3 个步骤（邮件留给续跑），实际 %d 个: %+v", len(results), results)
	}

	inapp, push, delayRes := results[0], results[1], results[2]
	if inapp.StepID != "comment_inapp" || !inapp.Sent {
		t.Errorf("站内信应投递成功，实际 %+v", inapp)
	}
	if !strings.Contains(inapp.Rendered, "这个聚合窗口的写法我得再看两遍") {
		t.Errorf("站内信正文未渲染评论内容: %q", inapp.Rendered)
	}
	if !push.Skipped || push.SkipReason != "condition" {
		t.Errorf("短评论不应触发推送，实际 %+v", push)
	}
	if n := len(rig.provider(domain.ChannelPush).Deliveries()); n != 0 {
		t.Errorf("条件不满足时不应调用推送渠道，实际 %d 次", n)
	}
	if !delayRes.Deferred {
		t.Fatalf("delay 步骤应登记续跑，实际 %+v", delayRes)
	}
	if want := rig.now.Add(10 * time.Minute); !delayRes.DeferredUntil.Equal(want) {
		t.Errorf("到期时刻 = %s, 期望 %s", delayRes.DeferredUntil, want)
	}

	// 站内信落盘：标题与正文都来自步骤模板，归属字段指向创作者。
	rows := readInboxRaw(t, rig.inboxPath)
	if len(rows) != 1 {
		t.Fatalf("收件箱应有 1 条站内信，实际 %d 条", len(rows))
	}
	if rows[0]["title"] != "momo 评论了你的帖子" {
		t.Errorf("站内信标题 = %v", rows[0]["title"])
	}
	if rows[0]["subscriber_id"] != "creator_1" || rows[0]["workflow"] != "post_commented" {
		t.Errorf("站内信归属字段不正确: %v", rows[0])
	}

	// 调度器拿到的续跑任务：消息原样保留，剩余步骤只剩邮件。
	deferrals := rig.deferred()
	if len(deferrals) != 1 {
		t.Fatalf("应登记 1 个延迟任务，实际 %d 个", len(deferrals))
	}
	if deferrals[0].StepID != "comment_delay" || deferrals[0].WorkflowName != "post_commented" {
		t.Errorf("续跑任务归属不正确: %+v", deferrals[0])
	}
	if len(deferrals[0].Remaining) != 1 || deferrals[0].Remaining[0].ID != "comment_email" {
		t.Errorf("续跑剩余步骤应只有邮件，实际 %+v", deferrals[0].Remaining)
	}
	if deferrals[0].Message.TriggerID != "t-comment-1" {
		t.Errorf("续跑任务丢失原始触发 ID: %+v", deferrals[0].Message)
	}

	// 编码往返：调度器按 JSON 存储任务，续跑时从 JSON 还原。
	payload, err := deferrals[0].Encode()
	if err != nil {
		t.Fatalf("续跑任务编码失败: %v", err)
	}
	decoded, resumed, err := rig.pipe.Resume(payload)
	if err != nil {
		t.Fatalf("续跑失败: %v", err)
	}
	if decoded.StepID != "comment_delay" {
		t.Errorf("还原后的延迟步骤 = %q", decoded.StepID)
	}
	if len(resumed) != 1 || resumed[0].StepID != "comment_email" || !resumed[0].Sent {
		t.Fatalf("续跑应发出 1 封邮件，实际 %+v", resumed)
	}

	email := rig.provider(domain.ChannelEmail).Last()
	if email.To != "creator@example.com" {
		t.Errorf("邮件收件人 = %q", email.To)
	}
	if email.Subject != "《给内容社区写一条通知管线》有新评论" {
		t.Errorf("邮件标题未渲染: %q", email.Subject)
	}
	if !strings.HasPrefix(email.Body, "MOMO 说：") {
		t.Errorf("邮件正文应经 upper 函数处理，实际 %q", email.Body)
	}
	if email.TriggerID != "t-comment-1" || email.StepID != "comment_email" {
		t.Errorf("邮件溯源字段不正确: %+v", email)
	}

	// 续跑不应重复写站内信（同一条消息只落一次）。
	if rows := readInboxRaw(t, rig.inboxPath); len(rows) != 1 {
		t.Errorf("续跑后收件箱仍应只有 1 条，实际 %d 条", len(rows))
	}
}

// TestPhase2IntegrationLongCommentTriggersPush 覆盖长评论：同一套工作流下
// 条件分支放行推送，推送与站内信各自拿到正确的收件地址与渲染结果。
func TestPhase2IntegrationLongCommentTriggersPush(t *testing.T) {
	rig := newPhase2Rig(t)
	wf := commentWorkflow()

	longComment := strings.Repeat("把窗口边界和重放一起想清楚，", 10)
	msg := decodeEvent(t, commentEventJSON(t, "t-comment-2", "老周", longComment, nil))

	results := rig.pipe.Run(NewContext(msg), wf)
	if len(results) != 3 {
		t.Fatalf("应执行 3 个步骤（邮件留给续跑），实际 %d 个: %+v", len(results), results)
	}
	if !results[0].Sent {
		t.Errorf("站内信应投递成功，实际 %+v", results[0])
	}
	push := results[1]
	if push.Skipped || !push.Sent {
		t.Fatalf("长评论应触发推送，实际 %+v", push)
	}

	deliveries := rig.provider(domain.ChannelPush).Deliveries()
	if len(deliveries) != 1 {
		t.Fatalf("推送渠道应收到 1 次投递，实际 %d 次", len(deliveries))
	}
	if deliveries[0].To != "device-creator-1" {
		t.Errorf("推送目标 = %q, 期望设备令牌", deliveries[0].To)
	}
	wantLen := utf8.RuneCountInString(longComment)
	if !strings.Contains(deliveries[0].Body, "个字") ||
		!strings.Contains(deliveries[0].Body, strconv.Itoa(wantLen)) {
		t.Errorf("推送正文未渲染评论字数（期望 %d）: %q", wantLen, deliveries[0].Body)
	}

	// 延迟与续跑仍然成立：邮件不会因为前一步推送而漏发。
	if !results[2].Deferred {
		t.Fatalf("delay 步骤应登记续跑，实际 %+v", results[2])
	}
	if _, resumed, err := rig.pipe.Resume(rig.deferredPayload(t)); err != nil || len(resumed) != 1 || !resumed[0].Sent {
		t.Fatalf("续跑应发出邮件: err=%v results=%+v", err, resumed)
	}
	if body := rig.provider(domain.ChannelEmail).Last().Body; !strings.Contains(body, "老周 说：") {
		t.Errorf("邮件正文未正确渲染: %q", body)
	}
}

// TestPhase2IntegrationFailureStopsChainAndRecovers 覆盖渠道故障：
// 邮件渠道失败后，其后的 delay 与推送都不执行（不登记延迟任务），
// 站内信已经落盘；故障恢复后重跑同一条消息，整条链路补完且不重复写站内信。
func TestPhase2IntegrationFailureStopsChainAndRecovers(t *testing.T) {
	rig := newPhase2Rig(t)
	rig.provider(domain.ChannelEmail).failReason = "SMTP 连接被拒绝"

	wf := domain.Workflow{
		ID:      "wf_post_mentioned",
		Name:    "post_mentioned",
		Version: 2,
		Active:  true,
		Steps: []domain.Step{
			{
				ID:   "mention_inapp",
				Type: domain.ChannelInApp,
				Name: "提及站内信",
				Config: map[string]any{
					"title":    "{{ payload.actor }} 在评论里提到了你",
					"template": "《{{ payload.post_title }}》：{{ payload.comment }}",
				},
			},
			{
				ID:   "mention_email",
				Type: domain.ChannelEmail,
				Name: "提及邮件",
				Config: map[string]any{
					"subject":  "有人在《{{ payload.post_title }}》里提到了你",
					"template": "{{ payload.actor }}：{{ payload.comment }}",
				},
			},
			{
				ID:     "mention_delay",
				Type:   domain.ChannelDelay,
				Name:   "五分钟后再推送",
				Config: map[string]any{"minutes": 5},
			},
			{
				ID:   "mention_push",
				Type: domain.ChannelPush,
				Name: "提及推送",
				Config: map[string]any{
					"title":    "有人提到了你",
					"template": "{{ payload.actor }} 在《{{ payload.post_title }}》里提到了你",
				},
			},
		},
	}

	msg := decodeEvent(t, commentEventJSON(t, "t-mention-1", "小林", "@creator_1 这版窗口设计稳吗", nil))
	results := rig.pipe.Run(NewContext(msg), wf)
	if len(results) != 4 {
		t.Fatalf("应返回 4 个步骤结果（含被短路的步骤），实际 %d 个: %+v", len(results), results)
	}
	if !results[0].Sent {
		t.Errorf("站内信应投递成功，实际 %+v", results[0])
	}
	if results[1].Error == "" || !strings.Contains(results[1].Error, "SMTP 连接被拒绝") {
		t.Errorf("邮件步骤应记录渠道失败原因，实际 %+v", results[1])
	}
	for _, res := range results[2:] {
		if !res.ShortCircuited {
			t.Errorf("上游失败后步骤 %s 应被短路，实际 %+v", res.StepID, res)
		}
		if res.Error == "" {
			t.Errorf("被短路的步骤 %s 应带上短路原因", res.StepID)
		}
	}
	if n := len(rig.deferred()); n != 0 {
		t.Errorf("链路中断时不应登记延迟任务，实际 %d 个", n)
	}
	if n := len(rig.provider(domain.ChannelPush).Deliveries()); n != 0 {
		t.Errorf("链路中断时不应调用推送渠道，实际 %d 次", n)
	}
	if rows := readInboxRaw(t, rig.inboxPath); len(rows) != 1 {
		t.Fatalf("故障前已投递的站内信应保留，实际 %d 条", len(rows))
	}

	// 渠道恢复：重放同一条消息，链路补完，站内信按条目 ID 幂等去重。
	rig.provider(domain.ChannelEmail).failReason = ""
	results = rig.pipe.Run(NewContext(msg), wf)
	for _, res := range results {
		if res.Error != "" && !res.ShortCircuited {
			t.Fatalf("恢复后步骤 %s 仍失败: %s", res.StepID, res.Error)
		}
	}
	if !results[1].Sent {
		t.Errorf("恢复后邮件应投递成功，实际 %+v", results[1])
	}
	if len(rig.deferred()) != 1 {
		t.Fatalf("恢复后应登记 1 个延迟任务，实际 %d 个", len(rig.deferred()))
	}
	if rows := readInboxRaw(t, rig.inboxPath); len(rows) != 1 {
		t.Errorf("重放不应新增站内信，实际 %d 条", len(rows))
	}

	_, push, err := rig.pipe.Resume(rig.deferredPayload(t))
	if err != nil || len(push) != 1 {
		t.Fatalf("续跑失败: err=%v results=%+v", err, push)
	}
	if !push[0].Sent {
		t.Errorf("续跑后的推送应投递成功，实际 %+v", push[0])
	}
	if to := rig.provider(domain.ChannelPush).Last().To; to != "device-creator-1" {
		t.Errorf("续跑推送目标 = %q", to)
	}
}

// deferredPayload 编码第一个延迟任务，模拟调度器保存的任务载荷。
func (r *phase2Rig) deferredPayload(t *testing.T) []byte {
	t.Helper()
	deferred := r.deferred()
	if len(deferred) == 0 {
		t.Fatal("没有已登记的延迟任务")
	}
	payload, err := deferred[0].Encode()
	if err != nil {
		t.Fatalf("延迟任务编码失败: %v", err)
	}
	return payload
}
