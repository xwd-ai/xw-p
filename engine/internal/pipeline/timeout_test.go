package pipeline

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// 超时测试：验证单步超时能中断等待、整体预算能拦住后续步骤、
// 步骤级配置能覆盖默认值，且超时不会被误当成「已投递」。

// blockingProvider 阻塞到 context 结束，模拟下游不应答的渠道。
// 它实现 ContextSender，因此超时能真正中断等待（而不是放弃后请求仍在跑）。
type blockingProvider struct {
	channel domain.Channel
	called  chan struct{}
	once    sync.Once
	// finished 在观察结果写完之后关闭：管线在超时那一刻就返回，provider 的
	// goroutine 还要过一会儿才走到写回，用例读状态前必须等这一下。
	finished chan struct{}
	doneOnce sync.Once

	mu       sync.Mutex
	observed error
}

func newBlockingProvider(ch domain.Channel) *blockingProvider {
	return &blockingProvider{
		channel:  ch,
		called:   make(chan struct{}),
		finished: make(chan struct{}),
	}
}

func (b *blockingProvider) Channel() domain.Channel { return b.channel }

// Send 不应被调用：dispatch 会优先选 SendContext。真被调用时如实报错。
func (b *blockingProvider) Send(Delivery) SendResult {
	return SendResult{Sent: false, Reason: "该 provider 只支持 SendContext"}
}

func (b *blockingProvider) SendContext(ctx context.Context, _ Delivery) SendResult {
	b.once.Do(func() { close(b.called) })
	<-ctx.Done()
	defer b.doneOnce.Do(func() { close(b.finished) })

	b.mu.Lock()
	b.observed = ctx.Err()
	b.mu.Unlock()
	return SendResult{Sent: false, Reason: ctx.Err().Error()}
}

// observedErr 返回 provider 观察到的 context 结束原因。
func (b *blockingProvider) observedErr() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.observed
}

// waitFinished 等待 provider 走完一次 SendContext（写完观察结果）。
func (b *blockingProvider) waitFinished(timeout time.Duration) bool {
	select {
	case <-b.finished:
		return true
	case <-time.After(timeout):
		return false
	}
}

// sleepProvider 不感知取消，只按固定时长睡眠后返回成功：
// 用于验证「超时后管线不再等待」这一兜底语义。
type sleepProvider struct {
	channel domain.Channel
	delay   time.Duration
	mu      sync.Mutex
	calls   int
}

func (s *sleepProvider) Channel() domain.Channel { return s.channel }

func (s *sleepProvider) Send(Delivery) SendResult {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	time.Sleep(s.delay)
	return SendResult{Sent: true, Reason: "mock"}
}

func (s *sleepProvider) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// timeoutWorkflow 构造 steps 个 email 投递步骤的工作流，名称使用社区互动场景。
func timeoutWorkflow(count int, cfg func(i int) map[string]any) domain.Workflow {
	steps := make([]domain.Step, 0, count)
	for i := 0; i < count; i++ {
		var config map[string]any
		if cfg != nil {
			config = cfg(i)
		}
		steps = append(steps, domain.Step{
			ID:     "s" + string(rune('1'+i)),
			Type:   domain.ChannelEmail,
			Name:   "互动通知邮件",
			Config: config,
		})
	}
	return domain.Workflow{
		ID:      "wf_post_commented",
		Name:    "post_commented",
		Version: 1,
		Active:  true,
		Steps:   steps,
	}
}

// timeoutMessage 构造一条评论互动触发消息。
func timeoutMessage() queue.QueuedMessage {
	return queue.QueuedMessage{
		TriggerID:    "t_timeout",
		Name:         "post_commented",
		SubscriberID: "creator_1",
		Email:        "creator@example.com",
		Payload:      map[string]any{"post_id": "P1001", "actor": "小林"},
	}
}

func TestParseStepTimeout(t *testing.T) {
	cases := []struct {
		name       string
		config     map[string]any
		wantLimit  time.Duration
		wantConfig bool
		wantErr    bool
	}{
		{name: "未配置", config: nil, wantConfig: false},
		{name: "duration 字符串", config: map[string]any{"timeout": "1500ms"}, wantLimit: 1500 * time.Millisecond, wantConfig: true},
		{name: "数字按秒", config: map[string]any{"timeout": 2}, wantLimit: 2 * time.Second, wantConfig: true},
		{name: "小数秒", config: map[string]any{"timeout": 0.5}, wantLimit: 500 * time.Millisecond, wantConfig: true},
		{name: "毫秒字段", config: map[string]any{"timeout_ms": 800}, wantLimit: 800 * time.Millisecond, wantConfig: true},
		{name: "显式关闭", config: map[string]any{"timeout": 0}, wantLimit: 0, wantConfig: true},
		{name: "两者并存", config: map[string]any{"timeout": "1s", "timeout_ms": 500}, wantErr: true},
		{name: "负值", config: map[string]any{"timeout": "-1s"}, wantErr: true},
		{name: "无法解析", config: map[string]any{"timeout": "很快"}, wantErr: true},
		{name: "类型错误", config: map[string]any{"timeout_ms": true}, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			step := domain.Step{ID: "s1", Type: domain.ChannelEmail, Config: tc.config}
			limit, configured, err := ParseStepTimeout(step)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("期望报错，实际 limit=%s configured=%v", limit, configured)
				}
				return
			}
			if err != nil {
				t.Fatalf("解析失败: %v", err)
			}
			if configured != tc.wantConfig {
				t.Errorf("configured = %v, 期望 %v", configured, tc.wantConfig)
			}
			if limit != tc.wantLimit {
				t.Errorf("limit = %s, 期望 %s", limit, tc.wantLimit)
			}
		})
	}
}

func TestStepTimeoutCancelsSlowStep(t *testing.T) {
	provider := newBlockingProvider(domain.ChannelEmail)
	registry := NewProviderRegistry()
	registry.Register(provider)

	p := NewWithRegistry(discardLogger(), registry).
		WithTimeouts(TimeoutPolicy{Step: 50 * time.Millisecond})

	started := time.Now()
	results := p.Run(NewContext(timeoutMessage()), timeoutWorkflow(2, nil))
	elapsed := time.Since(started)

	if len(results) != 2 {
		t.Fatalf("步骤结果数 = %d, 期望 2", len(results))
	}
	if !results[0].TimedOut {
		t.Errorf("第一步应标记超时，实际 %+v", results[0])
	}
	if !strings.Contains(results[0].Error, ErrStepTimeout.Error()) {
		t.Errorf("超时原因 = %q, 期望包含 %q", results[0].Error, ErrStepTimeout.Error())
	}
	if results[0].Sent {
		t.Errorf("超时的步骤不应记为已投递")
	}
	if !results[1].ShortCircuited {
		t.Errorf("超时之后应短路后续步骤，实际 %+v", results[1])
	}
	if elapsed > time.Second {
		t.Errorf("耗时 %s，应远小于超时之外的等待", elapsed)
	}

	// provider 确实被调用，且观察到 context 取消——超时真的传到了下游。
	select {
	case <-provider.called:
	default:
		t.Fatalf("provider 未收到投递调用")
	}
	if !provider.waitFinished(2 * time.Second) {
		t.Fatalf("provider 未在超时后收尾")
	}
	if observed := provider.observedErr(); observed == nil {
		t.Errorf("provider 未观察到 context 取消，超时没有传下去")
	}
}

func TestStepTimeoutConfigOverridesPolicy(t *testing.T) {
	provider := newBlockingProvider(domain.ChannelEmail)
	registry := NewProviderRegistry()
	registry.Register(provider)

	// 策略默认 5s，步骤用 timeout_ms 收紧到 40ms：耗时应由步骤配置决定。
	p := NewWithRegistry(discardLogger(), registry).
		WithTimeouts(TimeoutPolicy{Step: 5 * time.Second})

	wf := timeoutWorkflow(1, func(int) map[string]any {
		return map[string]any{"timeout_ms": 40}
	})

	started := time.Now()
	results := p.Run(NewContext(timeoutMessage()), wf)
	elapsed := time.Since(started)

	if !results[0].TimedOut {
		t.Fatalf("步骤应超时，实际 %+v", results[0])
	}
	if elapsed > time.Second {
		t.Errorf("耗时 %s，步骤配置的超时未生效", elapsed)
	}
}

func TestWorkflowBudgetStopsRemainingSteps(t *testing.T) {
	provider := &sleepProvider{channel: domain.ChannelEmail, delay: 80 * time.Millisecond}
	registry := NewProviderRegistry()
	registry.Register(provider)

	// 总预算 150ms：第一步 80ms 完成，第二步被剩余预算（约 70ms）收紧后超时，
	// 第三步不再尝试。
	p := NewWithRegistry(discardLogger(), registry).
		WithTimeouts(TimeoutPolicy{Step: 0, Workflow: 150 * time.Millisecond})

	results := p.Run(NewContext(timeoutMessage()), timeoutWorkflow(3, nil))

	if len(results) != 3 {
		t.Fatalf("步骤结果数 = %d, 期望 3", len(results))
	}
	if results[0].Sent != true {
		t.Fatalf("第一步应在预算内完成，实际 %+v", results[0])
	}
	if !results[1].TimedOut {
		t.Fatalf("第二步应因剩余预算耗尽而超时，实际 %+v", results[1])
	}
	if !strings.Contains(results[1].Error, ErrWorkflowTimeout.Error()) {
		t.Errorf("超时原因 = %q, 期望指向整体预算", results[1].Error)
	}
	if !results[2].ShortCircuited {
		t.Errorf("预算耗尽后第三步不应执行，实际 %+v", results[2])
	}
	if !strings.Contains(results[2].Error, ErrWorkflowTimeout.Error()) {
		t.Errorf("短路原因 = %q, 期望说明整体超时", results[2].Error)
	}
	if calls := provider.callCount(); calls != 2 {
		t.Errorf("provider 调用次数 = %d, 期望 2（第三步不应触达渠道）", calls)
	}
}

func TestZeroTimeoutPolicyRunsInline(t *testing.T) {
	provider := &sleepProvider{channel: domain.ChannelEmail, delay: 10 * time.Millisecond}
	registry := NewProviderRegistry()
	registry.Register(provider)

	// 策略与步骤均不限时：直接同步执行，不引入 fork 与 goroutine。
	p := NewWithRegistry(discardLogger(), registry).
		WithTimeouts(TimeoutPolicy{}).
		WithClock(time.Now)

	results := p.Run(NewContext(timeoutMessage()), timeoutWorkflow(2, nil))

	for i, res := range results {
		if res.Error != "" {
			t.Errorf("第 %d 步出错: %s", i+1, res.Error)
		}
		if !res.Sent {
			t.Errorf("第 %d 步应投递成功", i+1)
		}
		if res.TimedOut {
			t.Errorf("不限时时不应有时间超时标记")
		}
	}
}

func TestInvalidStepTimeoutFailsStep(t *testing.T) {
	registry := NewProviderRegistry()
	registry.Register(&sleepProvider{channel: domain.ChannelEmail, delay: time.Millisecond})

	p := NewWithRegistry(discardLogger(), registry).
		WithTimeouts(TimeoutPolicy{Step: time.Second})

	wf := timeoutWorkflow(2, func(i int) map[string]any {
		if i == 0 {
			return map[string]any{"timeout": "不是时长"}
		}
		return nil
	})

	results := p.Run(NewContext(timeoutMessage()), wf)

	if results[0].Error == "" || !strings.Contains(results[0].Error, "超时配置非法") {
		t.Fatalf("配置非法应报错，实际 %+v", results[0])
	}
	if results[0].TimedOut {
		t.Errorf("配置非法属于配置错误，不应标记为超时")
	}
	if !results[1].ShortCircuited {
		t.Errorf("前一步失败后应短路后续步骤，实际 %+v", results[1])
	}
}

// panicProvider 模拟 provider 内部 panic：管线应把它转成步骤失败，
// 而不是让整个消费者 goroutine 崩掉。
type panicProvider struct {
	channel domain.Channel
}

func (p *panicProvider) Channel() domain.Channel { return p.channel }

func (p *panicProvider) Send(Delivery) SendResult { panic("渠道实现内部错误") }

func TestProviderPanicBecomesStepError(t *testing.T) {
	registry := NewProviderRegistry()
	registry.Register(&panicProvider{channel: domain.ChannelEmail})

	p := NewWithRegistry(discardLogger(), registry).
		WithTimeouts(TimeoutPolicy{Step: 50 * time.Millisecond})

	results := p.Run(NewContext(timeoutMessage()), timeoutWorkflow(2, nil))

	if !strings.Contains(results[0].Error, "panic") {
		t.Fatalf("panic 应转为步骤错误，实际 %+v", results[0])
	}
	if !results[1].ShortCircuited {
		t.Errorf("panic 之后应短路后续步骤，实际 %+v", results[1])
	}
}

func TestContextForkIsolatesResults(t *testing.T) {
	ctx := NewContext(timeoutMessage())
	ctx.Results["s1"] = StepResult{StepID: "s1", Sent: true}

	child := ctx.Fork(context.Background())
	child.Results["s1"] = StepResult{StepID: "s1", Sent: false}
	child.Results["s2"] = StepResult{StepID: "s2", Sent: true}

	if !ctx.Results["s1"].Sent {
		t.Errorf("子上下文的结果改动不应影响父上下文")
	}
	if _, ok := ctx.Results["s2"]; ok {
		t.Errorf("未合并前父上下文不应看到子上下文的新结果")
	}

	ctx.MergeResult(child, "s2")
	if !ctx.Results["s2"].Sent {
		t.Errorf("合并后父上下文应看到子上下文的步骤结果")
	}
	if !ctx.Results["s1"].Sent {
		t.Errorf("合并只应覆盖指定步骤，s1 应保持父上下文的值")
	}
}
