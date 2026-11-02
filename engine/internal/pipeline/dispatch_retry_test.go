package pipeline

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// flakyProvider 按脚本返回结果，脚本用完后重复最后一条；用于模拟「前几次
// 抖动、之后恢复」的对端。
type flakyProvider struct {
	id      string
	ch      domain.Channel
	results []SendResult

	mu    sync.Mutex
	calls int
}

func newFlaky(id string, ch domain.Channel, results ...SendResult) *flakyProvider {
	return &flakyProvider{id: id, ch: ch, results: results}
}

func (p *flakyProvider) Channel() domain.Channel { return p.ch }

func (p *flakyProvider) ID() string { return p.id }

func (p *flakyProvider) Send(Delivery) SendResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	idx := p.calls
	if idx >= len(p.results) {
		idx = len(p.results) - 1
	}
	p.calls++
	return p.results[idx]
}

func (p *flakyProvider) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// fastRetries 构造一份等待极短的策略，避免测试真的睡上几百毫秒。
func fastRetries(t *testing.T, ch domain.Channel, attempts int) RetryTable {
	t.Helper()
	table, err := NewRetryTable(DefaultRetryTable().Defaults(), map[domain.Channel]RetryPolicy{
		ch: {MaxAttempts: attempts, Base: time.Millisecond, Max: 2 * time.Millisecond, Jitter: 0},
	})
	if err != nil {
		t.Fatalf("构造重试表失败：%v", err)
	}
	return table
}

// 同一次投递里重试成功：结果标记成功、计入重试次数，且不算回退。
func TestDispatchRetriesSameProviderThenSucceeds(t *testing.T) {
	flaky := newFlaky("smtp-email", domain.ChannelEmail,
		SendResult{Sent: false, Reason: "连接被重置"},
		SendResult{Sent: false, Reason: "连接被重置"},
		SendResult{Sent: true, Reason: "已接收"},
	)
	reg := NewProviderRegistry()
	registerStub(t, reg, flaky, "smtp-email", 10)

	pipe := NewWithRegistry(discardLogger(), reg).WithProviderRetries(fastRetries(t, domain.ChannelEmail, 3))
	if !pipe.ProviderRetries().Enabled() {
		t.Error("管线应记住注入的重试策略")
	}

	results := pipe.Run(NewContext(queueMessageWithEmail()), emailWorkflow())

	res := results[0]
	if !res.Sent {
		t.Fatalf("重试后应投递成功：%s", res.Error)
	}
	if got := flaky.count(); got != 3 {
		t.Errorf("实现被调用 %d 次, 期望 3", got)
	}
	if res.ProviderRetries != 2 {
		t.Errorf("ProviderRetries = %d, 期望 2", res.ProviderRetries)
	}
	if len(res.ProviderAttempts) != 3 || res.ProviderAttempts[2] != "smtp-email" {
		t.Errorf("尝试序列 = %v, 期望三次都是 smtp-email", res.ProviderAttempts)
	}
	if res.ProviderFallback {
		t.Error("同一实现内重试不应记为回退")
	}
	if res.UsedProvider != "smtp-email" {
		t.Errorf("实际实现 = %q", res.UsedProvider)
	}
}

// 重试次数用尽仍未成功：结果带次数说明，后续步骤按失败短路。
func TestDispatchRetriesExhaustedReportsAttempts(t *testing.T) {
	flaky := newFlaky("smtp-email", domain.ChannelEmail,
		SendResult{Sent: false, Reason: "连接超时"})
	reg := NewProviderRegistry()
	registerStub(t, reg, flaky, "smtp-email", 10)

	pipe := NewWithRegistry(discardLogger(), reg).WithProviderRetries(fastRetries(t, domain.ChannelEmail, 2))
	results := pipe.Run(NewContext(queueMessageWithEmail()), emailWorkflow())

	res := results[0]
	if res.Sent {
		t.Fatal("全部尝试失败时不应判成功")
	}
	if got := flaky.count(); got != 2 {
		t.Errorf("实现被调用 %d 次, 期望 2", got)
	}
	if res.ProviderRetries != 1 {
		t.Errorf("ProviderRetries = %d, 期望 1", res.ProviderRetries)
	}
	if !strings.Contains(res.Error, "先后尝试 2 次均失败") {
		t.Errorf("失败原因 %q 应说明尝试次数", res.Error)
	}
}

// 内容校验失败（Terminal）不重试，也不该被计成一次重试。
func TestDispatchDoesNotRetryTerminalFailure(t *testing.T) {
	flaky := newFlaky("smtp-email", domain.ChannelEmail,
		SendResult{Sent: false, Reason: "收件邮箱为空", Terminal: true})
	reg := NewProviderRegistry()
	registerStub(t, reg, flaky, "smtp-email", 10)

	pipe := NewWithRegistry(discardLogger(), reg).WithProviderRetries(fastRetries(t, domain.ChannelEmail, 4))
	results := pipe.Run(NewContext(queueMessageWithEmail()), emailWorkflow())

	if got := flaky.count(); got != 1 {
		t.Errorf("不可回退的失败不应重试，实际调用 %d 次", got)
	}
	if results[0].ProviderRetries != 0 {
		t.Errorf("ProviderRetries = %d, 期望 0", results[0].ProviderRetries)
	}
	if !strings.Contains(results[0].Error, "收件邮箱为空") {
		t.Errorf("失败原因 = %q, 期望保留原原因", results[0].Error)
	}
}
