package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
	"github.com/xwd-ai/xw-p/engine/internal/provider"
)

// emailRig 组装一个只有 email 渠道、主备可分别控制结果的管线。
func emailRig(t *testing.T) (*Pipeline, *stubProvider, *stubProvider) {
	t.Helper()
	primary := &stubProvider{
		id: "smtp-email", ch: domain.ChannelEmail,
		res: SendResult{Sent: false, Reason: "SMTP 连接失败：connection refused"},
	}
	standby := &stubProvider{
		id: "console-email", ch: domain.ChannelEmail,
		res: SendResult{Sent: true, Reason: "email 已发送至 creator@example.com"},
	}
	reg := NewProviderRegistry()
	registerStub(t, reg, primary, "smtp-email", 10)
	registerStub(t, reg, standby, "console-email", 100)
	return NewWithRegistry(discardLogger(), reg), primary, standby
}

func TestDispatchRecordsHealthOnSuccess(t *testing.T) {
	primary := &stubProvider{
		id: "console-email", ch: domain.ChannelEmail,
		res: SendResult{Sent: true, Reason: "email 已发送至 creator@example.com"},
	}
	reg := NewProviderRegistry()
	registerStub(t, reg, primary, "console-email", 100)
	pipe := NewWithRegistry(discardLogger(), reg)

	pipe.Run(NewContext(queueMessageWithEmail()), emailWorkflow())

	st, ok := pipe.Health().Status("console-email")
	if !ok {
		t.Fatal("管线应带有健康记录器")
	}
	if st.State != provider.StateUp || st.Successes != 1 || st.Failures != 0 {
		t.Errorf("投递成功应记为 up：%+v", st)
	}
}

func TestDispatchRecordsHealthOnFallback(t *testing.T) {
	pipe, _, _ := emailRig(t)
	pipe.Run(NewContext(queueMessageWithEmail()), emailWorkflow())

	primary, _ := pipe.Health().Status("smtp-email")
	if primary.State != provider.StateDegraded || primary.Failures != 1 {
		t.Errorf("主实现失败应记为 degraded：%+v", primary)
	}
	if primary.LastError == "" {
		t.Error("失败原因应写进健康快照")
	}
	standby, _ := pipe.Health().Status("console-email")
	if standby.State != provider.StateUp || standby.Successes != 1 {
		t.Errorf("完成投递的备选应记为 up：%+v", standby)
	}
}

func TestDispatchTerminalFailureDoesNotCount(t *testing.T) {
	// 收件邮箱为空是消息本身的问题：算在渠道头上会让一条坏消息把渠道标红。
	primary := &stubProvider{
		id: "smtp-email", ch: domain.ChannelEmail,
		res: SendResult{Reason: "收件邮箱为空", Terminal: true},
	}
	reg := NewProviderRegistry()
	registerStub(t, reg, primary, "smtp-email", 10)
	pipe := NewWithRegistry(discardLogger(), reg)

	msg := queueMessageWithEmail()
	msg.Email = ""
	pipe.Run(NewContext(msg), emailWorkflow())

	st, _ := pipe.Health().Status("smtp-email")
	if st.State != provider.StateUnknown || st.Failures != 0 {
		t.Errorf("与实现无关的失败不应计入健康：%+v", st)
	}
}

// sequenceProvider 按调用次序返回预设结果，用来观察「先失败后重试成功」。
type sequenceProvider struct {
	id      string
	ch      domain.Channel
	results []SendResult
	calls   int
}

func (s *sequenceProvider) Channel() domain.Channel { return s.ch }
func (s *sequenceProvider) ID() string              { return s.id }

func (s *sequenceProvider) Send(Delivery) SendResult {
	if s.calls >= len(s.results) {
		return s.results[len(s.results)-1]
	}
	res := s.results[s.calls]
	s.calls++
	return res
}

func TestDispatchHealthFollowsRetries(t *testing.T) {
	// 同一实现重试后成功：中间那次失败会被记下，最终成功把状态拉回 up。
	flaky := &sequenceProvider{
		id: "smtp-email", ch: domain.ChannelEmail,
		results: []SendResult{
			{Sent: false, Reason: "SMTP 连接超时"},
			{Sent: true, Reason: "第二次提交成功"},
		},
	}
	reg := NewProviderRegistry()
	registerStub(t, reg, flaky, "smtp-email", 100)
	retries, err := NewRetryTable(RetryPolicy{MaxAttempts: 2, Base: time.Millisecond, Max: time.Millisecond}, nil)
	if err != nil {
		t.Fatalf("构造重试表失败：%v", err)
	}
	pipe := NewWithRegistry(discardLogger(), reg).WithProviderRetries(retries)

	results := pipe.Run(NewContext(queueMessageWithEmail()), emailWorkflow())
	if len(results) != 1 || !results[0].Sent {
		t.Fatalf("重试后应投递成功：%+v", results)
	}

	st, _ := pipe.Health().Status("smtp-email")
	if st.State != provider.StateUp {
		t.Errorf("最终成功应回到 up：%+v", st)
	}
	if st.Failures != 1 || st.Successes != 1 || st.ConsecutiveFailures != 0 {
		t.Errorf("两次尝试应各记一次观测：%+v", st)
	}
}

func TestPipelineHealthCanBeReplaced(t *testing.T) {
	pipe, _, _ := emailRig(t)
	replacement := NewHealthTracker(pipe.Registry())
	pipe.WithHealth(replacement)
	pipe.Run(NewContext(queueMessageWithEmail()), emailWorkflow())

	if _, ok := pipe.Health().Status("smtp-email"); !ok {
		t.Fatal("替换后的记录器应被使用")
	}
	if pipe.Health() != replacement {
		t.Error("Health 应返回注入的记录器")
	}

	pipe.WithHealth(nil)
	if pipe.Health() == nil {
		t.Error("传 nil 应重置为新记录器而不是禁用")
	}
	if st, _ := pipe.Health().Status("smtp-email"); st.State != provider.StateUnknown {
		t.Errorf("新的记录器应从 unknown 开始：%+v", st)
	}
}

// 全被停用与「没配」是两件事：前者必须报失败，不能退化成 mock 打印。
func TestDispatchFailsWhenChannelFullyDisabled(t *testing.T) {
	only := &stubProvider{
		id: "console-email", ch: domain.ChannelEmail,
		res: SendResult{Sent: true, Reason: "不应被调用"},
	}
	reg := NewProviderRegistry()
	registerStub(t, reg, only, "console-email", 100)
	if err := reg.SetEnabled("console-email", false); err != nil {
		t.Fatalf("停用失败：%v", err)
	}
	pipe := NewWithRegistry(discardLogger(), reg)

	results := pipe.Run(NewContext(queueMessageWithEmail()), emailWorkflow())
	if len(results) != 1 {
		t.Fatalf("步骤结果数 = %d，期望 1", len(results))
	}
	if results[0].Sent {
		t.Error("实现全部停用时不应记为投递成功")
	}
	if !strings.Contains(results[0].Error, "均已停用") {
		t.Errorf("失败原因应说明实现已停用，实际 %q", results[0].Error)
	}
	if only.count() != 0 {
		t.Errorf("停用的实现不应被调用，实际 %d 次", only.count())
	}
}
