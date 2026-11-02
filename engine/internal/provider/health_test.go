package provider

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// fakeSender 是不带探测能力的渠道实现，用来验证「只靠投递结果」的状态。
type fakeSender struct {
	id string
	ch domain.Channel
}

func (f *fakeSender) Channel() domain.Channel { return f.ch }
func (f *fakeSender) ID() string              { return f.id }
func (f *fakeSender) Send(Delivery) Result    { return Result{Sent: true} }

// probeSender 额外实现 Prober，探测结果由 probeErr / probeFn 控制。
type probeSender struct {
	id       string
	ch       domain.Channel
	probeErr error
	probeFn  func(context.Context) error
	calls    atomic.Int32
}

func (p *probeSender) Channel() domain.Channel { return p.ch }
func (p *probeSender) ID() string              { return p.id }
func (p *probeSender) Send(Delivery) Result    { return Result{Sent: true} }

func (p *probeSender) Probe(ctx context.Context) error {
	p.calls.Add(1)
	if p.probeFn != nil {
		return p.probeFn(ctx)
	}
	return p.probeErr
}

func newRegistryWith(t *testing.T, entries ...Entry) *Registry {
	t.Helper()
	reg := NewRegistry()
	for _, e := range entries {
		e.Enabled = true
		if err := reg.RegisterEntry(e); err != nil {
			t.Fatalf("登记 %s 失败：%v", e.ID, err)
		}
	}
	return reg
}

func TestHealthSnapshotStartsUnknown(t *testing.T) {
	reg := newRegistryWith(t,
		Entry{ID: "console-in-app", Channel: domain.ChannelInApp, Priority: 100, Sender: &fakeSender{id: "console-in-app", ch: domain.ChannelInApp}},
		Entry{ID: "console-email", Channel: domain.ChannelEmail, Priority: 100, Sender: &fakeSender{id: "console-email", ch: domain.ChannelEmail}},
	)
	tracker := NewHealthTracker(reg)

	snapshot := tracker.Snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("快照应包含全部登记条目，实际 %d", len(snapshot))
	}
	for _, st := range snapshot {
		if st.State != StateUnknown {
			t.Errorf("%s 未被观测时状态应为 unknown，实际 %s", st.ID, st.State)
		}
		if !st.Enabled {
			t.Errorf("%s 应为启用状态", st.ID)
		}
	}
	// 没有实现 Prober 时不能标成「支持探测」，否则运维会以为一直在探。
	if snapshot[0].ProbeSupported {
		t.Errorf("未实现 Prober 的条目不应标记为支持探测")
	}

	sum := tracker.Summary()
	if sum.Total != 2 || sum.Enabled != 2 || sum.Unknown != 2 || sum.Disabled != 0 {
		t.Errorf("汇总不符：%+v", sum)
	}
}

func TestHealthStateTransitions(t *testing.T) {
	reg := newRegistryWith(t, Entry{
		ID: "smtp-email", Channel: domain.ChannelEmail, Sender: &fakeSender{id: "smtp-email", ch: domain.ChannelEmail},
	})
	tracker := NewHealthTracker(reg)

	if !tracker.Record("smtp-email", true, "已提交") {
		t.Fatal("已登记条目应能记录观测")
	}
	st, _ := tracker.Status("smtp-email")
	if st.State != StateUp || st.Successes != 1 || st.LastSuccessAt == nil {
		t.Fatalf("成功后状态应为 up：%+v", st)
	}

	// 前两次失败只是 degraded：一次超时常见于对端抖动，直接判死会让主备抖动。
	tracker.Record("smtp-email", false, "连接超时")
	tracker.Record("smtp-email", false, "连接超时")
	st, _ = tracker.Status("smtp-email")
	if st.State != StateDegraded || st.ConsecutiveFailures != 2 {
		t.Fatalf("连续两次失败应为 degraded：%+v", st)
	}
	if st.Failures != 2 || st.Successes != 1 || st.LastError != "连接超时" {
		t.Errorf("计数不符：%+v", st)
	}

	tracker.Record("smtp-email", false, "连接被拒")
	st, _ = tracker.Status("smtp-email")
	if st.State != StateDown || st.ConsecutiveFailures != 3 {
		t.Fatalf("第三次失败应判为 down：%+v", st)
	}

	// 恢复一次就回到 up，并把连续失败清零：健康状态看的是「现在还通不通」。
	tracker.Record("smtp-email", true, "")
	st, _ = tracker.Status("smtp-email")
	if st.State != StateUp || st.ConsecutiveFailures != 0 || st.LastError != "" {
		t.Fatalf("恢复后应回到 up 并清空失败原因：%+v", st)
	}
	if st.Failures != 3 || st.Successes != 2 {
		t.Errorf("累计计数不应被重置：%+v", st)
	}
}

func TestHealthDownAfterIsConfigurable(t *testing.T) {
	reg := newRegistryWith(t, Entry{
		ID: "console-sms", Channel: domain.ChannelSMS, Sender: &fakeSender{id: "console-sms", ch: domain.ChannelSMS},
	})
	tracker := NewHealthTracker(reg).WithDownAfter(2)
	if tracker.DownAfter() != 2 {
		t.Fatalf("阈值应生效，实际 %d", tracker.DownAfter())
	}
	tracker.Record("console-sms", false, "网关 500")
	if st, _ := tracker.Status("console-sms"); st.State != StateDegraded {
		t.Fatalf("一次失败应为 degraded，实际 %s", st.State)
	}
	tracker.Record("console-sms", false, "网关 500")
	if st, _ := tracker.Status("console-sms"); st.State != StateDown {
		t.Fatalf("第二次失败应为 down，实际 %s", st.State)
	}

	// 非法阈值回退默认，而不是把状态机卡在「永远不判死」。
	if got := tracker.WithDownAfter(0).DownAfter(); got != DefaultDownAfter {
		t.Errorf("非正阈值应回退默认 %d，实际 %d", DefaultDownAfter, got)
	}
}

func TestHealthIgnoresUnregisteredIDs(t *testing.T) {
	reg := newRegistryWith(t, Entry{
		ID: "console-chat", Channel: domain.ChannelChat, Sender: &fakeSender{id: "console-chat", ch: domain.ChannelChat},
	})
	tracker := NewHealthTracker(reg)
	if tracker.Record("removed-provider", true, "") {
		t.Error("未登记的标识不应被记录")
	}
	if _, ok := tracker.Status("removed-provider"); ok {
		t.Error("未登记的标识不应有快照")
	}
	if n := len(tracker.Snapshot()); n != 1 {
		t.Errorf("快照不应出现未登记条目，实际 %d 条", n)
	}
}

func TestHealthSnapshotOrderAndDisabled(t *testing.T) {
	// 故意打乱登记顺序：快照要按渠道（in_app → email → sms）再按优先级排列。
	reg := NewRegistry()
	entries := []Entry{
		{ID: "email-backup", Channel: domain.ChannelEmail, Priority: 200, Enabled: true,
			Sender: &fakeSender{id: "email-backup", ch: domain.ChannelEmail}},
		{ID: "sms-only", Channel: domain.ChannelSMS, Priority: 100, Enabled: true,
			Sender: &fakeSender{id: "sms-only", ch: domain.ChannelSMS}},
		{ID: "email-primary", Channel: domain.ChannelEmail, Priority: 100, Enabled: true,
			Sender: &fakeSender{id: "email-primary", ch: domain.ChannelEmail}},
		{ID: "inapp", Channel: domain.ChannelInApp, Priority: 100, Enabled: false,
			Sender: &fakeSender{id: "inapp", ch: domain.ChannelInApp}},
	}
	for _, e := range entries {
		if err := reg.RegisterEntry(e); err != nil {
			t.Fatalf("登记 %s 失败：%v", e.ID, err)
		}
	}

	snapshot := NewHealthTracker(reg).Snapshot()
	want := []string{"inapp", "email-primary", "email-backup", "sms-only"}
	if len(snapshot) != len(want) {
		t.Fatalf("快照条数应为 %d，实际 %d", len(want), len(snapshot))
	}
	for i, id := range want {
		if snapshot[i].ID != id {
			t.Errorf("第 %d 条应为 %s，实际 %s", i, id, snapshot[i].ID)
		}
	}
	if snapshot[0].Enabled {
		t.Error("停用条目仍应出现在快照里，但 enabled 为 false")
	}

	sum := NewHealthTracker(reg).Summary()
	if sum.Total != 4 || sum.Enabled != 3 || sum.Disabled != 1 || sum.Unknown != 3 {
		t.Errorf("汇总应把停用条目单独计数：%+v", sum)
	}
}

func TestHealthReset(t *testing.T) {
	reg := newRegistryWith(t, Entry{
		ID: "console-push", Channel: domain.ChannelPush, Sender: &fakeSender{id: "console-push", ch: domain.ChannelPush},
	})
	tracker := NewHealthTracker(reg)
	tracker.Record("console-push", false, "连接失败")
	if !tracker.Reset("console-push") {
		t.Fatal("已登记条目应能重置")
	}
	st, _ := tracker.Status("console-push")
	if st.State != StateUnknown || st.ConsecutiveFailures != 0 || st.Failures != 0 {
		t.Errorf("重置后应回到 unknown 且计数清零：%+v", st)
	}
	if tracker.Reset("nope") {
		t.Error("未登记条目不应命中重置")
	}
}

func TestHealthConcurrentRecords(t *testing.T) {
	reg := newRegistryWith(t, Entry{
		ID: "console-email", Channel: domain.ChannelEmail, Sender: &fakeSender{id: "console-email", ch: domain.ChannelEmail},
	})
	tracker := NewHealthTracker(reg)

	const goroutines, rounds = 16, 25
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < rounds; j++ {
				ok := (i+j)%4 != 0
				tracker.Record("console-email", ok, "并发观测")
				_ = tracker.Snapshot()
			}
		}(i)
	}
	wg.Wait()

	st, _ := tracker.Status("console-email")
	if st.Successes+st.Failures != goroutines*rounds {
		t.Errorf("并发观测不应丢计数：successes=%d failures=%d 总计应为 %d",
			st.Successes, st.Failures, goroutines*rounds)
	}
}

func TestCheckerProbesProberEntries(t *testing.T) {
	okProvider := &probeSender{id: "console-email", ch: domain.ChannelEmail}
	badProvider := &probeSender{id: "smtp-email", ch: domain.ChannelEmail, probeErr: errors.New("中继不可达")}
	plain := &fakeSender{id: "console-chat", ch: domain.ChannelChat}

	reg := newRegistryWith(t,
		Entry{ID: "console-email", Channel: domain.ChannelEmail, Priority: 100, Sender: okProvider},
		Entry{ID: "smtp-email", Channel: domain.ChannelEmail, Priority: 200, Sender: badProvider},
		Entry{ID: "console-chat", Channel: domain.ChannelChat, Priority: 100, Sender: plain},
	)
	tracker := NewHealthTracker(reg)
	checker := NewChecker(reg, tracker, nil)

	statuses := checker.CheckOnce(context.Background())
	if len(statuses) != 2 {
		t.Fatalf("只应探测实现了 Prober 的条目，实际 %d 条", len(statuses))
	}
	if okProvider.calls.Load() != 1 || badProvider.calls.Load() != 1 {
		t.Errorf("每条应各探一次：ok=%d bad=%d", okProvider.calls.Load(), badProvider.calls.Load())
	}

	if st, _ := tracker.Status("console-email"); st.State != StateUp || !st.ProbeSupported || st.LastProbeAt == nil {
		t.Errorf("探测成功应记为 up：%+v", st)
	}
	if st, _ := tracker.Status("smtp-email"); st.State != StateDegraded || !strings.Contains(st.ProbeError, "不可达") {
		t.Errorf("探测失败应记为 degraded 并带上原因：%+v", st)
	}
	// 没有 Prober 的条目不应被探测改动，也不该探出个「up」来。
	if st, _ := tracker.Status("console-chat"); st.State != StateUnknown || st.ProbeSupported {
		t.Errorf("不支持探测的条目应保持 unknown：%+v", st)
	}
}

func TestCheckerSkipsDisabledEntries(t *testing.T) {
	prober := &probeSender{id: "console-sms", ch: domain.ChannelSMS}
	reg := newRegistryWith(t, Entry{ID: "console-sms", Channel: domain.ChannelSMS, Sender: prober})
	if err := reg.SetEnabled("console-sms", false); err != nil {
		t.Fatalf("停用失败：%v", err)
	}
	checker := NewChecker(reg, NewHealthTracker(reg), nil)
	if statuses := checker.CheckOnce(context.Background()); len(statuses) != 0 {
		t.Errorf("停用条目不应被探测，实际 %d 条", len(statuses))
	}
	if prober.calls.Load() != 0 {
		t.Errorf("停用条目不应真正发起探测，实际调用 %d 次", prober.calls.Load())
	}
}

func TestCheckerProbeErrors(t *testing.T) {
	plain := &fakeSender{id: "console-push", ch: domain.ChannelPush}
	reg := newRegistryWith(t, Entry{ID: "console-push", Channel: domain.ChannelPush, Sender: plain})
	checker := NewChecker(reg, NewHealthTracker(reg), nil)

	if _, err := checker.Probe(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("未登记条目应返回 ErrNotFound，实际 %v", err)
	}
	if _, err := checker.Probe(context.Background(), "console-push"); !errors.Is(err, ErrNotProbeable) {
		t.Errorf("不支持探测的实现应返回 ErrNotProbeable，实际 %v", err)
	}
}

func TestCheckerProbeTimeout(t *testing.T) {
	blocking := &probeSender{
		id: "smtp-email", ch: domain.ChannelEmail,
		probeFn: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	reg := newRegistryWith(t, Entry{ID: "smtp-email", Channel: domain.ChannelEmail, Sender: blocking})
	tracker := NewHealthTracker(reg)
	checker := NewChecker(reg, tracker, nil).WithTimeout(20 * time.Millisecond)

	start := time.Now()
	if _, err := checker.Probe(context.Background(), "smtp-email"); err == nil {
		t.Fatal("阻塞的探测应因超时失败")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("探测应被超时截断，实际耗时 %s", elapsed)
	}
	if st, _ := tracker.Status("smtp-email"); st.State != StateDegraded {
		t.Errorf("超时应记为一次失败：%+v", st)
	}
}

func TestCheckerRunProbesImmediatelyAndStops(t *testing.T) {
	prober := &probeSender{id: "console-in-app", ch: domain.ChannelInApp}
	reg := newRegistryWith(t, Entry{ID: "console-in-app", Channel: domain.ChannelInApp, Sender: prober})
	tracker := NewHealthTracker(reg)
	checker := NewChecker(reg, tracker, nil).WithInterval(time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		checker.Run(ctx)
	}()

	// 启动即探一轮：等状态从 unknown 变为 up，而不是等一个探测间隔。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if st, _ := tracker.Status("console-in-app"); st.State == StateUp {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if st, _ := tracker.Status("console-in-app"); st.State != StateUp {
		t.Fatalf("启动后应立刻探一轮：%+v", st)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run 应在 ctx 取消后退出")
	}
}

func TestCheckerNilSafe(t *testing.T) {
	var checker *Checker
	if st := checker.CheckOnce(context.Background()); st != nil {
		t.Error("nil 探测器不应返回结果")
	}
	if _, err := checker.Probe(context.Background(), "x"); !errors.Is(err, ErrNotFound) {
		t.Errorf("nil 探测器应返回 ErrNotFound，实际 %v", err)
	}
	checker.Run(context.Background())
}

// Registered 区分「没配」与「配了但停用」：前者允许分发回退 mock，
// 后者必须报错，否则停用会变成「假装发成功」。
func TestRegistryRegistered(t *testing.T) {
	reg := newRegistryWith(t, Entry{
		ID: "console-email", Channel: domain.ChannelEmail,
		Sender: &fakeSender{id: "console-email", ch: domain.ChannelEmail},
	})
	if !reg.Registered(domain.ChannelEmail) {
		t.Error("登记过的渠道应返回 true")
	}
	if reg.Registered(domain.ChannelSMS) {
		t.Error("未登记的渠道应返回 false")
	}
	if err := reg.SetEnabled("console-email", false); err != nil {
		t.Fatalf("停用失败：%v", err)
	}
	if !reg.Registered(domain.ChannelEmail) {
		t.Error("停用后仍算登记过")
	}
	if reg.Has(domain.ChannelEmail) {
		t.Error("停用后不应还有可用实现")
	}
}
