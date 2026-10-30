package provider

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// testSender 是只记录调用次数的桩实现，用于验证注册表的取用顺序。
type testSender struct {
	ch    domain.Channel
	label string
	mu    sync.Mutex
	sent  int
}

func newTestSender(ch domain.Channel, label string) *testSender {
	return &testSender{ch: ch, label: label}
}

func (s *testSender) Channel() domain.Channel { return s.ch }

func (s *testSender) Send(d Delivery) Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent++
	return Result{Sent: true, Reason: s.label + " 已接收"}
}

// namedSender 在桩实现上增加稳定标识，验证 Identifier 优先于自动编号。
type namedSender struct {
	*testSender
	id string
}

func (s *namedSender) ID() string { return s.id }

func TestRegisterAssignsAutoIDs(t *testing.T) {
	reg := NewRegistry()
	first := newTestSender(domain.ChannelEmail, "first")
	second := newTestSender(domain.ChannelEmail, "second")

	if err := reg.Register(first); err != nil {
		t.Fatalf("登记首个实现失败: %v", err)
	}
	if err := reg.Register(second); err != nil {
		t.Fatalf("登记第二个实现失败: %v", err)
	}

	entries := reg.Entries()
	if len(entries) != 2 {
		t.Fatalf("登记条目数 = %d, 期望 2", len(entries))
	}
	if entries[0].ID != "email" || entries[1].ID != "email-2" {
		t.Errorf("自动编号 = %q/%q, 期望 email/email-2", entries[0].ID, entries[1].ID)
	}
	if entries[0].Priority != DefaultPriority {
		t.Errorf("默认优先级 = %d, 期望 %d", entries[0].Priority, DefaultPriority)
	}
	if !entries[0].Enabled {
		t.Error("新登记的条目应默认启用")
	}

	// 同渠道多个实现时，默认取用先登记者。
	got, ok := reg.Lookup(domain.ChannelEmail)
	if !ok || got != first {
		t.Error("Lookup 应返回先登记的 email 实现")
	}
}

func TestRegisterPrefersIdentifier(t *testing.T) {
	reg := NewRegistry()
	named := &namedSender{testSender: newTestSender(domain.ChannelSMS, "named"), id: "console-sms"}
	if err := reg.Register(named); err != nil {
		t.Fatalf("登记失败: %v", err)
	}
	if _, ok := reg.LookupByID("console-sms"); !ok {
		t.Fatal("应能按实现自带标识查到条目")
	}
	// 自带标识占用后，自动编号从渠道名-2 开始，不会与之冲突。
	plain := newTestSender(domain.ChannelSMS, "plain")
	if err := reg.Register(plain); err != nil {
		t.Fatalf("登记失败: %v", err)
	}
	if _, ok := reg.LookupByID("sms"); !ok {
		t.Error("未自带标识的实现应回退到渠道名编号")
	}
}

func TestRegisterEntryValidation(t *testing.T) {
	reg := NewRegistry()

	cases := []struct {
		name  string
		entry Entry
		want  error
	}{
		{
			name:  "空实现",
			entry: Entry{ID: "x", Channel: domain.ChannelEmail},
			want:  ErrNilSender,
		},
		{
			name:  "空渠道",
			entry: Entry{ID: "x", Sender: newTestSender(domain.ChannelEmail, "s")},
			want:  ErrInvalidChannel,
		},
		{
			name:  "编排步骤不是投递渠道",
			entry: Entry{ID: "x", Channel: domain.ChannelDelay, Sender: newTestSender(domain.ChannelDelay, "s")},
			want:  ErrInvalidChannel,
		},
		{
			name:  "渠道与实现声明不一致",
			entry: Entry{ID: "x", Channel: domain.ChannelPush, Sender: newTestSender(domain.ChannelEmail, "s")},
			want:  ErrChannelMismatch,
		},
		{
			name:  "负优先级",
			entry: Entry{ID: "x", Channel: domain.ChannelEmail, Priority: -1, Sender: newTestSender(domain.ChannelEmail, "s")},
			want:  ErrNegativePriority,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := reg.RegisterEntry(tc.entry)
			if !errors.Is(err, tc.want) {
				t.Fatalf("错误 = %v, 期望 %v", err, tc.want)
			}
		})
	}
	if reg.Len() != 0 {
		t.Errorf("校验失败的条目不应入库, 当前 %d 条", reg.Len())
	}
}

func TestSelectPrefersLowestPriority(t *testing.T) {
	reg := NewRegistry()
	backup := newTestSender(domain.ChannelPush, "backup")
	primary := newTestSender(domain.ChannelPush, "primary")
	secondary := newTestSender(domain.ChannelPush, "secondary")

	mustRegisterEntry(t, reg, Entry{ID: "push-backup", Channel: domain.ChannelPush, Priority: 300, Enabled: true, Sender: backup})
	mustRegisterEntry(t, reg, Entry{ID: "push-primary", Channel: domain.ChannelPush, Priority: 10, Enabled: true, Sender: primary})
	mustRegisterEntry(t, reg, Entry{ID: "push-secondary", Channel: domain.ChannelPush, Priority: 10, Enabled: true, Sender: secondary})

	entry, ok := reg.Select(domain.ChannelPush)
	if !ok || entry.ID != "push-primary" {
		t.Fatalf("取用条目 = %q, 期望 push-primary（同优先级先登记）", entry.ID)
	}

	candidates := reg.Candidates(domain.ChannelPush)
	got := make([]string, len(candidates))
	for i, c := range candidates {
		got[i] = c.ID
	}
	want := []string{"push-primary", "push-secondary", "push-backup"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("取用序列 = %v, 期望 %v", got, want)
	}
}

func TestSetEnabledSkipsProvider(t *testing.T) {
	reg := NewRegistry()
	primary := newTestSender(domain.ChannelChat, "primary")
	backup := newTestSender(domain.ChannelChat, "backup")
	mustRegisterEntry(t, reg, Entry{ID: "chat-primary", Channel: domain.ChannelChat, Priority: 10, Enabled: true, Sender: primary})
	mustRegisterEntry(t, reg, Entry{ID: "chat-backup", Channel: domain.ChannelChat, Priority: 20, Enabled: true, Sender: backup})

	if err := reg.SetEnabled("chat-primary", false); err != nil {
		t.Fatalf("停用失败: %v", err)
	}
	if got, ok := reg.Lookup(domain.ChannelChat); !ok || got != backup {
		t.Error("默认实现停用后应回退到备选实现")
	}
	if reg.Len() != 2 {
		t.Errorf("停用不应删除条目, 当前 %d 条", reg.Len())
	}
	if len(reg.Candidates(domain.ChannelChat)) != 1 {
		t.Error("停用的实现不应出现在取用序列里")
	}

	// 全部停用后视为该渠道没有可用实现。
	if err := reg.SetEnabled("chat-backup", false); err != nil {
		t.Fatalf("停用失败: %v", err)
	}
	if reg.Has(domain.ChannelChat) {
		t.Error("全部停用后不应报告该渠道可用")
	}
	if _, ok := reg.Lookup(domain.ChannelChat); ok {
		t.Error("全部停用后不应取到实现")
	}

	if err := reg.SetEnabled("not-exists", true); !errors.Is(err, ErrNotFound) {
		t.Errorf("停用不存在的条目错误 = %v, 期望 ErrNotFound", err)
	}

	// 重新启用后恢复原优先级顺序。
	if err := reg.SetEnabled("chat-primary", true); err != nil {
		t.Fatalf("启用失败: %v", err)
	}
	if got, ok := reg.Lookup(domain.ChannelChat); !ok || got != primary {
		t.Error("重新启用后应回到默认实现")
	}
}

func TestRegisterReplacesSameID(t *testing.T) {
	reg := NewRegistry()
	oldSender := newTestSender(domain.ChannelInApp, "old")
	newSender := newTestSender(domain.ChannelInApp, "new")
	mustRegisterEntry(t, reg, Entry{ID: "in-app-main", Channel: domain.ChannelInApp, Priority: 50, Enabled: true, Sender: oldSender})

	if err := reg.RegisterEntry(Entry{ID: "in-app-main", Channel: domain.ChannelInApp, Priority: 5, Enabled: true, Sender: newSender}); err != nil {
		t.Fatalf("替换失败: %v", err)
	}

	if reg.Len() != 1 {
		t.Fatalf("同标识替换后条目数 = %d, 期望 1", reg.Len())
	}
	entry, ok := reg.LookupByID("in-app-main")
	if !ok {
		t.Fatal("替换后应仍能按标识查到")
	}
	if entry.Sender != newSender {
		t.Error("替换后应取到新的实现")
	}
	if entry.Priority != 5 {
		t.Errorf("替换后优先级 = %d, 期望 5", entry.Priority)
	}
}

func TestRemove(t *testing.T) {
	reg := NewRegistry()
	first := newTestSender(domain.ChannelEmail, "first")
	second := newTestSender(domain.ChannelEmail, "second")
	mustRegisterEntry(t, reg, Entry{ID: "e1", Channel: domain.ChannelEmail, Enabled: true, Sender: first})
	mustRegisterEntry(t, reg, Entry{ID: "e2", Channel: domain.ChannelEmail, Enabled: true, Sender: second})

	if !reg.Remove("e1") {
		t.Fatal("移除已登记条目应返回 true")
	}
	if reg.Remove("e1") {
		t.Error("重复移除同一条目应返回 false")
	}
	if _, ok := reg.LookupByID("e1"); ok {
		t.Error("移除后不应再查到该标识")
	}
	// 后续条目的下标前移后仍可按标识命中。
	if entry, ok := reg.LookupByID("e2"); !ok || entry.Sender != second {
		t.Error("移除前一条目后，后一条目应仍可寻址")
	}
	if got, ok := reg.Lookup(domain.ChannelEmail); !ok || got != second {
		t.Error("移除默认实现后应取到剩余实现")
	}
}

func TestChannelsFollowDomainOrder(t *testing.T) {
	reg := NewRegistry()
	mustRegisterEntry(t, reg, Entry{ID: "chat", Channel: domain.ChannelChat, Enabled: true, Sender: newTestSender(domain.ChannelChat, "chat")})
	mustRegisterEntry(t, reg, Entry{ID: "in-app", Channel: domain.ChannelInApp, Enabled: true, Sender: newTestSender(domain.ChannelInApp, "in-app")})
	mustRegisterEntry(t, reg, Entry{ID: "email", Channel: domain.ChannelEmail, Enabled: true, Sender: newTestSender(domain.ChannelEmail, "email")})

	got := make([]string, 0, 3)
	for _, ch := range reg.Channels() {
		got = append(got, string(ch))
	}
	want := "in_app,email,chat"
	if strings.Join(got, ",") != want {
		t.Errorf("渠道顺序 = %v, 期望 %s（与 domain.AllChannels 一致）", got, want)
	}
}

func TestEntriesReturnsSnapshot(t *testing.T) {
	reg := NewRegistry()
	sender := newTestSender(domain.ChannelEmail, "s")
	mustRegisterEntry(t, reg, Entry{ID: "e1", Channel: domain.ChannelEmail, Enabled: true, Sender: sender})

	snapshot := reg.Entries()
	snapshot[0].ID = "changed"
	snapshot[0].Enabled = false

	if _, ok := reg.LookupByID("e1"); !ok {
		t.Error("修改快照不应影响注册表内部状态")
	}
	if !reg.Has(domain.ChannelEmail) {
		t.Error("修改快照里的启用标记不应影响取用")
	}
}

func TestConcurrentRegistrationAndLookup(t *testing.T) {
	reg := NewRegistry()
	const workers = 16
	const rounds = 50

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				id := fmt.Sprintf("p-%d-%d", worker, i)
				_ = reg.RegisterEntry(Entry{
					ID:       id,
					Channel:  domain.ChannelEmail,
					Priority: i % 5,
					Enabled:  true,
					Sender:   newTestSender(domain.ChannelEmail, id),
				})
				_, _ = reg.Lookup(domain.ChannelEmail)
				_ = reg.Candidates(domain.ChannelSMS)
				_ = reg.SetEnabled(id, false)
				_ = reg.SetEnabled(id, true)
				if i%10 == 0 {
					reg.Remove(id)
				}
				_ = reg.Channels()
				_ = reg.String()
			}
		}(w)
	}
	wg.Wait()

	// 并发写入后注册表仍自洽：条目都能按标识找回，且下标索引与切片一致。
	for _, e := range reg.Entries() {
		if _, ok := reg.LookupByID(e.ID); !ok {
			t.Fatalf("条目 %q 在索引里找不到", e.ID)
		}
	}
}

func TestString(t *testing.T) {
	reg := NewRegistry()
	mustRegisterEntry(t, reg, Entry{ID: "e1", Channel: domain.ChannelEmail, Enabled: true, Sender: newTestSender(domain.ChannelEmail, "s")})
	text := reg.String()
	if !strings.Contains(text, "entries=1") || !strings.Contains(text, "email") {
		t.Errorf("描述 = %q, 期望包含条目数与渠道", text)
	}
}

func mustRegisterEntry(t *testing.T, reg *Registry, e Entry) {
	t.Helper()
	if err := reg.RegisterEntry(e); err != nil {
		t.Fatalf("登记 %q 失败: %v", e.ID, err)
	}
}
