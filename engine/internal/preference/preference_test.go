package preference

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// 偏好判定测试：记录解析、三层覆盖链、文件快照的缓存与重读、读取失败时的
// 放行语义。判定错一位的后果是「该发的没发」或「不该发的发了」，因此每条
// 规则都单独锁定。

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestParseRecordAcceptsValidRows(t *testing.T) {
	cases := []struct {
		name     string
		line     string
		scope    Scope
		target   string
		enabled  bool
		hasStamp bool
	}{
		{
			name:    "全局关闭",
			line:    `{"subscriber_id":"creator_1","scope":"global","enabled":false}`,
			scope:   ScopeGlobal,
			enabled: false,
		},
		{
			name:    "工作流关闭",
			line:    `{"subscriber_id":"creator_1","scope":"workflow","target":"post_liked","enabled":false}`,
			scope:   ScopeWorkflow,
			target:  "post_liked",
			enabled: false,
		},
		{
			name:    "渠道开启（缺 enabled 时按接收）",
			line:    `{"subscriber_id":" creator_1 ","scope":"channel","target":"email"}`,
			scope:   ScopeChannel,
			target:  "email",
			enabled: true,
		},
		{
			name:     "带时间戳",
			line:     `{"subscriber_id":"creator_1","scope":"channel","target":"push","enabled":true,"updated_at":"2026-11-03T08:00:00.123456+00:00"}`,
			scope:    ScopeChannel,
			target:   "push",
			enabled:  true,
			hasStamp: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			record, err := ParseRecord([]byte(tc.line))
			if err != nil {
				t.Fatalf("解析失败: %v", err)
			}
			if record.SubscriberID != "creator_1" {
				t.Errorf("订阅者标识未去空白: %q", record.SubscriberID)
			}
			if record.Scope != tc.scope || record.Target != tc.target {
				t.Errorf("层级/目标 = %s/%q, 期望 %s/%q", record.Scope, record.Target, tc.scope, tc.target)
			}
			if record.Enabled != tc.enabled {
				t.Errorf("enabled = %v, 期望 %v", record.Enabled, tc.enabled)
			}
			if tc.hasStamp && record.UpdatedAt.IsZero() {
				t.Error("时间戳未解析")
			}
		})
	}
}

func TestParseRecordRejectsInvalidRows(t *testing.T) {
	cases := map[string]string{
		"不是 JSON":  `not json`,
		"缺订阅者":     `{"scope":"global","enabled":false}`,
		"未知层级":     `{"subscriber_id":"c1","scope":"tenant","target":"x"}`,
		"全局带目标":    `{"subscriber_id":"c1","scope":"global","target":"post_liked"}`,
		"工作流缺目标":   `{"subscriber_id":"c1","scope":"workflow"}`,
		"工作流名带空格":  `{"subscriber_id":"c1","scope":"workflow","target":"post liked"}`,
		"超出工作流名长度": `{"subscriber_id":"c1","scope":"workflow","target":"` + strings.Repeat("a", MaxWorkflowLength+1) + `"}`,
		"渠道缺失":     `{"subscriber_id":"c1","scope":"channel"}`,
		"渠道不是投递渠道": `{"subscriber_id":"c1","scope":"channel","target":"digest"}`,
		"渠道未知取值":   `{"subscriber_id":"c1","scope":"channel","target":"fax"}`,
		"订阅者标识过长":  `{"subscriber_id":"` + strings.Repeat("s", MaxSubscriberIDLength+1) + `","scope":"global"}`,
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			if record, err := ParseRecord([]byte(line)); err == nil {
				t.Fatalf("应拒绝该记录，实际解析出 %+v", record)
			}
		})
	}
}

func TestResolvePrecedence(t *testing.T) {
	records := []Record{
		{SubscriberID: "creator_1", Scope: ScopeGlobal, Enabled: true},
		{SubscriberID: "creator_1", Scope: ScopeWorkflow, Target: "post_liked", Enabled: false},
		{SubscriberID: "creator_1", Scope: ScopeChannel, Target: "email", Enabled: true},
	}
	set := NewSet(records...)

	// 渠道层最具体：渠道开着就发，不管工作流层怎么设。
	decision := set.Resolve("creator_1", "post_liked", "email")
	if !decision.Allowed || decision.Scope != ScopeChannel || decision.Target != "email" {
		t.Fatalf("渠道层应优先: %+v", decision)
	}
	if decision.SkipMessage() != "" {
		t.Errorf("放行时不该有跳过原因: %q", decision.SkipMessage())
	}

	// 渠道层没记录时落到工作流层。
	decision = set.Resolve("creator_1", "post_liked", "push")
	if decision.Allowed || decision.Scope != ScopeWorkflow {
		t.Fatalf("应命中的是工作流层: %+v", decision)
	}
	if !strings.Contains(decision.SkipMessage(), "偏好跳过：工作流 post_liked 已关闭") {
		t.Errorf("跳过原因文案不对: %q", decision.SkipMessage())
	}

	// 工作流层也没记录时落到全局层。
	decision = set.Resolve("creator_1", "post_commented", "push")
	if !decision.Allowed || decision.Scope != ScopeGlobal {
		t.Fatalf("应命中全局层: %+v", decision)
	}

	// 别的订阅者不受影响。
	decision = set.Resolve("creator_2", "post_liked", "email")
	if !decision.Allowed || decision.Matched {
		t.Fatalf("未设置偏好的订阅者应按接收: %+v", decision)
	}
}

func TestResolveDefaultsToAllowed(t *testing.T) {
	decision := NewSet().Resolve("creator_1", "post_liked", "email")
	if !decision.Allowed || decision.Matched {
		t.Fatalf("没有记录时应默认接收: %+v", decision)
	}
	if decision.Reason != "未设置偏好，按默认接收" {
		t.Errorf("默认结论的原因文案不对: %q", decision.Reason)
	}
}

func TestResolveIgnoresNonDeliveryChannelAndEmptyWorkflow(t *testing.T) {
	set := NewSet(
		Record{SubscriberID: "creator_1", Scope: ScopeChannel, Target: "email", Enabled: false},
		Record{SubscriberID: "creator_1", Scope: ScopeGlobal, Enabled: true},
	)

	// digest 不是投递渠道：拿它查询不会命中渠道层，落到全局层。
	decision := set.Resolve("creator_1", "post_liked", "digest")
	if !decision.Allowed || decision.Scope != ScopeGlobal {
		t.Fatalf("编排步骤不该参与渠道层判定: %+v", decision)
	}

	// 没有工作流名时只判渠道层与全局层。
	decision = set.Resolve("creator_1", "", "email")
	if decision.Allowed || decision.Scope != ScopeChannel {
		t.Fatalf("空工作流名应跳过工作流层: %+v", decision)
	}
}

func TestFileMissingFileMeansNoRecords(t *testing.T) {
	file := NewFile(filepath.Join(t.TempDir(), "absent.jsonl"))
	set, err := file.Snapshot()
	if err != nil {
		t.Fatalf("文件不存在不应报错: %v", err)
	}
	if set.Len() != 0 {
		t.Errorf("期望空快照，实际 %d 条", set.Len())
	}
}

func TestFileReloadsWhenContentChanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "preferences.jsonl")
	writeFile(t, path, `{"subscriber_id":"creator_1","scope":"channel","target":"email","enabled":false}`)

	file := NewFile(path)
	first, err := file.Snapshot()
	if err != nil {
		t.Fatalf("首次读取失败: %v", err)
	}
	if decision := first.Resolve("creator_1", "post_liked", "email"); decision.Allowed {
		t.Fatal("应读到关闭邮件的记录")
	}

	// 文件没变：返回同一份快照（证明走了缓存而不是重读）。
	again, err := file.Snapshot()
	if err != nil {
		t.Fatalf("重复读取失败: %v", err)
	}
	if again != first {
		t.Error("文件未变化时应复用同一份快照")
	}

	// 改写后重新读：新记录立刻生效，旧记录被覆盖。
	writeFile(t, path, `{"subscriber_id":"creator_1","scope":"channel","target":"email","enabled":true}`)
	third, err := file.Snapshot()
	if err != nil {
		t.Fatalf("重读失败: %v", err)
	}
	if third == first {
		t.Error("文件变化后应重新加载")
	}
	if decision := third.Resolve("creator_1", "post_liked", "email"); !decision.Allowed {
		t.Fatal("改写后应读到开启状态")
	}
}

func TestFileSkipsBadLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "preferences.jsonl")
	writeFile(t, path,
		`{"subscriber_id":"creator_1","scope":"channel","target":"email","enabled":false}`,
		`not json`,
		`{"subscriber_id":"creator_1","scope":"channel","target":"digest","enabled":false}`,
		``,
		`{"subscriber_id":"creator_2","scope":"global","enabled":false}`,
	)

	file := NewFile(path)
	set, err := file.Snapshot()
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if set.Len() != 2 {
		t.Fatalf("应留下 2 条有效记录，实际 %d", set.Len())
	}
	if got := file.Skipped(); got != 2 {
		t.Errorf("应跳过 2 行坏记录，实际 %d", got)
	}
	if decision := set.Resolve("creator_1", "post_liked", "email"); decision.Allowed {
		t.Error("有效记录不该被坏行牵连")
	}
	if decision := set.Resolve("creator_2", "post_liked", "email"); decision.Allowed {
		t.Error("第二条有效记录未生效")
	}
}

func TestFileReportsUnreadablePath(t *testing.T) {
	dir := t.TempDir()
	// 用目录占位：路径存在但读不了，属于「读不到偏好」而不是「没有偏好」。
	blocked := filepath.Join(dir, "preferences.jsonl")
	if err := os.MkdirAll(blocked, 0o755); err != nil {
		t.Fatalf("创建目录失败: %v", err)
	}
	if _, err := NewFile(blocked).Snapshot(); err == nil {
		t.Fatal("路径被目录占位时应报错")
	}
}

// errSource 是总是失败的偏好来源，用于验证「读不到就放行」。
type errSource struct{}

func (errSource) Snapshot() (*Set, error) { return nil, errors.New("磁盘不可读") }

func TestGateAllowsWhenSourceFails(t *testing.T) {
	gate := NewGate(errSource{}, testLogger())
	decision := gate.Decide("creator_1", "post_liked", "email")
	if !decision.Allowed {
		t.Fatalf("读取失败应按默认接收放行: %+v", decision)
	}
	if !strings.Contains(decision.Reason, "偏好读取失败") {
		t.Errorf("原因应说明是读取失败: %q", decision.Reason)
	}
}

func TestGateSkipsStorageForAnonymousMessage(t *testing.T) {
	counter := &countingSource{set: NewSet()}
	gate := NewGate(counter, testLogger())
	if decision := gate.Decide("  ", "post_liked", "email"); !decision.Allowed {
		t.Fatalf("没有订阅者时应放行: %+v", decision)
	}
	if counter.calls != 0 {
		t.Errorf("没有订阅者时不该读存储，实际读了 %d 次", counter.calls)
	}
}

func TestGateWithoutSourceAllows(t *testing.T) {
	if decision := NewGate(nil, testLogger()).Decide("creator_1", "post_liked", "email"); !decision.Allowed {
		t.Fatalf("未装配来源时应放行: %+v", decision)
	}
	var nilGate *Gate
	if decision := nilGate.Decide("creator_1", "post_liked", "email"); !decision.Allowed {
		t.Fatalf("nil 判定器应放行: %+v", decision)
	}
}

func TestGateDenyCarriesLayer(t *testing.T) {
	gate := NewGate(NewStatic(
		Record{SubscriberID: "creator_1", Scope: ScopeGlobal, Enabled: false},
	), testLogger())
	decision := gate.Decide("creator_1", "post_mentioned", "push")
	if decision.Allowed || !decision.Matched {
		t.Fatalf("全局关闭应拦下: %+v", decision)
	}
	if got := decision.SkipMessage(); got != "偏好跳过：全局总开关已关闭" {
		t.Errorf("跳过原因 = %q", got)
	}
}

// countingSource 记录被取快照的次数。
type countingSource struct {
	set   *Set
	calls int
}

func (c *countingSource) Snapshot() (*Set, error) {
	c.calls++
	return c.set, nil
}

func TestConcurrentSnapshotsAreSafe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "preferences.jsonl")
	var lines []string
	for i := 0; i < 20; i++ {
		lines = append(lines, fmt.Sprintf(`{"subscriber_id":"creator_%d","scope":"channel","target":"email","enabled":false}`, i))
	}
	writeFile(t, path, lines...)

	file := NewFile(path)
	gate := NewGate(file, testLogger())

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				decision := gate.Decide(fmt.Sprintf("creator_%d", i), "post_liked", "email")
				if decision.Allowed {
					t.Errorf("creator_%d 的邮件偏好是关闭的，不该放行", i)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

// writeFile 覆盖写入偏好文件，内容为给定行。
func writeFile(t *testing.T, path string, lines ...string) {
	t.Helper()
	payload := strings.Join(lines, "\n")
	if payload != "" {
		payload += "\n"
	}
	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		t.Fatalf("写入 %s 失败: %v", path, err)
	}
	// 同一纳秒内连续改写时，文件系统可能给出相同的修改时刻；显式把时间戳
	// 往后推一点，让「文件变了」这件事可被观察到。
	future := time.Now().Add(time.Duration(len(lines)+1) * time.Millisecond)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatalf("调整时间戳失败: %v", err)
	}
}
