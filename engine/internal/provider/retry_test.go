package provider

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

func TestDefaultRetryTableDoesNotRetry(t *testing.T) {
	table := DefaultRetryTable()

	if table.Enabled() {
		t.Error("默认表不应重试")
	}
	policy := table.Lookup(domain.ChannelEmail)
	if policy.Attempts() != 1 {
		t.Errorf("默认尝试次数 = %d, 期望 1", policy.Attempts())
	}
	if !strings.Contains(table.String(), "default=关闭") {
		t.Errorf("摘要应写明关闭：%q", table.String())
	}
}

// 零值表也要能安全查表：Failover 未注入策略时走的就是这条路。
func TestZeroRetryTableLookup(t *testing.T) {
	var table RetryTable
	policy := table.Lookup(domain.ChannelSMS)
	if policy.Attempts() != 1 {
		t.Errorf("零值表的尝试次数 = %d, 期望 1", policy.Attempts())
	}
	if len(table.Channels()) != 0 {
		t.Errorf("零值表不应有渠道配置，实际 %v", table.Channels())
	}
}

func TestRetryTableUsesChannelOverride(t *testing.T) {
	table, err := NewRetryTable(
		RetryPolicy{MaxAttempts: 2, Base: 100 * time.Millisecond, Max: time.Second, Jitter: 0},
		map[domain.Channel]RetryPolicy{
			domain.ChannelEmail: {MaxAttempts: 5, Base: 200 * time.Millisecond, Max: 3 * time.Second},
		},
	)
	if err != nil {
		t.Fatalf("构造重试表失败：%v", err)
	}

	if got := table.Lookup(domain.ChannelEmail); got.Attempts() != 5 {
		t.Errorf("email 尝试次数 = %d, 期望 5", got.Attempts())
	}
	if got := table.Lookup(domain.ChannelSMS); got.Attempts() != 2 {
		t.Errorf("sms 应沿用默认的 2 次，实际 %d", got.Attempts())
	}
	if !table.Enabled() {
		t.Error("有渠道配置时应视为已启用")
	}
	if channels := table.Channels(); len(channels) != 1 || channels[0] != domain.ChannelEmail {
		t.Errorf("单独配置的渠道 = %v, 期望 [email]", channels)
	}
	if !strings.Contains(table.String(), "email=5 次") {
		t.Errorf("摘要应含 email 的策略：%q", table.String())
	}
}

func TestRetryTableValidation(t *testing.T) {
	cases := []struct {
		name    string
		policy  RetryPolicy
		wantErr string
	}{
		{
			name:    "次数超上限",
			policy:  RetryPolicy{MaxAttempts: MaxRetryAttempts + 1},
			wantErr: "max_attempts",
		},
		{
			name:    "次数为负",
			policy:  RetryPolicy{MaxAttempts: -1},
			wantErr: "不能为负数",
		},
		{
			name:    "上限小于基础值",
			policy:  RetryPolicy{MaxAttempts: 2, Base: 2 * time.Second, Max: time.Second},
			wantErr: "不能小于 base_delay",
		},
		{
			name:    "抖动越界",
			policy:  RetryPolicy{MaxAttempts: 2, Jitter: 1.5},
			wantErr: "jitter",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := NewRetryTable(c.policy, nil)
			if err == nil {
				t.Fatal("非法策略应被拒绝")
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("错误信息 %q 缺少 %q", err.Error(), c.wantErr)
			}
		})
	}
}

func TestRetryTableRejectsNonDeliveryChannel(t *testing.T) {
	_, err := NewRetryTable(RetryPolicy{}, map[domain.Channel]RetryPolicy{
		domain.ChannelDigest: {MaxAttempts: 2},
	})
	if err == nil {
		t.Fatal("编排步骤不应接受重试配置")
	}
	if !strings.Contains(err.Error(), "编排步骤") {
		t.Errorf("错误信息应指出这不是投递渠道：%v", err)
	}
}

func TestRetryTableIssuesListedTogether(t *testing.T) {
	_, err := NewRetryTable(RetryPolicy{MaxAttempts: 20, Jitter: 3}, map[domain.Channel]RetryPolicy{
		domain.ChannelEmail: {MaxAttempts: 2, Base: 5 * time.Second, Max: time.Second},
		domain.ChannelSMS:   {MaxAttempts: -2},
	})
	if err == nil {
		t.Fatal("多项问题应被拒绝")
	}
	text := err.Error()
	for _, want := range []string{"default.max_attempts", "default.jitter", "email.max_delay", "sms.max_attempts"} {
		if !strings.Contains(text, want) {
			t.Errorf("错误信息 %q 缺少 %q", text, want)
		}
	}
}

const retryProvidersJSON = `{"providers": [
	{"id": "console-email", "channel": "email", "kind": "console"},
	{"id": "console-sms", "channel": "sms", "kind": "console"}
]`

// 时长既接受带单位的字符串，也接受毫秒数。
func TestParseSetReadsRetrySection(t *testing.T) {
	raw := retryProvidersJSON + `,
	"retries": {
		"default": {"max_attempts": 2, "base_delay": "120ms", "max_delay": "900ms", "jitter": 0},
		"email": {"max_attempts": 4, "base_delay": 250, "max_delay": 2000}
	}}`

	set, err := ParseSet([]byte(raw))
	if err != nil {
		t.Fatalf("解析配置失败：%v", err)
	}

	email := set.Retries().Lookup(domain.ChannelEmail)
	if email.Attempts() != 4 {
		t.Errorf("email 尝试次数 = %d, 期望 4", email.Attempts())
	}
	if email.Base != 250*time.Millisecond || email.Max != 2*time.Second {
		t.Errorf("email 等待 = %s→%s, 期望 250ms→2s", email.Base, email.Max)
	}
	// 只写了次数与时长，抖动应沿用默认段。
	if email.Jitter != 0 {
		t.Errorf("email 抖动 = %g, 期望沿用默认段的 0", email.Jitter)
	}
	// 只写了 base_delay 的渠道要继承默认段的次数，而不是被清零。
	sms := set.Retries().Lookup(domain.ChannelSMS)
	if sms.Attempts() != 2 || sms.Base != 120*time.Millisecond {
		t.Errorf("sms 策略 = %d 次 %s, 期望沿用默认段", sms.Attempts(), sms.Base)
	}
	if !strings.Contains(set.String(), "retries=") {
		t.Errorf("配置摘要应带上重试策略：%q", set.String())
	}
}

func TestParseSetWithoutRetrySection(t *testing.T) {
	set, err := ParseSet([]byte(retryProvidersJSON + `}`))
	if err != nil {
		t.Fatalf("解析配置失败：%v", err)
	}
	if set.Retries().Enabled() {
		t.Error("未写 retries 段时不应重试")
	}
}

func TestParseSetRejectsBadRetrySection(t *testing.T) {
	cases := []struct {
		name    string
		retries string
		wantErr string
	}{
		{
			name:    "未知字段",
			retries: `{"default": {"max_attempt": 3}}`,
			wantErr: "max_attempt",
		},
		{
			name:    "未知渠道",
			retries: `{"demail": {"max_attempts": 2}}`,
			wantErr: "未知渠道",
		},
		{
			name:    "时长格式错误",
			retries: `{"email": {"max_attempts": 2, "base_delay": "半秒"}}`,
			wantErr: "时长格式不正确",
		},
		{
			name:    "段落不是对象",
			retries: `{"email": 3}`,
			wantErr: "解析渠道重试配置失败",
		},
		{
			name:    "次数超上限",
			retries: `{"email": {"max_attempts": 99}}`,
			wantErr: "max_attempts",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw := retryProvidersJSON + `, "retries": ` + c.retries + `}`
			_, err := ParseSet([]byte(raw))
			if err == nil {
				t.Fatal("非法重试配置应被拒绝")
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("错误信息 %q 缺少 %q", err.Error(), c.wantErr)
			}
		})
	}
}

func TestLoadSetReportsRetryIssuesWithSource(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/providers.json"
	content := retryProvidersJSON + `, "retries": {"email": {"max_attempts": 50}}}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("写测试配置失败：%v", err)
	}

	_, err := LoadSet(path)
	if err == nil {
		t.Fatal("非法重试配置应被拒绝")
	}
	if !strings.Contains(err.Error(), path) {
		t.Errorf("错误信息应带上配置文件路径：%v", err)
	}
}
