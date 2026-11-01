package provider

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

func intPtr(v int) *int    { return &v }
func boolPtr(v bool) *bool { return &v }

// consoleEmail 返回一条合法的 console 邮件配置，供各用例按需改写。
func consoleEmail() Config {
	return Config{
		ID:       "console-email",
		Channel:  domain.ChannelEmail,
		Kind:     KindConsole,
		Priority: intPtr(100),
	}
}

func TestCredentialsHelpers(t *testing.T) {
	creds := Credentials{
		"host":     "  smtp.example.com  ",
		"password": "s3cr3t",
		"from":     "   ",
	}

	if got := creds.Get("host"); got != "smtp.example.com" {
		t.Errorf("Get 去空白 = %q", got)
	}
	if creds.Has("from") {
		t.Error("空白值应视为未设置")
	}
	if keys := creds.Keys(); len(keys) != 2 || keys[0] != "host" || keys[1] != "password" {
		t.Errorf("Keys = %v, 期望 [host password]（跳过空值、按名称排序）", keys)
	}
	if missing := creds.Missing("host", "from", "port"); len(missing) != 2 ||
		missing[0] != "from" || missing[1] != "port" {
		t.Errorf("Missing = %v, 期望 [from port] 且保持传入顺序", missing)
	}

	redacted := creds.Redacted()
	if redacted["password"] != "***" || redacted["host"] != "***" {
		t.Errorf("Redacted 未脱敏 = %v", redacted)
	}
	if redacted["from"] != "" {
		t.Errorf("未设置的凭据应为空串，得到 %q", redacted["from"])
	}
	if creds.Get("password") != "s3cr3t" {
		t.Error("Redacted 不应修改原凭据")
	}

	clone := creds.Clone()
	clone["password"] = "changed"
	if creds.Get("password") != "s3cr3t" {
		t.Error("Clone 应返回深拷贝")
	}
	var nilCreds Credentials
	if nilCreds.Clone() != nil {
		t.Error("nil 凭据的 Clone 应返回 nil")
	}
}

// TestValidateConfigIssues 覆盖各字段的校验分支，确保问题定位到具体字段。
func TestValidateConfigIssues(t *testing.T) {
	smtpValid := Config{
		ID:      "email-smtp",
		Channel: domain.ChannelEmail,
		Kind:    KindSMTP,
		Credentials: Credentials{
			"host": "smtp.example.com", "from": "notify@example.com",
		},
	}
	if issues := validateConfigs([]Config{smtpValid}); len(issues) != 0 {
		t.Fatalf("合法配置不应报错，得到 %v", issues)
	}

	cases := []struct {
		name      string
		config    Config
		wantField string
		wantText  string
	}{
		{
			name:      "缺少标识",
			config:    Config{Channel: domain.ChannelEmail, Kind: KindConsole},
			wantField: "id",
			wantText:  "必填",
		},
		{
			name:      "未知渠道",
			config:    Config{ID: "x", Channel: "order", Kind: KindConsole},
			wantField: "channel",
			wantText:  "未知渠道",
		},
		{
			name:      "编排步骤不是投递渠道",
			config:    Config{ID: "x", Channel: domain.ChannelDigest, Kind: KindConsole},
			wantField: "channel",
			wantText:  "编排步骤",
		},
		{
			name:      "未知实现种类",
			config:    Config{ID: "x", Channel: domain.ChannelEmail, Kind: "carrier-pigeon"},
			wantField: "kind",
			wantText:  "未知实现种类",
		},
		{
			name: "种类与渠道不匹配",
			config: Config{
				ID: "x", Channel: domain.ChannelSMS, Kind: KindSMTP,
				Credentials: Credentials{"host": "h", "from": "f"},
			},
			wantField: "kind",
			wantText:  "不能用于渠道",
		},
		{
			name:      "负优先级",
			config:    Config{ID: "x", Channel: domain.ChannelEmail, Kind: KindConsole, Priority: intPtr(-1)},
			wantField: "priority",
			wantText:  "不能为负数",
		},
		{
			name:      "缺少必填凭据",
			config:    Config{ID: "x", Channel: domain.ChannelEmail, Kind: KindSMTP, Credentials: Credentials{"host": "h"}},
			wantField: "credentials",
			wantText:  "from",
		},
		{
			name: "凭据项拼写错误",
			config: Config{
				ID: "x", Channel: domain.ChannelEmail, Kind: KindSMTP,
				Credentials: Credentials{"host": "h", "from": "f", "Password": "p"},
			},
			wantField: "credentials.Password",
			wantText:  `是否应写 "password"`,
		},
		{
			name: "未识别的凭据项",
			config: Config{
				ID: "x", Channel: domain.ChannelEmail, Kind: KindSMTP,
				Credentials: Credentials{"host": "h", "from": "f", "retries": "3"},
			},
			wantField: "credentials.retries",
			wantText:  "未识别的凭据项",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issues := validateConfigs([]Config{tc.config})
			if len(issues) != 1 {
				t.Fatalf("问题条数 = %d, 期望 1：%v", len(issues), issues)
			}
			if issues[0].Field != tc.wantField {
				t.Errorf("字段 = %q, 期望 %q", issues[0].Field, tc.wantField)
			}
			if !strings.Contains(issues[0].Problem, tc.wantText) {
				t.Errorf("问题描述 %q 未包含 %q", issues[0].Problem, tc.wantText)
			}
		})
	}
}

func TestValidateConfigDetectsDuplicateID(t *testing.T) {
	cfg := consoleEmail()
	issues := validateConfigs([]Config{cfg, cfg})
	if len(issues) != 1 {
		t.Fatalf("问题条数 = %d, 期望 1：%v", len(issues), issues)
	}
	if issues[0].Index != 2 {
		t.Errorf("重复项序号 = %d, 期望 2", issues[0].Index)
	}
	if !strings.Contains(issues[0].Problem, "与第 1 项相同") {
		t.Errorf("问题描述 = %q", issues[0].Problem)
	}
}

func TestConfigErrorListsEveryIssue(t *testing.T) {
	var configs []Config
	for i := 0; i < 8; i++ {
		configs = append(configs, Config{Channel: "bogus", Kind: KindConsole})
	}
	_, err := NewSet(configs)
	if err == nil {
		t.Fatal("期望校验失败")
	}
	var cfgErr *ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("错误类型 = %T, 期望 *ConfigError", err)
	}
	if len(cfgErr.Issues) != 16 {
		t.Fatalf("问题条数 = %d, 期望 16（每条两项：id 与 channel）", len(cfgErr.Issues))
	}
	msg := err.Error()
	if !strings.Contains(msg, "第 1 项") {
		t.Errorf("错误信息应带序号：%s", msg)
	}
	if !strings.Contains(msg, "另有") {
		t.Errorf("问题超过上限时应提示剩余条数：%s", msg)
	}
}

func TestParseSetAcceptsBothShapes(t *testing.T) {
	arrayForm := `[
		{"id": "console-email", "channel": "email", "kind": "console"},
		{"id": "email-smtp", "channel": "email", "kind": "smtp",
		 "priority": 10,
		 "credentials": {"host": "smtp.example.com", "from": "notify@example.com"}}
	]`
	objectForm := `{"providers": [
		{"id": "console-email", "channel": "email", "kind": "console"},
		{"id": "email-smtp", "channel": "email", "kind": "smtp",
		 "priority": 10,
		 "credentials": {"host": "smtp.example.com", "from": "notify@example.com"}}
	]}`

	for name, raw := range map[string]string{"数组": arrayForm, "对象": objectForm} {
		t.Run(name, func(t *testing.T) {
			set, err := ParseSet([]byte(raw))
			if err != nil {
				t.Fatalf("解析失败: %v", err)
			}
			if set.Len() != 2 {
				t.Fatalf("配置条数 = %d, 期望 2", set.Len())
			}
			primary, ok := set.Primary(domain.ChannelEmail)
			if !ok {
				t.Fatal("email 渠道应有默认实现")
			}
			if primary.ID != "email-smtp" {
				t.Errorf("默认实现 = %q, 期望优先级更小的 email-smtp", primary.ID)
			}
			if primary.EffectivePriority() != 10 {
				t.Errorf("优先级 = %d, 期望 10", primary.EffectivePriority())
			}
		})
	}
}

func TestParseSetRejectsMalformedInput(t *testing.T) {
	cases := map[string]string{
		"空内容":         "   ",
		"未知字段":        `[{"id": "a", "channel": "email", "kind": "console", "credential": {}}]`,
		"缺少providers": `{"items": []}`,
		"两个文档":        `[{"id": "a", "channel": "email", "kind": "console"}] []`,
		"非JSON":       `id=email`,
	}

	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseSet([]byte(raw)); err == nil {
				t.Fatal("期望解析失败")
			}
		})
	}
}

func TestSetBindingOrderAndPrimary(t *testing.T) {
	set, err := NewSet([]Config{
		{ID: "email-console", Channel: domain.ChannelEmail, Kind: KindConsole, Priority: intPtr(100)},
		{ID: "email-backup", Channel: domain.ChannelEmail, Kind: KindConsole, Priority: intPtr(10)},
		{ID: "email-same-a", Channel: domain.ChannelEmail, Kind: KindConsole, Priority: intPtr(10)},
		{ID: "sms-console", Channel: domain.ChannelSMS, Kind: KindConsole},
		{ID: "chat-console", Channel: domain.ChannelChat, Kind: KindConsole, Enabled: boolPtr(false)},
	})
	if err != nil {
		t.Fatalf("构造配置集失败: %v", err)
	}

	list := set.Channel(domain.ChannelEmail)
	got := make([]string, len(list))
	for i, cfg := range list {
		got[i] = cfg.ID
	}
	// 优先级相同时保持配置顺序。
	want := []string{"email-backup", "email-same-a", "email-console"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("email 取用序列 = %v, 期望 %v", got, want)
	}
	if primary, ok := set.Primary(domain.ChannelEmail); !ok || primary.ID != "email-backup" {
		t.Errorf("email 默认实现 = %q, 期望 email-backup", primary.ID)
	}
	if _, ok := set.Primary(domain.ChannelChat); ok {
		t.Error("全部停用的渠道不应有默认实现")
	}
	if _, ok := set.Primary(domain.ChannelPush); ok {
		t.Error("未配置的渠道不应有默认实现")
	}

	channels := set.Channels()
	if len(channels) != 3 || channels[0] != domain.ChannelEmail ||
		channels[1] != domain.ChannelSMS || channels[2] != domain.ChannelChat {
		t.Errorf("渠道 = %v, 期望按 domain 顺序 [email sms chat]", channels)
	}
	if set.Len() != 5 {
		t.Errorf("Len = %d, 期望 5（含停用）", set.Len())
	}

	// 停用的配置不算可用实现，一并算作缺口。
	unbound := set.Unbound(domain.DeliveryChannels())
	if len(unbound) != 3 || unbound[0] != domain.ChannelInApp ||
		unbound[1] != domain.ChannelPush || unbound[2] != domain.ChannelChat {
		t.Errorf("未绑定渠道 = %v, 期望 [in_app push chat]", unbound)
	}
}

func TestSetSnapshotIsImmutable(t *testing.T) {
	set, err := NewSet([]Config{{
		ID: "email-smtp", Channel: domain.ChannelEmail, Kind: KindSMTP,
		Credentials: Credentials{"host": "h", "from": "f"},
	}})
	if err != nil {
		t.Fatalf("构造配置集失败: %v", err)
	}

	all := set.All()
	all[0].Credentials["password"] = "injected"
	all[0].ID = "hacked"

	again, ok := set.Primary(domain.ChannelEmail)
	if !ok {
		t.Fatal("应能取到默认实现")
	}
	if again.ID != "email-smtp" {
		t.Errorf("修改返回值影响了配置集：ID = %q", again.ID)
	}
	if again.Credentials.Has("password") {
		t.Error("修改返回值影响了配置集里的凭据")
	}
}

func TestSetStringMasksCredentials(t *testing.T) {
	set, err := NewSet([]Config{
		{
			ID: "email-smtp", Channel: domain.ChannelEmail, Kind: KindSMTP,
			Credentials: Credentials{"host": "h", "from": "f", "password": "topsecret"},
		},
		{ID: "email-console", Channel: domain.ChannelEmail, Kind: KindConsole, Enabled: boolPtr(false)},
	})
	if err != nil {
		t.Fatalf("构造配置集失败: %v", err)
	}

	text := set.String()
	if strings.Contains(text, "topsecret") {
		t.Errorf("摘要泄露了凭据值：%s", text)
	}
	if !strings.Contains(text, "email-smtp") || !strings.Contains(text, "email-console(off)") {
		t.Errorf("摘要应包含标识与开关状态：%s", text)
	}
	if !strings.Contains(text, "email:[") {
		t.Errorf("摘要应包含渠道绑定：%s", text)
	}

	cfg := set.All()[0]
	desc := cfg.String()
	if strings.Contains(desc, "topsecret") || strings.Contains(desc, "=f ") {
		t.Errorf("单条描述泄露了凭据值：%s", desc)
	}
	if !strings.Contains(desc, "credentials=[") || !strings.Contains(desc, "password") {
		t.Errorf("单条描述应只列凭据名：%s", desc)
	}
	var nilSet *Set
	if nilSet.String() != "Set{empty}" || nilSet.Len() != 0 || nilSet.All() != nil {
		t.Error("nil 配置集应安全返回空值")
	}
}

func TestBindBuildsRegistryFromConfig(t *testing.T) {
	set, err := NewSet([]Config{
		{ID: "email-console", Channel: domain.ChannelEmail, Kind: KindConsole, Priority: intPtr(100)},
		{ID: "email-primary", Channel: domain.ChannelEmail, Kind: KindConsole, Priority: intPtr(5)},
		{ID: "sms-console", Channel: domain.ChannelSMS, Kind: KindConsole, Enabled: boolPtr(false)},
	})
	if err != nil {
		t.Fatalf("构造配置集失败: %v", err)
	}

	registry := NewRegistry()
	report, err := set.Bind(registry, testFactory())
	if err != nil {
		t.Fatalf("装配失败: %v", err)
	}
	if len(report.Bound) != 3 || len(report.Skipped) != 0 {
		t.Fatalf("装配结果 bound=%d skipped=%d, 期望 3/0", len(report.Bound), len(report.Skipped))
	}

	entry, ok := registry.Select(domain.ChannelEmail)
	if !ok {
		t.Fatal("email 渠道应有可用实现")
	}
	if entry.ID != "email-primary" || entry.Priority != 5 {
		t.Errorf("默认实现 = %s(priority=%d), 期望 email-primary(5)", entry.ID, entry.Priority)
	}
	if entry, ok := registry.LookupByID("sms-console"); !ok || entry.Enabled {
		t.Error("停用的配置应登记为停用条目，便于运行期再启用")
	}
	if candidates := registry.Candidates(domain.ChannelEmail); len(candidates) != 2 ||
		candidates[0].ID != "email-primary" {
		t.Errorf("email 候选序列 = %v", candidates)
	}

	// sms 只登记了停用条目，同样属于缺口。
	wantMissing := []domain.Channel{
		domain.ChannelInApp, domain.ChannelSMS, domain.ChannelPush, domain.ChannelChat,
	}
	if len(report.Missing) != len(wantMissing) {
		t.Fatalf("缺口 = %v, 期望 %v", report.Missing, wantMissing)
	}
	for i, ch := range wantMissing {
		if report.Missing[i] != ch {
			t.Errorf("缺口[%d] = %s, 期望 %s", i, report.Missing[i], ch)
		}
	}
}

func TestPushMockKindSharedWithAPI(t *testing.T) {
	// 配置文件与 API 侧共用：推送 mock 的条目在这里应当被识别、校验通过，
	// 由工厂报告「实现尚未接入」而不是当成未知种类让引擎起不来。
	set, err := NewSet([]Config{{
		ID:      "push-mock",
		Channel: domain.ChannelPush,
		Kind:    KindPushMock,
		Credentials: Credentials{
			"platform": "apns",
			"topic":    "com.example.xw",
			"capacity": "100",
		},
	}})
	if err != nil {
		t.Fatalf("push-mock 配置应被接受: %v", err)
	}

	report, err := set.Bind(NewRegistry(), FactoryFunc(func(cfg Config) (Sender, error) {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedKind, cfg.Kind)
	}))
	if err != nil {
		t.Fatalf("未接入的种类不应让装配失败: %v", err)
	}
	if len(report.Bound) != 0 || len(report.Skipped) != 1 || report.Skipped[0].Kind != KindPushMock {
		t.Fatalf("装配报告 = %+v，期望跳过 push-mock", report)
	}

	// fcm 的凭据按 HTTP v1 约定：项目标识加 OAuth2 访问令牌。
	if _, err := NewSet([]Config{{
		ID:          "push-fcm",
		Channel:     domain.ChannelPush,
		Kind:        KindFCM,
		Credentials: Credentials{"project_id": "xw-notify"},
	}}); err == nil || !strings.Contains(err.Error(), "access_token") {
		t.Fatalf("缺少 access_token 应在装配阶段报出，实际: %v", err)
	}
}

func TestBindSkipsUnsupportedKind(t *testing.T) {
	set, err := NewSet([]Config{
		{ID: "email-console", Channel: domain.ChannelEmail, Kind: KindConsole},
		{
			ID: "email-smtp", Channel: domain.ChannelEmail, Kind: KindSMTP,
			Credentials: Credentials{"host": "h", "from": "f"},
		},
	})
	if err != nil {
		t.Fatalf("构造配置集失败: %v", err)
	}

	registry := NewRegistry()
	report, err := set.Bind(registry, FactoryFunc(func(cfg Config) (Sender, error) {
		if cfg.Kind != KindConsole {
			return nil, fmt.Errorf("%w: %s", ErrUnsupportedKind, cfg.Kind)
		}
		return newTestSender(cfg.Channel, cfg.ID), nil
	}))
	if err != nil {
		t.Fatalf("未接入的种类不应让装配失败: %v", err)
	}
	if len(report.Bound) != 1 || report.Bound[0].ID != "email-console" {
		t.Errorf("已登记 = %v, 期望只有 email-console", report.Bound)
	}
	if len(report.Skipped) != 1 || report.Skipped[0].Kind != KindSMTP {
		t.Fatalf("跳过记录 = %v, 期望一条 smtp", report.Skipped)
	}
	if !strings.Contains(report.Skipped[0].Reason, "尚未接入") {
		t.Errorf("跳过原因 = %q", report.Skipped[0].Reason)
	}
}

func TestBindAbortsWithoutPartialRegistration(t *testing.T) {
	set, err := NewSet([]Config{
		{ID: "email-console", Channel: domain.ChannelEmail, Kind: KindConsole},
		{ID: "sms-console", Channel: domain.ChannelSMS, Kind: KindConsole},
	})
	if err != nil {
		t.Fatalf("构造配置集失败: %v", err)
	}

	registry := NewRegistry()
	_, err = set.Bind(registry, FactoryFunc(func(cfg Config) (Sender, error) {
		if cfg.Channel == domain.ChannelSMS {
			return nil, errors.New("短信账号不可用")
		}
		return newTestSender(cfg.Channel, cfg.ID), nil
	}))
	if err == nil {
		t.Fatal("构造失败应向上返回")
	}
	if !strings.Contains(err.Error(), "sms-console") {
		t.Errorf("错误信息应指出是哪条配置：%v", err)
	}
	if registry.Len() != 0 {
		t.Errorf("装配失败后注册表应为空，实际 %d 条", registry.Len())
	}
}

func TestBindRejectsInvalidArguments(t *testing.T) {
	set, err := NewSet([]Config{consoleEmail()})
	if err != nil {
		t.Fatalf("构造配置集失败: %v", err)
	}
	if _, err := set.Bind(nil, testFactory()); err == nil {
		t.Error("注册表为空时应报错")
	}
	if _, err := set.Bind(NewRegistry(), nil); err == nil {
		t.Error("未提供工厂时应报错")
	}
	var nilSet *Set
	if _, err := nilSet.Bind(NewRegistry(), testFactory()); err == nil {
		t.Error("配置集为空时应报错")
	}
}

func TestLoadSetFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "providers.json")
	content := `{"providers": [
		{"id": "email-console", "channel": "email", "kind": "console"},
		{"id": "inapp", "channel": "in_app", "kind": "inapp"}
	]}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("写测试配置失败: %v", err)
	}

	set, err := LoadSet(path)
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	if set.Source() != path {
		t.Errorf("来源 = %q, 期望 %q", set.Source(), path)
	}
	if set.Len() != 2 || len(set.Channels()) != 2 {
		t.Errorf("加载结果 = %v", set)
	}

	badPath := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(badPath, []byte(`[{"id": "", "channel": "email", "kind": "console"}]`), 0o600); err != nil {
		t.Fatalf("写测试配置失败: %v", err)
	}
	_, err = LoadSet(badPath)
	if err == nil {
		t.Fatal("非法配置应返回错误")
	}
	if !strings.Contains(err.Error(), badPath) {
		t.Errorf("错误信息应带文件路径：%v", err)
	}

	if _, err := LoadSet(filepath.Join(dir, "missing.json")); err == nil {
		t.Error("文件不存在时应返回错误")
	}
}

func TestKindChannelsAndKinds(t *testing.T) {
	channels, ok := KindChannels(KindSMTP)
	if !ok || len(channels) != 1 || channels[0] != domain.ChannelEmail {
		t.Errorf("smtp 渠道 = %v, 期望 [email]", channels)
	}
	if _, ok := KindChannels("carrier-pigeon"); ok {
		t.Error("未知种类应返回 false")
	}
	kinds := Kinds()
	if len(kinds) != len(kindSpecs) {
		t.Fatalf("Kinds 条数 = %d, 期望 %d", len(kinds), len(kindSpecs))
	}
	for i := 1; i < len(kinds); i++ {
		if kinds[i-1] > kinds[i] {
			t.Fatalf("Kinds 未排序：%v", kinds)
		}
	}
	// 返回值应是副本，改动它不影响内部表。
	channels[0] = "mutated"
	if again, _ := KindChannels(KindSMTP); again[0] != domain.ChannelEmail {
		t.Error("KindChannels 应返回副本")
	}
}

// testFactory 只支持 console 种类，其余按「尚未接入」处理。
func testFactory() Factory {
	return FactoryFunc(func(cfg Config) (Sender, error) {
		if cfg.Kind != KindConsole {
			return nil, fmt.Errorf("%w: %s", ErrUnsupportedKind, cfg.Kind)
		}
		return newTestSender(cfg.Channel, cfg.ID), nil
	})
}
