package webhook

import (
	"strings"
	"testing"
)

// 只接受 https，http 仅限本机回环，且地址不能带凭据或片段。
func TestParseTarget(t *testing.T) {
	valid := []string{
		"https://community.example.com/hooks/xw",
		"https://community.example.com/hooks/xw?token=abc",
		"http://127.0.0.1:8099/cb",
		"http://localhost:9000/v1/callback",
		"http://[::1]:9000/cb",
		"  https://community.example.com/cb  ",
	}
	for _, raw := range valid {
		if _, err := ParseTarget(raw); err != nil {
			t.Errorf("%q 应被接受：%v", raw, err)
		}
	}

	invalid := map[string]string{
		"空地址":       "",
		"只有空白":      "   ",
		"ftp 协议":    "ftp://example.com/cb",
		"非回环 http":  "http://community.example.com/cb",
		"缺少主机":      "https:///cb",
		"缺少路径":      "https://community.example.com",
		"带凭据":       "https://user:pass@example.com/cb",
		"带片段":       "https://example.com/cb#section",
		"地址过长":      "https://example.com/" + strings.Repeat("a", MaxURLLength),
		"相对地址":      "/hooks/xw",
		"私有网段 http": "http://10.0.0.8/cb",
	}
	for name, raw := range invalid {
		if _, err := ParseTarget(raw); err == nil {
			t.Errorf("%s：%q 应被拒绝", name, raw)
		}
	}
}

// 日志与错误信息里只能出现协议与主机，路径与查询串里的令牌不能泄露。
func TestTargetMasksCredentials(t *testing.T) {
	target, err := ParseTarget("https://hooks.example.com/services/T000/B000/s3cr3t-token?sign=abc")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if got := target.String(); got != "https://hooks.example.com" {
		t.Errorf("脱敏值应只保留协议与主机，实际 %q", got)
	}
	if strings.Contains(target.String(), "s3cr3t") || strings.Contains(target.String(), "sign=") {
		t.Errorf("脱敏值泄露了令牌：%q", target.String())
	}
	if target.Host() != "hooks.example.com" {
		t.Errorf("主机名不符: %q", target.Host())
	}
	if !strings.HasPrefix(target.URL(), "https://hooks.example.com/services/") {
		t.Errorf("原值应保留完整地址: %q", target.URL())
	}
}

// 零值表示未配置回调。
func TestTargetZeroValue(t *testing.T) {
	var target Target
	if !target.IsZero() {
		t.Error("零值应视为未配置")
	}
	if target.String() != "" {
		t.Errorf("零值的脱敏值应为空，实际 %q", target.String())
	}
}
