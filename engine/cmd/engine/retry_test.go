package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/config"
	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

const retryProvidersFile = `{
  "providers": [
    {"id": "console-email", "channel": "email", "kind": "console"},
    {"id": "console-sms", "channel": "sms", "kind": "console"}
  ],
  "retries": {
    "default": {"max_attempts": 1},
    "email": {"max_attempts": 3, "base_delay": "80ms", "max_delay": "600ms", "jitter": 0}
  }
}`

func writeProvidersFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "providers.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("写测试配置失败：%v", err)
	}
	return path
}

// 渠道文件里的 retries 段应随注册表一起装配出来。
func TestBuildRegistryReadsRetryTable(t *testing.T) {
	path := writeProvidersFile(t, retryProvidersFile)

	_, retries, err := buildRegistry(discardLogger(), config.Config{ProvidersFile: path})
	if err != nil {
		t.Fatalf("装配失败：%v", err)
	}
	if !retries.Enabled() {
		t.Fatal("email 配了 3 次重试，应视为已启用")
	}
	if got := retries.Lookup(domain.ChannelEmail).Attempts(); got != 3 {
		t.Errorf("email 尝试次数 = %d, 期望 3", got)
	}
	if got := retries.Lookup(domain.ChannelSMS).Attempts(); got != 1 {
		t.Errorf("sms 沿用默认段，尝试次数 = %d, 期望 1", got)
	}
}

// 重试配置写错时整份配置失效，而不是带着默认值启动。
func TestBuildRegistryRejectsInvalidRetryTable(t *testing.T) {
	path := writeProvidersFile(t, strings.Replace(
		retryProvidersFile,
		`"max_attempts": 3`,
		`"max_attempts": 99`,
		1,
	))

	registry, _, err := buildRegistry(discardLogger(), config.Config{ProvidersFile: path})
	if err == nil {
		t.Fatal("非法的重试配置应让装配失败")
	}
	if registry != nil {
		t.Error("失败时不应返回注册表")
	}
	if !strings.Contains(err.Error(), "max_attempts") {
		t.Errorf("错误信息应指出问题字段：%v", err)
	}
}

// 装配完成后的管线要带上同一份策略，否则配置只在校验时生效、投递时被忽略。
func TestNewPipelineAppliesRetryTable(t *testing.T) {
	path := writeProvidersFile(t, retryProvidersFile)

	pipe, closePipe, err := newPipeline(discardLogger(), config.Config{
		ProvidersFile: path,
		DigestStore:   "memory",
	})
	if err != nil {
		t.Fatalf("装配管线失败：%v", err)
	}
	defer closePipe()

	if got := pipe.ProviderRetries().Lookup(domain.ChannelEmail).Attempts(); got != 3 {
		t.Errorf("管线上的 email 尝试次数 = %d, 期望 3", got)
	}
}
