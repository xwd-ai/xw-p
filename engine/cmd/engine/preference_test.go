package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xwd-ai/xw-p/engine/internal/config"
)

// 启动装配要把偏好判定接到管线上：配置成文件就读文件，写 off 就完全关闭。
// 这两条路径各写错一次，后果都不轻——前者是偏好设置被静默忽略，后者是
// 一个手滑的 off 让所有渠道都按接收处理。

func TestNewPipelineWiresPreferences(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "preferences.jsonl")
	content := `{"subscriber_id":"creator_1","scope":"channel","target":"email","enabled":false}` + "\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("写入偏好文件失败: %v", err)
	}

	pipe, closePipe, err := newPipeline(discardLogger(), config.Config{
		DigestStore:     "memory",
		PreferencesFile: path,
	})
	if err != nil {
		t.Fatalf("装配管线失败：%v", err)
	}
	defer closePipe()

	gate := pipe.Preferences()
	if gate == nil {
		t.Fatal("配置了偏好文件时应装配判定器")
	}
	if decision := gate.Decide("creator_1", "post_liked", "email"); decision.Allowed {
		t.Fatalf("应读到关闭邮件的记录: %+v", decision)
	}
	if decision := gate.Decide("creator_1", "post_liked", "in_app"); !decision.Allowed {
		t.Fatalf("未设置偏好的渠道应按接收: %+v", decision)
	}
}

func TestNewPipelineDisablesPreferences(t *testing.T) {
	pipe, closePipe, err := newPipeline(discardLogger(), config.Config{
		DigestStore:     "memory",
		PreferencesFile: "off",
	})
	if err != nil {
		t.Fatalf("装配管线失败：%v", err)
	}
	defer closePipe()
	if pipe.Preferences() != nil {
		t.Error("写 off 时不该装配判定器")
	}
}
