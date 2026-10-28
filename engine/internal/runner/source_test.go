package runner

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cdgjhg/xw-p/engine/internal/domain"
)

func TestStaticSourceRegisterAndLookup(t *testing.T) {
	s := NewStaticSource()

	// 没有步骤或没有名称的定义执行不出任何东西，登记时应被忽略。
	s.Register(domain.Workflow{Name: "post_liked"})
	s.Register(domain.Workflow{Steps: []domain.Step{{ID: "s1", Type: domain.ChannelInApp}}})
	if s.Len() != 0 {
		t.Fatalf("无效定义不应入表，实际 %d 条", s.Len())
	}

	wf := domain.Workflow{
		Name:    "post_liked",
		Version: 2,
		Steps:   []domain.Step{{ID: "step-1", Type: domain.ChannelInApp}},
	}
	s.Register(wf)
	got, ok := s.Lookup("post_liked")
	if !ok || got.Version != 2 || len(got.Steps) != 1 {
		t.Fatalf("查找结果错误: %+v ok=%v", got, ok)
	}
	if _, ok := s.Lookup("post_commented"); ok {
		t.Errorf("未登记的工作流不应命中")
	}
}

func TestLoadWorkflowsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workflows.json")
	content := `[
		{"name": "post_liked", "version": 3, "steps": [
			{"id": "step-1", "type": "in_app", "name": "站内信",
			 "config": {"template": "{{ payload.actor }} 赞了你的帖子"}}
		]},
		{"name": "post_mentioned", "version": 1, "steps": [
			{"id": "step-1", "type": "push", "name": "推送", "config": {}}
		]}
	]`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("写入临时文件失败: %v", err)
	}

	source, found, err := LoadWorkflowsFile(path)
	if err != nil || !found {
		t.Fatalf("加载失败: found=%v err=%v", found, err)
	}
	if source.Len() != 2 {
		t.Fatalf("应加载 2 份定义，实际 %d", source.Len())
	}
	wf, ok := source.Lookup("post_liked")
	if !ok || wf.Version != 3 || wf.Steps[0].Type != domain.ChannelInApp {
		t.Fatalf("定义内容错误: %+v", wf)
	}
	if wf.Steps[0].Config["template"] == "" {
		t.Errorf("步骤配置丢失: %+v", wf.Steps[0].Config)
	}
}

func TestLoadWorkflowsFileMissing(t *testing.T) {
	source, found, err := LoadWorkflowsFile(filepath.Join(t.TempDir(), "absent.json"))
	if err != nil {
		t.Fatalf("文件不存在不应报错: %v", err)
	}
	if found {
		t.Errorf("文件不存在时 found 应为 false")
	}
	if source.Len() != 0 {
		t.Errorf("应返回空表，实际 %d 条", source.Len())
	}
}

func TestLoadWorkflowsFileInvalidJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broken.json")
	if err := os.WriteFile(path, []byte("{not-an-array"), 0o600); err != nil {
		t.Fatalf("写入临时文件失败: %v", err)
	}
	if _, _, err := LoadWorkflowsFile(path); err == nil {
		t.Fatal("解析失败应返回错误")
	}
}
