package runner

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// StaticSource 是进程内的工作流快照表。
//
// 用途：给没有携带步骤定义的消息（历史消息、手工构造的消息）提供按名称
// 回退查找。数据来自启动时加载的 JSON 文件，格式与工作流 CRUD 接口返回的
// 列表一致 —— 一个 Workflow 对象数组：
//
//	[{"name": "post_liked", "version": 3, "steps": [{"id": "step-1", "type": "digest", ...}]}]
//
// 表内容只在启动时加载：引擎不负责维护模板，模板的增删改由 API 侧完成，
// 消息自带的快照才是执行依据。这里存在的意义是「让没有快照的老消息也能跑」。
type StaticSource struct {
	mu     sync.RWMutex
	byName map[string]domain.Workflow
}

// NewStaticSource 用给定的工作流构造查找表；同名的后一个覆盖前一个。
func NewStaticSource(workflows ...domain.Workflow) *StaticSource {
	s := &StaticSource{byName: make(map[string]domain.Workflow, len(workflows))}
	for _, wf := range workflows {
		s.Register(wf)
	}
	return s
}

// Register 登记/覆盖一份工作流定义。名称为空或没有步骤的定义会被忽略，
// 因为它们执行不出任何东西，留在表里只会掩盖「消息其实不可执行」。
func (s *StaticSource) Register(wf domain.Workflow) {
	if wf.Name == "" || len(wf.Steps) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byName[wf.Name] = wf
}

// Lookup 按名称取工作流定义。
func (s *StaticSource) Lookup(name string) (domain.Workflow, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	wf, ok := s.byName[name]
	return wf, ok
}

// Len 返回已登记的工作流数量。
func (s *StaticSource) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byName)
}

// LoadWorkflowsFile 从 JSON 文件加载工作流定义。文件不存在时返回空表与 false，
// 调用方据此决定是否提示「未配置文件」；文件存在但解析失败属于配置错误。
func LoadWorkflowsFile(path string) (*StaticSource, bool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return NewStaticSource(), false, nil
		}
		return nil, false, fmt.Errorf("读取工作流文件 %s 失败：%w", path, err)
	}

	var workflows []domain.Workflow
	if err := json.Unmarshal(raw, &workflows); err != nil {
		return nil, true, fmt.Errorf("解析工作流文件 %s 失败：%w", path, err)
	}
	return NewStaticSource(workflows...), true, nil
}
