package pipeline

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// 站内信（In-app）落库：InboxStore 抽象 + 内存/JSONL 两种实现。
//
// 站内信与 email/sms/push 的差别在于「投递即落库」——它没有外部网关，
// 投递动作本身就是把一条消息写进订阅者的收件箱，等待客户端拉取。
// 收件箱因此需要一层可替换的存储：
//
//   - MemoryInboxStore：进程内存储，用于单元测试与无需持久化的场景；
//   - JSONLInboxStore：追加写入 JSONL 文件，进程重启后仍可回放，
//     默认落在 api/data/inbox.jsonl，由 Python API 侧收件箱接口读取。
//
// 两条实现都按条目 ID 去重：同一条触发消息重放时不会在收件箱里出现两次。

// InboxEntry 是收件箱中的一条站内信。
//
// JSON 字段名与 Python 侧 api/app/inbox.py 的收件箱条目保持一致（snake_case），
// 引擎写入后由 API 直接读取渲染。CreatedAt 以 RFC3339 序列化。
type InboxEntry struct {
	// ID 是收件箱条目唯一标识，用于幂等去重（默认 trigger_id:step_id）。
	ID string `json:"id"`
	// TriggerID 是触发这次投递的事件回执 ID。
	TriggerID string `json:"trigger_id"`
	// SubscriberID 是收件人（创作者）标识。
	SubscriberID string `json:"subscriber_id"`
	// Workflow 是触发的工作流名（如 post_liked）。
	Workflow string `json:"workflow"`
	// StepID 是产生这条站内信的工作流步骤 ID。
	StepID string `json:"step_id"`
	// Title 是收件箱条目标题。
	Title string `json:"title"`
	// Body 是收件箱条目正文（已渲染）。
	Body string `json:"body"`
	// CreatedAt 是写入收件箱的时间。
	CreatedAt time.Time `json:"created_at"`
	// Read 标记是否已读；引擎侧始终写入 false，已读状态由 API 侧维护。
	Read bool `json:"read"`
}

// InboxStore 是收件箱存储接口。
type InboxStore interface {
	// Save 写入一条站内信；返回 inserted=false 表示同 ID 条目已存在（幂等）。
	Save(entry InboxEntry) (inserted bool, err error)
	// List 返回某订阅者的收件箱条目（按写入顺序）。
	List(subscriberID string) ([]InboxEntry, error)
}

// MemoryInboxStore 是进程内收件箱存储，线程安全。
type MemoryInboxStore struct {
	mu      sync.Mutex
	entries []InboxEntry
	index   map[string]struct{}
}

// NewMemoryInboxStore 创建空的内存收件箱。
func NewMemoryInboxStore() *MemoryInboxStore {
	return &MemoryInboxStore{index: make(map[string]struct{})}
}

// Save 写入条目；同 ID 已存在时返回 false。
func (s *MemoryInboxStore) Save(entry InboxEntry) (bool, error) {
	if entry.ID == "" {
		return false, errors.New("收件箱条目缺少 ID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.index[entry.ID]; ok {
		return false, nil
	}
	s.index[entry.ID] = struct{}{}
	s.entries = append(s.entries, entry)
	return true, nil
}

// List 返回某订阅者的全部收件箱条目副本。
func (s *MemoryInboxStore) List(subscriberID string) ([]InboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]InboxEntry, 0, len(s.entries))
	for _, e := range s.entries {
		if e.SubscriberID == subscriberID {
			out = append(out, e)
		}
	}
	return out, nil
}

// Len 返回条目总数（测试与观测用）。
func (s *MemoryInboxStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// JSONLInboxStore 以「一行一个 JSON」追加写入收件箱文件。
//
// 启动时回放文件把已有条目 ID 载入内存索引，从而在进程内实现幂等去重；
// 追加写为串行加锁，避免同一引擎多 goroutine 并发投递时写坏行。
type JSONLInboxStore struct {
	mu    sync.Mutex
	path  string
	index map[string]struct{}
}

// NewJSONLInboxStore 打开（或创建）JSONL 收件箱文件并载入去重索引。
// 文件不存在时视为空收件箱，不报错。
func NewJSONLInboxStore(path string) (*JSONLInboxStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("收件箱文件路径为空")
	}
	s := &JSONLInboxStore{path: path, index: make(map[string]struct{})}
	if err := s.loadIndex(); err != nil {
		return nil, err
	}
	return s, nil
}

// loadIndex 回放文件，收集已有条目 ID（跳过损坏行，保证坏数据不阻塞启动）。
func (s *JSONLInboxStore) loadIndex() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var e InboxEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			continue
		}
		if e.ID != "" {
			s.index[e.ID] = struct{}{}
		}
	}
	return nil
}

// Save 追加写入条目；同 ID 已存在时返回 false 且不落盘。
func (s *JSONLInboxStore) Save(entry InboxEntry) (bool, error) {
	if entry.ID == "" {
		return false, errors.New("收件箱条目缺少 ID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.index[entry.ID]; ok {
		return false, nil
	}

	line, err := json.Marshal(entry)
	if err != nil {
		return false, err
	}
	if dir := filepath.Dir(s.path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return false, err
		}
	}
	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return false, err
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		return false, err
	}
	s.index[entry.ID] = struct{}{}
	return true, nil
}

// List 读取文件并返回某订阅者的条目。
func (s *JSONLInboxStore) List(subscriberID string) ([]InboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []InboxEntry
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var e InboxEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			continue
		}
		if e.SubscriberID == subscriberID {
			out = append(out, e)
		}
	}
	return out, nil
}

// 默认收件箱文件路径：引擎与 API 共用同一份数据目录，故默认指向 api/data。
// 可通过 XW_INBOX_FILE 覆盖（容器部署时通常挂载到共享卷）。
const defaultInboxFile = "api/data/inbox.jsonl"

// DefaultInboxStore 构造引擎默认使用的收件箱存储。
//
// 优先使用 JSONL 文件（与 API 共享），文件无法打开时降级为内存存储并告警——
// 收件箱落盘失败不应让整个引擎起不来。
func DefaultInboxStore(logger *slog.Logger) InboxStore {
	if logger == nil {
		logger = slog.Default()
	}
	path := strings.TrimSpace(os.Getenv("XW_INBOX_FILE"))
	if path == "" {
		path = defaultInboxFile
	}
	store, err := NewJSONLInboxStore(path)
	if err != nil {
		logger.Warn("[in_app] 收件箱文件不可用，降级为内存存储", "path", path, "error", err)
		return NewMemoryInboxStore()
	}
	return store
}
