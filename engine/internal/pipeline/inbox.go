package pipeline

import (
	"errors"
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
//   - JSONLInboxStore：追加到 JSONL 文件，进程重启后仍可回放，
//     默认落在 api/data/inbox.jsonl，由 Python API 侧收件箱接口读取。
//     文件是「一行一个版本」的追加流，改写同一条目等于再追加一版，
//     读侧按 ID 取最后一行，细节见 inbox_jsonl.go。
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
	// CreatedAt 是写入收件箱的时间（聚合卡片为开卡时刻，窗口内不变）。
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt 是条目内容最近一次变更的时间：普通站内信与 CreatedAt 相同，
	// 聚合卡片每次命中窗口都会推进它。API 侧用它判定「已读的卡片又被新互动
	// 顶起来」应该重新算未读。
	UpdatedAt time.Time `json:"updated_at"`
	// Tenant / Environment 是隔离维度：同一个收件箱文件里存着多个租户、
	// 多个环境的消息，API 读取时按这两个字段过滤。
	Tenant      string `json:"tenant,omitempty"`
	Environment string `json:"environment,omitempty"`
	// Read 标记是否已读；引擎侧始终写入 false，已读状态由 API 侧维护。
	Read bool `json:"read"`
}

// InboxStore 是收件箱存储接口。
type InboxStore interface {
	// Save 写入一条站内信；返回 inserted=false 表示同 ID 条目已存在（幂等）。
	Save(entry InboxEntry) (inserted bool, err error)
	// Update 覆盖写入同 ID 条目，用于摘要卡片（digest）原地累加计数；
	// 返回 updated=false 表示该条目已不存在（需要按新条目重新落库）。
	Update(entry InboxEntry) (updated bool, err error)
	// List 返回某订阅者的收件箱条目（按写入顺序）。
	List(subscriberID string) ([]InboxEntry, error)
}

// InboxStoreProvider 由「投递即落库」的 provider 实现（当前为 in_app）。
//
// digest 步骤需要往收件箱里写聚合卡片、并在窗口内原地改写同一条目，
// 因此它必须拿到与 in_app provider 完全相同的存储实例；由 provider 主动
// 暴露存储，避免管线另外 new 一份（两份索引会各自漂移）。
type InboxStoreProvider interface {
	InboxStore() InboxStore
}

// MemoryInboxStore 是进程内收件箱存储，线程安全。
type MemoryInboxStore struct {
	mu      sync.Mutex
	entries []InboxEntry
	// index 记录条目 ID → entries 下标，供 Update 原地改写而不打乱写入顺序。
	index map[string]int
}

// NewMemoryInboxStore 创建空的内存收件箱。
func NewMemoryInboxStore() *MemoryInboxStore {
	return &MemoryInboxStore{index: make(map[string]int)}
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
	s.index[entry.ID] = len(s.entries)
	s.entries = append(s.entries, entry)
	return true, nil
}

// Update 原地覆盖同 ID 条目（摘要卡片累加计数走这条路，不新增条目）。
func (s *MemoryInboxStore) Update(entry InboxEntry) (bool, error) {
	if entry.ID == "" {
		return false, errors.New("收件箱条目缺少 ID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pos, ok := s.index[entry.ID]
	if !ok {
		return false, nil
	}
	s.entries[pos] = entry
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
