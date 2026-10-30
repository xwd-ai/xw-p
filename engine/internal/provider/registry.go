package provider

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// DefaultPriority 是 Register 登记实现时使用的默认优先级。
//
// 数值越小越优先；同一渠道内优先级相同时按登记先后取用先登记者。
const DefaultPriority = 100

var (
	// ErrNilSender 表示登记的实现为空。
	ErrNilSender = errors.New("provider: 登记的实现不能为空")
	// ErrInvalidChannel 表示渠道不是可投递渠道（空值或 delay/digest 等编排步骤）。
	ErrInvalidChannel = errors.New("provider: 渠道必须是可投递渠道")
	// ErrChannelMismatch 表示登记条目的渠道与实现自身声明的渠道不一致。
	ErrChannelMismatch = errors.New("provider: 登记条目与实现声明的渠道不一致")
	// ErrNegativePriority 表示优先级为负数。
	ErrNegativePriority = errors.New("provider: 优先级不能为负数")
	// ErrNotFound 表示按标识没有找到登记条目。
	ErrNotFound = errors.New("provider: 未找到该实现")
)

// Entry 是注册表中的一条登记：一个渠道实现，加上它在该渠道内的调度属性。
//
// ID 在注册表内唯一，是启停与替换的寻址键；Priority 决定同渠道内的取用
// 顺序（小的优先）；Enabled 为假时取用会被跳过，但条目仍留在表里，便于
// 临时摘除后再放回。
type Entry struct {
	ID       string
	Channel  domain.Channel
	Priority int
	Enabled  bool
	Sender   Sender
}

// Registry 是渠道实现的注册表：按渠道索引，同一渠道可并存多个实现。
//
// 读多写少，内部用读写锁保护：投递路径上 N 个消费者并发按渠道取用，
// 管理路径（启停、替换）随时可能写入，二者不会互相踩到半更新的切片。
type Registry struct {
	mu      sync.RWMutex
	entries []Entry
	// position 是 ID → entries 下标，用于 O(1) 寻址与替换。
	position map[string]int
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{position: make(map[string]int)}
}

// Register 以默认优先级登记一个渠道实现。
//
// 标识优先取实现自带的（Identifier），没有则按渠道自动编号：首个为渠道名
// （如 "email"），其后依次为 "email-2"、"email-3"。同一标识再次登记视为
// 原地替换（保留原有取用位置），便于运行期换掉某个实现。
func (r *Registry) Register(s Sender) error {
	if s == nil {
		return ErrNilSender
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	id := ""
	if named, ok := s.(Identifier); ok {
		id = strings.TrimSpace(named.ID())
	}
	if id == "" {
		id = r.uniqueIDLocked(s.Channel())
	}
	return r.upsertLocked(Entry{
		ID:       id,
		Channel:  s.Channel(),
		Priority: DefaultPriority,
		Enabled:  true,
		Sender:   s,
	})
}

// RegisterEntry 登记一条完整配置（标识、优先级、开关）。
// 标识为空时自动编号；已存在的标识按替换处理。
func (r *Registry) RegisterEntry(e Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if strings.TrimSpace(e.ID) == "" {
		e.ID = r.uniqueIDLocked(e.Channel)
	}
	return r.upsertLocked(e)
}

// upsertLocked 校验并写入一条登记，调用方必须持有写锁。
func (r *Registry) upsertLocked(e Entry) error {
	if e.Sender == nil {
		return ErrNilSender
	}
	if e.Channel == "" || !e.Channel.IsDelivery() {
		return fmt.Errorf("%w: %q", ErrInvalidChannel, string(e.Channel))
	}
	if declared := e.Sender.Channel(); declared != e.Channel {
		return fmt.Errorf("%w: 条目 %q, 实现 %q", ErrChannelMismatch, string(e.Channel), string(declared))
	}
	if e.Priority < 0 {
		return fmt.Errorf("%w: %d", ErrNegativePriority, e.Priority)
	}
	e.ID = strings.TrimSpace(e.ID)
	if idx, ok := r.position[e.ID]; ok {
		r.entries[idx] = e
		return nil
	}
	r.position[e.ID] = len(r.entries)
	r.entries = append(r.entries, e)
	return nil
}

// uniqueIDLocked 为没有自带标识的实现生成渠道内唯一的标识，调用方必须持有写锁。
func (r *Registry) uniqueIDLocked(ch domain.Channel) string {
	base := string(ch)
	if base == "" {
		base = "provider"
	}
	if _, taken := r.position[base]; !taken {
		return base
	}
	for i := 2; ; i++ {
		candidate := base + "-" + strconv.Itoa(i)
		if _, taken := r.position[candidate]; !taken {
			return candidate
		}
	}
}

// Lookup 按渠道取用实现：返回该渠道内优先级最高且启用的实现。
func (r *Registry) Lookup(ch domain.Channel) (Sender, bool) {
	e, ok := r.Select(ch)
	if !ok {
		return nil, false
	}
	return e.Sender, true
}

// Select 按渠道取用登记条目（含标识与优先级），供调用方打日志或做后续回退。
func (r *Registry) Select(ch domain.Channel) (Entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	best := -1
	for i := range r.entries {
		e := &r.entries[i]
		if e.Channel != ch || !e.Enabled {
			continue
		}
		// 严格小于：优先级相同时保留先登记的，取用顺序不随后续登记漂移。
		if best < 0 || e.Priority < r.entries[best].Priority {
			best = i
		}
	}
	if best < 0 {
		return Entry{}, false
	}
	return r.entries[best], true
}

// Candidates 按渠道返回全部启用实现，按优先级、登记顺序排列。
//
// 这是主备切换的取用序列：第一个是默认实现，其后是依次回退的备选。
func (r *Registry) Candidates(ch domain.Channel) []Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]Entry, 0, len(r.entries))
	for _, e := range r.entries {
		if e.Channel == ch && e.Enabled {
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Priority < out[j].Priority })
	return out
}

// LookupByID 按标识查找登记条目。
func (r *Registry) LookupByID(id string) (Entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	idx, ok := r.position[strings.TrimSpace(id)]
	if !ok {
		return Entry{}, false
	}
	return r.entries[idx], true
}

// SetEnabled 启停一条登记；条目不存在时返回 ErrNotFound。
func (r *Registry) SetEnabled(id string, enabled bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	idx, ok := r.position[strings.TrimSpace(id)]
	if !ok {
		return fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	r.entries[idx].Enabled = enabled
	return nil
}

// Remove 移除一条登记，返回是否命中。
func (r *Registry) Remove(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	idx, ok := r.position[strings.TrimSpace(id)]
	if !ok {
		return false
	}
	r.entries = append(r.entries[:idx], r.entries[idx+1:]...)
	delete(r.position, id)
	// 后面的条目下标整体前移，重建索引（条目数量级很小，直接全量重建）。
	for i := idx; i < len(r.entries); i++ {
		r.position[r.entries[i].ID] = i
	}
	return true
}

// Has 判断某渠道是否有可用的启用实现。
func (r *Registry) Has(ch domain.Channel) bool {
	_, ok := r.Select(ch)
	return ok
}

// Channels 返回登记过的渠道（去重），顺序与 domain.AllChannels 一致，便于日志稳定可读。
func (r *Registry) Channels() []domain.Channel {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[domain.Channel]bool, len(r.entries))
	for _, e := range r.entries {
		seen[e.Channel] = true
	}
	out := make([]domain.Channel, 0, len(seen))
	for _, ch := range domain.AllChannels() {
		if seen[ch] {
			delete(seen, ch)
			out = append(out, ch)
		}
	}
	// 理论上不会有未知渠道（登记时已校验），保险起见把剩余项按名称补在后面。
	if len(seen) > 0 {
		rest := make([]string, 0, len(seen))
		for ch := range seen {
			rest = append(rest, string(ch))
		}
		sort.Strings(rest)
		for _, ch := range rest {
			out = append(out, domain.Channel(ch))
		}
	}
	return out
}

// Entries 返回全部登记的快照（按登记顺序），调用方可安全修改返回值。
func (r *Registry) Entries() []Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]Entry, len(r.entries))
	copy(out, r.entries)
	return out
}

// Len 返回登记条目总数（含已停用的）。
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

// String 返回注册表的人类可读描述（调试与启动日志用）。
func (r *Registry) String() string {
	return fmt.Sprintf("Registry{entries=%d channels=%v}", r.Len(), r.Channels())
}
