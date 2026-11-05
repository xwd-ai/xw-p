package webhook

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/execution"
)

// 回调审计：把每一次发送尝试落成一行可查询的记录。
//
// 分发器原本只有计数（成功几条、失败几条）与日志。计数回答不了「上午十点
// 那条 delivery.failed 到底发出去没有」——日志是流水，按事件标识回查要靠
// grep；而回调失败通常由接收方发起排查，对方手里只有事件标识。审计表就是
// 为这个问题存在的：一事件多尝试，每次都给一行。
//
// 记录与事件正文分开存放：正文只在重放时需要，按事件标识追加写一份即可；
// 审计行会被反复扫描（运维翻页、按状态筛），两者放在同一行会让每次查询
// 都把正文读出来。

const (
	// AuditDelivered / AuditRetrying / AuditFailed 是一次尝试的三种归宿。
	//
	// retrying 单独一档而不是并入 failed：一次最终成功的事件中间也会有失败
	// 尝试，把中间态记成失败会让「最近失败」列表里混进已经恢复的事件。
	AuditDelivered = "delivered"
	AuditRetrying  = "retrying"
	AuditFailed    = "failed"

	// DefaultAuditLimit / MaxAuditLimit 是查询一页的条数与上限。
	DefaultAuditLimit = 100
	MaxAuditLimit     = 500

	// DefaultReplayLimit 是一次重放允许的事件数。
	//
	// 重放会立刻产生真实 HTTP 请求，误操作的影响与一次群发相当；上限压得比
	// 死信重放小，逼调用方分批确认。
	DefaultReplayLimit = 100
)

// AuditEntry 是一次回调尝试的审计记录。
type AuditEntry struct {
	// Seq 是进程内自增序号：同一毫秒内的多条记录靠它稳定排序。
	Seq int64 `json:"seq"`
	// EventID 是事件标识，与报文里的 id 一致，接收方按它回查。
	EventID string `json:"event_id"`
	// Event 是事件类型（delivery.result 等）。
	Event string `json:"event"`
	// TriggerID 是触发标识，便于按一次触发把多条事件串起来。
	TriggerID string `json:"trigger_id,omitempty"`
	// Target 是脱敏后的回调地址（协议 + 主机 + 路径，不含查询串与凭据）。
	Target string `json:"target"`
	// Attempt 是第几次尝试，从 1 开始。
	Attempt int `json:"attempt"`
	// Status 是本次尝试的归宿：delivered / retrying / failed。
	Status string `json:"status"`
	// HTTPStatus 是接收方状态码；请求没发出去时为 0。
	HTTPStatus int `json:"http_status,omitempty"`
	// Error 是失败原因，成功时为空。
	Error string `json:"error,omitempty"`
	// Retryable 表示这次失败是否还会再试。
	Retryable bool `json:"retryable,omitempty"`
	// DurationMS 是本次请求耗时（毫秒）。
	DurationMS int64 `json:"duration_ms"`
	// At 是记录时刻。
	At execution.Timestamp `json:"at"`
	// Replay 标记这条记录来自人工重放，与首次投递区分开。
	Replay bool `json:"replay,omitempty"`
}

// AuditQuery 是审计查询条件。零值表示不过滤。
type AuditQuery struct {
	EventID string
	Type    string
	Status  string
	Since   time.Time
	Until   time.Time
	Limit   int
	Offset  int
}

// Normalized 收敛越界取值。
func (q AuditQuery) Normalized() AuditQuery {
	if q.Limit <= 0 {
		q.Limit = DefaultAuditLimit
	}
	if q.Limit > MaxAuditLimit {
		q.Limit = MaxAuditLimit
	}
	if q.Offset < 0 {
		q.Offset = 0
	}
	q.EventID = strings.TrimSpace(q.EventID)
	q.Type = strings.TrimSpace(q.Type)
	q.Status = strings.TrimSpace(q.Status)
	return q
}

// AuditStore 是审计记录的存储能力。
type AuditStore interface {
	// Append 追加一条记录；实现须并发安全（多个发送 goroutine 同时写）。
	Append(entry AuditEntry) error
	// List 按条件返回记录，最新在前。
	List(q AuditQuery) ([]AuditEntry, error)
	// Close 释放文件句柄。
	Close() error
}

// EventArchive 保存事件正文，供人工重放。
type EventArchive interface {
	// Save 追加保存一条事件；同一事件标识重复保存时以最后一次为准。
	Save(ev Event) error
	// Load 按事件标识取回事件正文；不存在时返回 ErrEventNotFound。
	Load(eventID string) (Event, error)
	Close() error
}

// ErrEventNotFound 表示审计表里有记录，但事件正文没有留存。
var ErrEventNotFound = errors.New("回调事件正文不存在")

// maxArchivedEventBytes 是单条事件正文的留存上限。
//
// 回调报文本身不大（聚合卡片带互动者列表，极端情况下也只有几十 KB），
// 超过这个量的多半不是本引擎写出来的记录。
const maxArchivedEventBytes = 1 << 20

// 文件滚动参数：单个文件超过 maxFileBytes 时只保留最近 keepLines 行。
//
// 审计与归档都是追加写，没有滚动就会随运行时长无限增长——线上跑到第三个月
// 才发现审计文件有几十 GB，那时连「按标识回查」都会变成一次全文件扫描。
// 保留最近若干行而不是按时间删：排查总是最近的事故优先，且按行裁剪不需要
// 解析时间字段。
const (
	maxFileBytes = 8 << 20
	keepLines    = 5000
)

// compactJSONL 把文件裁剪到「最近 keep 行且总量不超过 halfBytes」。
//
// 两个条件都要满足：只按行数裁，行长一变（事件带互动者列表）就可能裁完
// 仍然超阈值，于是每次写入都触发一次重写，等于把滚动变成常态开销；只按
// 字节裁，则一行都可能留不下。取「行数上限 + 目标总量为阈值的一半」，
// 裁剪后至少有一半余量，下一次裁剪不必等太久。
//
// 调用方必须已经关闭写句柄并在返回后重新打开：重写同一个路径时，追加句柄
// 仍指向旧 inode，继续写会把内容写到已被替换的文件里。先写临时文件再改名，
// 是让「读到一半崩掉」最多留下一个残缺的临时文件，而不是半份审计。
func compactJSONL(path string, keep int, halfBytes int64) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if keep < 1 {
		keep = 1
	}
	start := len(lines)
	var used int64
	for start > 0 && len(lines)-start < keep {
		size := int64(len(lines[start-1]) + 1)
		if start < len(lines) && used+size > halfBytes {
			break
		}
		used += size
		start--
	}
	lines = lines[start:]
	tmp := path + ".tmp"
	body := strings.Join(lines, "\n")
	if body != "" {
		body += "\n"
	}
	if err := os.WriteFile(tmp, []byte(body), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// JSONLAuditStore 把审计记录追加写入 JSONL 文件。
//
// 查询按文件顺序读、在内存里过滤后倒序返回：审计表是排查用的旁路数据，
// 量级与回调量同阶，不值得为它建索引；顺序读还能顺带跳过写坏的半行。
type JSONLAuditStore struct {
	mu   sync.Mutex
	file *os.File
	path string
	seq  int64
	// maxBytes 是滚动阈值；0 表示用 maxFileBytes。测试会把它调小。
	maxBytes int64
}

// NewJSONLAuditStore 打开（或创建）审计文件。
func NewJSONLAuditStore(path string) (*JSONLAuditStore, error) {
	return newJSONLAuditStore(path, true)
}

func newJSONLAuditStore(path string, create bool) (*JSONLAuditStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("审计文件路径为空")
	}
	if create {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("创建审计目录失败: %w", err)
		}
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("打开审计文件失败: %w", err)
	}
	return &JSONLAuditStore{file: file, path: path}, nil
}

// Path 返回审计文件路径。
func (s *JSONLAuditStore) Path() string { return s.path }

// Append 写入一条记录。
func (s *JSONLAuditStore) Append(entry AuditEntry) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return errors.New("审计文件已关闭")
	}
	s.seq++
	entry.Seq = s.seq
	if entry.At.IsZero() {
		entry.At = execution.Timestamp{Time: time.Now().UTC()}
	}
	line, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("序列化审计记录失败: %w", err)
	}
	if _, err := s.file.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("写入审计记录失败: %w", err)
	}
	s.maybeRotate()
	return nil
}

// maybeRotate 在文件超过阈值后裁剪到最近 keepLines 行。
//
// 裁剪失败不回滚、也不报错给调用方：审计是旁路数据，写失败值得告警，
// 但不能因为裁剪不顺让一次已经发出去的回调变成错误。下次写入会再试。
func (s *JSONLAuditStore) maybeRotate() {
	limit := s.maxBytes
	if limit <= 0 {
		limit = maxFileBytes
	}
	info, err := s.file.Stat()
	if err != nil || info.Size() <= limit {
		return
	}
	// 重写同一路径前必须先放掉追加句柄：旧 inode 上的写入会丢。
	if err := s.file.Close(); err != nil {
		return
	}
	s.file = nil
	if err := compactJSONL(s.path, keepLines, limit/2); err != nil {
		return
	}
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	s.file = file
}

// List 读取并过滤审计记录，最新在前。
func (s *JSONLAuditStore) List(q AuditQuery) ([]AuditEntry, error) {
	if s == nil {
		return nil, nil
	}
	q = q.Normalized()

	s.mu.Lock()
	path := s.path
	s.mu.Unlock()

	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []AuditEntry{}, nil
		}
		return nil, fmt.Errorf("读取审计文件失败: %w", err)
	}
	defer file.Close()

	entries := make([]AuditEntry, 0, q.Limit+q.Offset)
	scanner := bufio.NewScanner(file)
	// 默认 64 KiB 的行长上限对审计行足够；写坏的超长行会被跳过而不是
	// 让整个查询失败。
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var entry AuditEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if !matchesAudit(entry, q) {
			continue
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取审计文件失败: %w", err)
	}

	// 文件按写入顺序追加，倒序即最新在前。
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
	if q.Offset >= len(entries) {
		return []AuditEntry{}, nil
	}
	entries = entries[q.Offset:]
	if len(entries) > q.Limit {
		entries = entries[:q.Limit]
	}
	return entries, nil
}

// Close 关闭文件句柄。
func (s *JSONLAuditStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func matchesAudit(entry AuditEntry, q AuditQuery) bool {
	if q.EventID != "" && entry.EventID != q.EventID {
		return false
	}
	if q.Type != "" && entry.Event != q.Type {
		return false
	}
	if q.Status != "" && entry.Status != q.Status {
		return false
	}
	at := entry.At.Time
	if !q.Since.IsZero() && at.Before(q.Since) {
		return false
	}
	if !q.Until.IsZero() && at.After(q.Until) {
		return false
	}
	return true
}

// MemoryAuditStore 是进程内的审计存储，用于测试与不落盘的场景。
type MemoryAuditStore struct {
	mu      sync.Mutex
	entries []AuditEntry
	seq     int64
}

// NewMemoryAuditStore 创建内存审计存储。
func NewMemoryAuditStore() *MemoryAuditStore { return &MemoryAuditStore{} }

// Append 写入一条记录。
func (s *MemoryAuditStore) Append(entry AuditEntry) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	entry.Seq = s.seq
	if entry.At.IsZero() {
		entry.At = execution.Timestamp{Time: time.Now().UTC()}
	}
	s.entries = append(s.entries, entry)
	return nil
}

// List 过滤后倒序返回。
func (s *MemoryAuditStore) List(q AuditQuery) ([]AuditEntry, error) {
	if s == nil {
		return nil, nil
	}
	q = q.Normalized()
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]AuditEntry, 0, len(s.entries))
	for i := len(s.entries) - 1; i >= 0; i-- {
		if matchesAudit(s.entries[i], q) {
			out = append(out, s.entries[i])
		}
	}
	if q.Offset >= len(out) {
		return []AuditEntry{}, nil
	}
	out = out[q.Offset:]
	if len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

// Close 无操作。
func (s *MemoryAuditStore) Close() error { return nil }

// JSONLEventArchive 把事件正文追加写入文件。
//
// 不做索引：重放是低频人工操作，按文件顺序找一条事件的代价可以接受；
// 维护一份常驻内存的索引反而要为每个进程都占一份内存。
type JSONLEventArchive struct {
	mu   sync.Mutex
	file *os.File
	path string
	// maxBytes 是滚动阈值；0 表示用 maxFileBytes。测试会把它调小。
	maxBytes int64
}

// NewJSONLEventArchive 打开（或创建）事件正文文件。
func NewJSONLEventArchive(path string) (*JSONLEventArchive, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("事件正文文件路径为空")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("创建事件正文目录失败: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("打开事件正文文件失败: %w", err)
	}
	return &JSONLEventArchive{file: file, path: path}, nil
}

// Path 返回事件正文文件路径。
func (a *JSONLEventArchive) Path() string { return a.path }

// Save 追加保存一条事件。
func (a *JSONLEventArchive) Save(ev Event) error {
	if a == nil {
		return nil
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("序列化事件失败: %w", err)
	}
	if len(raw) > maxArchivedEventBytes {
		return fmt.Errorf("事件正文超过 %d 字节，未留存", maxArchivedEventBytes)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.file == nil {
		return errors.New("事件正文文件已关闭")
	}
	if _, err := a.file.Write(append(raw, '\n')); err != nil {
		return fmt.Errorf("写入事件正文失败: %w", err)
	}
	a.maybeRotate()
	return nil
}

// maybeRotate 与审计侧同一套滚动策略：超阈值只留最近 keepLines 条。
func (a *JSONLEventArchive) maybeRotate() {
	limit := a.maxBytes
	if limit <= 0 {
		limit = maxFileBytes
	}
	info, err := a.file.Stat()
	if err != nil || info.Size() <= limit {
		return
	}
	if err := a.file.Close(); err != nil {
		return
	}
	a.file = nil
	if err := compactJSONL(a.path, keepLines, limit/2); err != nil {
		return
	}
	file, err := os.OpenFile(a.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	a.file = file
}

// Load 取回最近一次保存的同标识事件。
func (a *JSONLEventArchive) Load(eventID string) (Event, error) {
	if a == nil {
		return Event{}, ErrEventNotFound
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return Event{}, ErrEventNotFound
	}
	file, err := os.Open(a.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Event{}, ErrEventNotFound
		}
		return Event{}, fmt.Errorf("读取事件正文失败: %w", err)
	}
	defer file.Close()

	var found Event
	var ok bool
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), maxArchivedEventBytes)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var ev Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		if ev.ID == eventID {
			found, ok = ev, true
		}
	}
	if err := scanner.Err(); err != nil {
		return Event{}, fmt.Errorf("读取事件正文失败: %w", err)
	}
	if !ok {
		return Event{}, ErrEventNotFound
	}
	return found, nil
}

// Close 关闭文件句柄。
func (a *JSONLEventArchive) Close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.file == nil {
		return nil
	}
	err := a.file.Close()
	a.file = nil
	return err
}

// MemoryEventArchive 是进程内的事件正文存储。
type MemoryEventArchive struct {
	mu     sync.Mutex
	events map[string]Event
	order  []string
}

// NewMemoryEventArchive 创建内存事件正文存储。
func NewMemoryEventArchive() *MemoryEventArchive {
	return &MemoryEventArchive{events: map[string]Event{}}
}

// Save 保存事件；同一标识覆盖旧值。
func (a *MemoryEventArchive) Save(ev Event) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.events[ev.ID]; !exists {
		a.order = append(a.order, ev.ID)
	}
	a.events[ev.ID] = ev
	return nil
}

// Load 取回事件正文。
func (a *MemoryEventArchive) Load(eventID string) (Event, error) {
	if a == nil {
		return Event{}, ErrEventNotFound
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	ev, ok := a.events[eventID]
	if !ok {
		return Event{}, ErrEventNotFound
	}
	return ev, nil
}

// Close 无操作。
func (a *MemoryEventArchive) Close() error { return nil }
