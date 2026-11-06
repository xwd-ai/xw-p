package pipeline

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// 收件箱的 JSONL 实现：只追加的版本流 + 定期压缩。
//
// 文件里一行一个条目版本，同一条目（ID 相同）每次改写都追加一行新版本，
// 旧版本留在原处不删；读侧按 ID 去重、取最后一行。于是改写是一次 append，
// 成本与文件大小无关——聚合卡片在窗口内每命中一次互动都要改写同一张卡，
// 这条路径决定了收件箱能承受多少互动。
//
// 代价是文件里会积累被覆盖的历史行，所以追加到一定比例后做一次压缩：
// 全量读、按 ID 保留最新、临时文件改名替换。压缩是 O(文件大小) 的，但被
// 摊薄到很多次追加之后，且只在「废弃行超过半数、文件超过阈值」时触发。
//
// 多实例共享同一份文件时，索引靠文件身份 + 偏移刷新：同一条文件且只是变长，
// 只读新增的尾部；文件被替换（压缩、或别的实例重写）或变短，才全量重扫。

const (
	// defaultInboxCompactMinBytes 是触发压缩的文件大小下限：小文件重写一次
	// 比留着重复行更亏，重复行本身也只有几 KB。
	defaultInboxCompactMinBytes = 1 << 20
	// defaultInboxCompactDeadRatio 是被覆盖行占全部行的比例阈值。
	defaultInboxCompactDeadRatio = 0.5
)

// errInboxConcurrentWrite 表示压缩期间文件被其它进程改过，本轮放弃。
var errInboxConcurrentWrite = errors.New("收件箱在压缩期间被其它进程写入")

// JSONLInboxStore 以「一行一个版本」的追加流保存收件箱。
//
// 索引只记条目 ID 与已读到的文件偏移；条目内容不常驻内存，读时直接扫文件。
type JSONLInboxStore struct {
	mu     sync.Mutex
	path   string
	logger *slog.Logger

	// index 是文件里出现过的条目 ID，用于幂等判断与压缩比例估算。
	index map[string]struct{}
	// offset 是已索引到的文件字节位置，只统计完整行（带换行符）。
	offset int64
	// lines 是已索引的行数，含被后续版本覆盖的行。
	lines int
	// stamp 是上次建立索引时的文件身份；文件被替换后必须全量重扫。
	stamp os.FileInfo
	// partialTail 表示文件末尾还有没换行的残片（别的进程写到一半就退出了）。
	// 下一次追加要先补一个换行，否则自己的整行会和残片粘成一条坏记录。
	partialTail bool

	compactMinBytes  int64
	compactDeadRatio float64
}

// NewJSONLInboxStore 打开（或创建）JSONL 收件箱文件并载入条目索引。
// 文件不存在时视为空收件箱，不报错。
func NewJSONLInboxStore(path string) (*JSONLInboxStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("收件箱文件路径为空")
	}
	s := &JSONLInboxStore{
		path:             path,
		logger:           slog.Default(),
		index:            make(map[string]struct{}),
		compactMinBytes:  defaultInboxCompactMinBytes,
		compactDeadRatio: defaultInboxCompactDeadRatio,
	}
	if err := s.reload(); err != nil {
		return nil, err
	}
	return s, nil
}

// Path 返回收件箱文件路径。
func (s *JSONLInboxStore) Path() string { return s.path }

// probe 检查收件箱文件当前是否可写：打开（必要时创建）后立即关闭，不写内容。
//
// 健康探测不该往收件箱里塞测试消息，但「目录只读 / 磁盘挂载丢了」这类故障
// 在第一条消息到来之前就该暴露，所以这里只验证能否拿到追加写的句柄。
func (s *JSONLInboxStore) Probe() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if dir := filepath.Dir(s.path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("收件箱目录不可用: %w", err)
		}
	}
	f, err := os.OpenFile(s.path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("收件箱文件不可写: %w", err)
	}
	return f.Close()
}

// Save 追加一个新条目；同 ID 已存在时返回 false 且不落盘。
func (s *JSONLInboxStore) Save(entry InboxEntry) (bool, error) {
	if entry.ID == "" {
		return false, errors.New("收件箱条目缺少 ID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refresh(); err != nil {
		return false, err
	}
	if _, ok := s.index[entry.ID]; ok {
		return false, nil
	}
	if err := s.appendEntry(entry); err != nil {
		return false, err
	}
	s.maybeCompact()
	return true, nil
}

// Update 以追加新版本的方式改写同 ID 条目。
//
// 旧版本不删除：原地改写要么整文件重写（成本随文件增长），要么要求新旧内容
// 等长（卡片计数会进位），都不适合高频改写。版本流把改写降成一次 append，
// 由读侧按 ID 取最后一行保证语义。
func (s *JSONLInboxStore) Update(entry InboxEntry) (bool, error) {
	if entry.ID == "" {
		return false, errors.New("收件箱条目缺少 ID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refresh(); err != nil {
		return false, err
	}
	if _, ok := s.index[entry.ID]; !ok {
		return false, nil
	}
	if err := s.appendEntry(entry); err != nil {
		return false, err
	}
	s.maybeCompact()
	return true, nil
}

// List 返回某订阅者的收件箱条目（按各条目首次写入的顺序）。
//
// 同一条目有多行版本时只返回最后一行。
func (s *JSONLInboxStore) List(subscriberID string) ([]InboxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, _, err := readLatestEntries(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]InboxEntry, 0, len(entries))
	for _, e := range entries {
		if e.SubscriberID == subscriberID {
			out = append(out, e)
		}
	}
	return out, nil
}

// Len 返回当前条目数（测试与观测用）。
func (s *JSONLInboxStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 读不出来时按已知索引回答：观测接口不该因为一次 IO 失败返回 0。
	_ = s.refresh()
	return len(s.index)
}

// appendEntry 追加一行并更新索引；调用方需持有锁。
func (s *JSONLInboxStore) appendEntry(entry InboxEntry) error {
	line, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if dir := filepath.Dir(s.path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	// 残片可能是别的进程写到一半崩溃留下的：先补一个换行把它收尾，
	// 否则本行会与残片拼成一条解析不出来的记录，连这次写入一起丢掉。
	prefix := ""
	if s.partialTail {
		prefix = "\n"
		s.partialTail = false
	}
	payload := append([]byte(prefix), line...)
	payload = append(payload, '\n')
	if _, err := f.Write(payload); err != nil {
		return err
	}
	s.index[entry.ID] = struct{}{}
	s.offset += int64(len(payload))
	s.lines++
	return nil
}

// reload 全量重扫文件，重建索引与偏移。
func (s *JSONLInboxStore) reload() error {
	s.index = make(map[string]struct{})
	s.offset = 0
	s.lines = 0
	s.partialTail = false

	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			s.stamp = nil
			return nil
		}
		return err
	}
	s.partialTail = s.applyTail(data)
	// 读完之后再取身份：期间若有别进程追加，offset 会小于文件大小，
	// 下一次 refresh 只补读那一段。
	if info, err := os.Stat(s.path); err == nil {
		s.stamp = info
	}
	return nil
}

// refresh 把索引推进到文件当前末尾；调用方需持有锁。
//
// 同一条文件且只是变长时只读新增尾部——多实例共享卷时，别的实例每追加
// 一条就整文件重扫一遍会让读放大成 O(n²)。文件身份变了（被压缩替换）或
// 变短（被截断）才全量重扫。
func (s *JSONLInboxStore) refresh() error {
	info, err := os.Stat(s.path)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		if s.offset != 0 || len(s.index) != 0 || s.stamp != nil {
			s.index = make(map[string]struct{})
			s.offset = 0
			s.lines = 0
			s.stamp = nil
			s.partialTail = false
		}
		return nil
	}
	if s.stamp == nil || !os.SameFile(s.stamp, info) || info.Size() < s.offset {
		return s.reload()
	}
	if info.Size() == s.offset {
		return nil
	}

	f, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Seek(s.offset, io.SeekStart); err != nil {
		return err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	if len(data) > 0 {
		s.partialTail = s.applyTail(data)
	}
	s.stamp = info
	return nil
}

// applyTail 解析一段尚未索引的字节，推进 offset 与索引。
//
// 只消费以换行结尾的完整行：末尾没有换行的残片可能是别的进程正在写的半行，
// 留到下次读取——把半行当成条目会让一次正常写入看起来像坏数据。返回值表示
// 这段数据末尾是否留下了残片。
func (s *JSONLInboxStore) applyTail(data []byte) bool {
	for len(data) > 0 {
		n := bytes.IndexByte(data, '\n')
		if n < 0 {
			return true
		}
		line := bytes.TrimSpace(data[:n])
		data = data[n+1:]
		s.offset += int64(n + 1)
		if len(line) == 0 {
			continue
		}
		s.lines++
		var e InboxEntry
		if err := json.Unmarshal(line, &e); err != nil {
			continue
		}
		if e.ID != "" {
			s.index[e.ID] = struct{}{}
		}
	}
	return false
}

// maybeCompact 在废弃行占比过高时压缩文件；调用方需持有锁。
func (s *JSONLInboxStore) maybeCompact() {
	if s.lines == 0 || s.offset < s.compactMinBytes {
		return
	}
	dead := s.lines - len(s.index)
	if dead <= 0 || float64(dead)/float64(s.lines) < s.compactDeadRatio {
		return
	}
	if err := s.compact(); err != nil {
		if errors.Is(err, errInboxConcurrentWrite) {
			// 有别的实例在写，本轮跳过；下次追加后再判断。
			return
		}
		s.logger.Warn("[in_app] 收件箱压缩失败，保留原文件", "path", s.path, "error", err)
	}
}

// compact 按 ID 保留最新版本后原子替换文件；调用方需持有锁。
func (s *JSONLInboxStore) compact() error {
	entries, fileBytes, err := readLatestEntries(s.path)
	if err != nil {
		return err
	}
	info, err := os.Stat(s.path)
	if err != nil {
		return err
	}
	if info.Size() != fileBytes {
		// 读到统计之间有人追加：这次不替换，否则会把刚写的版本丢掉。
		return errInboxConcurrentWrite
	}

	var buf bytes.Buffer
	for _, e := range entries {
		line, err := json.Marshal(e)
		if err != nil {
			continue
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	if err := writeAtomic(s.path, buf.Bytes()); err != nil {
		return err
	}

	s.index = make(map[string]struct{}, len(entries))
	for _, e := range entries {
		s.index[e.ID] = struct{}{}
	}
	s.offset = int64(buf.Len())
	s.lines = len(entries)
	if st, err := os.Stat(s.path); err == nil {
		s.stamp = st
	}
	return nil
}

// readLatestEntries 读取整份文件，按 ID 去重保留最后一行。
//
// 返回条目顺序沿用各自首次出现的顺序；第二个返回值是本次读到的字节数
// （含末尾没换行的残片），供压缩判断读取期间文件是否又被追加。
func readLatestEntries(path string) ([]InboxEntry, int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	fileBytes := int64(len(data))
	order := make([]string, 0, 16)
	latest := make(map[string]InboxEntry)
	for len(data) > 0 {
		n := bytes.IndexByte(data, '\n')
		if n < 0 {
			break
		}
		line := bytes.TrimSpace(data[:n])
		data = data[n+1:]
		if len(line) == 0 {
			continue
		}
		var e InboxEntry
		if err := json.Unmarshal(line, &e); err != nil {
			continue
		}
		if e.ID == "" {
			continue
		}
		if _, ok := latest[e.ID]; !ok {
			order = append(order, e.ID)
		}
		latest[e.ID] = e
	}
	out := make([]InboxEntry, 0, len(order))
	for _, id := range order {
		out = append(out, latest[id])
	}
	return out, fileBytes, nil
}

// writeAtomic 以「同目录临时文件 + 改名」整体替换文件。
func writeAtomic(path string, content []byte) error {
	dir := filepath.Dir(path)
	if dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(content); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
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
	store.logger = logger
	return store
}
