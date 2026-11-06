package execution

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Store 是执行明细的落地目标。
//
// 只要求追加一条记录：执行明细是流水账，写入即事实，不支持改写（同一次触发的
// 重试会留下多行，正好能看出「发了两次」）。
type Store interface {
	Append(record Record) error
}

// ContextStore 是支持调用方取消或超时的 Store。
//
// 单独一个接口而不是给 Store 的方法加参数：JSONL 与内存后端都是本地写入，
// 加一个永远不会被取消的 ctx 只会让所有实现与测试跟着改一遍。远程后端
// （Postgres）实现它，Recorder 按类型断言选择走哪条路径。
type ContextStore interface {
	Store
	AppendContext(ctx context.Context, record Record) error
}

// ErrStoreClosed 表示存储已经关闭。
var ErrStoreClosed = errors.New("execution: 存储已关闭")

// JSONLStore 把执行明细追加到 JSONL 文件，一行一条。
//
// 与 API 侧的 JSONLExecutionStore 共用同一份文件：引擎负责写引擎路径上的投递
// 结果，API 的查询接口负责从文件尾部倒序读。写入用 O_APPEND 打开后一直持有句柄，
// 每次一条记录只发一次 write —— 多进程追加同一份文件时，各自的一次 write 在
// 内核里是原子的，不会把两条记录拼到同一行。
type JSONLStore struct {
	path string

	mu     sync.Mutex
	file   *os.File
	closed bool
}

// NewJSONLStore 打开（必要时创建）执行明细文件。
//
// 目录不存在时一并创建：容器里挂载的空卷第一次启动时目录往往还没有。
func NewJSONLStore(path string) (*JSONLStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("execution: 执行明细文件路径为空")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("execution: 创建执行明细目录 %s 失败: %w", dir, err)
		}
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("execution: 打开执行明细文件 %s 失败: %w", path, err)
	}
	return &JSONLStore{path: path, file: file}, nil
}

// Path 返回执行明细文件路径。
func (s *JSONLStore) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

// Append 追加一条记录。记录在写入前会被序列化为单行，因此单次 write 就是一整行。
func (s *JSONLStore) Append(record Record) error {
	record = record.clone()

	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("execution: 序列化执行明细失败: %w", err)
	}
	line = append(line, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.file == nil {
		return ErrStoreClosed
	}
	if _, err := s.file.Write(line); err != nil {
		return fmt.Errorf("execution: 写入执行明细失败: %w", err)
	}
	return nil
}

// Records 读回文件里的全部记录，供测试与排查使用。
//
// 损坏的行（进程被杀时写了一半等）跳过而不是报错：一行脏数据不该让整份
// 执行明细都读不出来。
func (s *JSONLStore) Records() ([]Record, error) {
	if s == nil {
		return nil, nil
	}
	file, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("execution: 读取执行明细失败: %w", err)
	}
	defer file.Close()

	var records []Record
	scanner := bufio.NewScanner(file)
	// 单行记录含失败原因也远小于默认的 64 KiB，放宽到 1 MiB 只作为兜底。
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record Record
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("execution: 读取执行明细失败: %w", err)
	}
	return records, nil
}

// Close 关闭文件句柄；重复调用无副作用。
func (s *JSONLStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.file == nil {
		return nil
	}
	return s.file.Close()
}

// MemoryStore 是进程内存储，用于单元测试与不需要落盘的场景。
type MemoryStore struct {
	mu      sync.Mutex
	records []Record
	err     error
}

// NewMemoryStore 创建空的内存存储；err 非空时每次写入都返回该错误，
// 用于测试写入失败的处理路径。
func NewMemoryStore(err error) *MemoryStore {
	return &MemoryStore{err: err}
}

// Append 记录一条明细。
func (s *MemoryStore) Append(record Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.records = append(s.records, record.clone())
	return nil
}

// Records 返回已写入的记录快照。
func (s *MemoryStore) Records() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Record, len(s.records))
	copy(out, s.records)
	return out
}
