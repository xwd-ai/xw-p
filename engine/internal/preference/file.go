package preference

import (
	"bufio"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"strings"
	"sync"
)

// Source 提供某一时刻的偏好快照。
//
// 取快照而不是逐条查询：一次投递要判三层，逐层问存储会在同一个人身上做多次
// 读取，还要自己保证三次读取看到的是同一份数据。
type Source interface {
	Snapshot() (*Set, error)
}

// Static 是固定不变的一份快照，用于测试与「启动时一次性加载」的场景。
type Static struct {
	set *Set
}

// NewStatic 用给定记录构造固定来源。
func NewStatic(records ...Record) *Static {
	return &Static{set: NewSet(records...)}
}

// Snapshot 实现 Source。
func (s *Static) Snapshot() (*Set, error) { return s.set, nil }

// Set 返回固定来源里的快照（便于测试断言）。
func (s *Static) Set() *Set { return s.set }

// fileStamp 是判断文件是否被改写过的依据。
//
// 用「大小 + 修改时刻」而不是每次都读内容：偏好是 API 写、引擎读的单向数据，
// 绝大多数投递发生时文件没有变化，命中缓存时整条投递路径不产生 I/O。
type fileStamp struct {
	size   int64
	modNS  int64
	exists bool
}

// File 从磁盘读取偏好文件的快照。
//
// 文件被改写（API 换了一条配置）后下一次 Snapshot 会重读整份文件：偏好是
// 覆盖写语义、整体重写，重读比做增量解析简单，也不会留下半更新的索引。
// 写入方用的是「临时文件 + rename」，因此不会读到写了一半的内容。
type File struct {
	path string

	mu      sync.Mutex
	set     *Set
	stamp   fileStamp
	loaded  bool
	skipped int
}

// NewFile 创建文件来源；文件不存在时视为「没有任何偏好记录」。
func NewFile(path string) *File {
	return &File{path: strings.TrimSpace(path)}
}

// Path 返回文件路径。
func (f *File) Path() string { return f.path }

// Snapshot 实现 Source：文件没变时返回同一份快照。
func (f *File) Snapshot() (*Set, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	stamp, err := stampOf(f.path)
	if err != nil {
		// 读不了文件（权限、路径被目录占位）不该被当成「没有偏好」：上层据此
		// 放行并留告警，与「文件里确实一条都没有」区分得开。
		return nil, err
	}
	if f.loaded && stamp == f.stamp {
		return f.set, nil
	}
	if f.path == "" {
		f.set, f.stamp, f.loaded, f.skipped = NewSet(), stamp, true, 0
		return f.set, nil
	}
	set, skipped, err := loadFile(f.path)
	if err != nil {
		return nil, err
	}
	f.set, f.stamp, f.loaded, f.skipped = set, stamp, true, skipped
	if skipped > 0 {
		// 坏行跳过而不是整份放弃：一条手写错的记录不该让所有人的偏好失效。
		slog.Default().Warn("偏好文件存在无效记录，已跳过",
			"file", f.path, "skipped", skipped, "records", set.Len())
	}
	return set, nil
}

// Skipped 返回最近一次加载时跳过的坏行数。
func (f *File) Skipped() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.skipped
}

func stampOf(path string) (fileStamp, error) {
	if path == "" {
		return fileStamp{}, nil
	}
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return fileStamp{}, nil
	}
	if err != nil {
		return fileStamp{}, err
	}
	return fileStamp{size: info.Size(), modNS: info.ModTime().UnixNano(), exists: true}, nil
}

// loadFile 读取整份偏好文件，返回快照与被跳过的坏行数。
func loadFile(path string) (*Set, int, error) {
	handle, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return NewSet(), 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	defer handle.Close()

	scanner := bufio.NewScanner(handle)
	// 默认单行上限 64 KiB；偏好行远小于此，但把上限抬到 1 MiB 可以让一行写坏的
	// 长记录被当作坏行跳过，而不是让整份文件读失败。
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)

	records := make([]Record, 0, 64)
	skipped := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		record, err := ParseRecord([]byte(line))
		if err != nil {
			skipped++
			continue
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, err
	}
	return NewSet(records...), skipped, nil
}

// Gate 把偏好快照接成投递前的判定。
//
// 两条规则写在这里，分发阶段与聚合卡片共用同一份结论：
//
//  1. 读不到偏好时按默认接收放行。存储损坏、文件权限、路径被占位都属于
//     「不知道用户怎么设置」，而不是「用户不想收」——通知的价值在于送达，
//     静默丢消息比多发一条更难被发现。
//  2. 消息没带订阅者标识时不读存储。没有归属就查不到记录，白读一次文件只会
//     让每条匿名消息都去碰一次磁盘。
type Gate struct {
	source Source
	logger *slog.Logger
}

// NewGate 创建判定器；source 为 nil 时一律放行（用于关闭偏好判定的装配）。
func NewGate(source Source, logger *slog.Logger) *Gate {
	if logger == nil {
		logger = slog.Default()
	}
	return &Gate{source: source, logger: logger}
}

// Decide 判定某条通知能否在某个渠道上发出。
func (g *Gate) Decide(subscriberID, workflow, channel string) Decision {
	if g == nil || g.source == nil {
		return Decision{Allowed: true, Reason: "未配置偏好判定，按默认接收"}
	}
	subscriber := strings.TrimSpace(subscriberID)
	if subscriber == "" {
		return Decision{Allowed: true, Reason: "消息未带订阅者标识，未查询偏好"}
	}
	set, err := g.source.Snapshot()
	if err != nil {
		g.logger.Warn("偏好读取失败，按默认接收放行", "error", err)
		return Decision{Allowed: true, Reason: "偏好读取失败，按默认接收"}
	}
	return set.Resolve(subscriber, workflow, channel)
}
