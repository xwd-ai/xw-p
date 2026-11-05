package eventbus

import "sync"

// DefaultFeedCapacity 是实时事件流保留的最近事件数。
//
// 运维页面要回答的是「刚刚发生了什么」，不是「历史上都发生了什么」；
// 保留最近 200 条足够覆盖一次排查的时间窗，内存占用也只有几十 KB。
const DefaultFeedCapacity = 200

// Feed 保留最近若干条事件，供运维接口读取。
//
// 它是一个普通订阅者：挂在总线上，处理函数只是把事件塞进环形缓冲。
// 单独做成一个类型而不是让接口层自己订阅，是因为「最近事件」的读取方
// 需要稳定的快照与顺序，把这段状态放在 HTTP 处理器里会在每次请求时重建。
type Feed struct {
	mu       sync.Mutex
	capacity int
	items    []Message
	next     int
	filled   bool
	total    int64
}

// NewFeed 创建事件流缓冲。
func NewFeed(capacity int) *Feed {
	if capacity <= 0 {
		capacity = DefaultFeedCapacity
	}
	if capacity > MaxBuffer {
		capacity = MaxBuffer
	}
	return &Feed{capacity: capacity, items: make([]Message, capacity)}
}

// Record 记下一条事件，超过容量时覆盖最旧的一条。
func (f *Feed) Record(msg Message) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.items[f.next] = msg
	f.next = (f.next + 1) % f.capacity
	if f.next == 0 {
		f.filled = true
	}
	f.total++
}

// Handler 返回可直接注册到总线上的处理函数。
func (f *Feed) Handler() Handler {
	return func(msg Message) { f.Record(msg) }
}

// Recent 返回最近的事件，最新在前。
func (f *Feed) Recent(limit int) []Message {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	size := f.next
	if f.filled {
		size = f.capacity
	}
	if limit <= 0 || limit > size {
		limit = size
	}
	out := make([]Message, 0, limit)
	for i := 0; i < limit; i++ {
		idx := (f.next - 1 - i + f.capacity) % f.capacity
		out = append(out, f.items[idx])
	}
	return out
}

// Total 返回自启动以来记录的事件总数（含已被覆盖的）。
func (f *Feed) Total() int64 {
	if f == nil {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.total
}
