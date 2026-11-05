// Package eventbus 提供进程内的事件总线：按主题把事件分发给若干订阅者。
//
// 回调、审计、运维页面的实时事件流原本是三条各自直连的分支：runner 把事件
// 交给 webhook.Dispatcher，Dispatcher 顺手写审计，后台页面再自己想办法。
// 每加一个消费方就要在发送路径上多接一根线，而且新增的分支一旦变慢就会
// 拖住投递路径。总线把这层关系倒过来：发送方只管发布，谁订阅谁自己承担
// 自己的队列与消费速度。
//
// 几条刻意的语义：
//
//   - 发布是非阻塞的：订阅者队列满时丢弃并计数，而不是让发布方等。事件是
//     旁路数据，丢一条回执比停住一次投递要好。
//   - 每个订阅者一个 goroutine、一条有界队列：订阅者之间互不影响，慢的
//     那个只会丢自己的事件。
//   - 主题只做前缀通配（delivery.*），不做正则。事件类型是有限的枚举，
//     正则匹配让「谁订阅了什么」变得难以回答。
//   - 发布方拿到的返回值是「有几个订阅者收下了」，不是「几个处理成功」。
//     后者需要同步等待，与旁路定位冲突。
package eventbus

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultBuffer 是单个订阅者的默认队列长度。
	DefaultBuffer = 128
	// MaxBuffer 是队列长度上限，防止一个数字写错就吃掉几百 MB。
	MaxBuffer = 8192
	// DefaultCloseGrace 是关闭时等待在途事件的上限。
	DefaultCloseGrace = 2 * time.Second
)

// Message 是总线上的一个事件。
type Message struct {
	// ID 是事件标识，与报文里的 id 一致，便于跨订阅者关联同一条事件。
	ID string
	// Topic 是事件类型（delivery.sent、digest.updated 等）。
	Topic string
	// At 是事件发生时刻。
	At time.Time
	// Payload 是原始事件对象，由订阅者按自己的需要断言类型。
	Payload any
}

// Handler 处理一条事件。实现可以是阻塞的：它在订阅者自己的 goroutine 里跑。
type Handler func(Message)

// Options 是总线的可调参数。
type Options struct {
	// Buffer 是每个订阅者的默认队列长度，<=0 用 DefaultBuffer。
	Buffer int
	// Logger 为空时用默认 logger。
	Logger *slog.Logger
}

func (o Options) normalized() Options {
	if o.Buffer <= 0 {
		o.Buffer = DefaultBuffer
	}
	if o.Buffer > MaxBuffer {
		o.Buffer = MaxBuffer
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

// Stats 是某个订阅者的计数。
type Stats struct {
	// Received 是入队成功的事件数（含尚未处理的）。
	Received int64
	// Dropped 是队列已满或总线已关闭而丢弃的事件数。
	Dropped int64
	// Handled 是处理完成的事件数（含处理出错与 panic 的）。
	Handled int64
	// Failed 是处理时 panic 的次数。panic 被恢复，订阅者继续工作。
	Failed int64
	// Pending 是当前排队等待处理的事件数。
	Pending int
}

// Subscription 是一个订阅者。
type Subscription struct {
	bus     *Bus
	pattern string
	queue   chan Message
	handler Handler

	received atomic.Int64
	dropped  atomic.Int64
	handled  atomic.Int64
	failed   atomic.Int64
}

// Pattern 返回订阅的主题规则。
func (s *Subscription) Pattern() string { return s.pattern }

// Stats 返回当前计数快照。
func (s *Subscription) Stats() Stats {
	if s == nil {
		return Stats{}
	}
	return Stats{
		Received: s.received.Load(),
		Dropped:  s.dropped.Load(),
		Handled:  s.handled.Load(),
		Failed:   s.failed.Load(),
		Pending:  len(s.queue),
	}
}

// Bus 是进程内事件总线。
type Bus struct {
	logger *slog.Logger
	buffer int

	mu     sync.RWMutex
	subs   []*Subscription
	closed bool
	wg     sync.WaitGroup
}

// New 创建总线。
func New(opts Options) *Bus {
	opts = opts.normalized()
	return &Bus{logger: opts.Logger, buffer: opts.Buffer}
}

// Subscribe 注册一个订阅者，返回可查询计数的句柄。
//
// 主题规则：`*` 匹配全部，`delivery.*` 匹配前缀（含 `delivery.` 自身），
// 其余按全等匹配。规则写错（空串、星号不在末尾、多个星号）时返回 nil，
// 调用方按「没有这个订阅」处理。
func (b *Bus) Subscribe(pattern string, buffer int, handler Handler) *Subscription {
	if b == nil || handler == nil {
		return nil
	}
	pattern = strings.TrimSpace(pattern)
	if !validPattern(pattern) {
		return nil
	}
	if buffer <= 0 {
		buffer = b.buffer
	}
	if buffer > MaxBuffer {
		buffer = MaxBuffer
	}

	sub := &Subscription{
		bus:     b,
		pattern: pattern,
		queue:   make(chan Message, buffer),
		handler: handler,
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.subs = append(b.subs, sub)
	b.wg.Add(1)
	b.mu.Unlock()

	go sub.run()
	return sub
}

// validPattern 校验主题规则。
func validPattern(pattern string) bool {
	if pattern == "" {
		return false
	}
	if pattern == "*" {
		return true
	}
	idx := strings.IndexByte(pattern, '*')
	if idx < 0 {
		return true
	}
	// 只允许末尾一个 `*.` 前缀通配，例如 delivery.*。
	return idx == len(pattern)-1 && strings.HasSuffix(strings.TrimSuffix(pattern, "*"), ".")
}

// Matches 判断主题是否命中规则；导出供测试与排查使用。
func (s *Subscription) Matches(topic string) bool {
	return matchTopic(s.pattern, topic)
}

func matchTopic(pattern, topic string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(topic, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == topic
}

// Publish 把事件投递给所有命中的订阅者，返回入队成功的订阅者数量。
//
// 非阻塞：某个订阅者队列满时只丢它那一份，其余照常收到。事件没有持久化，
// 发布后进程崩溃即丢失——总线面向的是可重建的旁路数据（回调可由执行明细
// 补查、页面流可以重连），需要投递保证的场合应当走队列。
func (b *Bus) Publish(msg Message) int {
	if b == nil {
		return 0
	}
	if msg.At.IsZero() {
		msg.At = time.Now().UTC()
	}

	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return 0
	}
	accepted := 0
	for _, sub := range b.subs {
		if !matchTopic(sub.pattern, msg.Topic) {
			continue
		}
		select {
		case sub.queue <- msg:
			sub.received.Add(1)
			accepted++
		default:
			dropped := sub.dropped.Add(1)
			b.logger.Warn("[eventbus] 订阅者队列已满，事件被丢弃",
				"topic", msg.Topic,
				"eventId", msg.ID,
				"pattern", sub.pattern,
				"dropped", dropped,
			)
		}
	}
	return accepted
}

// BusStats 是总线级汇总：订阅者数量与各订阅者的计数之和。
type BusStats struct {
	Subscriptions int
	Received      int64
	Dropped       int64
	Handled       int64
	Failed        int64
	Pending       int
}

// Stats 汇总全部订阅者的计数。
func (b *Bus) Stats() BusStats {
	if b == nil {
		return BusStats{}
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := BusStats{Subscriptions: len(b.subs)}
	for _, sub := range b.subs {
		snapshot := sub.Stats()
		out.Received += snapshot.Received
		out.Dropped += snapshot.Dropped
		out.Handled += snapshot.Handled
		out.Failed += snapshot.Failed
		out.Pending += snapshot.Pending
	}
	return out
}

// Close 停止接收新事件，把各订阅者队列里已有的事件处理完。
//
// 与投递队列的关闭语义一致：已经收下的事件要处理完，否则「发布成功」就成了
// 空话。ctx 到期后不再等待，未处理的事件随进程退出丢弃。
func (b *Bus) Close(ctx context.Context) error {
	if b == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	for _, sub := range b.subs {
		close(sub.queue)
	}
	b.mu.Unlock()

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run 串行处理一个订阅者的事件。
func (s *Subscription) run() {
	defer s.bus.wg.Done()
	for msg := range s.queue {
		s.invoke(msg)
		s.handled.Add(1)
	}
}

// invoke 调用处理函数并恢复 panic。
//
// 一个订阅者的处理函数写错不该让整个订阅停摆：恢复后继续处理下一条，
// 同时把堆栈交给日志。事件本身已经丢了，但队列还能用。
func (s *Subscription) invoke(msg Message) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.failed.Add(1)
			s.bus.logger.Error("[eventbus] 订阅者处理事件时 panic",
				"topic", msg.Topic,
				"eventId", msg.ID,
				"pattern", s.pattern,
				"panic", recovered,
			)
		}
	}()
	s.handler(msg)
}

// ErrClosed 表示总线已关闭。
var ErrClosed = errors.New("eventbus: 总线已关闭")
