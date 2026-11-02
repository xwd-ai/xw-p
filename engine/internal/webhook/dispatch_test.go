package webhook

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// fakeSender 记录被提交的事件，行为可按用例替换。
type fakeSender struct {
	mu    sync.Mutex
	count int
	// deliver 为空时视为成功返回 200。
	deliver func(ctx context.Context, target Target, ev Event) Result
}

func (f *fakeSender) Deliver(ctx context.Context, target Target, ev Event) Result {
	f.mu.Lock()
	f.count++
	f.mu.Unlock()
	if f.deliver != nil {
		return f.deliver(ctx, target, ev)
	}
	return Result{Status: 200}
}

func (f *fakeSender) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count
}

func quietOptions() Options {
	return Options{
		Workers:   2,
		QueueSize: 16,
		Timeout:   time.Second,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// 入队的事件最终都会发出去；Close 等待队列排空后才返回。
func TestDispatcherDeliversQueuedEvents(t *testing.T) {
	sender := &fakeSender{}
	opts := quietOptions()
	// 缓冲足够大，用例只关心「入队后都会发出」，不掺进队列满的丢弃。
	opts.QueueSize = 64
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), sender, opts)
	if d == nil {
		t.Fatal("应创建分发器")
	}

	const total = 20
	for i := 0; i < total; i++ {
		ev := sampleEvent()
		ev.ID = "evt-" + string(rune('a'+i))
		if !d.Notify(ev) {
			t.Fatalf("第 %d 条应入队成功", i)
		}
	}
	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}

	stats := d.Stats()
	if stats.Enqueued != total || stats.Delivered != total {
		t.Errorf("计数不符: %+v", stats)
	}
	if stats.Failed != 0 || stats.Dropped != 0 || stats.Pending != 0 {
		t.Errorf("不应有失败或丢弃: %+v", stats)
	}
	if sender.calls() != total {
		t.Errorf("发送次数不符: %d", sender.calls())
	}
}

// 接收方失败只影响计数与日志，不会让队列卡住。
func TestDispatcherCountsFailures(t *testing.T) {
	sender := &fakeSender{deliver: func(context.Context, Target, Event) Result {
		return Result{Status: 500, Err: "回调被拒绝（500）", Retryable: true}
	}}
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), sender, quietOptions())

	for i := 0; i < 3; i++ {
		d.Notify(sampleEvent())
	}
	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}

	stats := d.Stats()
	if stats.Enqueued != 3 || stats.Failed != 3 || stats.Delivered != 0 {
		t.Errorf("计数不符: %+v", stats)
	}
}

// 队列满时立即返回 false，不阻塞调用方——消费路径不能等回调。
func TestDispatcherDropsWhenQueueFull(t *testing.T) {
	started := make(chan struct{}, 1)
	gate := make(chan struct{})
	sender := &fakeSender{deliver: func(context.Context, Target, Event) Result {
		select {
		case started <- struct{}{}:
		default:
		}
		<-gate
		return Result{Status: 200}
	}}

	opts := quietOptions()
	opts.Workers = 1
	opts.QueueSize = 1
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), sender, opts)

	if !d.Notify(sampleEvent()) {
		t.Fatal("第一条应入队")
	}
	<-started // 确认 worker 已经在发第一条
	if !d.Notify(sampleEvent()) {
		t.Fatal("第二条应进入缓冲区")
	}

	start := time.Now()
	if d.Notify(sampleEvent()) {
		t.Fatal("缓冲区已满，第三条应被丢弃")
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("丢弃应立即返回，实际耗时 %v", elapsed)
	}

	close(gate)
	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}
	stats := d.Stats()
	if stats.Dropped != 1 {
		t.Errorf("应记录 1 条丢弃，实际 %+v", stats)
	}
	if stats.Delivered != 2 {
		t.Errorf("入队的两条都应发出，实际 %+v", stats)
	}
}

// 关闭之后不再接收事件。
func TestDispatcherNotifyAfterClose(t *testing.T) {
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), &fakeSender{}, quietOptions())
	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}
	if d.Notify(sampleEvent()) {
		t.Fatal("关闭后不应再入队")
	}
	if stats := d.Stats(); stats.Dropped != 1 {
		t.Errorf("关闭后的丢弃应计数，实际 %+v", stats)
	}
	// 重复关闭是退出路径的常态（defer 链 + 显式调用），必须幂等。
	if err := d.Close(context.Background()); err != nil {
		t.Errorf("重复关闭不应报错: %v", err)
	}
}

// 不合法的事件在入队前就被挡住，不会占用发送 goroutine。
func TestDispatcherRejectsInvalidEvent(t *testing.T) {
	sender := &fakeSender{}
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), sender, quietOptions())
	defer d.Close(context.Background())

	ev := sampleEvent()
	ev.Status = "done"
	if d.Notify(ev) {
		t.Fatal("状态非法的事件不应入队")
	}
	if stats := d.Stats(); stats.Enqueued != 0 {
		t.Errorf("不应计入入队: %+v", stats)
	}
	if sender.calls() != 0 {
		t.Errorf("不应发生发送，实际 %d 次", sender.calls())
	}
}

// 退出宽限到期时不等了，未发出的事件记为丢弃。
func TestDispatcherCloseRespectsContext(t *testing.T) {
	gate := make(chan struct{})
	defer close(gate)
	sender := &fakeSender{deliver: func(context.Context, Target, Event) Result {
		<-gate
		return Result{Status: 200}
	}}
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), sender, quietOptions())
	d.Notify(sampleEvent())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := d.Close(ctx); err == nil {
		t.Fatal("宽限到期时应返回错误")
	}
}

// 未配置回调（地址为空）或没有发送实现时不创建分发器，
// 调用方按 nil 处理也不会 panic。
func TestNewDispatcherNilCases(t *testing.T) {
	if d := NewDispatcher(Target{}, &fakeSender{}, quietOptions()); d != nil {
		t.Error("空地址不应创建分发器")
	}
	if d := NewDispatcher(mustTarget(t, "https://example.com/cb"), nil, quietOptions()); d != nil {
		t.Error("没有发送实现不应创建分发器")
	}

	var d *Dispatcher
	if d.Notify(sampleEvent()) {
		t.Error("nil 分发器不应入队")
	}
	if err := d.Close(context.Background()); err != nil {
		t.Errorf("nil 分发器关闭不应报错: %v", err)
	}
	if stats := d.Stats(); stats != (Stats{}) {
		t.Errorf("nil 分发器应给出空计数: %+v", stats)
	}
}

// 参数越界时收敛到合法区间。
func TestOptionsNormalized(t *testing.T) {
	opts := Options{Workers: -1, QueueSize: 100000, Timeout: 0}.Normalized()
	if opts.Workers != DefaultWorkers {
		t.Errorf("并发数不符: %d", opts.Workers)
	}
	if opts.QueueSize != MaxQueueSize {
		t.Errorf("队列长度应收敛到上限: %d", opts.QueueSize)
	}
	if opts.Timeout != DefaultTimeout {
		t.Errorf("超时应回退默认值: %v", opts.Timeout)
	}
	if opts.Logger == nil {
		t.Error("应补上默认 logger")
	}

	opts = Options{Workers: 1000, QueueSize: 0, Timeout: time.Hour}.Normalized()
	if opts.Workers != MaxWorkers {
		t.Errorf("并发数应收敛到上限: %d", opts.Workers)
	}
	if opts.QueueSize != DefaultQueueSize {
		t.Errorf("队列长度不符: %d", opts.QueueSize)
	}
	if opts.Timeout != MaxTimeout {
		t.Errorf("超时应收敛到上限: %v", opts.Timeout)
	}
}
