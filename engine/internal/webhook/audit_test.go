package webhook

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/backoff"
	"github.com/xwd-ai/xw-p/engine/internal/execution"
)

func waitDelivered(t *testing.T, d *Dispatcher, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if d.Stats().Delivered+d.Stats().Failed >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待发送完成超时：delivered=%d failed=%d want=%d",
		d.Stats().Delivered, d.Stats().Failed, want)
}

// 审计表按写入顺序追加，查询按最新在前返回，并能按事件标识、类型、
// 状态与时间过滤。
func TestJSONLAuditStoreQuery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	store, err := NewJSONLAuditStore(path)
	if err != nil {
		t.Fatalf("打开审计文件失败: %v", err)
	}
	defer store.Close()

	base := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	records := []AuditEntry{
		{EventID: "e1", Event: EventDeliveryResult, Status: AuditDelivered, At: ts(base)},
		{EventID: "e1", Event: EventDeliveryResult, Status: AuditRetrying, At: ts(base.Add(time.Second))},
		{EventID: "e1", Event: EventDeliveryResult, Status: AuditFailed, At: ts(base.Add(2 * time.Second))},
		{EventID: "e2", Event: EventDeliverySent, Status: AuditDelivered, At: ts(base.Add(3 * time.Second))},
	}
	for _, record := range records {
		if err := store.Append(record); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
	}
	// 写坏一行：查询应跳过它而不是整体失败。
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("打开文件失败: %v", err)
	}
	if _, err := file.WriteString("{broken\n"); err != nil {
		t.Fatalf("写入坏行失败: %v", err)
	}
	file.Close()

	all, err := store.List(AuditQuery{})
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if len(all) != 4 {
		t.Fatalf("应返回 4 条，实际 %d", len(all))
	}
	if all[0].EventID != "e2" {
		t.Errorf("应按最新在前排序，首条 = %s", all[0].EventID)
	}
	if all[0].Seq == 0 {
		t.Error("应补上进程内序号")
	}

	byEvent, _ := store.List(AuditQuery{EventID: "e1"})
	if len(byEvent) != 3 {
		t.Errorf("按事件标识过滤应得 3 条，实际 %d", len(byEvent))
	}
	failed, _ := store.List(AuditQuery{Status: AuditFailed})
	if len(failed) != 1 || failed[0].EventID != "e1" {
		t.Errorf("按状态过滤结果不符: %+v", failed)
	}
	sent, _ := store.List(AuditQuery{Type: EventDeliverySent})
	if len(sent) != 1 || sent[0].EventID != "e2" {
		t.Errorf("按类型过滤结果不符: %+v", sent)
	}
	page, _ := store.List(AuditQuery{Limit: 1, Offset: 1})
	if len(page) != 1 || page[0].EventID != "e1" {
		t.Errorf("分页不符: %+v", page)
	}
	sinceOnly, _ := store.List(AuditQuery{Since: base.Add(2 * time.Second)})
	if len(sinceOnly) != 2 {
		t.Errorf("按时间过滤应得 2 条，实际 %d", len(sinceOnly))
	}
}

// 审计文件不存在时返回空列表而不是报错：首次启动还没有任何回调。
func TestJSONLAuditStoreMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "none.jsonl")
	store, err := NewJSONLAuditStore(path)
	if err != nil {
		t.Fatalf("打开失败: %v", err)
	}
	defer store.Close()
	entries, err := store.List(AuditQuery{})
	if err != nil {
		t.Fatalf("查询不存在文件不应报错: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("应为空: %+v", entries)
	}
}

// 事件正文按标识留存，重放时取回原文；改写的字段（例如 replay 标记）
// 不影响原文。
func TestJSONLEventArchiveRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	archive, err := NewJSONLEventArchive(path)
	if err != nil {
		t.Fatalf("打开失败: %v", err)
	}
	defer archive.Close()

	ev := sampleEvent()
	if err := archive.Save(ev); err != nil {
		t.Fatalf("保存失败: %v", err)
	}
	// 同标识再存一次（重放时 Notify 会重新留档），取回最后一次。
	ev.Replay = true
	if err := archive.Save(ev); err != nil {
		t.Fatalf("保存失败: %v", err)
	}

	got, err := archive.Load(ev.ID)
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if got.ID != ev.ID || !got.Replay {
		t.Errorf("取回内容不符: %+v", got)
	}
	if _, err := archive.Load("missing"); !errors.Is(err, ErrEventNotFound) {
		t.Errorf("不存在的标识应返回 ErrEventNotFound，实际 %v", err)
	}
}

// 成功发送写一条 delivered；重试耗尽的失败写 retrying、retrying、failed。
func TestDispatcherAuditsAttempts(t *testing.T) {
	audit := NewMemoryAuditStore()
	archive := NewMemoryEventArchive()
	opts := quietOptions()
	opts.Audit = audit
	opts.Archive = archive
	sender := &fakeSender{}
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), sender, opts)
	if !d.Notify(sampleEvent()) {
		t.Fatal("应入队")
	}
	waitDelivered(t, d, 1)

	entries, _ := audit.List(AuditQuery{})
	if len(entries) != 1 || entries[0].Status != AuditDelivered {
		t.Fatalf("成功发送应记一条 delivered，实际 %+v", entries)
	}
	if entries[0].Target != "https://example.com" {
		t.Errorf("审计里的目标应是脱敏值，实际 %q", entries[0].Target)
	}
	if _, err := archive.Load(sampleEvent().ID); err != nil {
		t.Errorf("入队时应留存事件正文: %v", err)
	}

	// 始终 503 的接收方：三次尝试后落 failed。
	failing := &fakeSender{deliver: func(context.Context, Target, Event) Result {
		return Result{Status: 503, Err: "503 服务不可用", Retryable: true}
	}}
	audit2 := NewMemoryAuditStore()
	retryOpts := opts
	retryOpts.Audit = audit2
	retryOpts.Retry = backoff.Policy{MaxAttempts: 3, Base: time.Millisecond, Max: time.Millisecond}
	d2 := NewDispatcher(mustTarget(t, "https://example.com/cb"), failing, retryOpts)
	if !d2.Notify(sampleEvent()) {
		t.Fatal("应入队")
	}
	waitDelivered(t, d2, 1)

	entries, _ = audit2.List(AuditQuery{EventID: sampleEvent().ID})
	if len(entries) != 3 {
		t.Fatalf("三次尝试应记三条，实际 %d：%+v", len(entries), entries)
	}
	// 最新在前：failed、retrying、retrying。
	if entries[0].Status != AuditFailed || entries[1].Status != AuditRetrying || entries[2].Status != AuditRetrying {
		t.Errorf("尝试状态序列不符: %+v", []string{entries[0].Status, entries[1].Status, entries[2].Status})
	}
	if entries[0].Attempt != 3 || entries[2].Attempt != 1 {
		t.Errorf("尝试序号不符: %+v", entries)
	}
	d.Close(context.Background())
	d2.Close(context.Background())
}

// 队列满或分发器已关闭时，事件根本没发出去：审计记录 attempt=0 并写明原因，
// 与「发送失败」区分开。
func TestDispatcherAuditsDroppedEvents(t *testing.T) {
	audit := NewMemoryAuditStore()
	opts := quietOptions()
	opts.Audit = audit
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), &fakeSender{}, opts)
	if err := d.Close(context.Background()); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}
	if d.Notify(sampleEvent()) {
		t.Fatal("关闭后不应入队")
	}

	entries, _ := audit.List(AuditQuery{})
	if len(entries) != 1 {
		t.Fatalf("应记一条丢弃记录，实际 %+v", entries)
	}
	if entries[0].Attempt != 0 || entries[0].Status != AuditFailed || entries[0].Error == "" {
		t.Errorf("丢弃记录应带原因且 attempt=0: %+v", entries[0])
	}
}

// 重放把留档的事件重新发出：标识保持不变、报文带 replay 标记，
// 缺正文与已取消订阅的分别报出来。
func TestDispatcherReplay(t *testing.T) {
	audit := NewMemoryAuditStore()
	archive := NewMemoryEventArchive()
	opts := quietOptions()
	opts.Audit = audit
	opts.Archive = archive
	sender := &fakeSender{}
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), sender, opts)

	ev := sampleEvent()
	if !d.Notify(ev) {
		t.Fatal("应入队")
	}
	waitDelivered(t, d, 1)

	report, err := d.Replay([]string{ev.ID, "missing-id"})
	if err != nil {
		t.Fatalf("重放失败: %v", err)
	}
	if len(report.Replayed) != 1 || report.Replayed[0] != ev.ID {
		t.Errorf("应重放一条: %+v", report)
	}
	if len(report.Missing) != 1 || report.Missing[0] != "missing-id" {
		t.Errorf("缺正文的应报告: %+v", report)
	}
	waitDelivered(t, d, 2)
	if sender.calls() != 2 {
		t.Errorf("接收方应收到两次，实际 %d", sender.calls())
	}

	entries, _ := audit.List(AuditQuery{EventID: ev.ID})
	if len(entries) != 2 {
		t.Fatalf("审计应有两条，实际 %d", len(entries))
	}
	if !entries[0].Replay {
		t.Error("重放产生的审计应带 replay 标记")
	}

	if _, err := d.Replay(nil); err == nil {
		t.Error("空重放列表应报错")
	}
	d.Close(context.Background())
}

// 订阅集合已经不含该类型时，重放不悄悄发出去，而是报 filtered。
func TestDispatcherReplayRespectsSelection(t *testing.T) {
	audit := NewMemoryAuditStore()
	archive := NewMemoryEventArchive()
	opts := quietOptions()
	opts.Audit = audit
	opts.Archive = archive
	opts.Selection = mustSelection(t, EventDeliveryResult)
	d := NewDispatcher(mustTarget(t, "https://example.com/cb"), &fakeSender{}, opts)

	// 直接留档一条不在订阅集合里的事件：模拟「事件产生时订阅更宽」。
	ev := sampleEvent()
	ev.Event = EventDeliverySent
	ev.Status = StatusSent
	ev.Delivery = &Delivery{Channel: "email", Status: StatusSent}
	if err := archive.Save(ev); err != nil {
		t.Fatalf("保存失败: %v", err)
	}
	report, err := d.Replay([]string{ev.ID})
	if err != nil {
		t.Fatalf("重放失败: %v", err)
	}
	if len(report.Filtered) != 1 || len(report.Replayed) != 0 {
		t.Errorf("应报 filtered: %+v", report)
	}
	d.Close(context.Background())
}

func mustSelection(t *testing.T, types ...string) Selection {
	t.Helper()
	sel, err := ParseSelection(joinTypes(types))
	if err != nil {
		t.Fatalf("解析订阅失败: %v", err)
	}
	return sel
}

func joinTypes(types []string) string {
	out := ""
	for i, item := range types {
		if i > 0 {
			out += ","
		}
		out += item
	}
	return out
}

func ts(t time.Time) execution.Timestamp {
	return execution.Timestamp{Time: t}
}

// 审计与归档都是追加写：没有滚动就会随运行时长无限增长。这里把阈值调小，
// 确认超过阈值后只保留最近的行，并且句柄仍然可用（滚动后重新打开）。
func TestJSONLAuditStoreRotates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	store, err := NewJSONLAuditStore(path)
	if err != nil {
		t.Fatalf("打开审计文件失败: %v", err)
	}
	defer store.Close()
	store.maxBytes = 4 << 10

	for i := 0; i < keepLines+200; i++ {
		if err := store.Append(AuditEntry{
			EventID: "e-" + itoa(i),
			Event:   EventDeliveryResult,
			Status:  AuditDelivered,
			At:      ts(time.Now()),
		}); err != nil {
			t.Fatalf("第 %d 条写入失败: %v", i, err)
		}
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("读取文件状态失败: %v", err)
	}
	if info.Size() > 4<<10 {
		t.Errorf("滚动后文件仍有 %d 字节，超过阈值", info.Size())
	}

	// 滚动之后句柄仍然可写：新的记录能读回来
	if err := store.Append(AuditEntry{
		EventID: "after-rotate",
		Event:   EventDeliveryResult,
		Status:  AuditFailed,
		At:      ts(time.Now()),
	}); err != nil {
		t.Fatalf("滚动后写入失败: %v", err)
	}
	entries, err := store.List(AuditQuery{EventID: "after-rotate"})
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("滚动后写入的记录应能查到，实际 %d 条", len(entries))
	}
}

func TestJSONLEventArchiveRotates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	archive, err := NewJSONLEventArchive(path)
	if err != nil {
		t.Fatalf("打开事件文件失败: %v", err)
	}
	defer archive.Close()
	archive.maxBytes = 8 << 10

	var last Event
	for i := 0; i < keepLines+100; i++ {
		ev := sampleEvent()
		ev.ID = "ev-" + itoa(i)
		if err := archive.Save(ev); err != nil {
			t.Fatalf("第 %d 条保存失败: %v", i, err)
		}
		last = ev
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("读取文件状态失败: %v", err)
	}
	if info.Size() > 8<<10 {
		t.Errorf("滚动后文件仍有 %d 字节，超过阈值", info.Size())
	}
	// 最近的事件必须还在：滚动只裁旧行，重放要靠它
	if _, err := archive.Load(last.ID); err != nil {
		t.Errorf("滚动后应能取回最近的事件: %v", err)
	}
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := make([]byte, 0, 12)
	for value > 0 {
		digits = append([]byte{byte('0' + value%10)}, digits...)
		value /= 10
	}
	return string(digits)
}
