package schedule

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// 重启场景测试：延迟任务存在 Redis 里，引擎进程退出后未到期的任务必须留在
// 时间轴上，由下一个进程继续执行，且不能因为重启而重复执行已完成的任务。
//
// 这里用「关掉旧连接、用新连接新建存储与调度器」模拟一次完整的进程重启：
// RedisStore 不持有进程内状态，新旧实例之间唯一的交接物就是 Redis 里的两个键。

// restartRecorder 记录被执行的载荷，供重启前后的断言比对。
type restartRecorder struct {
	mu   sync.Mutex
	seen []string
	fail bool
}

func (r *restartRecorder) handle(_ context.Context, item Item) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, string(item.Payload))
	if r.fail {
		return errors.New("模拟下游故障")
	}
	return nil
}

func (r *restartRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.seen...)
}

func (r *restartRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seen)
}

// newRestartFixture 启动进程内 Redis，并返回一个「打开连接」的构造函数：
// 每次调用等价于引擎进程启动时新建一次 Redis 客户端。
func newRestartFixture(t *testing.T) (*miniredis.Miniredis, string, func() *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	prefix := "xw:delay:restart"
	open := func() *redis.Client {
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		return client
	}
	return mr, prefix, open
}

// 进程在 Tick 之前退出：已到期但还没执行的任务与未到期任务都要留到下一个进程。
func TestRestartKeepsPendingTasks(t *testing.T) {
	_, prefix, open := newRestartFixture(t)
	now := fixedTime()

	// —— 第一个进程：登记三条任务后直接退出，一次 Tick 都没跑。
	storeA := NewRedisStoreWithPrefix(open(), prefix)
	recorderA := &restartRecorder{}
	schedulerA := New(storeA, recorderA.handle, nil)

	overdue, err := schedulerA.Submit(Item{DueAt: now.Add(-time.Minute), Payload: []byte("like-1")})
	if err != nil {
		t.Fatalf("登记已到期任务失败：%v", err)
	}
	future, err := schedulerA.Submit(Item{DueAt: now.Add(10 * time.Minute), Payload: []byte("comment-2")})
	if err != nil {
		t.Fatalf("登记未到期任务失败：%v", err)
	}
	behind, err := schedulerA.Submit(Item{DueAt: now.Add(2 * time.Hour), Payload: []byte("mention-3")})
	if err != nil {
		t.Fatalf("登记远期任务失败：%v", err)
	}

	// —— 第二个进程：新连接、新存储、新调度器，共用同一个键前缀。
	storeB := NewRedisStoreWithPrefix(open(), prefix)
	recorderB := &restartRecorder{}
	schedulerB := New(storeB, recorderB.handle, nil, WithBatch(10))

	if n := schedulerB.Len(); n != 3 {
		t.Fatalf("重启后待执行任务数 = %d, 期望 3（未到期与已到期都应保留）", n)
	}
	if n := recorderA.count(); n != 0 {
		t.Fatalf("第一个进程不该执行任何任务，实际执行 %d 条", n)
	}

	// 续跑进程的第一轮只处理已经到期的那个。
	if n, err := schedulerB.Tick(context.Background(), now); err != nil || n != 1 {
		t.Fatalf("首轮处理 %d 条（err=%v），期望 1 条", n, err)
	}
	if n := schedulerB.Len(); n != 2 {
		t.Errorf("首轮后待执行数 = %d, 期望 2（远期任务不该被提前取出）", n)
	}

	// 到期时刻到达后，剩余任务按自己的时间点依次执行。
	if n, err := schedulerB.Tick(context.Background(), now.Add(15*time.Minute)); err != nil || n != 1 {
		t.Fatalf("第二轮处理 %d 条（err=%v），期望 1 条", n, err)
	}
	if n, err := schedulerB.Tick(context.Background(), now.Add(3*time.Hour)); err != nil || n != 1 {
		t.Fatalf("第三轮处理 %d 条（err=%v），期望 1 条", n, err)
	}

	got := recorderB.snapshot()
	if len(got) != 3 {
		t.Fatalf("重启后执行 %d 条，期望 3 条：%v", len(got), got)
	}
	order := map[string]bool{"like-1": true, "comment-2": true, "mention-3": true}
	for _, payload := range got {
		if !order[payload] {
			t.Errorf("出现了意外的载荷 %q", payload)
		}
		delete(order, payload)
	}
	if len(order) != 0 {
		t.Errorf("有任务没被执行：%v", order)
	}

	// —— 第三个进程：全部执行完后再启动，时间轴与任务体都不该残留。
	if n := NewRedisStoreWithPrefix(open(), prefix).Len(); n != 0 {
		t.Errorf("全部执行后待执行数 = %d, 期望 0", n)
	}
	schedulerC := New(NewRedisStoreWithPrefix(open(), prefix), recorderB.handle, nil)
	if n, err := schedulerC.Tick(context.Background(), now.Add(4*time.Hour)); err != nil || n != 0 {
		t.Errorf("再次启动不该重放已完成的任务：n=%d err=%v", n, err)
	}

	// 任务 id 在重启过程中保持不变，便于用日志对账。
	due, err := storeA.Due(now.Add(4*time.Hour), 10)
	if err != nil {
		t.Fatalf("读取时间轴失败：%v", err)
	}
	if len(due) != 0 {
		t.Errorf("时间轴应已清空，剩余 %d 条", len(due))
	}
	for _, id := range []string{overdue, future, behind} {
		if id == "" {
			t.Error("任务 id 不应为空")
		}
	}
}

// 失败后已重新排期的任务在重启后继续按剩余次数执行，载荷与次数都不丢。
func TestRestartKeepsRescheduledTask(t *testing.T) {
	_, prefix, open := newRestartFixture(t)
	now := fixedTime()

	// 第一个进程：执行失败一次，任务被退避到 now+30s，随后进程退出。
	storeA := NewRedisStoreWithPrefix(open(), prefix)
	recorderA := &restartRecorder{fail: true}
	schedulerA := New(storeA, recorderA.handle, nil, WithBaseBackoff(30*time.Second), WithMaxAttempts(3))

	id, err := schedulerA.Submit(Item{DueAt: now, Payload: []byte("comment-9")})
	if err != nil {
		t.Fatalf("登记任务失败：%v", err)
	}
	if n, err := schedulerA.Tick(context.Background(), now); err != nil || n != 0 {
		t.Fatalf("失败的任务不该计入成功数：n=%d err=%v", n, err)
	}
	if n := recorderA.count(); n != 1 {
		t.Fatalf("首次执行次数 = %d, 期望 1", n)
	}

	// 第二个进程：退避窗口内取不到，窗口结束后取到的是带尝试次数的同一条任务。
	storeB := NewRedisStoreWithPrefix(open(), prefix)
	if due, err := storeB.Due(now.Add(29*time.Second), 10); err != nil || len(due) != 0 {
		t.Fatalf("退避窗口内不该取到任务：n=%d err=%v", len(due), err)
	}
	due, err := storeB.Due(now.Add(30*time.Second), 10)
	if err != nil {
		t.Fatalf("读取到期任务失败：%v", err)
	}
	if len(due) != 1 {
		t.Fatalf("退避后应取到 1 条，实际 %d 条", len(due))
	}
	if due[0].ID != id || string(due[0].Payload) != "comment-9" || due[0].Attempts != 1 {
		t.Errorf("重启后任务状态不一致：%+v（期望 id=%s attempts=1）", due[0], id)
	}

	recorderB := &restartRecorder{}
	schedulerB := New(storeB, recorderB.handle, nil, WithBaseBackoff(30*time.Second), WithMaxAttempts(3))
	if n, err := schedulerB.Tick(context.Background(), now.Add(30*time.Second)); err != nil || n != 1 {
		t.Fatalf("续跑应成功：n=%d err=%v", n, err)
	}
	if got := recorderB.snapshot(); len(got) != 1 || got[0] != "comment-9" {
		t.Errorf("续跑执行的载荷 = %v, 期望 [comment-9]", got)
	}
	if n := storeB.Len(); n != 0 {
		t.Errorf("续跑成功后待执行数 = %d, 期望 0", n)
	}
}

// 键前缀是环境/实例的隔离边界：换了前缀就看不到别人的任务，也不会误执行。
func TestRestartPrefixIsolation(t *testing.T) {
	_, prefix, open := newRestartFixture(t)
	now := fixedTime()

	storeA := NewRedisStoreWithPrefix(open(), prefix)
	if _, err := storeA.Add(Item{DueAt: now, Payload: []byte("like-1")}); err != nil {
		t.Fatalf("登记任务失败：%v", err)
	}

	recorder := &restartRecorder{}
	other := New(NewRedisStoreWithPrefix(open(), prefix+":other"), recorder.handle, nil)
	if n := other.Len(); n != 0 {
		t.Fatalf("不同前缀下待执行数 = %d, 期望 0", n)
	}
	if n, err := other.Tick(context.Background(), now); err != nil || n != 0 {
		t.Errorf("不同前缀不该执行到任务：n=%d err=%v", n, err)
	}
	if n := storeA.Len(); n != 1 {
		t.Errorf("原前缀下的任务应保持待执行，实际 %d", n)
	}
}
