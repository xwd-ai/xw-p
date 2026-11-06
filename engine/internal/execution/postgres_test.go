package execution

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

const testTriggerID = "6f1c2c1e-0b1a-4a7b-9d6e-2a5d1f8c3b40"

func pointTo(text string) *string { return &text }

func TestPostgresStoreAppliesPoolSettings(t *testing.T) {
	store, _ := newFakeStore(t, PostgresConfig{MaxOpenConns: 5, MaxIdleConns: 99})

	stats := store.Stats()
	if stats.MaxOpen != 5 {
		t.Fatalf("MaxOpen 期望 5，实际 %d", stats.MaxOpen)
	}
	// 空闲连接数不能超过总连接数：配错了应该夹紧，而不是让 database/sql
	// 在内部悄悄截断、让配置与运行状态对不上。
	if stats.MaxOpen < 1 {
		t.Fatalf("MaxOpen 必须为正，实际 %d", stats.MaxOpen)
	}
}

func TestPostgresStoreAppendWritesExpectedColumns(t *testing.T) {
	store, fake := newFakeStore(t, PostgresConfig{})

	err := store.Append(Record{
		TriggerID:  testTriggerID,
		Channel:    "email",
		Status:     StatusSent,
		DurationMS: 128,
	})
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	execs, prepares, _, _, _ := fake.snapshot()
	if prepares != 1 {
		t.Fatalf("预处理语句应只建一次，实际 %d", prepares)
	}
	if len(execs) != 1 {
		t.Fatalf("期望 1 次执行，实际 %d", len(execs))
	}
	args := execs[0]
	if len(args) != 7 {
		t.Fatalf("期望 7 个参数，实际 %d: %#v", len(args), args)
	}
	if args[0] != testTriggerID {
		t.Fatalf("trigger_id 参数错误: %#v", args[0])
	}
	if args[1] != "email" || args[2] != "sent" {
		t.Fatalf("channel/status 参数错误: %#v %#v", args[1], args[2])
	}
	if args[3] != nil {
		t.Fatalf("成功的记录 reason 应为 NULL，实际 %#v", args[3])
	}
	if args[4] != int64(128) {
		t.Fatalf("duration_ms 参数错误: %#v", args[4])
	}
	// 空的隔离维度按表上的 DEFAULT 补齐，否则默认租户的视图看不到这条记录。
	if args[5] != "default" || args[6] != "development" {
		t.Fatalf("租户/环境应为缺省值，实际 %#v %#v", args[5], args[6])
	}
}

func TestPostgresStoreAppendKeepsTenantAndReason(t *testing.T) {
	store, fake := newFakeStore(t, PostgresConfig{})

	err := store.Append(Record{
		TriggerID:   testTriggerID,
		Tenant:      "acme",
		Environment: "production",
		Channel:     "sms",
		Status:      StatusFailed,
		Reason:      pointTo("短信通道返回 500"),
	})
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	execs, _, _, _, _ := fake.snapshot()
	args := execs[0]
	if args[3] != "短信通道返回 500" {
		t.Fatalf("reason 参数错误: %#v", args[3])
	}
	if args[5] != "acme" || args[6] != "production" {
		t.Fatalf("租户/环境参数错误: %#v %#v", args[5], args[6])
	}
}

func TestPostgresStoreRejectsNonUUIDTrigger(t *testing.T) {
	store, fake := newFakeStore(t, PostgresConfig{})

	err := store.Append(Record{TriggerID: "trig-7f31", Channel: "email", Status: StatusSent})
	if err == nil {
		t.Fatal("非 UUID 的 trigger_id 应被拒绝")
	}
	if !strings.Contains(err.Error(), "不是 UUID") {
		t.Fatalf("错误信息应说明原因，实际: %v", err)
	}
	// 预校验发生在送库之前：不该产生一次必然失败的往返。
	execs, _, _, _, _ := fake.snapshot()
	if len(execs) != 0 {
		t.Fatalf("不应有 SQL 执行，实际 %d 次", len(execs))
	}
}

func TestPostgresStoreRejectsInvalidRecord(t *testing.T) {
	store, fake := newFakeStore(t, PostgresConfig{})

	// failed 却没有原因：与「只写 status」没有区别，本地就该拦下。
	err := store.Append(Record{TriggerID: testTriggerID, Channel: "email", Status: StatusFailed})
	if err == nil {
		t.Fatal("失败但无原因的记录应被拒绝")
	}
	execs, _, _, _, _ := fake.snapshot()
	if len(execs) != 0 {
		t.Fatalf("不应有 SQL 执行，实际 %d 次", len(execs))
	}
}

func TestPostgresStoreRespectsCanceledContext(t *testing.T) {
	store, fake := newFakeStore(t, PostgresConfig{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := store.AppendContext(ctx, Record{TriggerID: testTriggerID, Channel: "push", Status: StatusSent})
	if err == nil {
		t.Fatal("已取消的上下文应中断写入")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("错误链应包含 context.Canceled，实际: %v", err)
	}
	execs, _, _, _, _ := fake.snapshot()
	if len(execs) != 0 {
		t.Fatalf("取消后不应执行 SQL，实际 %d 次", len(execs))
	}

	// 取消只影响这一条：存储本身仍然可用，后面的消息不能被一起拖垮。
	if err := store.Append(Record{TriggerID: testTriggerID, Channel: "push", Status: StatusSent}); err != nil {
		t.Fatalf("取消一次后存储应仍可写入: %v", err)
	}
}

func TestPostgresStoreClosed(t *testing.T) {
	fake := &fakeDB{execErr: map[int]error{}}
	name := "xw-fake-pg-closed"
	sql.Register(name, &fakeDriver{db: fake})
	store, err := NewPostgresStore(context.Background(), PostgresConfig{
		DSN: "postgres://fake/xw", Driver: name, ConnectTimeout: time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("构造失败: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}
	if err := store.Append(Record{TriggerID: testTriggerID, Channel: "email", Status: StatusSent}); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("关闭后写入应返回 ErrStoreClosed，实际: %v", err)
	}
	// 重复关闭是退出路径上的常态（信号处理与 defer 都会调），不能报错。
	if err := store.Close(); err != nil {
		t.Fatalf("重复关闭不应报错: %v", err)
	}
}

func TestPostgresStoreBatchCommitsInOneTransaction(t *testing.T) {
	store, fake := newFakeStore(t, PostgresConfig{})

	written, err := store.AppendBatch(context.Background(), []Record{
		{TriggerID: testTriggerID, Channel: "email", Status: StatusSent},
		{TriggerID: testTriggerID, Channel: "push", Status: StatusSent},
		{TriggerID: testTriggerID, Channel: "in_app", Status: StatusSent},
	})
	if err != nil {
		t.Fatalf("批量写入失败: %v", err)
	}
	if written != 3 {
		t.Fatalf("期望写入 3 条，实际 %d", written)
	}

	execs, _, begins, commits, rollbacks := fake.snapshot()
	if len(execs) != 3 || begins != 1 || commits != 1 || rollbacks != 0 {
		t.Fatalf("事务边界错误: execs=%d begins=%d commits=%d rollbacks=%d",
			len(execs), begins, commits, rollbacks)
	}
	fake.mu.Lock()
	isolation := fake.isolation
	fake.mu.Unlock()
	if len(isolation) != 1 || isolation[0] != sql.LevelReadCommitted {
		t.Fatalf("隔离级别应为 ReadCommitted，实际 %#v", isolation)
	}
}

func TestPostgresStoreBatchRollsBackOnFailure(t *testing.T) {
	store, fake := newFakeStore(t, PostgresConfig{})
	fake.execErr[1] = errors.New("模拟第二条写入失败")

	written, err := store.AppendBatch(context.Background(), []Record{
		{TriggerID: testTriggerID, Channel: "email", Status: StatusSent},
		{TriggerID: testTriggerID, Channel: "push", Status: StatusSent},
		{TriggerID: testTriggerID, Channel: "in_app", Status: StatusSent},
	})
	if err == nil {
		t.Fatal("第二条失败时整批应失败")
	}
	if written != 1 {
		t.Fatalf("期望在第二条中止（written=1），实际 %d", written)
	}

	_, _, _, commits, rollbacks := fake.snapshot()
	if commits != 0 || rollbacks != 1 {
		t.Fatalf("应回滚且不提交: commits=%d rollbacks=%d", commits, rollbacks)
	}
}

func TestPostgresStoreBatchValidatesBeforeBeginning(t *testing.T) {
	store, fake := newFakeStore(t, PostgresConfig{})

	_, err := store.AppendBatch(context.Background(), []Record{
		{TriggerID: testTriggerID, Channel: "email", Status: StatusSent},
		{TriggerID: testTriggerID, Status: StatusSent}, // 缺 channel
	})
	if err == nil {
		t.Fatal("含非法记录的批次应被拒绝")
	}
	if !strings.Contains(err.Error(), "第 2 条") {
		t.Fatalf("错误信息应指出是哪一条，实际: %v", err)
	}
	_, _, begins, _, _ := fake.snapshot()
	if begins != 0 {
		t.Fatalf("校验失败时不应开事务，实际 %d 次", begins)
	}
}

func TestPostgresStorePingFailureAtStartup(t *testing.T) {
	fake := &fakeDB{execErr: map[int]error{}, pingErr: errors.New("connection refused")}
	name := "xw-fake-pg-ping"
	sql.Register(name, &fakeDriver{db: fake})

	_, err := NewPostgresStore(context.Background(), PostgresConfig{
		DSN: "postgres://fake/xw", Driver: name, ConnectTimeout: time.Second,
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "连接数据库失败") {
		t.Fatalf("连不上时应返回构造错误，实际: %v", err)
	}
}

func TestPostgresStorePrepareFailureMentionsMigration(t *testing.T) {
	fake := &fakeDB{execErr: map[int]error{}, prepareErr: errors.New(`relation "execution_details" does not exist`)}
	name := "xw-fake-pg-prepare"
	sql.Register(name, &fakeDriver{db: fake})

	_, err := NewPostgresStore(context.Background(), PostgresConfig{
		DSN: "postgres://fake/xw", Driver: name, ConnectTimeout: time.Second,
	}, nil)
	if err == nil {
		t.Fatal("表不存在时应返回构造错误")
	}
	if !strings.Contains(err.Error(), "004") {
		t.Fatalf("错误信息应指向建表迁移，实际: %v", err)
	}
}

func TestNewPostgresStoreRequiresDSN(t *testing.T) {
	if _, err := NewPostgresStore(context.Background(), PostgresConfig{}, nil); err == nil {
		t.Fatal("缺少连接串时应返回错误")
	}
}

func TestPostgresConfigDefaults(t *testing.T) {
	cfg := PostgresConfig{MaxOpenConns: 1000, MaxIdleConns: 50}.withDefaults()
	if cfg.Driver != defaultPGDriver {
		t.Fatalf("默认驱动应为 %s，实际 %s", defaultPGDriver, cfg.Driver)
	}
	if cfg.MaxOpenConns != pgMaxOpenLimit {
		t.Fatalf("超限的连接数应被夹紧到 %d，实际 %d", pgMaxOpenLimit, cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > cfg.MaxOpenConns {
		t.Fatalf("空闲连接数不能超过总连接数: %d > %d", cfg.MaxIdleConns, cfg.MaxOpenConns)
	}
}

func TestNormalizePostgresDSN(t *testing.T) {
	cases := map[string]string{
		"postgresql+psycopg://u:p@localhost:5432/db": "postgres://u:p@localhost:5432/db",
		"postgresql://u:p@localhost:5432/db":         "postgresql://u:p@localhost:5432/db",
		"  postgres://u:p@db/xw  ":                   "postgres://u:p@db/xw",
		"":                                           "",
	}
	for input, want := range cases {
		if got := normalizePostgresDSN(input); got != want {
			t.Fatalf("normalizePostgresDSN(%q) = %q，期望 %q", input, got, want)
		}
	}
}

func TestRecorderAppliesWriteTimeout(t *testing.T) {
	fake := &fakeDB{execErr: map[int]error{}, execDelay: 200 * time.Millisecond}
	name := "xw-fake-pg-timeout"
	sql.Register(name, &fakeDriver{db: fake})
	store, err := NewPostgresStore(context.Background(), PostgresConfig{
		DSN: "postgres://fake/xw", Driver: name, ConnectTimeout: time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("构造失败: %v", err)
	}
	defer store.Close()

	recorder := NewRecorder(store, nil).WithWriteTimeout(20 * time.Millisecond)
	start := time.Now()
	_, err = recorder.Write(Record{TriggerID: testTriggerID, Channel: "email", Status: StatusSent})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("写入超过预算时应返回错误")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("错误链应包含 DeadlineExceeded，实际: %v", err)
	}
	if elapsed > 150*time.Millisecond {
		t.Fatalf("写入预算没有生效：耗时 %s", elapsed)
	}
}
