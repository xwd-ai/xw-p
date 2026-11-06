package execution

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 本文件是 database/sql 的假驱动，只为实现 PostgresStore 的单元测试。
//
// 不用 sqlmock 之类的库有两个原因：一是这个包目前零测试依赖，引入一个 mock
// 框架只为一处断言不划算；二是假驱动能直接验证「连接池参数是否真的设上了」
// （sql.DB.Stats() 读的是 database/sql 自己的状态），而 SQL 文本匹配的 mock
// 库反而做不到这一点。
//
// 假驱动不证明 SQL 在 Postgres 上语法正确——那由 postgres_integration_test.go
// 连真库验证。这里只验证 Go 侧的行为：参数顺序、事务边界、上下文取消、
// 关闭语义。
type fakeDB struct {
	mu sync.Mutex

	prepared  []string
	execs     [][]driver.Value
	begins    int
	commits   int
	rollbacks int
	isolation []sql.IsolationLevel

	// 故障注入：按下标指定第 n 次执行失败（0 开始），便于验证「批量写到一半」。
	pingErr    error
	prepareErr error
	beginErr   error
	commitErr  error
	execErr    map[int]error
	execCount  int

	// execDelay 模拟一次慢查询，用来验证调用方的超时会生效。
	execDelay time.Duration
}

func (f *fakeDB) recordExec(args []driver.Value) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execCount++
	f.execs = append(f.execs, append([]driver.Value(nil), args...))
}

func (f *fakeDB) snapshot() (execs [][]driver.Value, prepares int, begins, commits, rollbacks int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]driver.Value(nil), f.execs...), len(f.prepared), f.begins, f.commits, f.rollbacks
}

// fakeDriver 每次 Open 都返回同一个连接：测试是串行的，共用一个连接能让
// 「预处理语句被复用」这件事可断言（每条连接各一份预处理语句会掩盖复用）。
type fakeDriver struct{ db *fakeDB }

func (d *fakeDriver) Open(string) (driver.Conn, error) { return &fakeConn{db: d.db}, nil }

type fakeConn struct{ db *fakeDB }

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *fakeConn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	if c.db.prepareErr != nil {
		return nil, c.db.prepareErr
	}
	c.db.prepared = append(c.db.prepared, query)
	return &fakeStmt{db: c.db}, nil
}

func (c *fakeConn) Close() error { return nil }

func (c *fakeConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *fakeConn) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	if c.db.beginErr != nil {
		return nil, c.db.beginErr
	}
	c.db.begins++
	c.db.isolation = append(c.db.isolation, sql.IsolationLevel(opts.Isolation))
	return &fakeTx{db: c.db}, nil
}

func (c *fakeConn) Ping(context.Context) error {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	return c.db.pingErr
}

type fakeStmt struct{ db *fakeDB }

func (s *fakeStmt) Close() error  { return nil }
func (s *fakeStmt) NumInput() int { return -1 }

func (s *fakeStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("fake: 应使用 ExecContext")
}

func (s *fakeStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("fake: 执行明细只有写入，不支持查询")
}

func (s *fakeStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.db.mu.Lock()
	delay := s.db.execDelay
	s.db.mu.Unlock()
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	s.db.mu.Lock()
	index := s.db.execCount
	injected := s.db.execErr[index]
	s.db.mu.Unlock()
	if injected != nil {
		s.db.recordExec(nil)
		return nil, injected
	}

	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	s.db.recordExec(values)
	return driver.RowsAffected(1), nil
}

type fakeTx struct{ db *fakeDB }

func (t *fakeTx) Commit() error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	if t.db.commitErr != nil {
		return t.db.commitErr
	}
	t.db.commits++
	return nil
}

func (t *fakeTx) Rollback() error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	t.db.rollbacks++
	return nil
}

// fakeDriverSeq 保证每个用例注册一个独立的驱动名：database/sql 不允许
// 重复注册，而测试并行执行时共用一个名字会互相覆盖。
var fakeDriverSeq atomic.Int64

// newFakeStore 用假驱动构造一个 PostgresStore，返回存储与可断言的假库状态。
func newFakeStore(t *testing.T, cfg PostgresConfig) (*PostgresStore, *fakeDB) {
	t.Helper()

	fake := &fakeDB{execErr: map[int]error{}}
	name := fmt.Sprintf("xw-fake-pg-%d", fakeDriverSeq.Add(1))
	sql.Register(name, &fakeDriver{db: fake})

	cfg.DSN = "postgres://fake/xw"
	cfg.Driver = name
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = time.Second
	}
	store, err := NewPostgresStore(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("构造 PostgresStore 失败: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("关闭 PostgresStore 失败: %v", err)
		}
	})
	return store, fake
}
