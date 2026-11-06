package execution

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// 本文件是执行明细的 Postgres 后端。
//
// 与 JSONL 后端的分工不是「一个简单一个复杂」，而是两种部署形态：
//
//   - 单机 demo / 本地开发：JSONL 够用，零依赖，一行一条，从文件尾部倒序读；
//   - 多实例部署：多个引擎进程追加同一份文件，只能靠「单次 write 在内核里原子」
//     保证不串行。记录一多就无法按渠道 / 时间过滤，也无法建索引。
//
// Postgres 后端把这件事交给数据库：写入是一条 INSERT，查询侧的过滤、排序与
// 游标分页都能下推到 006 建的复合索引上，和 API 的读取实现共用同一张表。
//
// 三处刻意的选择：
//
//  1. 预处理语句在构造时建一次，之后每次写入复用它。执行明细是高频单行插入，
//     每条都重新解析 SQL 没有意义；database/sql 的 *sql.Stmt 在连接失效时会
//     自行重新预处理，这里不需要自己维护「失效后重试」的状态机。
//  2. 池参数显式设置而不是用默认值。默认 MaxOpenConns 不限，突发流量下会把
//     数据库的连接数打满，故障从引擎扩散到整个库。
//  3. 批量写入走一个事务。要么整批落库，要么一条都不落——审计链路里「写了一半」
//     比「一条没写」更难查，因为看记录的人不知道缺的是哪几条。
type PostgresStore struct {
	db     *sql.DB
	insert *sql.Stmt
	logger *slog.Logger

	// mu 让关闭与写入互斥：Append 持读锁，Close 持写锁。少了这把锁，
	// 关闭时正在执行的语句会拿到一个已释放的 *sql.Stmt。
	mu     sync.RWMutex
	closed bool
}

// insertSQL 与 API 侧 api/app/execution_log.py 的 PostgresExecutionStore 保持
// 同一组列：引擎写、API 读，两边的列名一旦分叉，读出来的就是一张空表。
//
// 只有这七列是 API 会读的。JSONL 记录里多出来的 step_id / provider / attempts /
// fallback 属于排查字段，表结构里没有对应列，切到 Postgres 后端时这几个字段不落库。
const insertSQL = `
INSERT INTO execution_details
    (trigger_id, channel, status, reason, duration_ms, tenant, environment)
VALUES
    ($1, $2, $3, $4, $5, $6, $7)`

// PostgresConfig 是 Postgres 后端的连接参数。
//
// 零值会被替换成下面的默认值：配置文件里漏写一个变量时，引擎应该按合理的
// 默认值跑起来，而不是连不上或者把连接数开到无限。
type PostgresConfig struct {
	// DSN 是连接串，形如 postgres://user:pass@host:5432/db?sslmode=disable。
	// 允许沿用 API 侧的 postgresql+psycopg:// 写法，连接前会去掉驱动后缀。
	DSN string
	// Driver 是 database/sql 的驱动名，默认 pgx；测试用它注入假驱动。
	Driver string
	// MaxOpenConns 是同时打开的最大连接数，<=0 时取默认值。
	MaxOpenConns int
	// MaxIdleConns 是保留在池里的空闲连接数，不能大于 MaxOpenConns。
	MaxIdleConns int
	// ConnMaxLifetime 是单条连接的最长存活时间：数据库侧重启、连接被中间设备
	// 静默断开时，靠它把陈旧连接换掉，而不是等第一次写入失败才发现。
	ConnMaxLifetime time.Duration
	// ConnMaxIdleTime 是空闲连接的回收时间，避免低峰期占着连接不放。
	ConnMaxIdleTime time.Duration
	// ConnectTimeout 是启动时 Ping 与预处理语句的超时。
	ConnectTimeout time.Duration
}

// 默认连接池参数。取值偏保守：执行明细是旁路写入，不值得为它抢占数据库的
// 连接配额；写入量真正上来时应该先看批量接口，而不是把池开大。
const (
	defaultPGDriver          = "pgx"
	defaultPGMaxOpenConns    = 8
	defaultPGMaxIdleConns    = 4
	defaultPGConnMaxLifetime = 30 * time.Minute
	defaultPGConnMaxIdleTime = 5 * time.Minute
	defaultPGConnectTimeout  = 5 * time.Second

	// pgMaxOpenLimit 是 MaxOpenConns 的上限。配置写错（比如多写一个零）时
	// 直接夹紧，而不是让引擎把数据库的连接数打满。
	pgMaxOpenLimit = 64
)

func (c PostgresConfig) withDefaults() PostgresConfig {
	if strings.TrimSpace(c.Driver) == "" {
		c.Driver = defaultPGDriver
	}
	if c.MaxOpenConns <= 0 {
		c.MaxOpenConns = defaultPGMaxOpenConns
	}
	if c.MaxOpenConns > pgMaxOpenLimit {
		c.MaxOpenConns = pgMaxOpenLimit
	}
	if c.MaxIdleConns <= 0 {
		c.MaxIdleConns = defaultPGMaxIdleConns
	}
	if c.MaxIdleConns > c.MaxOpenConns {
		c.MaxIdleConns = c.MaxOpenConns
	}
	if c.ConnMaxLifetime <= 0 {
		c.ConnMaxLifetime = defaultPGConnMaxLifetime
	}
	if c.ConnMaxIdleTime <= 0 {
		c.ConnMaxIdleTime = defaultPGConnMaxIdleTime
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = defaultPGConnectTimeout
	}
	c.DSN = normalizePostgresDSN(c.DSN)
	return c
}

// normalizePostgresDSN 把 API 侧习惯的 SQLAlchemy 连接串换成 database/sql 能识别的写法。
//
// 两个进程共用 XW_DATABASE_URL：API 的默认值带 +psycopg 后缀，psql 迁移脚本也做
// 同样的剥离。不处理的话，同一份配置在引擎侧会报「unknown driver」或者解析失败。
func normalizePostgresDSN(dsn string) string {
	dsn = strings.TrimSpace(dsn)
	for _, prefix := range []string{"postgresql+psycopg://", "postgres+psycopg://"} {
		if strings.HasPrefix(dsn, prefix) {
			return "postgres://" + strings.TrimPrefix(dsn, prefix)
		}
	}
	return dsn
}

// NewPostgresStore 建立连接池、验证连通性并预编译写入语句。
//
// 三步都在构造期做完，失败就直接返回错误：让引擎带着一个写不进去的存储启动，
// 表现是「投递都在跑，执行明细一直是空的」，比启动时报错难查得多。
// 调用方拿到错误后应当关闭执行明细回写并继续投递——审计链路坏了不该拦住消息。
func NewPostgresStore(ctx context.Context, cfg PostgresConfig, logger *slog.Logger) (*PostgresStore, error) {
	cfg = cfg.withDefaults()
	if cfg.DSN == "" {
		return nil, errors.New("execution: 数据库连接串为空（XW_DATABASE_URL）")
	}
	if logger == nil {
		logger = slog.Default()
	}

	db, err := sql.Open(cfg.Driver, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("execution: 打开数据库失败: %w", err)
	}
	// sql.Open 只做参数校验、不建连接，因此池参数可以在这里设。
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("execution: 连接数据库失败（%s）: %w", cfg.DSN, err)
	}

	prepCtx, cancelPrep := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancelPrep()
	stmt, err := db.PrepareContext(prepCtx, insertSQL)
	if err != nil {
		_ = db.Close()
		// 这里最常见的失败是「表不存在」：迁移没跑，或者连到了另一个库。
		// 把下一步该做什么写进错误里，省得对着 42P01 猜。
		return nil, fmt.Errorf("execution: 预编译写入语句失败（确认 execution_details 表已由 scripts/migrations/004 建好）: %w", err)
	}

	store := &PostgresStore{db: db, insert: stmt, logger: logger}
	logger.Info("[execution] 执行明细将写入 Postgres",
		"maxOpen", cfg.MaxOpenConns,
		"maxIdle", cfg.MaxIdleConns,
		"connMaxLifetime", cfg.ConnMaxLifetime.String(),
	)
	return store, nil
}

// Append 实现 Store 接口。没有调用方上下文的路径走这里：写入用一个后台
// context，由 Recorder 自己的超时兜住。
func (s *PostgresStore) Append(record Record) error {
	return s.AppendContext(context.Background(), record)
}

// AppendContext 写入一条执行明细，ctx 的取消与超时会中断本次查询。
//
// 超时比取消更常用：数据库变慢时，一次写入不该把 worker 的 goroutine 挂在
// 那里等着。ExecContext 返回后连接会回到池里（或被丢弃），不会留下半开的事务。
func (s *PostgresStore) AppendContext(ctx context.Context, record Record) error {
	if s == nil {
		return ErrStoreClosed
	}
	args, err := insertArgs(record)
	if err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.insert == nil {
		return ErrStoreClosed
	}

	if _, err := s.insert.ExecContext(ctx, args...); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("execution: 写入执行明细被中断: %w", ctxErr)
		}
		return fmt.Errorf("execution: 写入执行明细失败（trigger_id=%s channel=%s）: %w",
			record.TriggerID, record.Channel, err)
	}
	return nil
}

// AppendBatch 在一个事务里写入多条记录，返回已写入条数。
//
// 先整批校验再开事务：一条不合法就一条都不写。否则调用方拿到「写了 2 条、
// 第 3 条失败」，还要自己去猜前两条是否已经提交。
//
// 隔离级别显式取 ReadCommitted：这是 Postgres 的默认级别，也是追加写唯一
// 需要的保证——本事务只插入自己的行，不读别人的未提交数据。
func (s *PostgresStore) AppendBatch(ctx context.Context, records []Record) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}
	args := make([][]any, len(records))
	for i, record := range records {
		prepared, err := insertArgs(record)
		if err != nil {
			return 0, fmt.Errorf("execution: 第 %d 条记录不合法: %w", i+1, err)
		}
		args[i] = prepared
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.db == nil {
		return 0, ErrStoreClosed
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("execution: 开启事务失败: %w", err)
	}
	// 事务内复用已预编译的语句：tx.StmtContext 会把它绑定到本事务的连接上，
	// 不会为每条记录重新解析一次 SQL。
	stmt := tx.StmtContext(ctx, s.insert)
	defer stmt.Close()

	for i, prepared := range args {
		if _, err := stmt.ExecContext(ctx, prepared...); err != nil {
			// 回滚失败不覆盖原始错误：写不进去的原因比「回滚也没成功」更值得看。
			if rbErr := tx.Rollback(); rbErr != nil {
				s.logger.Warn("[execution] 批量写入失败后回滚也失败",
					"written", i, "error", err, "rollbackError", rbErr)
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return i, fmt.Errorf("execution: 批量写入被中断（已回滚）: %w", ctxErr)
			}
			return i, fmt.Errorf("execution: 第 %d 条写入失败（已回滚）: %w", i+1, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("execution: 提交事务失败: %w", err)
	}
	return len(records), nil
}

// PoolStats 是连接池的运行时快照。
//
// WaitCount / WaitDuration 是判断「池子够不够」的唯一依据：它们只在所有连接
// 都被占用、调用方被迫排队时才增长。只调大池子而不看这两个数，等于不知道
// 之前到底有没有排队。
type PoolStats struct {
	MaxOpen           int
	Open              int
	InUse             int
	Idle              int
	WaitCount         int64
	WaitDuration      time.Duration
	MaxIdleClosed     int64
	MaxLifetimeClosed int64
}

// Stats 返回当前连接池状态，用于退出时打一行日志与排查「池满排队」。
func (s *PostgresStore) Stats() PoolStats {
	if s == nil || s.db == nil {
		return PoolStats{}
	}
	st := s.db.Stats()
	return PoolStats{
		MaxOpen:           st.MaxOpenConnections,
		Open:              st.OpenConnections,
		InUse:             st.InUse,
		Idle:              st.Idle,
		WaitCount:         st.WaitCount,
		WaitDuration:      st.WaitDuration,
		MaxIdleClosed:     st.MaxIdleClosed,
		MaxLifetimeClosed: st.MaxLifetimeClosed,
	}
}

// Close 释放语句与连接池；重复调用无副作用。
func (s *PostgresStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true

	var errs []error
	if s.insert != nil {
		if err := s.insert.Close(); err != nil {
			errs = append(errs, fmt.Errorf("关闭预处理语句: %w", err))
		}
	}
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("关闭连接池: %w", err))
		}
	}
	return errors.Join(errs...)
}

// insertArgs 把一条记录折算成 INSERT 的参数，并在送库之前挡掉两类必然失败：
// 必填字段缺失、trigger_id 不是 UUID。
//
// 预校验的意义不只是错误信息好看：trigger_id 列是 UUID 类型，非法值送到数据库
// 会走一次网络往返再返回 22P02；在本地挡掉既省一次往返，也让「记录不合法」
// 与「数据库不可用」在错误链上分得开。
func insertArgs(record Record) ([]any, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	triggerID := strings.TrimSpace(record.TriggerID)
	if !looksLikeUUID(triggerID) {
		return nil, fmt.Errorf("%w：trigger_id %q 不是 UUID，execution_details 的该列是 UUID 类型",
			ErrInvalidRecord, record.TriggerID)
	}

	var reason any
	if record.Reason != nil && strings.TrimSpace(*record.Reason) != "" {
		reason = *record.Reason
	}
	return []any{
		triggerID,
		record.Channel,
		string(record.Status),
		reason,
		record.DurationMS,
		dimensionOr(record.Tenant, "default"),
		dimensionOr(record.Environment, "development"),
	}, nil
}

// dimensionOr 把空的隔离维度折成缺省值，与表上的 DEFAULT 保持一致。
func dimensionOr(value, fallback string) string {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		return trimmed
	}
	return fallback
}

// looksLikeUUID 判断字符串是否是 8-4-4-4-12 形式的十六进制 UUID。
//
// 只做形状校验，不判断版本位：执行明细要的是「数据库收得下」，不是「这是个
// 合法的 v4」。放开版本位也能让测试用固定值的 UUID 构造记录。
func looksLikeUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, r := range s {
		switch i {
		case 8, 13, 18, 23:
			if r != '-' {
				return false
			}
		default:
			isDigit := r >= '0' && r <= '9'
			isLower := r >= 'a' && r <= 'f'
			isUpper := r >= 'A' && r <= 'F'
			if !isDigit && !isLower && !isUpper {
				return false
			}
		}
	}
	return true
}
