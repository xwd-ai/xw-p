package execution

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// 真库用例：验证 SQL 本身在 Postgres 上能跑通（类型、默认值、NULL 语义），
// 这是假驱动证明不了的部分。
//
// 默认跳过，只有显式给了连接串才执行：
//
//	XW_TEST_POSTGRES_DSN='postgres://user@localhost:5432/postgres?sslmode=disable' \
//	    go test ./internal/execution/ -run Integration -v
//
// 每次运行在独立 schema 里建表、跑完即 DROP，不碰执行明细的正式表，也不依赖
// 迁移是否已经跑过。schema 名带纳秒时间戳，并发跑多个用例不会互相覆盖。
func TestPostgresStoreIntegration(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("XW_TEST_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("未设置 XW_TEST_POSTGRES_DSN，跳过真库用例")
	}

	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("打开数据库失败: %v", err)
	}
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("连接数据库失败: %v", err)
	}

	schema := fmt.Sprintf("xwtest_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("创建测试 schema 失败: %v", err)
	}
	// 用 defer 而不是 t.Cleanup：defer 的顺序是后进先出，这条要排在
	// admin.Close() 之前执行，否则清理时连接已经关了。
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.ExecContext(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("清理测试 schema 失败: %v", err)
		}
	}()

	// 表结构取自 scripts/migrations/004 与 007：引擎写入的列必须与 API 读取
	// 的那张表一致，这里按同一份 DDL 建一遍，SQL 写错在真库上会立刻暴露。
	ddl := fmt.Sprintf(`CREATE TABLE %s.execution_details (
		id          BIGSERIAL   PRIMARY KEY,
		trigger_id  UUID        NOT NULL,
		channel     TEXT        NOT NULL,
		status      TEXT        NOT NULL DEFAULT 'sent',
		reason      TEXT,
		duration_ms INTEGER     NOT NULL DEFAULT 0,
		created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
		tenant      TEXT        NOT NULL DEFAULT 'default',
		environment TEXT        NOT NULL DEFAULT 'development'
	)`, schema)
	if _, err := admin.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("建表失败: %v", err)
	}

	storeDSN, err := withSearchPath(dsn, schema)
	if err != nil {
		t.Fatalf("构造测试连接串失败: %v", err)
	}
	store, err := NewPostgresStore(ctx, PostgresConfig{
		DSN:             storeDSN,
		MaxOpenConns:    2,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Minute,
		ConnectTimeout:  5 * time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("构造 PostgresStore 失败: %v", err)
	}
	defer store.Close()

	// 三条单写，覆盖 sent / failed / skipped 三种状态与 NULL reason。
	singles := []Record{
		{TriggerID: testTriggerID, Channel: "email", Status: StatusSent, DurationMS: 12},
		{TriggerID: testTriggerID, Channel: "sms", Status: StatusFailed, Reason: pointTo("短信通道返回 500"), DurationMS: 40},
		{TriggerID: testTriggerID, Channel: "push", Status: StatusSkipped, Reason: pointTo("渠道偏好关闭"), Tenant: "acme", Environment: "production"},
	}
	for _, record := range singles {
		if err := store.AppendContext(ctx, record); err != nil {
			t.Fatalf("写入 %s 失败: %v", record.Channel, err)
		}
	}

	// 批量写走一个事务：写完能查到，说明 COMMIT 真的发生了。
	written, err := store.AppendBatch(ctx, []Record{
		{TriggerID: testTriggerID, Channel: "in_app", Status: StatusSent},
		{TriggerID: testTriggerID, Channel: "chat", Status: StatusSent},
	})
	if err != nil {
		t.Fatalf("批量写入失败: %v", err)
	}
	if written != 2 {
		t.Fatalf("批量写入条数错误: %d", written)
	}

	// 用独立的连接回读，确认数据真的落到了库里而不是只经过了驱动。
	check, err := sql.Open("pgx", storeDSN)
	if err != nil {
		t.Fatalf("打开校验连接失败: %v", err)
	}
	defer check.Close()

	var total int
	if err := check.QueryRowContext(ctx, "SELECT count(*) FROM execution_details").Scan(&total); err != nil {
		t.Fatalf("统计失败: %v", err)
	}
	if total != 5 {
		t.Fatalf("期望 5 行，实际 %d", total)
	}

	var nullReasons int
	if err := check.QueryRowContext(ctx,
		"SELECT count(*) FROM execution_details WHERE reason IS NULL").Scan(&nullReasons); err != nil {
		t.Fatalf("统计 NULL reason 失败: %v", err)
	}
	if nullReasons != 3 {
		t.Fatalf("期望 3 行 reason 为 NULL，实际 %d", nullReasons)
	}

	var tenant, environment, status string
	if err := check.QueryRowContext(ctx,
		"SELECT tenant, environment, status FROM execution_details WHERE channel = 'sms'").Scan(&tenant, &environment, &status); err != nil {
		t.Fatalf("读取 sms 记录失败: %v", err)
	}
	if tenant != "default" || environment != "development" {
		t.Fatalf("缺省隔离维度未生效: tenant=%q environment=%q", tenant, environment)
	}
	if status != "failed" {
		t.Fatalf("status 应为 failed，实际 %q", status)
	}

	// trigger_id 是 UUID 列：按 UUID 过滤能命中，说明驱动把字符串转成了 uuid。
	var byTrigger int
	if err := check.QueryRowContext(ctx,
		"SELECT count(*) FROM execution_details WHERE trigger_id = $1", testTriggerID).Scan(&byTrigger); err != nil {
		t.Fatalf("按 trigger_id 过滤失败: %v", err)
	}
	if byTrigger != 5 {
		t.Fatalf("按 trigger_id 应命中 5 行，实际 %d", byTrigger)
	}
}

// withSearchPath 在连接串上追加 search_path，让引擎的每条连接都落在测试 schema 里。
//
// 不改全局 search_path、也不在连接上执行 SET：连接池会换连接，SET 只在当前
// 连接有效，一旦换连接就悄悄写回了正式表。
func withSearchPath(dsn, schema string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("连接串不是 URL 形式: %w", err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
