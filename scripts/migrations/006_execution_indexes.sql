-- xw-notify 执行明细查询索引（Phase R6）
-- GET /v1/executions 改为「过滤下推到存储 + 游标翻页」后配套的复合索引。
-- 用法：psql "$XW_DATABASE_URL" -f scripts/migrations/006_execution_indexes.sql
--
-- 排序键是 (created_at DESC, id DESC)：一次触发会写出多条渠道明细，
-- created_at 可能撞车，用自增主键做次级键才是全序。游标条件是
-- (created_at, id) < (?, ?)，因此这两列必须按同样的降序排在过滤列之后，
-- Postgres 才能直接按索引顺序取一页而不额外排序。
-- 004 建的单列索引被下面的复合索引覆盖（前缀列相同），可保留不影响正确性。

-- 无过滤条件的默认页
CREATE INDEX IF NOT EXISTS idx_execution_details_recent
    ON execution_details (created_at DESC, id DESC);

-- 按触发回查一次触发的全部渠道明细
CREATE INDEX IF NOT EXISTS idx_execution_details_trigger_recent
    ON execution_details (trigger_id, created_at DESC, id DESC);

-- 按渠道看某渠道的投递情况
CREATE INDEX IF NOT EXISTS idx_execution_details_channel_recent
    ON execution_details (channel, created_at DESC, id DESC);

-- 只看失败的投递（排查页）
CREATE INDEX IF NOT EXISTS idx_execution_details_status_recent
    ON execution_details (status, created_at DESC, id DESC);
