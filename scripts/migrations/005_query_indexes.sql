-- xw-notify 查询索引（Phase R6）
-- 活动流与统计接口改为「过滤下推到存储 / 数据库侧聚合」后，配套的复合索引。
-- 用法：psql "$XW_DATABASE_URL" -f scripts/migrations/005_query_indexes.sql
--
-- 索引列顺序与查询的排序键一致：(过滤列, created_at DESC, trigger_id DESC)。
-- 活动流按游标翻页，游标条件是 (created_at, trigger_id) < (?, ?)，因此这两个列
-- 必须按同样的降序排在过滤列之后，Postgres 才能直接用索引顺序返回而不排序。

-- 无过滤条件的活动流（默认页）
CREATE INDEX IF NOT EXISTS idx_activity_log_recent
    ON activity_log (created_at DESC, trigger_id DESC);

-- 环境隔离：前端几乎总带 environment 过滤
CREATE INDEX IF NOT EXISTS idx_activity_log_env_recent
    ON activity_log (environment, created_at DESC, trigger_id DESC);

-- 按创作者查自己的互动记录
CREATE INDEX IF NOT EXISTS idx_activity_log_subscriber_recent
    ON activity_log (subscriber_id, created_at DESC, trigger_id DESC);

-- 按工作流筛选（例如只看点赞聚合的触发）
CREATE INDEX IF NOT EXISTS idx_activity_log_workflow_recent
    ON activity_log (workflow, created_at DESC, trigger_id DESC);

-- 按触发标识精确回查单条触发
CREATE INDEX IF NOT EXISTS idx_activity_log_trigger
    ON activity_log (trigger_id);

-- 消息表按时间取最近窗口（趋势统计）；status 前置供成功率筛选扫描更少的页
CREATE INDEX IF NOT EXISTS idx_messages_status_created
    ON messages (status, created_at DESC);
