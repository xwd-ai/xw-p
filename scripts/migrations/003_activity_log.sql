-- xw-notify 活动日志表 DDL（Phase 1）
-- 与 api/app/activity_log.py 的 PostgresActivityStore 保持一致。
-- 触发 API 返回回执前同步写入一条活动日志，供活动流查询展示。

CREATE TABLE IF NOT EXISTS activity_log (
    id            BIGSERIAL   PRIMARY KEY,
    trigger_id    UUID        NOT NULL,
    workflow      TEXT        NOT NULL,
    subscriber_id TEXT        NOT NULL,
    channels      JSONB       NOT NULL DEFAULT '[]'::jsonb,
    status        TEXT        NOT NULL DEFAULT 'triggered',
    title         TEXT,
    environment   TEXT        NOT NULL DEFAULT 'development',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_activity_log_subscriber ON activity_log (subscriber_id);
CREATE INDEX IF NOT EXISTS idx_activity_log_workflow   ON activity_log (workflow);
CREATE INDEX IF NOT EXISTS idx_activity_log_created_at ON activity_log (created_at);
CREATE INDEX IF NOT EXISTS idx_activity_log_environment ON activity_log (environment);
