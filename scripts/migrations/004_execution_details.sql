-- xw-notify 消息执行明细表 DDL（Phase 1）
-- 与 api/app/execution_log.py 的 PostgresExecutionStore 保持一致。
-- worker 逐渠道投递时写入一条执行明细（sent/failed/reason），供后台排查。

CREATE TABLE IF NOT EXISTS execution_details (
    id          BIGSERIAL   PRIMARY KEY,
    trigger_id  UUID        NOT NULL,
    channel     TEXT        NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'sent',
    reason      TEXT,
    duration_ms INTEGER     NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_execution_details_trigger ON execution_details (trigger_id);
CREATE INDEX IF NOT EXISTS idx_execution_details_channel ON execution_details (channel);
CREATE INDEX IF NOT EXISTS idx_execution_details_created_at ON execution_details (created_at);
