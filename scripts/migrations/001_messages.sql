-- xw-notify 消息表 DDL（Phase 0 最小集）
-- 用于 STORAGE_BACKEND=postgres 时的持久化落库。
-- 用法：psql "$XW_DATABASE_URL" -f scripts/migrations/001_messages.sql

CREATE TABLE IF NOT EXISTS messages (
    id              BIGSERIAL PRIMARY KEY,
    trigger_id      UUID        NOT NULL,
    transaction_id  UUID        NOT NULL,
    workflow        TEXT        NOT NULL,
    subscriber_id   TEXT        NOT NULL,
    channels        JSONB       NOT NULL DEFAULT '[]'::jsonb,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT        NOT NULL DEFAULT 'sent',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_messages_subscriber ON messages (subscriber_id);
CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages (created_at);
