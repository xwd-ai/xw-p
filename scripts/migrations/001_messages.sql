-- xw-notify 消息表 DDL（Phase 0 最小集）
-- 用于 XW_STORAGE=postgres 时的持久化落库。

-- 保留独立建表的历史消息实例及其索引，为投递记录释放表名。
DO $$
DECLARE
    legacy_index text;
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_attribute
        WHERE attrelid = to_regclass('messages') AND attname = 'workflow_id' AND NOT attisdropped
    ) THEN
        ALTER TABLE messages RENAME TO workflow_messages;
        FOR legacy_index IN
            SELECT indexname FROM pg_indexes
            WHERE schemaname = current_schema() AND tablename = 'workflow_messages'
                AND indexname IN ('idx_messages_subscriber', 'idx_messages_workflow',
                                  'idx_messages_status', 'idx_messages_created_at')
        LOOP
            EXECUTE format('ALTER INDEX %I RENAME TO %I',
                legacy_index, replace(legacy_index, 'idx_messages_', 'idx_workflow_messages_'));
        END LOOP;
    END IF;
END
$$;

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
