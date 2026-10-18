-- xw-notify 核心实体建表 DDL（Phase 1）
-- 与 api/app/models/db.py 的 SQLAlchemy 模型保持一致。
-- 用法：psql "$XW_DATABASE_URL" -f scripts/migrations/002_core_entities.sql

-- 工作流（通知模板）
CREATE TABLE IF NOT EXISTS workflows (
    id          VARCHAR(36)  PRIMARY KEY,
    name        VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    steps       JSONB        NOT NULL DEFAULT '[]'::jsonb,
    version     INTEGER      NOT NULL DEFAULT 1,
    environment VARCHAR(32)  NOT NULL DEFAULT 'development',
    active      BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 订阅者（通知接收方）
CREATE TABLE IF NOT EXISTS subscribers (
    id           VARCHAR(36)  PRIMARY KEY,
    email        VARCHAR(255),
    phone        VARCHAR(64),
    device_token VARCHAR(512),
    channels     JSONB        NOT NULL DEFAULT '[]'::jsonb,
    environment  VARCHAR(32)  NOT NULL DEFAULT 'development',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 消息（一次触发投递产生的执行实例）
-- 注意：Phase 0 的 messages 表字段为最小集（trigger_id/transaction_id 等）。
-- 此处为核心领域消息模型，采用独立结构；若需合并，将在后续迁移中演进。
CREATE TABLE IF NOT EXISTS messages (
    id            VARCHAR(36)  PRIMARY KEY,
    workflow_id   VARCHAR(36)  NOT NULL REFERENCES workflows(id),
    subscriber_id VARCHAR(36)  NOT NULL REFERENCES subscribers(id),
    channel       VARCHAR(32)  NOT NULL,
    payload       JSONB        NOT NULL DEFAULT '{}'::jsonb,
    status        VARCHAR(32)  NOT NULL DEFAULT 'created',
    error         TEXT,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_messages_subscriber  ON messages (subscriber_id);
CREATE INDEX IF NOT EXISTS idx_messages_workflow    ON messages (workflow_id);
CREATE INDEX IF NOT EXISTS idx_messages_status      ON messages (status);
CREATE INDEX IF NOT EXISTS idx_messages_created_at  ON messages (created_at);
