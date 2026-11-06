-- xw-notify 核心实体建表 DDL（Phase 1）
-- 与 api/app/models/db.py 的 SQLAlchemy 模型保持一致。

-- 工作流（通知模板）
CREATE TABLE IF NOT EXISTS workflows (
    id          VARCHAR(36)  PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
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
-- 工作流消息实例独立于投递记录表 messages。
CREATE TABLE IF NOT EXISTS workflow_messages (
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

CREATE INDEX IF NOT EXISTS idx_workflow_messages_subscriber  ON workflow_messages (subscriber_id);
CREATE INDEX IF NOT EXISTS idx_workflow_messages_workflow    ON workflow_messages (workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_messages_status      ON workflow_messages (status);
CREATE INDEX IF NOT EXISTS idx_workflow_messages_created_at  ON workflow_messages (created_at);
