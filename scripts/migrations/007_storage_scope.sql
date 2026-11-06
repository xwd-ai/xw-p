-- 为现有记录补齐租户与环境维度。
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS tenant VARCHAR(32) NOT NULL DEFAULT 'default';
ALTER TABLE workflows DROP CONSTRAINT IF EXISTS workflows_name_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_workflows_tenant_environment_name
    ON workflows (tenant, environment, name);

ALTER TABLE activity_log ADD COLUMN IF NOT EXISTS tenant TEXT NOT NULL DEFAULT 'default';
ALTER TABLE execution_details
    ADD COLUMN IF NOT EXISTS tenant TEXT NOT NULL DEFAULT 'default',
    ADD COLUMN IF NOT EXISTS environment TEXT NOT NULL DEFAULT 'development';
