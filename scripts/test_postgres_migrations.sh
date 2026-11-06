#!/bin/sh
set -eu

scripts_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
workdir=$(mktemp -d /tmp/xw-migrations.XXXXXX)
cleanup() {
    pg_ctl -D "$workdir/data" -m immediate -w stop >/dev/null 2>&1 || true
    rm -rf "$workdir"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

initdb -D "$workdir/data" -A trust --no-locale -E UTF8 >/dev/null
pg_ctl -D "$workdir/data" -l "$workdir/postgres.log" -o "-k $workdir -h ''" -w start >/dev/null
export PGHOST="$workdir"

createdb fresh
export XW_DATABASE_URL=postgresql:///fresh
"$scripts_dir/migrate_postgres.sh" >"$workdir/first-run.log" 2>&1 &
first_pid=$!
"$scripts_dir/migrate_postgres.sh" >"$workdir/parallel-run.log" 2>&1 &
parallel_pid=$!
if ! wait "$first_pid"; then
    cat "$workdir/first-run.log" >&2
    exit 1
fi
if ! wait "$parallel_pid"; then
    cat "$workdir/parallel-run.log" >&2
    exit 1
fi
pg_dump --schema-only fresh >"$workdir/first.sql"
"$scripts_dir/migrate_postgres.sh" >/dev/null
pg_dump --schema-only fresh >"$workdir/second.sql"
# pg_dump 给每次导出生成不同的 psql 限制令牌。
sed '/^\\restrict /d; /^\\unrestrict /d' "$workdir/first.sql" >"$workdir/first-schema.sql"
sed '/^\\restrict /d; /^\\unrestrict /d' "$workdir/second.sql" >"$workdir/second-schema.sql"
diff -u "$workdir/first-schema.sql" "$workdir/second-schema.sql"

createdb legacy
export XW_DATABASE_URL=postgresql:///legacy
psql -X -v ON_ERROR_STOP=1 -d legacy -f "$scripts_dir/migrations/002_core_entities.sql" >/dev/null
psql -X -v ON_ERROR_STOP=1 -d legacy -f "$scripts_dir/migrations/003_activity_log.sql" >/dev/null
psql -X -v ON_ERROR_STOP=1 -d legacy -f "$scripts_dir/migrations/004_execution_details.sql" >/dev/null
psql -X -v ON_ERROR_STOP=1 -d legacy <<'SQL'
ALTER TABLE workflows ADD UNIQUE (name);
ALTER TABLE workflow_messages RENAME TO messages;
ALTER INDEX idx_workflow_messages_subscriber RENAME TO idx_messages_subscriber;
ALTER INDEX idx_workflow_messages_created_at RENAME TO idx_messages_created_at;
INSERT INTO workflows (id, name) VALUES ('wf-1', 'post_liked');
INSERT INTO subscribers (id) VALUES ('creator-1');
INSERT INTO messages (id, workflow_id, subscriber_id, channel)
VALUES ('like-1', 'wf-1', 'creator-1', 'in-app');
INSERT INTO activity_log (trigger_id, workflow, subscriber_id)
VALUES ('00000000-0000-0000-0000-000000000001', 'post_liked', 'creator-1');
INSERT INTO execution_details (trigger_id, channel)
VALUES ('00000000-0000-0000-0000-000000000001', 'in-app');
SQL
"$scripts_dir/migrate_postgres.sh" >/dev/null
"$scripts_dir/migrate_postgres.sh" >/dev/null
psql -X -v ON_ERROR_STOP=1 -d legacy <<'SQL'
INSERT INTO workflows (id, name, tenant, environment) VALUES
    ('wf-2', 'post_liked', 'creators', 'development'),
    ('wf-3', 'post_liked', 'default', 'production');
INSERT INTO messages (trigger_id, transaction_id, workflow, subscriber_id)
VALUES ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002',
        'post_liked', 'creator-1');
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM workflow_messages WHERE id = 'like-1' AND workflow_id = 'wf-1')
       OR NOT EXISTS (SELECT 1 FROM workflows WHERE id = 'wf-1' AND tenant = 'default')
       OR NOT EXISTS (SELECT 1 FROM activity_log WHERE tenant = 'default' AND workflow = 'post_liked')
       OR NOT EXISTS (
           SELECT 1 FROM execution_details
           WHERE tenant = 'default' AND environment = 'development' AND channel = 'in-app'
       ) THEN
        RAISE EXCEPTION 'Existing interaction records were not preserved';
    END IF;
    BEGIN
        INSERT INTO workflows (id, name) VALUES ('wf-4', 'post_liked');
        RAISE EXCEPTION 'Duplicate workflow name accepted within the same scope';
    EXCEPTION WHEN unique_violation THEN
        NULL;
    END;
END
$$;
SQL
