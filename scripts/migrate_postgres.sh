#!/bin/sh
set -eu

migrations_dir=$(CDPATH='' cd -- "$(dirname -- "$0")/migrations" && pwd)
database_url=${XW_DATABASE_URL:?Set XW_DATABASE_URL to a PostgreSQL connection URI}
case "$database_url" in
    postgresql+psycopg://*) database_url=postgresql://${database_url#postgresql+psycopg://} ;;
esac

set -- -X --dbname="$database_url" --set=ON_ERROR_STOP=1 --single-transaction \
    --command='SELECT pg_advisory_xact_lock(784091360270310728);'
for migration in "$migrations_dir"/[0-9][0-9][0-9]_*.sql; do
    set -- "$@" --file="$migration"
done
exec psql "$@"
