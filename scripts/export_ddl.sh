#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

DB_HOST="${DB_HOST:?DB_HOST is required}"
DB_PORT="${DB_PORT:-33060}"
DB_USER="${DB_USER:-root}"
DB_PASSWORD="${DB_PASSWORD:?DB_PASSWORD is required}"
DB_NAME_PATTERN="${DB_NAME_PATTERN:-%}"
DB_NAME_PATTERN_SQL="${DB_NAME_PATTERN//\'/\'\'}"
RESTORE_ROOT="${RESTORE_ROOT:-/usr/local/src/restoredb}"
DDL_DIR="${DDL_DIR:-$RESTORE_ROOT/ddl}"
DDL_BACKUP_DIR="${DDL_BACKUP_DIR:-$RESTORE_ROOT/ddl-backup}"
DDL_TMP_DIR="${DDL_DIR}.sync-tmp.$$"

cleanup_tmp() {
  rm -rf "$DDL_TMP_DIR"
}
trap cleanup_tmp EXIT

rm -rf "$DDL_TMP_DIR"
mkdir -p "$DDL_TMP_DIR"

export MYSQL_PWD="$DB_PASSWORD"
MYSQL_ARGS=(-h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" --protocol=tcp --batch --skip-column-names)
DATABASES="$(mysql "${MYSQL_ARGS[@]}" -e "
  SELECT SCHEMA_NAME
  FROM information_schema.SCHEMATA
  WHERE SCHEMA_NAME LIKE '${DB_NAME_PATTERN_SQL}'
    AND SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
  ORDER BY SCHEMA_NAME;
")"

if [[ -z "$DATABASES" ]]; then
  echo "[$(date '+%F %T')] no databases matched pattern: $DB_NAME_PATTERN"
  exit 0
fi

echo "[$(date '+%F %T')] start ddl sync from $DB_HOST:$DB_PORT"
while IFS= read -r database; do
  [[ -z "$database" ]] && continue
  db_dir="$DDL_TMP_DIR/$database"
  mkdir -p "$db_dir"

  TABLES="$(mysql "${MYSQL_ARGS[@]}" -e "
    SELECT TABLE_NAME
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = '${database}' AND TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME;
  ")"

  while IFS= read -r table; do
    [[ -z "$table" ]] && continue
    out_file="$db_dir/$table.sql"
    mysqldump -h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" \
      --protocol=tcp \
      --no-data \
      --skip-lock-tables \
      --single-transaction \
      --routines=false \
      --events=false \
      --triggers \
      --set-gtid-purged=OFF \
      --column-statistics=0 \
      "$database" "$table" > "$out_file"
  done <<< "$TABLES"
done <<< "$DATABASES"

echo "[$(date '+%F %T')] refresh ddl backup"
rm -rf "$DDL_BACKUP_DIR"
if [[ -d "$DDL_DIR" ]]; then
  mv "$DDL_DIR" "$DDL_BACKUP_DIR"
fi

mv "$DDL_TMP_DIR" "$DDL_DIR"
trap - EXIT
echo "[$(date '+%F %T')] ddl sync done"
