#!/bin/bash
# Usage: ./run_pg_mig_jq.sh json/pg2pgSql_info.json

set -euo pipefail

CONF_FILE="${1:-}"
if [ -z "$CONF_FILE" ] || [ ! -f "$CONF_FILE" ]; then
  echo "Config file not found: $CONF_FILE"
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "Required command not found: jq"
  exit 1
fi

SRC_HOST=$(jq -r '.source.host' "$CONF_FILE")
SRC_PORT=$(jq -r '.source.port // 5432' "$CONF_FILE")
SRC_USER=$(jq -r '.source.user' "$CONF_FILE")
SRC_PASS=$(jq -r '.source.pass' "$CONF_FILE")
SRC_DB=$(jq -r '.source.database' "$CONF_FILE")

TGT_HOST=$(jq -r '.target.host' "$CONF_FILE")
TGT_PORT=$(jq -r '.target.port // 5432' "$CONF_FILE")
TGT_USER=$(jq -r '.target.user' "$CONF_FILE")
TGT_PASS=$(jq -r '.target.pass' "$CONF_FILE")
TGT_DB=$(jq -r '.target.database' "$CONF_FILE")

jq -c '.jobs[]' "$CONF_FILE" | while read -r job; do
  SCHEMA=$(echo "$job" | jq -r '.schema')
  echo "[PostgreSQL] Processing Schema: $SCHEMA"

  for tbl in $(echo "$job" | jq -r '.tables[]'); do
    echo "  - Table: $SCHEMA.$tbl"
    PGPASSWORD="$SRC_PASS" pg_dump -h "$SRC_HOST" -p "$SRC_PORT" -U "$SRC_USER" -d "$SRC_DB" -t "$SCHEMA.$tbl" --clean --if-exists | \
    PGPASSWORD="$TGT_PASS" psql -h "$TGT_HOST" -p "$TGT_PORT" -U "$TGT_USER" -d "$TGT_DB"
  done
done
