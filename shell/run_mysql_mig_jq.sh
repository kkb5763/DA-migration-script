#!/bin/bash
# Usage: ./run_mysql_mig_jq.sh json/mysql2mysql_info.json

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
SRC_PORT=$(jq -r '.source.port // 3306' "$CONF_FILE")
SRC_USER=$(jq -r '.source.user' "$CONF_FILE")
SRC_PASS=$(jq -r '.source.pass' "$CONF_FILE")

TGT_HOST=$(jq -r '.target.host' "$CONF_FILE")
TGT_PORT=$(jq -r '.target.port // 3306' "$CONF_FILE")
TGT_USER=$(jq -r '.target.user' "$CONF_FILE")
TGT_PASS=$(jq -r '.target.pass' "$CONF_FILE")

jq -c '.jobs[]' "$CONF_FILE" | while read -r job; do
  SCHEMA=$(echo "$job" | jq -r '.schema')
  METHOD=$(echo "$job" | jq -r '.method // "mydumper"')
  TABLES=$(echo "$job" | jq -r '.tables | join(",")')

  echo "[MySQL] Processing Schema: $SCHEMA ($METHOD)"
  mkdir -p "./dump/$SCHEMA"

  if [ "$METHOD" = "mydumper" ]; then
    MYSQL_PWD="$SRC_PASS" mydumper -h "$SRC_HOST" -P "$SRC_PORT" -u "$SRC_USER" -B "$SCHEMA" -T "$TABLES" -o "./dump/$SCHEMA" --rows 100000
    MYSQL_PWD="$TGT_PASS" myloader -h "$TGT_HOST" -P "$TGT_PORT" -u "$TGT_USER" -B "$SCHEMA" -d "./dump/$SCHEMA" -o
  else
    for tbl in $(echo "$job" | jq -r '.tables[]'); do
      echo "  - Table: $tbl"
      MYSQL_PWD="$SRC_PASS" mysqldump -h "$SRC_HOST" -P "$SRC_PORT" -u "$SRC_USER" "$SCHEMA" "$tbl" | \
      MYSQL_PWD="$TGT_PASS" mysql -h "$TGT_HOST" -P "$TGT_PORT" -u "$TGT_USER" "$SCHEMA"
    done
  fi
done
