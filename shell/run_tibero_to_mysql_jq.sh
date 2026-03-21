#!/bin/bash
# Usage: ./run_tibero_to_mysql_jq.sh json/tibero2mysql_info.json

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

source ~/.bash_profile

SRC_USER=$(jq -r '.source_tibero.user' "$CONF_FILE")
SRC_PASS=$(jq -r '.source_tibero.pass' "$CONF_FILE")
SRC_SID=$(jq -r '.source_tibero.sid' "$CONF_FILE")

TGT_HOST=$(jq -r '.target_mysql.host' "$CONF_FILE")
TGT_PORT=$(jq -r '.target_mysql.port // 3306' "$CONF_FILE")
TGT_USER=$(jq -r '.target_mysql.user' "$CONF_FILE")
TGT_PASS=$(jq -r '.target_mysql.pass' "$CONF_FILE")

jq -c '.jobs[]' "$CONF_FILE" | while read -r job; do
  S_SCHEMA=$(echo "$job" | jq -r '.source_schema')
  T_SCHEMA=$(echo "$job" | jq -r '.target_schema')
  mkdir -p "./dump/$S_SCHEMA"

  for tbl in $(echo "$job" | jq -r '.tables[]'); do
    echo "[Tibero->MySQL] $S_SCHEMA.$tbl -> $T_SCHEMA.$tbl"
    DUMP_FILE="./dump/$S_SCHEMA/${tbl}.dat"
    tbexport user="$SRC_USER/$SRC_PASS" sid="$SRC_SID" table="$S_SCHEMA.$tbl" file="$DUMP_FILE" script=no
    MYSQL_PWD="$TGT_PASS" mysql -h "$TGT_HOST" -P "$TGT_PORT" -u "$TGT_USER" "$T_SCHEMA" -e "LOAD DATA INFILE '$DUMP_FILE' INTO TABLE ${tbl,,} FIELDS TERMINATED BY ',';"
  done
done
