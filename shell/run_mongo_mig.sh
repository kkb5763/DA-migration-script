#!/bin/bash
# Usage: ./run_mongo_mig.sh json/mongo2mongo_info.json

set -euo pipefail

CONF_FILE="${1:-}"
if [ -z "$CONF_FILE" ] || [ ! -f "$CONF_FILE" ]; then
  echo "Config file not found: $CONF_FILE"
  exit 1
fi

for cmd in jq mongodump mongorestore; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Required command not found: $cmd"
    exit 1
  fi
done

SRC_HOST=$(jq -r '.source.host' "$CONF_FILE")
SRC_PORT=$(jq -r '.source.port // 27017' "$CONF_FILE")
SRC_USER=$(jq -r '.source.user' "$CONF_FILE")
SRC_PASS=$(jq -r '.source.pass' "$CONF_FILE")
SRC_AUTH_DB=$(jq -r '.source.auth_db // "admin"' "$CONF_FILE")

TGT_HOST=$(jq -r '.target.host' "$CONF_FILE")
TGT_PORT=$(jq -r '.target.port // 27017' "$CONF_FILE")
TGT_USER=$(jq -r '.target.user' "$CONF_FILE")
TGT_PASS=$(jq -r '.target.pass' "$CONF_FILE")
TGT_AUTH_DB=$(jq -r '.target.auth_db // "admin"' "$CONF_FILE")

DUMP_DIR="./dump/mongo"
mkdir -p "$DUMP_DIR"

jq -c '.jobs[]' "$CONF_FILE" | while read -r job; do
  DB=$(echo "$job" | jq -r '.database')
  OPTIONS=$(echo "$job" | jq -r '.options // ""')

  for COLL in $(echo "$job" | jq -r '.collections[]'); do
    ARCHIVE_PATH="${DUMP_DIR}/${DB}_${COLL}.archive.gz"
    rm -f "$ARCHIVE_PATH"

    echo "[MongoDB] Dump: ${DB}.${COLL}"
    mongodump \
      --host "$SRC_HOST" \
      --port "$SRC_PORT" \
      --username "$SRC_USER" \
      --password "$SRC_PASS" \
      --authenticationDatabase "$SRC_AUTH_DB" \
      --db "$DB" \
      --collection "$COLL" \
      --archive="$ARCHIVE_PATH" \
      --gzip \
      $OPTIONS

    echo "[MongoDB] Restore: ${DB}.${COLL}"
    mongorestore \
      --host "$TGT_HOST" \
      --port "$TGT_PORT" \
      --username "$TGT_USER" \
      --password "$TGT_PASS" \
      --authenticationDatabase "$TGT_AUTH_DB" \
      --nsInclude "${DB}.${COLL}" \
      --archive="$ARCHIVE_PATH" \
      --gzip \
      --drop

    rm -f "$ARCHIVE_PATH"
  done
done
