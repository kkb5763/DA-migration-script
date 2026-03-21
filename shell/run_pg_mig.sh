#!/bin/bash
# Usage: ./run_pg_mig.sh json/pg_migration.json

CONF_FILE=$1
if [ ! -f "$CONF_FILE" ]; then echo "❌ Config file not found!"; exit 1; fi

export PGPASSWORD=$(jq -r '.source.pass' $CONF_FILE)
TGT_PASS=$(jq -r '.target.pass' $CONF_FILE)
SRC_HOST=$(jq -r '.source.host' $CONF_FILE)
TGT_HOST=$(jq -r '.target.host' $CONF_FILE)
SRC_DB=$(jq -r '.source.database' $CONF_FILE)
TGT_DB=$(jq -r '.target.database' $CONF_FILE)

jq -c '.jobs[]' $CONF_FILE | while read job; do
    SCHEMA=$(echo $job | jq -r '.schema')
    echo "▶️ [PostgreSQL] Processing Schema: $SCHEMA"

    for tbl in $(echo $job | jq -r '.tables[]'); do
        echo "  - Table: $SCHEMA.$tbl"
        # --clean 옵션으로 기존 테이블 삭제 후 생성(Idempotency 확보)
        pg_dump -h $SRC_HOST -U $(jq -r '.source.user' $CONF_FILE) -d $SRC_DB \
                -t "$SCHEMA.$tbl" --clean | \
        PGPASSWORD=$TGT_PASS psql -h $TGT_HOST -U $(jq -r '.target.user' $CONF_FILE) -d $TGT_DB
    done
done
unset PGPASSWORD