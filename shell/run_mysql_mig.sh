#!/bin/bash
# Usage: ./run_mysql_mig.sh json/mysql_migration.json

CONF_FILE=$1
if [ ! -f "$CONF_FILE" ]; then echo "❌ Config file not found!"; exit 1; fi

SRC_HOST=$(jq -r '.source.host' $CONF_FILE)
SRC_USER=$(jq -r '.source.user' $CONF_FILE)
export MYSQL_PWD=$(jq -r '.source.pass' $CONF_FILE)

TGT_HOST=$(jq -r '.target.host' $CONF_FILE)
TGT_USER=$(jq -r '.target.user' $CONF_FILE)
TGT_PASS=$(jq -r '.target.pass' $CONF_FILE)

jq -c '.jobs[]' $CONF_FILE | while read job; do
    SCHEMA=$(echo $job | jq -r '.schema')
    METHOD=$(echo $job | jq -r '.method')
    TABLES=$(echo $job | jq -r '.tables | join(",")')

    echo "▶️ [MySQL] Processing Schema: $SCHEMA ($METHOD)"
    mkdir -p "./dump/$SCHEMA"

    if [ "$METHOD" == "mydumper" ]; then
        mydumper -h $SRC_HOST -u $SRC_USER -B $SCHEMA -T $TABLES -o "./dump/$SCHEMA" --rows 100000
        MYSQL_PWD=$TGT_PASS myloader -h $TGT_HOST -u $TGT_USER -B $SCHEMA -d "./dump/$SCHEMA" -o
    else
        for tbl in $(echo $job | jq -r '.tables[]'); do
            echo "  - Table: $tbl"
            mysqldump -h $SRC_HOST -u $SRC_USER $SCHEMA $tbl | \
            MYSQL_PWD=$TGT_PASS mysql -h $TGT_HOST -u $TGT_USER $SCHEMA
        done
    fi
done
unset MYSQL_PWD