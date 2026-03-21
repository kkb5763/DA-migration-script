#!/bin/bash
# Usage: ./run_tibero_to_mysql.sh json/tibero_to_mysql.json

CONF_FILE=$1
source ~/.bash_profile # Tibero 환경변수(TB_HOME 등) 로드 필수

SRC_USER=$(jq -r '.source_tibero.user' $CONF_FILE)
SRC_PASS=$(jq -r '.source_tibero.pass' $CONF_FILE)
SRC_SID=$(jq -r '.source_tibero.sid' $CONF_FILE)

TGT_HOST=$(jq -r '.target_mysql.host' $CONF_FILE)
TGT_USER=$(jq -r '.target_mysql.user' $CONF_FILE)
export MYSQL_PWD=$(jq -r '.target_mysql.pass' $CONF_FILE)

jq -c '.jobs[]' $CONF_FILE | while read job; do
    S_SCHEMA=$(echo $job | jq -r '.source_schema')
    T_SCHEMA=$(echo $job | jq -r '.target_schema')
    
    for tbl in $(echo $job | jq -r '.tables[]'); do
        echo "▶️ [Tibero->MySQL] $S_SCHEMA.$tbl ➔ $T_SCHEMA.$tbl"
        # 1. Tibero에서 데이터 추출 (CSV 형태 샘플)
        # 실제 운영시는 tbexport 옵션을 정교하게 조정해야 함
        tbexport user=$SRC_USER/$SRC_PASS sid=$SRC_SID table=$S_SCHEMA.$tbl file="./dump/${tbl}.dat" script=no
        
        # 2. (생략가능) 데이터 타입 변환 로직 로드...
        
        # 3. MySQL 적재
        mysql -h $TGT_HOST -u $TGT_USER $T_SCHEMA -e "LOAD DATA INFILE './dump/${tbl}.dat' INTO TABLE $tbl ..."
    done
done
unset MYSQL_PWD