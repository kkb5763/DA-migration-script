import os
import sys
import jaydebeapi
import MySQLdb
from typing import Dict, Optional, Any

# ===== 1. 접속 정보 (동일) =====
TIBERO_CONFIG = {
    "host": "10.10.4.10", "port": 8629, "sid": "tibero",
    "user": "sys", "pass": "tibero_src_pwd",
    "jdbc_jar": "/data/airflow/lib/thr_jdbc.jar",
    "driver": "com.tmax.tibero.jdbc.TbDriver"
}

MYSQL_CONFIG = {
    "host": "10.10.1.20", "user": "root", "passwd": "tgt_mysql_pass456@", "db": "member_db",
}

# ===== 2. 수동 이관 설정 (여기서 직접 세팅) =====
# { "테이블명": ("기준컬럼", "시작값") }
# - 시작값이 None이면 해당 테이블은 전체 이관(Full)을 수행합니다.
# - 시작값이 있으면 "기준컬럼 > 시작값" 조건으로 증분 이관을 수행합니다.
TABLE_SPECIFIC_SETTINGS: Dict[str, tuple] = {
    "mbr_base": ("mbr_seq", 10000),      # mbr_seq가 10000보다 큰 데이터부터 가져옴
    "mbr_detail": ("detail_id", None),   # None이므로 전체 데이터 가져옴 (선이관)
    "mbr_log": ("reg_dt", "2026-04-01"), # 날짜 기준 수동 세팅 예시
}

CHUNK_SIZE = 1000

def _etl_tibero_to_mysql_manual(table: str, filter_col: str, start_val: Any) -> None:
    t_conn, m_conn = None, None
    try:
        # MySQL 연결
        m_conn = MySQLdb.connect(**MYSQL_CONFIG)
        m_cur = m_conn.cursor()

        # Tibero 연결 (JDBC)
        url = f"jdbc:tibero:thin:@{TIBERO_CONFIG['host']}:{TIBERO_CONFIG['port']}:{TIBERO_CONFIG['sid']}"
        t_conn = jaydebeapi.connect(
            TIBERO_CONFIG["driver"], url,
            [TIBERO_CONFIG["user"], TIBERO_CONFIG["pass"]],
            TIBERO_CONFIG["jdbc_jar"]
        )
        t_cur = t_conn.cursor()

        # [핵심] 수동 설정값에 따른 쿼리 생성
        query = f"SELECT * FROM {table}"
        
        if start_val is not None:
            # 값이 문자열(날짜 등)일 경우를 대비해 따옴표 처리 로직 포함
            formatted_val = f"'{start_val}'" if isinstance(start_val, str) else start_val
            query += f" WHERE {filter_col} > {formatted_val}"
            print(f"   >>> [MANUAL MODE] {table} 이관 시작점: {filter_col} > {start_val}")
        else:
            print(f"   >>> [FULL MODE] {table} 전체 이관 진행")

        t_cur.execute(query)
        
        # INSERT 준비 및 실행 (기존과 동일)
        col_count = len(t_cur.description)
        placeholders = ", ".join(["%s"] * col_count)
        insert_sql = f"REPLACE INTO {table} VALUES ({placeholders})"

        processed_count = 0
        while True:
            rows = t_cur.fetchmany(CHUNK_SIZE)
            if not rows: break
            
            m_cur.executemany(insert_sql, rows)
            m_conn.commit()
            
            processed_count += len(rows)
            print(f"   - {table}: {processed_count} rows migrated...", flush=True)

    finally:
        if t_conn: t_conn.close()
        if m_conn: m_conn.close()

def main():
    for table, (col, val) in TABLE_SPECIFIC_SETTINGS.items():
        try:
            print(f"\n[작업 시작] {table}")
            _etl_tibero_to_mysql_manual(table, col, val)
            print(f"[작업 완료] {table}")
        except Exception as e:
            print(f"[작업 실패] {table}: {e}")

if __name__ == "__main__":
    main()