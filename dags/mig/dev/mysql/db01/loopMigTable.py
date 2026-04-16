import MySQLdb
import MySQLdb.cursors
import sys
from datetime import datetime
from typing import Any, Dict

# ==========================================================
# 1. 환경 설정부
# ==========================================================
SRC_MYSQL_CONFIG = {
    "host": "10.10.1.10", "port": 3306, "user": "root", "passwd": "src_password123!", "db": "source_db"
}

TGT_MYSQL_CONFIG = {
    "host": "10.10.1.20", "port": 3306, "user": "root", "passwd": "tgt_password456@", "db": "target_db"
}

# [핵심] 한 번의 쿼리로 가져올 범위 크기 (데이터 밀도에 따라 조절)
STEP_SIZE = 100000 

TABLE_CONFIG: Dict[str, tuple] = {
    "user_master": ("user_id", 1, 10000000), # 1부터 1,000만까지 10만 단위 분할 실행
}

# ==========================================================
# 2. 루프 분할 ETL 로직
# ==========================================================
def run_partitioned_etl(table: str, filter_col: str, start_val: int, end_val: int):
    src_conn, tgt_conn = None, None
    
    try:
        src_conn = MySQLdb.connect(**SRC_MYSQL_CONFIG)
        tgt_conn = MySQLdb.connect(**TGT_MYSQL_CONFIG)
        
        # 소스 DB는 대량 조회를 위해 SSCursor 유지
        src_cur = src_conn.cursor(MySQLdb.cursors.SSCursor)
        tgt_cur = tgt_conn.cursor()

        current_start = start_val
        total_migrated = 0

        print(f"\n[START] {table} 마이그레이션 (전체 범위: {start_val} ~ {end_val})")

        # [핵심] 루프 분할 시작
        while current_start <= end_val:
            current_end = min(current_start + STEP_SIZE - 1, end_val)
            
            # 1. 현재 구간 데이터 추출 쿼리
            query = f"SELECT * FROM {table} WHERE {filter_col} BETWEEN {current_start} AND {current_end}"
            src_cur.execute(query)
            
            # 2. 첫 실행 시에만 컬럼 정보 확인 및 INSERT 구문 생성
            if total_migrated == 0:
                col_count = len(src_cur.description)
                placeholders = ", ".join(["%s"] * col_count)
                insert_sql = f"REPLACE INTO {table} VALUES ({placeholders})"

            # 3. 현재 구간 데이터 페치 및 삽입
            chunk_rows_count = 0
            while True:
                rows = src_cur.fetchmany(5000) # 내부 메모리 관리용 fetch
                if not rows:
                    break
                
                try:
                    tgt_cur.executemany(insert_sql, rows)
                    tgt_conn.commit()
                    chunk_rows_count += len(rows)
                    total_migrated += len(rows)
                except Exception as e:
                    tgt_conn.rollback()
                    print(f"\n ! [Error] 구간 {current_start}~{current_end} 적재 실패: {e}")
                    raise

            print(f" >>> [구간 완료] {current_start} ~ {current_end} | 누적 이관: {total_migrated} rows")
            
            # 다음 구간으로 이동
            current_start += STEP_SIZE

        print(f"[FINISH] {table} 전체 이관 완료 (총 {total_migrated}건)")

    except Exception as e:
        print(f"\n!!! [{table}] 마이그레이션 중단: {str(e)}")
        raise e
    finally:
        if src_conn: src_conn.close()
        if tgt_conn: tgt_conn.close()

# ==========================================================
# 3. Main 실행부
# ==========================================================
def main():
    for table_name, (col, start, end) in TABLE_CONFIG.items():
        if start is not None and end is not None and isinstance(start, int):
            run_partitioned_etl(table_name, col, start, end)
        else:
            print(f" ! [Skip] {table_name}: 루프 분할은 정수형 ID 범위가 필요합니다.")

if __name__ == "__main__":
    main()