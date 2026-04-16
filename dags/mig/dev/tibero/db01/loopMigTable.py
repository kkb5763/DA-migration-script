import MySQLdb
import MySQLdb.cursors
import pytibero
import sys
from datetime import datetime
from typing import Any, Dict

# ==========================================================
# 1. 환경 설정부
# ==========================================================

# Source DB (Tibero) 설정
SRC_TIBERO_CONFIG = {
    "host": "10.10.1.10",
    "port": 8629,
    "user": "tibero_user",
    "password": "tibero_password",
    "database": "TIBERO_DB_NAME"
}

# Target DB (MySQL) 설정
TGT_MYSQL_CONFIG = {
    "host": "10.10.1.20", "port": 3306, "user": "root", "passwd": "tgt_password456@", "db": "target_db"
}

# [핵심] 한 번의 쿼리로 가져올 범위 크기
STEP_SIZE = 100000 

# 마이그레이션 대상 설정
TABLE_CONFIG: Dict[str, tuple] = {
    "MBR_BASE_TB": ("MBR_NO", 1, 10000000, "mbr_base"), # 티베로테이블, 기준컬럼, 시작, 종료, 타겟테이블
}

# ==========================================================
# 2. 루프 분할 ETL 로직 (Tibero -> MySQL)
# ==========================================================

def run_tibero_to_mysql_etl(tib_table: str, filter_col: str, start_val: int, end_val: int, target_table: str):
    src_conn, tgt_conn = None, None
    
    try:
        # 1. DB 연결
        src_conn = pytibero.connect(**SRC_TIBERO_CONFIG)
        tgt_conn = MySQLdb.connect(**TGT_MYSQL_CONFIG)
        
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        current_start = start_val
        total_migrated = 0

        print(f"\n[START] {tib_table} -> {target_table} (범위: {start_val} ~ {end_val})")

        # 루프 분할 시작
        while current_start <= end_val:
            current_end = min(current_start + STEP_SIZE - 1, end_val)
            
            # Tibero용 추출 쿼리 (바인드 변수 :1, :2 사용)
            # SELECT * 는 컬럼 순서가 중요하므로 가급적 컬럼명을 명시하는 것을 권장합니다.
            query = f"SELECT * FROM {tib_table} WHERE {filter_col} BETWEEN :1 AND :2"
            src_cur.execute(query, (current_start, current_end))
            
            # 첫 실행 시 INSERT 문 생성
            if total_migrated == 0:
                col_count = len(src_cur.description)
                placeholders = ", ".join(["%s"] * col_count)
                # MySQL에서는 REPLACE INTO를 사용하여 중복 방지 및 재시작 안정성 확보
                insert_sql = f"REPLACE INTO {target_table} VALUES ({placeholders})"

            chunk_rows_count = 0
            while True:
                # 5,000건씩 끊어서 가져옴
                rows = src_cur.fetchmany(5000)
                if not rows:
                    break
                
                try:
                    # MySQL에 일괄 삽입
                    tgt_cur.executemany(insert_sql, rows)
                    tgt_conn.commit()
                    chunk_rows_count += len(rows)
                    total_migrated += len(rows)
                except Exception as e:
                    tgt_conn.rollback()
                    print(f"\n ! [MySQL Error] {current_start}~{current_end} 구간 적재 실패: {e}")
                    raise

            print(f" >>> [완료] {current_start} ~ {current_end} | 구간: {chunk_rows_count}건 | 누적: {total_migrated}건")
            
            # 다음 구간으로 이동
            current_start += STEP_SIZE

        print(f"[FINISH] {tib_table} 이관 완료 (총 {total_migrated}건)")

    except Exception as e:
        print(f"\n!!! [{tib_table}] 마이그레이션 중단: {str(e)}")
        raise e
    finally:
        if src_conn: src_conn.close()
        if tgt_conn: tgt_conn.close()

# ==========================================================
# 3. Main 실행부
# ==========================================================
def main():
    print(f"작업 시작 시간: {datetime.now()}")
    
    for tib_table, (col, start, end, tgt_table) in TABLE_CONFIG.items():
        if isinstance(start, int) and isinstance(end, int):
            run_tibero_to_mysql_etl(tib_table, col, start, end, tgt_table)
        else:
            print(f" ! [Skip] {tib_table}: ID 범위가 정수가 아닙니다.")
            
    print(f"\n모든 작업 종료 시간: {datetime.now()}")

if __name__ == "__main__":
    main()