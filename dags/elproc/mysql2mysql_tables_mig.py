import MySQLdb  # mysqlclient 라이브러리 (기존 환경에 설치됨)
import sys
from typing import List

# ===== Connection info (접속 정보) =====
SRC = {
    "host": "10.10.1.10",
    "port": 3306,
    "user": "root",
    "passwd": "src_mysql_pass123!", # mysqlclient는 'passwd' 키 사용
    "db": "member_db",              # 연결 시 DB 지정
}

TGT = {
    "host": "10.10.1.20",
    "port": 3306,
    "user": "root",
    "passwd": "tgt_mysql_pass456@",
    "db": "member_db",
}

# 복사할 테이블 리스트
TABLES: List[str] = [
    "mbr_base",
    "mbr_detail",
]

# 한 번에 처리할 데이터 양 (메모리 조절용)
CHUNK_SIZE = 1000 

# ===== 데이터 변환/복호화 로직 (Transform) =====
def transform_data(row: tuple) -> tuple:
    """
    추출된 행(row) 데이터를 가공하는 함수입니다.
    이곳에 복호화 로직을 넣으시면 됩니다.
    """
    row_list = list(row)
    
    # 예시: 만약 3번째 컬럼(index 2)이 암호화된 컬럼이라면 복호화 적용
    # row_list[2] = decrypt_function(row_list[2]) 
    
    # 예시: 데이터 뒤에 식별자 추가 (테스트용)
    # row_list[1] = f"{row_list[1]}_migrated" 

    return tuple(row_list)

def _etl_process(table: str) -> None:
    src_conn = None
    tgt_conn = None
    
    try:
        # 1. DB 연결
        src_conn = MySQLdb.connect(**SRC)
        tgt_conn = MySQLdb.connect(**TGT)
        
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        # 2. 소스 테이블에서 데이터 추출 (Extract)
        src_cur.execute(f"SELECT * FROM {table}")
        
        # INSERT 문 자동 생성을 위한 컬럼 개수 파악
        col_count = len(src_cur.description)
        placeholders = ", ".join(["%s"] * col_count)
        insert_sql = f"INSERT INTO {table} VALUES ({placeholders})"

        processed_count = 0
        while True:
            # 3. CHUNK_SIZE 만큼 메모리로 읽기
            rows = src_cur.fetchmany(CHUNK_SIZE)
            if not rows:
                break
            
            # 4. 데이터 가공 (Transform / 복호화)
            transformed_rows = [transform_data(r) for r in rows]

            # 5. 타겟 테이블에 적재 (Load)
            try:
                tgt_cur.executemany(insert_sql, transformed_rows)
                tgt_conn.commit() # 청크 단위로 커밋
            except Exception as e:
                tgt_conn.rollback()
                raise RuntimeError(f"Insert failed at row {processed_count}: {e}")
            
            processed_count += len(transformed_rows)
            print(f"   - {table}: {processed_count} rows processed...", flush=True)

    finally:
        if src_conn: src_conn.close()
        if tgt_conn: tgt_conn.close()

def main() -> int:
    failed = False
    for table in TABLES:
        try:
            print(f"[START ETL] {table}", flush=True)
            _etl_process(table)
            print(f"[OK]        {table}", flush=True)
        except Exception as e:
            failed = True
            print(f"[FAIL]      {table} -> Error: {e}", flush=True)

    return 1 if failed else 0

if __name__ == "__main__":
    sys.exit(main())