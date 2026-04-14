import MySQLdb
import MySQLdb.cursors
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict

# ==========================================================
# 1. 환경 설정 (Source & Target 접속 정보)
# ==========================================================
SRC_MYSQL_CONFIG = {
    "host": "10.10.1.10", 
    "user": "root", 
    "passwd": "src_password123!", 
    "db": "source_db",
    "charset": "utf8mb4"
}

TGT_MYSQL_CONFIG = {
    "host": "10.10.1.20", 
    "user": "root", 
    "passwd": "tgt_password456@", 
    "db": "target_db",
    "charset": "utf8mb4"
}

# ==========================================================
# 2. 이관 대상 테이블 리스트 및 범위 설정
# ==========================================================
TABLE_CONFIG: Dict[str, tuple] = {
    "user_master": ("user_id", 1, 100000),             # ID 범위
    "order_history": ("order_dt", "2026-01-01", None), # 특정 날짜 이후
    "system_log": ("reg_dt", "{{ ds }}", "{{ next_ds }}"), # 당일치 데이터
    "code_definition": ("code", None, None),           # 전체 이관
}

# ==========================================================
# 3. 이관 실행 핵심 함수
# ==========================================================
def _mysql_to_mysql_etl(table: str, filter_col: str, start_val: Any, end_val: Any, **context):
    src_conn, tgt_conn = None, None
    try:
        # DB 연결
        src_conn = MySQLdb.connect(**SRC_MYSQL_CONFIG)
        tgt_conn = MySQLdb.connect(**TGT_MYSQL_CONFIG)
        
        # Source는 대용량 조회를 위해 SScursor(Server-side cursor) 사용 권장
        src_cur = src_conn.cursor(MySQLdb.cursors.SSCursor)
        tgt_cur = tgt_conn.cursor()

        # 쿼리 생성
        query = f"SELECT * FROM {table}"
        conditions = []

        if start_val:
            v_start = f"'{start_val}'" if isinstance(start_val, str) else start_val
            conditions.append(f"{filter_col} >= {v_start}")
        
        if end_val:
            v_end = f"'{end_val}'" if isinstance(end_val, str) else end_val
            conditions.append(f"{filter_col} <= {v_end}")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        print(f"[{table}] 소스 쿼리 실행: {query}")

        # 데이터 추출 및 적재
        src_cur.execute(query)
        
        # 컬럼 수 파악 및 INSERT 구문 생성
        col_count = len(src_cur.description)
        placeholders = ", ".join(["%s"] * col_count)
        insert_sql = f"REPLACE INTO {table} VALUES ({placeholders})"

        total_rows = 0
        while True:
            # fetchmany를 통해 메모리 관리 (2TB 대응)
            rows = src_cur.fetchmany(5000) 
            if not rows:
                break
            
            tgt_cur.executemany(insert_sql, rows)
            tgt_conn.commit()
            total_rows += len(rows)
            print(f" >>> {table}: {total_rows} rows migrated...", flush=True)

    except Exception as e:
        print(f"!!! [{table}] 이관 중 오류 발생: {str(e)}")
        raise e
    finally:
        if src_conn: src_conn.close()
        if tgt_conn: tgt_conn.close()

# ==========================================================
# 4. DAG 정의
# ==========================================================
with DAG(
    dag_id="db_migration_mysql_to_mysql_v1",
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)},
    tags=['migration', 'mysql']
) as dag:

    for table_name, (col, start, end) in TABLE_CONFIG.items():
        PythonOperator(
            task_id=f"migrate_{table_name}",
            python_callable=_mysql_to_mysql_etl,
            op_kwargs={
                "table": table_name,
                "filter_col": col,
                "start_val": start,
                "end_val": end
            }
        )