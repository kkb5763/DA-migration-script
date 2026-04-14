import psycopg2
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict

# ==========================================================
# 1. 환경 설정 (Source & Target 접속 정보)
# ==========================================================
SRC_PG_CONFIG = "host=10.10.1.10 port=5432 dbname=source_db user=postgres password=src_pwd"
TGT_PG_CONFIG = "host=10.10.1.20 port=5432 dbname=target_db user=postgres password=tgt_pwd"

# ==========================================================
# 2. 이관 대상 테이블 리스트 및 범위 설정
# ==========================================================
TABLE_CONFIG: Dict[str, tuple] = {
    "user_profiles": ("user_id", 1, 50000),             # ID 범위
    "order_data": ("created_at", "2026-01-01", None),   # 특정 시점 이후
    "access_log": ("log_date", "{{ ds }}", "{{ next_ds }}"), # 당일치
    "meta_codes": (None, None, None),                   # 전체 이관
}

# ==========================================================
# 3. 이관 실행 핵심 함수
# ==========================================================
def _pgsql_to_pgsql_etl(table: str, filter_col: str, start_val: Any, end_val: Any, **context):
    src_conn, tgt_conn = None, None
    try:
        # DB 연결
        src_conn = psycopg2.connect(SRC_PG_CONFIG)
        tgt_conn = psycopg2.connect(TGT_PG_CONFIG)
        
        # Source는 대량 데이터를 스트리밍으로 읽기 위해 이름 있는 커서(Server-side cursor) 사용
        src_cur = src_conn.cursor(name=f"cursor_{table}")
        tgt_cur = tgt_conn.cursor()

        # 쿼리 생성
        query = f'SELECT * FROM "{table}"'
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

        # 데이터 적재 (execute_values를 통한 고속 벌크 인서트)
        src_cur.execute(query)
        
        # 컬럼 정보 가져오기 (UPSERT 처리를 위한 PK 파악용)
        # ※ 실제 운영 환경에서는 PK 컬럼명을 명시적으로 넣어주는 것이 좋습니다.
        total_rows = 0
        while True:
            rows = src_cur.fetchmany(5000)
            if not rows:
                break
            
            # PostgreSQL 고속 인서트 (ON CONFLICT 구문은 테이블 PK에 따라 커스텀 필요)
            # 여기서는 기본 INSERT로 작성하되 필요시 ON CONFLICT 추가
            placeholders = ",".join(["%s"] * len(src_cur.description))
            insert_sql = f'INSERT INTO "{table}" VALUES ({placeholders}) ON CONFLICT DO NOTHING'
            
            tgt_cur.executemany(insert_sql, rows)
            tgt_conn.commit()
            
            total_rows += len(rows)
            print(f" >>> {table}: {total_rows} rows migrated...", flush=True)

    except Exception as e:
        print(f"!!! [{table}] 이관 오류: {str(e)}")
        raise e
    finally:
        if src_conn: src_conn.close()
        if tgt_conn: tgt_conn.close()

# ==========================================================
# 4. DAG 정의
# ==========================================================
with DAG(
    dag_id="db_migration_pgsql_to_pgsql_v1",
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)},
    tags=['migration', 'postgres']
) as dag:

    for table_name, (col, start, end) in TABLE_CONFIG.items():
        PythonOperator(
            task_id=f"migrate_{table_name}",
            python_callable=_pgsql_to_pgsql_etl,
            op_kwargs={
                "table": table_name,
                "filter_col": col,
                "start_val": start,
                "end_val": end
            }
        )