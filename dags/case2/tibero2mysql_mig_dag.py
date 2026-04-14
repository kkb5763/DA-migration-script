import jaydebeapi
import MySQLdb
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Any, Dict

# ==========================================================
# 1. 환경 설정 (DB 접속 정보)
# ==========================================================
TIBERO_CONFIG = {
    "host": "10.10.4.10", "port": 8629, "sid": "tibero",
    "user": "sys", "pass": "tibero_src_pwd",
    "jdbc_jar": "/data/airflow/lib/thr_jdbc.jar",
    "driver": "com.tmax.tibero.jdbc.TbDriver"
}

MYSQL_CONFIG = {
    "host": "10.10.1.20", "user": "root", "passwd": "tgt_mysql_pass456@", "db": "member_db",
}

# ==========================================================
# 2. 이관 대상 테이블 리스트 및 범위 설정
# 구조: "테이블명": ("기준컬럼", "시작값", "종료값")
# - 시작/종료값에 None을 넣으면 해당 조건은 제외됩니다.
# - Airflow 매크로(예: {{ ds }})를 문자열로 넣으면 실행 시 자동 치환됩니다.
# ==========================================================
TABLE_CONFIG: Dict[str, tuple] = {
    "mbr_base": ("mbr_seq", 10000, 50000),             # ID 범위 이관 (Between)
    "mbr_detail": ("detail_id", 5000, None),          # 특정 ID 이후 전체 (Greater than)
    "mbr_log": ("reg_dt", "{{ ds }}", "{{ next_ds }}"), # 금일 발생 데이터만 (Daily Range)
    "mbr_payment": ("pay_id", None, None),            # 전체 이관 (Full)
}

# ==========================================================
# 3. 이관 실행 핵심 함수 (Worker)
# ==========================================================
def _etl_worker(table: str, filter_col: str, start_val: Any, end_val: Any, **context):
    """
    Tibero에서 데이터를 추출하여 MySQL로 REPLACE INTO (Upsert) 수행
    """
    t_conn, m_conn = None, None
    try:
        # DB 연결
        m_conn = MySQLdb.connect(**MYSQL_CONFIG)
        m_cur = m_conn.cursor()

        url = f"jdbc:tibero:thin:@{TIBERO_CONFIG['host']}:{TIBERO_CONFIG['port']}:{TIBERO_CONFIG['sid']}"
        t_conn = jaydebeapi.connect(
            TIBERO_CONFIG["driver"], url,
            [TIBERO_CONFIG["user"], TIBERO_CONFIG["pass"]],
            TIBERO_CONFIG["jdbc_jar"]
        )
        t_cur = t_conn.cursor()

        # 쿼리 동적 생성 (BETWEEN / GT / LT / FULL 자동 판별)
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

        print(f"[{table}] 실행 쿼리: {query}")

        # 데이터 적재 (Chunk 단위)
        t_cur.execute(query)
        col_count = len(t_cur.description)
        placeholders = ", ".join(["%s"] * col_count)
        # REPLACE INTO로 중복 데이터 발생 시 덮어쓰기 (정합성 유지)
        insert_sql = f"REPLACE INTO {table} VALUES ({placeholders})"

        total_rows = 0
        while True:
            rows = t_cur.fetchmany(2000) # 배치 크기 설정
            if not rows: break
            
            m_cur.executemany(insert_sql, rows)
            m_conn.commit()
            total_rows += len(rows)
            print(f" >>> {table}: {total_rows} rows migrated...")

    except Exception as e:
        print(f"!!! [{table}] 이관 실패: {str(e)}")
        raise e
    finally:
        if t_conn: t_conn.close()
        if m_conn: m_conn.close()

# ==========================================================
# 4. DAG 정의
# ==========================================================
default_args = {
    'owner': 'data_architect',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="db_migration_tibero_to_mysql_v1",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,      # 필요시 @daily 등으로 변경
    catchup=False,
    tags=['migration', 'tibero', 'mysql']
) as dag:

    # TABLE_CONFIG에 정의된 테이블 수만큼 태스크 동적 생성
    for table_name, (col, start, end) in TABLE_CONFIG.items():
        PythonOperator(
            task_id=f"migrate_{table_name}",
            python_callable=_etl_worker,
            op_kwargs={
                "table": table_name,
                "filter_col": col,
                "start_val": start,
                "end_val": end
            }
        )