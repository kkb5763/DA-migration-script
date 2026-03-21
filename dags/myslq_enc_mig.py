from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pymysql
import json
import os
import logging
from cryptography.fernet import Fernet

# 로깅 설정
logger = logging.getLogger("airflow.task")

# [Path 설정]
JSON_PATH = "/data/airflow/script/json/mysql2mysql_enc_info.json"

# --- [ETL 핵심 로직 함수] ---
def run_encryption_etl(**kwargs):
    # 1. JSON 설정 로드
    if not os.path.exists(JSON_PATH):
        raise FileNotFoundError(f"❌ 설정 파일 누락: {JSON_PATH}")
        
    with open(JSON_PATH, 'r') as f:
        config = json.load(f)
    
    # 암호화 키 (환경변수 권장, 없으면 임시 생성)
    enc_key = os.getenv('DB_MIG_ENC_KEY', Fernet.generate_key().decode())
    cipher = Fernet(enc_key.encode())

    source = config['source']
    target = config['target']

    for job in config['jobs']:
        schema = job['schema']
        table = job['table']
        enc_cols = job['enc_columns']
        chunk_size = job.get('chunk_size', 2000)

        logger.info(f"🚀 [ETL START] {schema}.{table} (Encryption: {enc_cols})")

        # DictCursor를 써야 컬럼명으로 접근하기 편함
        src_conn = pymysql.connect(**source, database=schema, cursorclass=pymysql.cursors.DictCursor)
        tgt_conn = pymysql.connect(**target, database=schema)

        try:
            with src_conn.cursor() as src_cur:
                # 2. 데이터 추출
                src_cur.execute(f"SELECT * FROM {table}")
                
                # 컬럼 순서 파악
                columns = [desc[0] for desc in src_cur.description]
                
                while True:
                    rows = src_cur.fetchmany(chunk_size)
                    if not rows: break

                    transformed_rows = []
                    for row in rows:
                        # 3. 데이터 변환 (암호화 대상 컬럼 처리)
                        for col in enc_cols:
                            if row[col]:
                                row[col] = cipher.encrypt(str(row[col]).encode()).decode()
                        
                        # 튜플 형태로 변환 (Insert용)
                        transformed_rows.append(tuple(row[c] for c in columns))

                    # 4. Target 적재 (REPLACE INTO로 멱등성 확보)
                    with tgt_conn.cursor() as tgt_cur:
                        placeholders = ", ".join(["%s"] * len(columns))
                        col_names = ", ".join(columns)
                        sql = f"REPLACE INTO {table} ({col_names}) VALUES ({placeholders})"
                        tgt_cur.executemany(sql, transformed_rows)
                    tgt_conn.commit()
                    logger.info(f" ✅ {len(transformed_rows)} rows migrated.")
                    
        finally:
            src_conn.close()
            tgt_conn.close()

# --- [Airflow DAG 정의] ---
with DAG(
    dag_id='mysql_enc_etl_unified_migration',
    start_date=datetime(2026, 3, 21),
    schedule_interval=None,
    catchup=False,
    tags=['MySQL', 'Encryption', 'Unified']
) as dag:

    start = EmptyOperator(task_id='start')

    # 별도의 파일 호출 없이 내부 함수 실행
    execute_etl = PythonOperator(
        task_id='run_enc_etl_process',
        python_callable=run_encryption_etl
    )

    end = EmptyOperator(task_id='end')

    start >> execute_etl >> end