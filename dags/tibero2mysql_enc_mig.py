from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pymysql
import jaydebeapi
import json
import os
import logging

# 로깅 설정
logger = logging.getLogger("airflow.task")

# [Path 설정] 이미지의 구조 반영
BASE_PATH = "/data/airflow/script"
JSON_PATH = f"{BASE_PATH}/json/tibero2mysql_enc_info.json"
JDBC_DRIVER = f"{BASE_PATH}/lib/thr_jdbc.jar" # JDBC 드라이버 경로 확인 필요

# --- [SafeDB 암호화 로직] ---
def apply_safedb_encryption(plain_text):
    """
    Diamo에서 복호화된 평문을 받아 SafeDB로 재암호화합니다.
    실제 환경의 SafeDB SDK 호출 코드로 대체하세요.
    """
    if not plain_text:
        return plain_text

    # 예시: safedb_lib.encrypt(plain_text)
    return f"SafeDB_{plain_text}"

def run_tibero_to_mysql_enc_etl(**kwargs):
    # 1. JSON 설정 로드
    if not os.path.exists(JSON_PATH):
        raise FileNotFoundError(f"❌ 설정 파일이 없습니다: {JSON_PATH}")

    with open(JSON_PATH, 'r', encoding='utf-8') as f:
        config = json.load(f)

    src = config['source_tibero']
    tgt = config['target_mysql']

    # 2. Tibero(JDBC) & MySQL 커넥션 생성
    tibero_url = f"jdbc:tibero:thin:@{src['host']}:{src['port']}:{src['sid']}"
    conn_tibero = jaydebeapi.connect(
        "com.tmax.tibero.jdbc.TbDriver",
        tibero_url,
        [src['user'], src['pass']],
        JDBC_DRIVER
    )
    conn_mysql = pymysql.connect(
        host=tgt['host'],
        port=tgt['port'],
        user=tgt['user'],
        password=tgt['pass'],
        charset=tgt.get('charset', 'utf8mb4')
    )

    try:
        t_cur = conn_tibero.cursor()
        m_cur = conn_mysql.cursor()

        for job in config['jobs']:
            s_schema = job['source_schema']
            t_schema = job['target_schema']
            # 암호화 설정 (테이블별 컬럼 리스트)
            enc_config = job.get('enc_columns', {})

            for table in job['tables']:
                logger.info(f"🚀 [ENC ETL] {s_schema}.{table} -> {t_schema}.{table.lower()}")

                # 원천 데이터 추출 (Diamo 복호화 상태 가정)
                t_cur.execute(f"SELECT * FROM {s_schema}.{table}")

                # 컬럼 정보 및 인덱스 파악
                columns = [desc[0] for desc in t_cur.description]
                target_cols = [c.lower() for c in columns]

                # 암호화 적용할 컬럼 인덱스 추출
                enc_target_cols = enc_config.get(table, [])
                enc_idx = [columns.index(c) for c in enc_target_cols if c in columns]

                while True:
                    rows = t_cur.fetchmany(2000)
                    if not rows:
                        break

                    processed_rows = []
                    for row in rows:
                        row_list = list(row)
                        # SafeDB 암호화 적용
                        for idx in enc_idx:
                            row_list[idx] = apply_safedb_encryption(row_list[idx])
                        processed_rows.append(tuple(row_list))

                    # 3. MySQL 적재 (REPLACE INTO)
                    col_names = ", ".join(target_cols)
                    placeholders = ", ".join(["%s"] * len(target_cols))
                    sql = f"REPLACE INTO {t_schema}.{table.lower()} ({col_names}) VALUES ({placeholders})"

                    m_cur.executemany(sql, processed_rows)
                    conn_mysql.commit()

                logger.info(f" ✅ {table} (SafeDB 적용) 완료")

    finally:
        conn_tibero.close()
        conn_mysql.close()

# --- [Airflow DAG 정의] ---
with DAG(
    dag_id='tibero2mysql_enc_etl_mig', # 파일명과 통일
    start_date=datetime(2026, 3, 21),
    schedule_interval=None,
    catchup=False,
    tags=['Tibero', 'SafeDB', 'ETL']
) as dag:

    start = EmptyOperator(task_id='start')

    execute_etl = PythonOperator(
        task_id='exec_tibero2mysql_enc_etl',
        python_callable=run_tibero_to_mysql_enc_etl
    )

    end = EmptyOperator(task_id='end')

    start >> execute_etl >> end
