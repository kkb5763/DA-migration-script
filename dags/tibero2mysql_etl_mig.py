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

# [Path 설정]
JSON_PATH = "/data/airflow/script/json/tibero2myslq_info.json"
# Tibero JDBC 드라이버 위치 (서버 환경에 맞춰 확인 필요)
JDBC_DRIVER_PATH = "/data/airflow/lib/thr_jdbc.jar" 

def run_tibero_to_mysql_etl(**kwargs):
    # 1. JSON 설정 로드
    if not os.path.exists(JSON_PATH):
        raise FileNotFoundError(f"❌ 설정 파일이 없습니다: {JSON_PATH}")
        
    with open(JSON_PATH, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    src_info = config['source_tibero']
    tgt_info = config['target_mysql']

    # 2. Tibero 연결 (JDBC)
    # URL 구성: jdbc:tibero:thin:@{host}:{port}:{sid}
    tibero_url = f"jdbc:tibero:thin:@{src_info['host']}:{src_info['port']}:{src_info['sid']}"
    
    logger.info(f"🔗 Connecting to Tibero: {src_info['host']}")
    conn_tibero = jaydebeapi.connect(
        "com.tmax.tibero.jdbc.TbDriver",
        tibero_url,
        [src_info['user'], src_info['pass']],
        JDBC_DRIVER_PATH
    )

    # 3. MySQL 연결
    logger.info(f"🔗 Connecting to MySQL: {tgt_info['host']}")
    conn_mysql = pymysql.connect(
        host=tgt_info['host'],
        port=tgt_info['port'],
        user=tgt_info['user'],
        password=tgt_info['pass'],
        charset=tgt_info.get('charset', 'utf8mb4')
    )

    try:
        t_cur = conn_tibero.cursor()
        m_cur = conn_mysql.cursor()

        for job in config['jobs']:
            s_schema = job['source_schema']
            t_schema = job['target_schema']
            
            # 타겟 데이터베이스 선택
            m_cur.execute(f"USE {t_schema}")

            for table in job['tables']:
                logger.info(f"🚀 [ETL] {s_schema}.{table} >> {t_schema}.{table.lower()}")
                
                # 원천 데이터 추출
                t_cur.execute(f"SELECT * FROM {s_schema}.{table}")
                
                # 컬럼 정보 파악 (소문자로 변환)
                columns = [desc[0].lower() for desc in t_cur.description]
                col_names = ", ".join(columns)
                placeholders = ", ".join(["%s"] * len(columns))
                
                # Chunk 단위 이행 (메모리 효율)
                while True:
                    rows = t_cur.fetchmany(5000) # 5천건씩 분할
                    if not rows:
                        break
                    
                    # Target 적재 (REPLACE INTO로 멱등성 확보)
                    sql = f"REPLACE INTO {table.lower()} ({col_names}) VALUES ({placeholders})"
                    m_cur.executemany(sql, [tuple(row) for row in rows])
                    conn_mysql.commit()
                
                logger.info(f" ✅ {table} 이행 완료")

    finally:
        conn_tibero.close()
        conn_mysql.close()

# --- DAG 설정 ---
with DAG(
    dag_id='tibero_to_mysql_full_migration',
    start_date=datetime(2026, 3, 21),
    schedule_interval=None,
    catchup=False,
    tags=['Tibero', 'MySQL', 'ETL', 'PaaS']
) as dag:

    start = EmptyOperator(task_id='start_migration')

    execute_migration = PythonOperator(
        task_id='exec_tibero_mysql_etl',
        python_callable=run_tibero_to_mysql_etl
    )

    end = EmptyOperator(task_id='migration_completed')

    start >> execute_migration >> end