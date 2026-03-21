from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import subprocess
import json
import os
import logging

# Airflow Task 로그 활성화
logger = logging.getLogger("airflow.task")

# [Path 설정] 
BASE_PATH = "/data/airflow/script"
JSON_PATH = f"{BASE_PATH}/json/pg2pgSql_info.json"

def run_pg_dump_restore(**kwargs):
    """
    pg_dump | psql 파이프라인을 이용해 중간 파일 없이 직접 이행
    """
    # 1. JSON 설정 파일 체크
    if not os.path.exists(JSON_PATH):
        raise FileNotFoundError(f" 설정 파일을 찾을 수 없습니다: {JSON_PATH}")
        
    with open(JSON_PATH, 'r') as f:
        config = json.load(f)
    
    source = config['source']
    target = config['target']
    
    # 2. PostgreSQL 패스워드 환경변수 설정 (PGPASSWORD)
    # 소스 접속용과 타겟 접속용을 분리하여 관리
    src_env = os.environ.copy()
    src_env["PGPASSWORD"] = source['pass']
    
    tgt_env = os.environ.copy()
    tgt_env["PGPASSWORD"] = target['pass']

    # 3. Job 루프 수행
    for job in config['jobs']:
        schema = job['schema']
        
        for table in job['tables']:
            logger.info(f"🚀 [MIGRATION START] {schema}.{table} (Source -> Target)")
            
            # pg_dump 커맨드 (Source로부터 추출)
            # --clean: 기존 테이블 삭제 후 생성 / --if-exists: 삭제 시 에러 방지
            dump_cmd = (
                f"pg_dump -h {source['host']} -p {source.get('port', 5432)} "
                f"-U {source['user']} -d {source['database']} "
                f"-t {schema}.{table} --clean --if-exists"
            )
            
            # psql 커맨드 (Target으로 적재)
            restore_cmd = (
                f"psql -h {target['host']} -p {target.get('port', 5432)} "
                f"-U {target['user']} -d {target['database']}"
            )
            
            # 파이프라인 실행: pg_dump | psql
            full_cmd = f"{dump_cmd} | {restore_cmd}"
            
            try:
                # shell=True를 사용하여 파이프라인(|) 처리
                # 타겟 패스워드 환경변수를 덮어씌워 실행
                subprocess.run(
                    full_cmd, 
                    env={**src_env, "PGPASSWORD": target['pass']}, 
                    check=True, 
                    shell=True,
                    capture_output=True, 
                    text=True
                )
                logger.info(f" 이행 완료: {schema}.{table}")
            except subprocess.CalledProcessError as e:
                logger.error(f" 이행 실패 ({schema}.{table}): {e.stderr}")
                raise Exception(f"PG Migration Error")

with DAG(
    dag_id='pg_simple_dump_migration',
    start_date=datetime(2026, 3, 21),
    schedule_interval=None,
    catchup=False,
    tags=['PostgreSQL', 'DUMP', 'Simple']
) as dag:

    start = EmptyOperator(task_id='start')

    run_pg_mig = PythonOperator(
        task_id='pg_dump_restore_process',
        python_callable=run_pg_dump_restore
    )

    end = EmptyOperator(task_id='end')

    start >> run_pg_mig >> end