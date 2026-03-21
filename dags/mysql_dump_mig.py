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

# [Path 설정] 사용자 요청에 따른 경로 확정
BASE_PATH = "/data/airflow/script"
JSON_PATH = f"{BASE_PATH}/json/mysql2mysql_info.json"
DUMP_ROOT = f"{BASE_PATH}/dump/mysql"

def run_mysql_dump_load(**kwargs):
    """
    JSON 설정을 읽어 mydumper(Export) -> myloader(Import)를 수행
    """
    # 1. JSON 설정 파일 체크
    if not os.path.exists(JSON_PATH):
        raise FileNotFoundError(f" 설정 파일을 찾을 수 없습니다: {JSON_PATH}")
        
    with open(JSON_PATH, 'r') as f:
        config = json.load(f)
    
    source = config['source']
    target = config['target']
    
    # 2. 패스워드 보안 환경변수 설정
    src_env = os.environ.copy()
    src_env["MYSQL_PWD"] = source['pass']
    
    tgt_env = os.environ.copy()
    tgt_env["MYSQL_PWD"] = target['pass']

    # 3. Job 루프 수행
    for job in config['jobs']:
        schema = job['schema']
        tables = ",".join(job['tables'])
        dump_path = f"{DUMP_ROOT}/{schema}"
        
        # 덤프 디렉토리 생성
        os.makedirs(dump_path, exist_ok=True)

        # --- [STEP 1] Mydumper (Export) ---
        logger.info(f" [EXPORT] Schema: {schema} / Tables: {tables}")
        
        dump_cmd = [
            "mydumper",
            "-h", source['host'],
            "-P", str(source.get('port', 3306)),
            "-u", source['user'],
            "-B", schema,
            "-T", tables,
            "-o", dump_path,
            "--rows", "100000",
            "--compress",
            "--build-empty-files"
        ]
        
        try:
            subprocess.run(dump_cmd, env=src_env, check=True, capture_output=True, text=True)
            logger.info(f" Export 완료: {schema}")
        except subprocess.CalledProcessError as e:
            logger.error(f" Export 실패: {e.stderr}")
            raise Exception(f"Mydumper Error")

        # --- [STEP 2] Myloader (Import) ---
        logger.info(f" [IMPORT] Schema: {schema} -> Target: {target['host']}")
        
        load_cmd = [
            "myloader",
            "-h", target['host'],
            "-P", str(target.get('port', 3306)),
            "-u", target['user'],
            "-d", dump_path,
            "-B", schema,
            "-o"  # Overwrite 옵션
        ]
        
        try:
            subprocess.run(load_cmd, env=tgt_env, check=True, capture_output=True, text=True)
            logger.info(f" Import 완료: {schema}")
        except subprocess.CalledProcessError as e:
            logger.error(f" Import 실패: {e.stderr}")
            raise Exception(f"Myloader Error")

with DAG(
    dag_id='mysql_bulk_dump_migration',
    start_date=datetime(2026, 3, 21),
    schedule_interval=None,
    catchup=False,
    tags=['MySQL', 'DUMP', 'FastTrack']
) as dag:

    start = EmptyOperator(task_id='start')

    run_dump_load = PythonOperator(
        task_id='mysql_dump_load_process',
        python_callable=run_mysql_dump_load
    )

    end = EmptyOperator(task_id='end')

    start >> run_dump_load >> end