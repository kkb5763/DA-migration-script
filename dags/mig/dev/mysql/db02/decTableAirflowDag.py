from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import math

# 위에서 작성한 마이그레이션 코드를 /opt/airflow/dags/scripts/migration_logic.py 에 저장했다고 가정합니다.
# 또는 DAG 파일 내에 직접 함수를 포함시켜도 됩니다.
from scripts.migration_logic import _etl_process, init_jvm, jpype

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'db_migration_tibero_to_mysql_partitioned',
    default_args=default_args,
    schedule_interval=None,  # 수동 실행
    catchup=False,
    tags=['migration', '2tb_project']
) as dag:

    def run_partitioned_etl(table_name, config, start_id, end_id):
        """범위 정보를 덮어씌워 ETL 실행"""
        # JVM은 각 Worker 프로세스에서 새로 시작해야 함
        if not jpype.isJVMStarted():
            init_jvm()
            
        partition_config = config.copy()
        partition_config['start_id'] = start_id
        partition_config['end_id'] = end_id
        
        _etl_process(table_name, partition_config)

    # 마이그레이션 설정 (2TB 규모를 고려하여 설정)
    TABLE_NAME = "MBR_BASE_TB"
    BASE_CONFIG = {
        "target_table": "mbr_base",
        "columns": ["MBR_NO", "MBR_ID", "MBR_NM", "ENC_EMAIL"],
        "decrypt_cols": ["ENC_EMAIL"],
        "range_col": "MBR_NO"
    }

    TOTAL_START = 1
    TOTAL_END = 20000000  # 예: 2천만 건
    CHUNK_SIZE = 100000   # 한 테스크당 10만 건씩 처리 (범위 분할)

    # 범위 계산 및 테스크 생성
    current_id = TOTAL_START
    task_idx = 1
    
    while current_id <= TOTAL_END:
        next_id = min(current_id + CHUNK_SIZE - 1, TOTAL_END)
        
        task_id = f'migrate_{TABLE_NAME.lower()}_{task_idx}'
        
        PythonOperator(
            task_id=task_id,
            python_callable=run_partitioned_etl,
            op_kwargs={
                'table_name': TABLE_NAME,
                'config': BASE_CONFIG,
                'start_id': current_id,
                'end_id': next_id
            }
        )
        
        current_id += CHUNK_SIZE
        task_idx += 1