from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime


BASE_PATH = "/data/airflow/script"
ELPROC_DIR = f"{BASE_PATH}/dags/elproc"


with DAG(
    dag_id="elproc_mysql2mysql_tables_mig",
    start_date=datetime(2026, 4, 2),
    schedule_interval=None,
    catchup=False,
    tags=["ELPROC", "MySQL", "MIG"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_mig = BashOperator(
        task_id="run_mysql2mysql_tables_mig",
        bash_command=f"python {ELPROC_DIR}/mysql2mysql_tables_mig.py",
    )

    end = EmptyOperator(task_id="end")

    start >> run_mig >> end

