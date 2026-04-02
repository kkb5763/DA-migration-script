from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime


BASE_PATH = "/data/airflow/script"
CHECK_DIR = f"{BASE_PATH}/dags/check"


with DAG(
    dag_id="check_connections",
    start_date=datetime(2026, 4, 2),
    schedule_interval=None,
    catchup=False,
    tags=["CHECK", "DB"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_mysql = BashOperator(
        task_id="check_mysql",
        bash_command=f"python {CHECK_DIR}/check_mysql.py",
    )

    check_postgresql = BashOperator(
        task_id="check_postgresql",
        bash_command=f"python {CHECK_DIR}/check_postgresql.py",
    )

    check_mongodb = BashOperator(
        task_id="check_mongodb",
        bash_command=f"python {CHECK_DIR}/check_mongodb.py",
    )

    check_tibero = BashOperator(
        task_id="check_tibero",
        bash_command=f"python {CHECK_DIR}/check_tibero.py",
    )

    end = EmptyOperator(task_id="end")

    start >> check_mysql >> check_postgresql >> check_mongodb >> check_tibero >> end

