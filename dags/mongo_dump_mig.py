from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import subprocess
import json
import os
import logging
import shlex

logger = logging.getLogger("airflow.task")

BASE_PATH = "/data/airflow/script"
JSON_PATH = f"{BASE_PATH}/json/mongo2mongo_info.json"
DUMP_ROOT = f"{BASE_PATH}/dump/mongo"


def run_mongo_dump_restore(**kwargs):
    if not os.path.exists(JSON_PATH):
        raise FileNotFoundError(f"설정 파일을 찾을 수 없습니다: {JSON_PATH}")

    with open(JSON_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)

    source = config["source"]
    target = config["target"]

    os.makedirs(DUMP_ROOT, exist_ok=True)

    for job in config["jobs"]:
        database = job["database"]
        collections = job.get("collections", [])
        extra_options = shlex.split(job.get("options", ""))

        for collection in collections:
            archive_path = f"{DUMP_ROOT}/{database}_{collection}.archive.gz"
            if os.path.exists(archive_path):
                os.remove(archive_path)

            logger.info(f"[MONGO EXPORT] {database}.{collection}")
            dump_cmd = [
                "mongodump",
                "--host",
                source["host"],
                "--port",
                str(source.get("port", 27017)),
                "--username",
                source["user"],
                "--password",
                source["pass"],
                "--authenticationDatabase",
                source.get("auth_db", "admin"),
                "--db",
                database,
                "--collection",
                collection,
                "--archive",
                archive_path,
                "--gzip",
            ] + extra_options

            try:
                subprocess.run(dump_cmd, check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as e:
                logger.error(f"mongodump 실패 ({database}.{collection}): {e.stderr}")
                raise Exception("Mongo dump error")

            logger.info(f"[MONGO IMPORT] {database}.{collection}")
            restore_cmd = [
                "mongorestore",
                "--host",
                target["host"],
                "--port",
                str(target.get("port", 27017)),
                "--username",
                target["user"],
                "--password",
                target["pass"],
                "--authenticationDatabase",
                target.get("auth_db", "admin"),
                "--nsInclude",
                f"{database}.{collection}",
                "--archive",
                archive_path,
                "--gzip",
                "--drop",
            ]

            try:
                subprocess.run(restore_cmd, check=True, capture_output=True, text=True)
                logger.info(f"이행 완료: {database}.{collection}")
            except subprocess.CalledProcessError as e:
                logger.error(f"mongorestore 실패 ({database}.{collection}): {e.stderr}")
                raise Exception("Mongo restore error")
            finally:
                if os.path.exists(archive_path):
                    os.remove(archive_path)


with DAG(
    dag_id="mongo_simple_dump_migration",
    start_date=datetime(2026, 3, 21),
    schedule_interval=None,
    catchup=False,
    tags=["MongoDB", "DUMP", "Simple"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_mongo_mig = PythonOperator(
        task_id="mongo_dump_restore_process",
        python_callable=run_mongo_dump_restore,
    )

    end = EmptyOperator(task_id="end")

    start >> run_mongo_mig >> end
