#!/bin/bash
# Usage: ./run_tibero_to_mysql.sh json/tibero2mysql_info.json

set -euo pipefail

CONF_FILE="${1:-}"
if [ -z "$CONF_FILE" ] || [ ! -f "$CONF_FILE" ]; then
  echo "Config file not found: $CONF_FILE"
  exit 1
fi

source ~/.bash_profile # Tibero 환경변수(TB_HOME 등) 로드

python - "$CONF_FILE" <<'PY'
import json
import os
import subprocess
import sys

conf_path = sys.argv[1]
with open(conf_path, "r", encoding="utf-8") as f:
    cfg = json.load(f)

src = cfg["source_tibero"]
tgt = cfg["target_mysql"]
os.makedirs("./dump", exist_ok=True)

for job in cfg["jobs"]:
    s_schema = job["source_schema"]
    t_schema = job["target_schema"]
    schema_dump_dir = os.path.join("./dump", s_schema)
    os.makedirs(schema_dump_dir, exist_ok=True)

    for table in job.get("tables", []):
        dump_file = os.path.join(schema_dump_dir, f"{table}.dat")
        print(f"[Tibero->MySQL] {s_schema}.{table} -> {t_schema}.{table.lower()}")

        export_cmd = [
            "tbexport",
            f"user={src['user']}/{src['pass']}",
            f"sid={src['sid']}",
            f"table={s_schema}.{table}",
            f"file={dump_file}",
            "script=no",
        ]
        subprocess.run(export_cmd, check=True)

        mysql_env = os.environ.copy()
        mysql_env["MYSQL_PWD"] = tgt["pass"]
        load_sql = f"LOAD DATA INFILE '{dump_file}' INTO TABLE {table.lower()} FIELDS TERMINATED BY ',';"
        load_cmd = [
            "mysql",
            "-h", tgt["host"],
            "-P", str(tgt.get("port", 3306)),
            "-u", tgt["user"],
            t_schema,
            "-e", load_sql,
        ]
        subprocess.run(load_cmd, env=mysql_env, check=True)
PY