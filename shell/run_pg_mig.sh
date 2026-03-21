#!/bin/bash
# Usage: ./run_pg_mig.sh json/pg2pgSql_info.json

set -euo pipefail

CONF_FILE="${1:-}"
if [ -z "$CONF_FILE" ] || [ ! -f "$CONF_FILE" ]; then
  echo "Config file not found: $CONF_FILE"
  exit 1
fi

python - "$CONF_FILE" <<'PY'
import json
import os
import subprocess
import sys

conf_path = sys.argv[1]
with open(conf_path, "r", encoding="utf-8") as f:
    cfg = json.load(f)

source = cfg["source"]
target = cfg["target"]

for job in cfg["jobs"]:
    schema = job["schema"]
    print(f"[PostgreSQL] Processing Schema: {schema}")
    for table in job.get("tables", []):
        print(f"  - Table: {schema}.{table}")
        src_env = os.environ.copy()
        src_env["PGPASSWORD"] = source["pass"]
        tgt_env = os.environ.copy()
        tgt_env["PGPASSWORD"] = target["pass"]

        dump_cmd = [
            "pg_dump", "-h", source["host"], "-p", str(source.get("port", 5432)),
            "-U", source["user"], "-d", source["database"], "-t", f"{schema}.{table}",
            "--clean", "--if-exists"
        ]
        restore_cmd = [
            "psql", "-h", target["host"], "-p", str(target.get("port", 5432)),
            "-U", target["user"], "-d", target["database"]
        ]

        p1 = subprocess.Popen(dump_cmd, env=src_env, stdout=subprocess.PIPE)
        p2 = subprocess.Popen(restore_cmd, env=tgt_env, stdin=p1.stdout)
        p1.stdout.close()
        rc2 = p2.wait()
        rc1 = p1.wait()
        if rc1 != 0 or rc2 != 0:
            raise RuntimeError(f"pg_dump/psql failed: {schema}.{table}")
PY