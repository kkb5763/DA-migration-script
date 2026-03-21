#!/bin/bash
# Usage: ./run_mysql_mig.sh json/mysql2mysql_info.json

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
    method = job.get("method", "mydumper")
    tables = job.get("tables", [])
    dump_dir = os.path.join(".", "dump", schema)
    os.makedirs(dump_dir, exist_ok=True)

    print(f"[MySQL] Processing Schema: {schema} ({method})")
    if method == "mydumper":
        src_env = os.environ.copy()
        src_env["MYSQL_PWD"] = source["pass"]
        dump_cmd = [
            "mydumper", "-h", source["host"], "-P", str(source.get("port", 3306)),
            "-u", source["user"], "-B", schema, "-T", ",".join(tables),
            "-o", dump_dir, "--rows", "100000"
        ]
        subprocess.run(dump_cmd, env=src_env, check=True)

        tgt_env = os.environ.copy()
        tgt_env["MYSQL_PWD"] = target["pass"]
        load_cmd = [
            "myloader", "-h", target["host"], "-P", str(target.get("port", 3306)),
            "-u", target["user"], "-B", schema, "-d", dump_dir, "-o"
        ]
        subprocess.run(load_cmd, env=tgt_env, check=True)
    else:
        for table in tables:
            print(f"  - Table: {table}")
            src_env = os.environ.copy()
            src_env["MYSQL_PWD"] = source["pass"]
            tgt_env = os.environ.copy()
            tgt_env["MYSQL_PWD"] = target["pass"]

            dump_cmd = [
                "mysqldump", "-h", source["host"], "-P", str(source.get("port", 3306)),
                "-u", source["user"], schema, table
            ]
            load_cmd = [
                "mysql", "-h", target["host"], "-P", str(target.get("port", 3306)),
                "-u", target["user"], schema
            ]

            p1 = subprocess.Popen(dump_cmd, env=src_env, stdout=subprocess.PIPE)
            p2 = subprocess.Popen(load_cmd, env=tgt_env, stdin=p1.stdout)
            p1.stdout.close()
            rc2 = p2.wait()
            rc1 = p1.wait()
            if rc1 != 0 or rc2 != 0:
                raise RuntimeError(f"mysqldump/mysql failed: {schema}.{table}")
PY