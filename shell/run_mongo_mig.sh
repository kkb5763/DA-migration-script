#!/bin/bash
# Usage: ./run_mongo_mig.sh json/mongo2mongo_info.json

set -euo pipefail

CONF_FILE="${1:-}"
if [ -z "$CONF_FILE" ] || [ ! -f "$CONF_FILE" ]; then
  echo "Config file not found: $CONF_FILE"
  exit 1
fi

for cmd in python mongodump mongorestore; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Required command not found: $cmd"
    exit 1
  fi
done

python - "$CONF_FILE" <<'PY'
import json
import os
import shlex
import subprocess
import sys

conf_path = sys.argv[1]
with open(conf_path, "r", encoding="utf-8") as f:
    cfg = json.load(f)

src = cfg["source"]
tgt = cfg["target"]
dump_dir = "./dump/mongo"
os.makedirs(dump_dir, exist_ok=True)

for job in cfg["jobs"]:
    db = job["database"]
    options = shlex.split(job.get("options", ""))
    for coll in job.get("collections", []):
        archive_path = os.path.join(dump_dir, f"{db}_{coll}.archive.gz")
        if os.path.exists(archive_path):
            os.remove(archive_path)

        print(f"[MongoDB] Dump: {db}.{coll}")
        dump_cmd = [
            "mongodump",
            "--host", src["host"],
            "--port", str(src.get("port", 27017)),
            "--username", src["user"],
            "--password", src["pass"],
            "--authenticationDatabase", src.get("auth_db", "admin"),
            "--db", db,
            "--collection", coll,
            "--archive", archive_path,
            "--gzip",
        ] + options
        subprocess.run(dump_cmd, check=True)

        print(f"[MongoDB] Restore: {db}.{coll}")
        restore_cmd = [
            "mongorestore",
            "--host", tgt["host"],
            "--port", str(tgt.get("port", 27017)),
            "--username", tgt["user"],
            "--password", tgt["pass"],
            "--authenticationDatabase", tgt.get("auth_db", "admin"),
            "--nsInclude", f"{db}.{coll}",
            "--archive", archive_path,
            "--gzip",
            "--drop",
        ]
        subprocess.run(restore_cmd, check=True)

        if os.path.exists(archive_path):
            os.remove(archive_path)
PY
