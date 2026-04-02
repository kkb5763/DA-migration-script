"""
MongoDB connection check (simple).

- No JSON
- No Python DB driver
- Uses mongosh (or mongo) to run ping
"""

import os
import subprocess
import sys
from urllib.parse import quote_plus


MONGODB_ENDPOINTS = [
    {
        "label": "mongodb_source",
        "host": os.environ.get("MONGO_SRC_HOST", "10.10.5.10"),
        "port": int(os.environ.get("MONGO_SRC_PORT", "27017")),
        "user": os.environ.get("MONGO_SRC_USER", "mongo_admin"),
        "pass": os.environ.get("MONGO_SRC_PASS", "src_mongo_pwd"),
        "auth_db": os.environ.get("MONGO_SRC_AUTH_DB", "admin"),
    },
    {
        "label": "mongodb_target",
        "host": os.environ.get("MONGO_TGT_HOST", "10.10.5.20"),
        "port": int(os.environ.get("MONGO_TGT_PORT", "27017")),
        "user": os.environ.get("MONGO_TGT_USER", "mongo_admin"),
        "pass": os.environ.get("MONGO_TGT_PASS", "tgt_mongo_pwd"),
        "auth_db": os.environ.get("MONGO_TGT_AUTH_DB", "admin"),
    },
]


def build_mongo_uri(ep: dict) -> str:
    host = ep["host"]
    port = int(ep.get("port", 27017))
    user = quote_plus(str(ep["user"]))
    password = quote_plus(str(ep["pass"]))
    auth_db = quote_plus(str(ep.get("auth_db", "admin")))
    return f"mongodb://{user}:{password}@{host}:{port}/?authSource={auth_db}"


def run_ping(ep: dict) -> str:
    uri = build_mongo_uri(ep)
    last_err = None

    for binary in ("mongosh", "mongo"):
        try:
            proc = subprocess.run(
                [binary, uri, "--quiet", "--eval", "db.runCommand({ ping: 1 })"],
                check=False,
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as e:
            last_err = e
            continue

        if proc.returncode != 0:
            last_err = RuntimeError((proc.stderr or proc.stdout or "").strip())
            continue

        return (proc.stdout or "").strip()

    raise RuntimeError(f"mongosh/mongo로 ping 실패: {last_err}")


def main() -> int:
    failed = False
    for ep in MONGODB_ENDPOINTS:
        try:
            out = run_ping(ep)
            print(f"[OK]   MongoDB {ep['label']}: {ep['host']}:{ep['port']} -> {out}")
        except Exception as e:
            failed = True
            print(f"[FAIL] MongoDB {ep['label']}: {ep['host']}:{ep['port']} ({type(e).__name__}: {e})")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

