"""
PostgreSQL connection check (simple).

- No JSON
- No Python DB driver
- Uses psql to run: SELECT 1;
"""

import os
import subprocess
import sys


POSTGRES_ENDPOINTS = [
    {
        "label": "postgres_source",
        "host": os.environ.get("PG_SRC_HOST", "10.10.2.10"),
        "port": int(os.environ.get("PG_SRC_PORT", "5432")),
        "user": os.environ.get("PG_SRC_USER", "postgres"),
        "pass": os.environ.get("PG_SRC_PASS", "src_pg_pass123!"),
        "database": os.environ.get("PG_SRC_DB", "postgres"),
    },
    {
        "label": "postgres_target",
        "host": os.environ.get("PG_TGT_HOST", "10.10.2.20"),
        "port": int(os.environ.get("PG_TGT_PORT", "5432")),
        "user": os.environ.get("PG_TGT_USER", "postgres"),
        "pass": os.environ.get("PG_TGT_PASS", "tgt_pg_pass456@"),
        "database": os.environ.get("PG_TGT_DB", "postgres"),
    },
]


def run_select_1(ep: dict) -> str:
    cmd = [
        "psql",
        "-h",
        ep["host"],
        "-p",
        str(int(ep.get("port", 5432))),
        "-U",
        ep["user"],
        "-d",
        ep["database"],
        "-tA",
        "-c",
        "SELECT 1;",
    ]

    env = os.environ.copy()
    env["PGPASSWORD"] = ep["pass"]

    proc = subprocess.run(
        cmd,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "").strip())

    return (proc.stdout or "").strip()


def main() -> int:
    failed = False
    for ep in POSTGRES_ENDPOINTS:
        try:
            out = run_select_1(ep)
            print(f"[OK]   PostgreSQL {ep['label']}: {ep['host']}:{ep['port']} -> {out}")
        except Exception as e:
            failed = True
            print(f"[FAIL] PostgreSQL {ep['label']}: {ep['host']}:{ep['port']} ({type(e).__name__}: {e})")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

