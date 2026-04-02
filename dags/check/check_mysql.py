"""
MySQL connection check (simple).

- No JSON
- No Python DB driver
- Uses mysql client to run: SELECT 1;
"""

import os
import subprocess
import sys


MYSQL_ENDPOINTS = [
    {
        "label": "mysql_source",
        "host": os.environ.get("MYSQL_SRC_HOST", "10.10.1.10"),
        "port": int(os.environ.get("MYSQL_SRC_PORT", "3306")),
        "user": os.environ.get("MYSQL_SRC_USER", "root"),
        "pass": os.environ.get("MYSQL_SRC_PASS", "src_mysql_pass123!"),
    },
    {
        "label": "mysql_target",
        "host": os.environ.get("MYSQL_TGT_HOST", "10.10.1.20"),
        "port": int(os.environ.get("MYSQL_TGT_PORT", "3306")),
        "user": os.environ.get("MYSQL_TGT_USER", "root"),
        "pass": os.environ.get("MYSQL_TGT_PASS", "tgt_mysql_pass456@"),
    },
]


def run_select_1(ep: dict) -> str:
    """
    mysql CLI로 SELECT 1 수행.
    주의: mysql client가 PATH에 있어야 합니다.
    """
    cmd = [
        "mysql",
        "-h",
        ep["host"],
        "-P",
        str(int(ep.get("port", 3306))),
        "-u",
        ep["user"],
        "-N",
        "-s",
        "-e",
        "SELECT 1;",
    ]

    env = os.environ.copy()
    env["MYSQL_PWD"] = ep["pass"]

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
    for ep in MYSQL_ENDPOINTS:
        try:
            out = run_select_1(ep)
            print(f"[OK]   MySQL {ep['label']}: {ep['host']}:{ep['port']} -> {out}")
        except Exception as e:
            failed = True
            print(f"[FAIL] MySQL {ep['label']}: {ep['host']}:{ep['port']} ({type(e).__name__}: {e})")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

