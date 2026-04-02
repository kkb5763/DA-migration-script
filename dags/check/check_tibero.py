"""
Tibero connection check (simple).

- No JSON
- Uses JDBC (jaydebeapi) to run: SELECT 1 FROM DUAL
"""

import os
import sys

try:
    import jaydebeapi
except Exception:  # pragma: no cover
    jaydebeapi = None


TIBERO_ENDPOINTS = [
    {
        "label": "tibero_source",
        "host": os.environ.get("TIBERO_HOST", "10.10.4.10"),
        "port": int(os.environ.get("TIBERO_PORT", "8629")),
        "sid": os.environ.get("TIBERO_SID", "tibero"),
        "user": os.environ.get("TIBERO_USER", "sys"),
        "pass": os.environ.get("TIBERO_PASS", "tibero_src_pwd"),
        "jdbc_jar": os.environ.get("TIBERO_JDBC_JAR", "/data/airflow/lib/thr_jdbc.jar"),
    },
]

JDBC_DRIVER = os.environ.get("TIBERO_JDBC_DRIVER", "com.tmax.tibero.jdbc.TbDriver")

def run_select_1(ep: dict) -> str:
    if jaydebeapi is None:
        raise RuntimeError("jaydebeapi가 없습니다. (pip install jaydebeapi JPype1 필요)")

    url = f"jdbc:tibero:thin:@{ep['host']}:{int(ep['port'])}:{ep['sid']}"
    conn = jaydebeapi.connect(
        JDBC_DRIVER,
        url,
        [ep["user"], ep["pass"]],
        ep["jdbc_jar"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM DUAL")
            row = cur.fetchone()
            return str(row[0]) if row else ""
    finally:
        conn.close()


def main() -> int:
    failed = False
    for ep in TIBERO_ENDPOINTS:
        try:
            out = run_select_1(ep)
            print(f"[OK]   Tibero {ep['label']}: {ep['host']}:{ep['port']} -> {out}")
        except Exception as e:
            failed = True
            print(f"[FAIL] Tibero {ep['label']}: {ep['host']}:{ep['port']} ({type(e).__name__}: {e})")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

