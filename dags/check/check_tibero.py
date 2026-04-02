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

try:
    import jpype
except Exception:  # pragma: no cover
    jpype = None


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

def _preflight(ep: dict) -> None:
    jar_path = ep.get("jdbc_jar")
    if not jar_path:
        raise RuntimeError("TIBERO_JDBC_JAR(jdbc_jar)가 비어 있습니다.")
    if not os.path.exists(jar_path):
        raise RuntimeError(f"JDBC JAR 파일이 없습니다: {jar_path}")

    if jaydebeapi is None:
        raise RuntimeError("jaydebeapi가 없습니다. (pip install jaydebeapi JPype1 필요)")

    if jpype is None:
        raise RuntimeError("JPype1(jpype)가 없습니다. (pip install JPype1 필요)")

    # JVM 경로를 못 찾는 환경이 많아서 힌트를 출력합니다.
    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        print("[WARN] JAVA_HOME이 설정되어 있지 않습니다. (JVM 로딩 실패 시 설정 필요)", flush=True)

    try:
        jvm_path = jpype.getDefaultJVMPath()
        print(f"[INFO] JVM path: {jvm_path}", flush=True)
    except Exception as e:
        print(f"[WARN] JVM path 확인 실패: {type(e).__name__}: {e}", flush=True)


def run_select_1(ep: dict) -> str:
    _preflight(ep)

    url = f"jdbc:tibero:thin:@{ep['host']}:{int(ep['port'])}:{ep['sid']}"
    try:
        conn = jaydebeapi.connect(
            JDBC_DRIVER,
            url,
            [ep["user"], ep["pass"]],
            ep["jdbc_jar"],
        )
    except Exception as e:
        raise RuntimeError(
            "JDBC connect 실패. (JAVA/JAR/DRIVER/url/네트워크 확인 필요) "
            f"driver={JDBC_DRIVER}, url={url}, jar={ep.get('jdbc_jar')}"
        ) from e
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
            print(f"[OK]   Tibero {ep['label']}: {ep['host']}:{ep['port']} -> {out}", flush=True)
        except Exception as e:
            failed = True
            print(
                f"[FAIL] Tibero {ep['label']}: {ep['host']}:{ep['port']} ({type(e).__name__}: {e})",
                flush=True,
            )

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

