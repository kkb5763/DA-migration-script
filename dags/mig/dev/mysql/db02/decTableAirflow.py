import MySQLdb
import MySQLdb.cursors
import pytibero  # Tibero 사용 시
import jpype
import sys
from typing import List, Dict, Any

# ==========================================
# 환경 설정 (공통)
# ==========================================
JAR_PATH = "/path/to/your/decryptor.jar"
JAVA_CLASS_PATH = "com.example.crypto.Decryptor"
DECRYPT_METHOD = "decrypt"

# DB 접속 정보는 보안을 위해 Airflow Connection을 사용하는 것이 좋으나, 
# 일단 기존 소스 방식을 유지하여 구성합니다.
SRC_TIBERO = {
    "host": "10.10.1.10", "port": 8629, "user": "user", "password": "pass", "db": "TIBERO"
}
TGT_MYSQL = {
    "host": "10.10.1.20", "port": 3306, "user": "root", "passwd": "pass", "db": "member_db"
}

def init_jvm():
    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=[JAR_PATH])

def call_java_decrypt(decryptor_instance, encrypted_value: str) -> str:
    if not encrypted_value: return encrypted_value
    try:
        method = getattr(decryptor_instance, DECRYPT_METHOD)
        decrypted_val = method(encrypted_value)
        return str(decrypted_val).strip()
    except Exception as e:
        return f"ERR_{encrypted_value}"

# 핵심 수정 부분: Airflow에서 호출할 수 있도록 인자를 받는 구조로 변경
def _etl_process(table_name: str, config: Dict):
    """
    Airflow 테스크가 이 함수를 호출합니다.
    config 안에는 start_id, end_id 정보가 포함되어 내려옵니다.
    """
    src_conn = None
    tgt_conn = None
    
    init_jvm() # Worker가 실행될 때 JVM 확인
    DecryptorClass = jpype.JClass(JAVA_CLASS_PATH)
    decryptor_instance = DecryptorClass()

    try:
        # Tibero 연결
        src_conn = pytibero.connect(
            host=SRC_TIBERO["host"], port=SRC_TIBERO["port"],
            user=SRC_TIBERO["user"], password=SRC_TIBERO["password"], database=SRC_TIBERO["db"]
        )
        # MySQL 연결
        tgt_conn = MySQLdb.connect(**TGT_MYSQL)
        
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        cols_str = ", ".join(config["columns"])
        placeholders = ", ".join(["%s"] * len(config["columns"]))
        insert_sql = f"INSERT INTO {config['target_table']} ({cols_str.lower()}) VALUES ({placeholders})"

        # Airflow가 넘겨준 범위를 쿼리에 적용
        range_query = f"""
            SELECT {cols_str} FROM {table_name} 
            WHERE {config['range_col']} BETWEEN :1 AND :2
        """
        
        src_cur.execute(range_query, (config['start_id'], config['end_id']))

        processed_count = 0
        while True:
            rows = src_cur.fetchmany(1000)
            if not rows: break
            
            transformed_rows = []
            for row in rows:
                row_list = list(row)
                for col_name in config["decrypt_cols"]:
                    idx = config["columns"].index(col_name)
                    if row_list[idx]:
                        row_list[idx] = call_java_decrypt(decryptor_instance, str(row_list[idx]))
                transformed_rows.append(tuple(row_list))

            tgt_cur.executemany(insert_sql, transformed_rows)
            tgt_conn.commit()
            
            processed_count += len(transformed_rows)
            print(f"   - {table_name}: {processed_count} rows in this range migrated...")

    finally:
        if src_conn: src_conn.close()
        if tgt_conn: tgt_conn.close()