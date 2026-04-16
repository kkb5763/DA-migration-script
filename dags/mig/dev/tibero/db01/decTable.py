import MySQLdb
import MySQLdb.cursors
import pytibero
import jpype
import jpype.imports
import base64
import sys
from typing import List, Dict, Any

# ==========================================
# 1. 환경 설정부
# ==========================================

JAR_PATH = "/path/to/your/decryptor.jar"
JAVA_CLASS_PATH = "com.example.crypto.Decryptor"
DECRYPT_METHOD = "decrypt"

SRC_TIBERO = {
    "host": "10.10.1.10", "port": 8629, "user": "user", "password": "password", "db": "TIBERO"
}

TGT_MYSQL = {
    "host": "10.10.1.20", "port": 3306, "user": "root", "passwd": "password@", "db": "member_db",
}

TABLE_CONFIG = {
    "MBR_BASE_TB": {
        "target_table": "mbr_base",
        "columns": ["MBR_NO", "MBR_ID", "MBR_NM", "ENC_EMAIL"],
        "decrypt_cols": ["ENC_EMAIL"],
        "range_col": "MBR_NO", 
        "start_id": 1,
        "end_id": 50000
    }
}

# ==========================================
# 2. 초기화 및 암복호화 유틸리티 함수
# ==========================================

def init_jvm():
    """JVM 초기화"""
    if not jpype.isJVMStarted():
        try:
            jpype.startJVM(classpath=[JAR_PATH])
        except Exception as e:
            print(f"[JVM ERROR] {e}")

# [기존] Java JAR 활용 복호화
def call_java_decrypt(decryptor_instance, value: str) -> str:
    if not value: return value
    try:
        method = getattr(decryptor_instance, DECRYPT_METHOD)
        return str(method(value)).strip()
    except Exception:
        return f"ERR_JAR_{value}"

# [추가 1] Base64 디코딩 함수 (필요시 인코딩으로 변경 가능)
def handle_base64_decode(value: str) -> str:
    """Base64 디코딩 수행"""
    if not value: return value
    try:
        return base64.b64decode(value.encode('utf-8')).decode('utf-8')
    except Exception:
        return f"ERR_B64_{value}"

# [추가 2] 단순 접두어 추가 함수 (테스트/식별용)
def add_enc_prefix(value: str) -> str:
    """단순히 ENC_ 접두어를 붙임"""
    if not value: return value
    return f"ENC_{value}"

# ==========================================
# 3. 데이터 가공 (Transform) 로직 분리
# ==========================================

def transform_row(row_tuple: tuple, columns: list, decrypt_cols: list, decryptor_instance) -> tuple:
    """한 행의 데이터를 전략에 따라 가공"""
    row_list = list(row_tuple)
    
    for col_name in decrypt_cols:
        idx = columns.index(col_name)
        val = row_list[idx]
        
        if val:
            # --- 상황에 맞는 함수를 선택하여 사용하세요 ---
            # 1. JAR 복호화 사용 시
            row_list[idx] = call_java_decrypt(decryptor_instance, str(val))
            
            # 2. Base64 디코딩 사용 시 (주석 해제 후 사용)
            # row_list[idx] = handle_base64_decode(str(val))
            
            # 3. 단순 접두어만 붙일 시 (주석 해제 후 사용)
            # row_list[idx] = add_enc_prefix(str(val))
            
    return tuple(row_list)

# ==========================================
# 4. ETL 메인 로직
# ==========================================

def _etl_process(tib_table_name: str, config: Dict):
    src_conn = None
    tgt_conn = None
    
    # Java 인스턴스 (JAR 미사용 시 이 부분은 에러나도 무시 가능하도록 처리)
    decryptor_instance = None
    try:
        DecryptorClass = jpype.JClass(JAVA_CLASS_PATH)
        decryptor_instance = DecryptorClass()
    except:
        print("   ! Info: Java Decryptor Class not loaded. (JAR functions might fail)")

    try:
        src_conn = pytibero.connect(
            host=SRC_TIBERO["host"], port=SRC_TIBERO["port"],
            user=SRC_TIBERO["user"], password=SRC_TIBERO["password"], database=SRC_TIBERO["db"]
        )
        tgt_conn = MySQLdb.connect(**TGT_MYSQL)
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        cols_str = ", ".join(config["columns"])
        placeholders = ", ".join(["%s"] * len(config["columns"]))
        insert_sql = f"INSERT INTO {config['target_table']} ({cols_str.lower()}) VALUES ({placeholders})"

        range_query = f"SELECT {cols_str} FROM {tib_table_name} WHERE {config['range_col']} BETWEEN :1 AND :2"
        
        print(f"   > Extracting: {config['start_id']} ~ {config['end_id']}")
        src_cur.execute(range_query, (config['start_id'], config['end_id']))

        processed_count = 0
        while True:
            rows = src_cur.fetchmany(1000)
            if not rows: break
            
            # 데이터 가공 함수 호출
            transformed_rows = [
                transform_row(r, config["columns"], config["decrypt_cols"], decryptor_instance) 
                for r in rows
            ]

            try:
                tgt_cur.executemany(insert_sql, transformed_rows)
                tgt_conn.commit()
            except Exception as e:
                tgt_conn.rollback()
                print(f"   ! MySQL Error: {e}")
                raise
            
            processed_count += len(transformed_rows)
            sys.stdout.write(f"\r   - {processed_count} rows migrated...")
            sys.stdout.flush()

    finally:
        if src_conn: src_conn.close()
        if tgt_conn: tgt_conn.close()

def main():
    init_jvm()
    try:
        for tib_table, config in TABLE_CONFIG.items():
            print(f"\n[START] {tib_table}")
            _etl_process(tib_table, config)
            print(f"\n[FINISH] {tib_table}")
    finally:
        if jpype.isJVMStarted():
            jpype.shutdownJVM()

if __name__ == "__main__":
    main()