import MySQLdb
import MySQLdb.cursors
import base64
import os
import sys
import jpype
import jpype.imports
from typing import List, Dict, Any

# ==========================================
# 1. 환경 설정부 (사용자 환경에 맞게 수정)
# ==========================================

# Java 관련 설정
JAR_PATH = "/path/to/your/decryptor.jar"       # JAR 파일의 절대 경로
JAVA_CLASS_PATH = "com.example.crypto.Decryptor" # 호출할 Java 클래스 (패키지명 포함)
DECRYPT_METHOD = "decrypt"                      # 호출할 Java 함수(메서드) 이름

# DB 접속 정보 (Source: 기존 데이터 / Target: 마이그레이션 대상)
SRC = {
    "host": "10.10.1.10", "port": 3306, "user": "root", "passwd": "src_mysql_pass123!", "db": "member_db",
    "cursorclass": MySQLdb.cursors.DictCursor
}

TGT = {
    "host": "10.10.1.20", "port": 3306, "user": "root", "passwd": "tgt_mysql_pass456@", "db": "member_db",
}

# 테이블별 마이그레이션 설정
TABLE_CONFIG = {
    "mbr_base": {
        "columns": ["mbr_no", "mbr_id", "mbr_nm", "enc_email"],
        "decrypt_cols": ["enc_email"],
        "range_col": "mbr_no", 
        "start_id": 1,
        "end_id": 50000
    }
}

# ==========================================
# 2. JVM(Java Virtual Machine) 초기화
# ==========================================
def init_jvm():
    if not jpype.isJVMStarted():
        try:
            # JVM 시작 및 JAR 로드
            jpype.startJVM(classpath=[JAR_PATH])
            print(f"[INFO] JVM Started with JAR: {JAR_PATH}")
        except Exception as e:
            print(f"[ERROR] Failed to start JVM: {e}")
            sys.exit(1)

# ==========================================
# 3. 데이터 가공 및 복호화 함수
# ==========================================

def call_java_decrypt(decryptor_instance, encrypted_value: str) -> str:
    """Java 메서드를 호출하여 복호화 수행"""
    if not encrypted_value:
        return encrypted_value
    
    try:
        # Java 메서드 호출 (메서드명에 따라 수정 가능)
        # 예: decryptor_instance.decrypt(value)
        method = getattr(decryptor_instance, DECRYPT_METHOD)
        decrypted_val = method(encrypted_value)
        return str(decrypted_val).strip()
    except Exception as e:
        print(f"   ! Java Decrypt Fail for [{encrypted_value}]: {e}")
        return f"ERR_DECRYPT_{encrypted_value}"

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

def transform_row(row: Dict[str, Any], decrypt_cols: List[str], decryptor_instance) -> tuple:
    """한 행(Row)의 데이터를 가공"""
    for col in decrypt_cols:
        if col in row and row[col]:
            # Java 복호화 수행
            row[col] = call_java_decrypt(decryptor_instance, row[col])
            
    # INSERT 문에 들어갈 순서대로 튜플 변환
    return tuple(row.values())

# ==========================================
# 4. ETL 프로세스 메인 로직
# ==========================================

def _etl_process(table_name: str, config: Dict):
    src_conn = None
    tgt_conn = None
    
    # Java 클래스 인스턴스 생성
    try:
        DecryptorClass = jpype.JClass(JAVA_CLASS_PATH)
        decryptor_instance = DecryptorClass() # 만약 static 함수라면 인스턴스화 생략 가능
    except Exception as e:
        print(f"[ERROR] Java Class Load Fail: {e}")
        return

    try:
        src_conn = MySQLdb.connect(**SRC)
        tgt_conn = MySQLdb.connect(**TGT)
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        cols_str = ", ".join(config["columns"])
        placeholders = ", ".join(["%s"] * len(config["columns"]))
        insert_sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"

        range_query = f"""
            SELECT {cols_str} FROM {table_name} 
            WHERE {config['range_col']} BETWEEN %s AND %s
        """
        
        print(f"   > Processing range: {config['start_id']} ~ {config['end_id']}")
        src_cur.execute(range_query, (config['start_id'], config['end_id']))

        processed_count = 0
        while True:
            rows = src_cur.fetchmany(1000) # 1,000건씩 끊어서 처리
            if not rows:
                break
            
            # 가공 및 복호화
            transformed_rows = [transform_row(r, config["decrypt_cols"], decryptor_instance) for r in rows]

            try:
                # Target DB에 일괄 삽입
                tgt_cur.executemany(insert_sql, transformed_rows)
                tgt_conn.commit()
            except Exception as e:
                tgt_conn.rollback()
                print(f"   ! Error during batch insert: {e}")
                raise
            
            processed_count += len(transformed_rows)
            # 진행 상태 출력 (줄바꿈 없이 갱신)
            sys.stdout.write(f"\r   - {table_name}: {processed_count} rows migrated...")
            sys.stdout.flush()

    finally:
        if src_conn: src_conn.close()
        if tgt_conn: tgt_conn.close()

# ==========================================
# 5. 실행부
# ==========================================

def main():
    # 1. JVM 초기화
    init_jvm()
    
    try:
        for table_name, config in TABLE_CONFIG.items():
            print(f"\n[START] {table_name}")
            _etl_process(table_name, config)
            print(f"\n[FINISH] {table_name} - Success")
    except Exception as e:
        print(f"\n[ABORT] Error during Migration: {e}")
    finally:
        # 2. JVM 종료
        if jpype.isJVMStarted():
            jpype.shutdownJVM()
            print("\n[INFO] JVM Shutdown.")

if __name__ == "__main__":
    main()