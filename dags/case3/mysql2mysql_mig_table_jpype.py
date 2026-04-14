import pyodbc
import MySQLdb
import MySQLdb.cursors
import jpype
import jpype.imports
from jpype.types import *
import base64
import sys
import os
from typing import List, Dict, Any

# ===== 설정부 =====
# JAR 파일이 있는 절대 경로 또는 상대 경로
JAR_PATH = "/path/to/your/decryptor.jar" 

# 1. Tibero 접속 정보
TIBERO_DSN = "DRIVER={Tibero6 Driver};HOSTNAME=10.10.1.30;PORT=8629;SN=tibero_db;UID=sys;PWD=pass123"

# 2. MySQL 접속 정보
MYSQL_TGT = {
    "host": "10.10.1.20", "port": 3306, "user": "root", "passwd": "tgt_mysql_pass456@", "db": "member_db",
}

# 3. 테이블별 마이그레이션 설정
TABLE_CONFIG = {
    "mbr_base": {
        "src_table": "TIB_MBR_BASE",
        "tgt_table": "mbr_base",
        "columns": ["MBR_NO", "MBR_ID", "ENC_EMAIL", "REG_DT"],
        "decrypt_cols": ["ENC_EMAIL"],
        "range_col": "MBR_NO",
        "start_id": 1,
        "end_id": 50000
    }
}

# ===== [핵심] JPype 및 JVM 설정 =====
def init_jvm(jar_path: str):
    if not jpype.isJVMStarted():
        # -Djava.class.path 대신 classpath 인자 사용 가능
        jpype.startJVM(classpath=[jar_path])
        print(f"JVM Started with {jar_path}")

    # 자바 클래스 로드 (실제 JAR 내부의 패키지 경로를 입력하세요)
    # 예: com.crypto.Decryptor 클래스라면 아래와 같이 선언
    from com.example import CryptoClass  # <--- 실제 클래스명으로 변경 필요
    global crypto_tool
    crypto_tool = CryptoClass() # 객체 생성 (싱글톤처럼 사용)

# ===== [수정] JPype 기반 복호화 호출 =====
def call_java_decrypt(val: str) -> str:
    if not val: return val
    try:
        # Java의 decrypt 메소드 호출 (메소드 이름이 decrypt라고 가정)
        return str(crypto_tool.decrypt(str(val)))
    except Exception as e:
        return f"ERR_{val}"

# ===== [데이터 가공] =====
def transform_row(row_dict: Dict, decrypt_cols: List[str]) -> tuple:
    # Tibero 결과는 보통 대문자 키로 반환됨
    for col in decrypt_cols:
        col_key = col.upper()
        if col_key in row_dict and row_dict[col_key]:
            # JPype 호출
            row_dict[col_key] = call_java_decrypt(str(row_dict[col_key]))
    
    return tuple(row_dict.values())

def _etl_tibero_to_mysql(table_name: str, config: Dict):
    tib_conn = None
    mys_conn = None
    
    try:
        tib_conn = pyodbc.connect(TIBERO_DSN)
        mys_conn = MySQLdb.connect(**MYSQL_TGT)
        
        tib_cur = tib_conn.cursor()
        mys_cur = mys_conn.cursor()

        cols_str = ", ".join(config["columns"])
        placeholders = ", ".join(["%s"] * len(config["columns"]))
        insert_sql = f"INSERT INTO {config['tgt_table']} ({cols_str}) VALUES ({placeholders})"

        select_sql = f"SELECT {cols_str} FROM {config['src_table']} WHERE {config['range_col']} BETWEEN ? AND ?"
        
        print(f"   > Tibero Extract: {config['start_id']} ~ {config['end_id']}")
        tib_cur.execute(select_sql, (config['start_id'], config['end_id']))

        columns = [column[0] for column in tib_cur.description]

        processed_count = 0
        while True:
            rows = tib_cur.fetchmany(1000) # 1000건씩 배치 처리
            if not rows: break
            
            transformed_rows = []
            for r in rows:
                row_dict = dict(zip(columns, r))
                transformed_rows.append(transform_row(row_dict, config["decrypt_cols"]))

            try:
                mys_cur.executemany(insert_sql, transformed_rows)
                mys_conn.commit()
            except Exception as e:
                mys_conn.rollback()
                raise e
            
            processed_count += len(transformed_rows)
            print(f"   - {table_name}: {processed_count} rows migrated...", end='\r')

    finally:
        if tib_conn: tib_conn.close()
        if mys_conn: mys_conn.close()

def main():
    # 1. JVM 초기화 (한 번만 실행)
    try:
        init_jvm(JAR_PATH)
    except Exception as e:
        print(f"JVM Init Failed: {e}")
        return

    # 2. 마이그레이션 실행
    for t_name, cfg in TABLE_CONFIG.items():
        print(f"\n[START TIBERO -> MYSQL] {t_name}")
        try:
            _etl_tibero_to_mysql(t_name, cfg)
            print(f"\n[OK]")
        except Exception as e:
            print(f"\n[FAIL] {e}")
    
    # 3. (선택사항) 모든 작업 완료 후 JVM 종료
    # jpype.shutdownJVM()

if __name__ == "__main__":
    main()