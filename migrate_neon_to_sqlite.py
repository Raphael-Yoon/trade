# -*- coding: utf-8 -*-
import os
import sqlite3
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from db_init import init_db, SQLITE_PATH

# .env 파일 로드
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
NEON_DATABASE_URL = os.getenv('NEON_DATABASE_URL')

def migrate():
    # 1. SQLite 테이블 구조 초기화
    print("[마이그레이션] SQLite 테이블 초기화 중...")
    init_db()
    print("[마이그레이션] SQLite 테이블 초기화 완료.")

    if not NEON_DATABASE_URL:
        print("[오류] .env 파일에 NEON_DATABASE_URL이 설정되어 있지 않습니다.")
        return

    print(f"[마이그레이션] Neon DB 연결 시도 중...\nURL: {NEON_DATABASE_URL}")
    
    try:
        pg_conn = psycopg2.connect(NEON_DATABASE_URL)
        pg_cur = pg_conn.cursor(cursor_factory=DictCursor)
        print("[마이그레이션] Neon DB 연결 성공!")
    except Exception as e:
        print(f"[오류] Neon DB 연결 실패: {e}")
        print("팁: .env의 NEON_DATABASE_URL 패스워드나 인증 정보가 올바른지 확인해주세요.")
        return

    try:
        lite_conn = sqlite3.connect(SQLITE_PATH)
        lite_cur = lite_conn.cursor()
        print("[마이그레이션] 로컬 SQLite DB 연결 성공!")
    except Exception as e:
        print(f"[오류] SQLite DB 연결 실패: {e}")
        pg_conn.close()
        return

    # 마이그레이션 대상 테이블 목록
    tables = [
        "tr_my_stocks",
        "tr_stock_daily_history",
        "tr_sell_history",
        "tr_stocks_master",
        "tr_portfolio_ai_cache",
        "tr_stock_pool",
        "tr_audit_recommendations"
    ]

    for table in tables:
        print(f"\n[테이블 이관] {table} 이관 시작...")
        try:
            # 1. Neon DB에서 데이터 조회
            pg_cur.execute(f"SELECT * FROM {table}")
            rows = pg_cur.fetchall()
            print(f" - Neon DB 레코드 수: {len(rows)}개")

            if not rows:
                print(f" - {table} 테이블에 데이터가 없어 생략합니다.")
                continue

            # 컬럼 목록 가져오기
            columns = rows[0].keys()
            col_list = ", ".join(columns)
            placeholders = ", ".join(["?"] * len(columns))

            # 2. SQLite 기존 데이터 제거 (정합성을 위해 초기화 후 적재)
            lite_cur.execute(f"DELETE FROM {table}")

            # 3. SQLite에 데이터 삽입
            insert_sql = f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})"
            
            insert_data = []
            for row in rows:
                row_data = []
                for col in columns:
                    val = row[col]
                    # Decimal 타입 등 SQLite가 처리 못 하는 타입을 float/int/str로 변환
                    from decimal import Decimal
                    if isinstance(val, Decimal):
                        val = float(val)
                    row_data.append(val)
                insert_data.append(tuple(row_data))

            lite_cur.executemany(insert_sql, insert_data)
            lite_conn.commit()
            print(f" - SQLite 이관 완료: {len(insert_data)}개 행 적재 완료.")

        except Exception as e:
            print(f" - [오류] {table} 이관 중 오류 발생: {e}")
            lite_conn.rollback()

    # 연결 종료
    pg_cur.close()
    pg_conn.close()
    lite_cur.close()
    lite_conn.close()
    print("\n[마이그레이션] 전체 프로세스 종료.")

if __name__ == "__main__":
    migrate()
