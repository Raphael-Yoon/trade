# -*- coding: utf-8 -*-
"""
운영서버 DB 마이그레이션 스크립트
역할: tr_audit_recommendations 테이블에 sector 컬럼을 추가합니다. (MySQL / SQLite 호환)
사용법: python migrate_db_sector.py
"""
import os
import sys
from urllib.parse import urlparse
from dotenv import load_dotenv

# UTF-8 출력 보장
if os.name == 'nt':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(base_dir, '.env'))
    database_url = os.getenv('DATABASE_URL')

    print("==================================================")
    print("  운영 DB 마이그레이션 (sector 컬럼 추가)")
    print("==================================================")

    if database_url and database_url.startswith('mysql'):
        print("[*] MySQL 데이터베이스 연결 중...")
        import pymysql
        parsed = urlparse(database_url)
        conn = pymysql.connect(
            host=parsed.hostname or '127.0.0.1',
            port=parsed.port or 3306,
            user=parsed.username or 'root',
            password=parsed.password or '',
            database=parsed.path.lstrip('/') if parsed.path else 'trade',
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        
        # 컬럼 존재 여부 확인 후 추가
        try:
            cursor.execute("""
                SELECT COLUMN_NAME 
                FROM information_schema.columns 
                WHERE table_schema = DATABASE() 
                  AND table_name = 'tr_audit_recommendations' 
                  AND column_name = 'sector'
            """)
            if cursor.fetchone():
                print("[+] MySQL tr_audit_recommendations 테이블에 이미 'sector' 컬럼이 존재합니다.")
            else:
                print("[*] MySQL tr_audit_recommendations 테이블에 'sector' 컬럼 추가 중...")
                cursor.execute("ALTER TABLE tr_audit_recommendations ADD COLUMN sector VARCHAR(255)")
                conn.commit()
                print("[+] MySQL 'sector' 컬럼 추가 성공!")
        except Exception as e:
            print(f"[오류] MySQL 마이그레이션 실패: {e}")
        finally:
            conn.close()
    else:
        print("[*] SQLite 데이터베이스 연결 중...")
        import sqlite3
        db_path = os.path.join(base_dir, 'trade.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("PRAGMA table_info(tr_audit_recommendations)")
            cols = [col[1] for col in cursor.fetchall()]
            if 'sector' in cols:
                print("[+] SQLite tr_audit_recommendations 테이블에 이미 'sector' 컬럼이 존재합니다.")
            else:
                print("[*] SQLite tr_audit_recommendations 테이블에 'sector' 컬럼 추가 중...")
                cursor.execute("ALTER TABLE tr_audit_recommendations ADD COLUMN sector TEXT")
                conn.commit()
                print("[+] SQLite 'sector' 컬럼 추가 성공!")
        except Exception as e:
            print(f"[오류] SQLite 마이그레이션 실패: {e}")
        finally:
            conn.close()

if __name__ == '__main__':
    main()
