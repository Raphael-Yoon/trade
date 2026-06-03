# -*- coding: utf-8 -*-
# PostgreSQL(Neon) 마이그레이션 이후 DB 락은 PG가 자체 처리하므로 이 가드는 no-op입니다.
import sys

def check_lock_and_exit(script_name):
    """PostgreSQL 동시성은 서버 자체가 처리하므로 별도 락 감지가 불필요합니다."""
    force_run = '--force-db' in sys.argv
    if not force_run:
        print(f"[INFO] {script_name} 시작 (PostgreSQL 동시 접근 안전)")
