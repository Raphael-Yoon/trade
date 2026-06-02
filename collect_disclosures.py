# -*- coding: utf-8 -*-
"""
DART 경영공시 수시 수집 엔진
[개발2팀] 김선화 PM

이 모듈은 시장 전체의 공시 자료를 일괄 수집한 후,
필터링 없이 데이터베이스(stock_disclosures)에 전량 적재합니다.
수집 작업 자체는 AI 호출을 포함하지 않는 순수 데이터 적재 파이프라인으로 작동합니다.
"""
import os
import sys
import sqlite3
import argparse
from datetime import datetime, timedelta
import OpenDartReader
from dotenv import load_dotenv

# Windows 콘솔 UTF-8 설정
if os.name == 'nt':
    try:
        import sys
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

# 환경 변수 로드
DB_DIR = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(DB_DIR, '.env')
load_dotenv(env_path)

DB_PATH = os.path.join(DB_DIR, 'trade.db')
DART_API_KEY = os.getenv("DART_API_KEY")

def collect_disclosures(days_ago=1):
    """지정된 기간 동안의 DART 공시 전체 수집 및 DB 적재 (순수 데이터 파이프라인)"""
    if not DART_API_KEY:
        print("[오류] DART_API_KEY가 설정되지 않았습니다.")
        return
        
    dart = OpenDartReader(DART_API_KEY)
    
    # 날짜 범위 설정
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_ago)
    
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    
    print(f"[DART] {start_str} ~ {end_str} 기간 시장 전체 공시 수집 시작...")
    
    # 1. 전체 시장 공시 수집 (corp_code=None)
    try:
        df = dart.list(None, start=start_str, end=end_str)
    except Exception as e:
        print(f"[오류] DART 공시 목록 조회 실패: {e}")
        return
        
    if df is None or len(df) == 0:
        print("[정보] 수집된 공시가 없습니다.")
        return
        
    # Pandas DataFrame을 dict 목록으로 변환
    disclosures = df.to_dict('records')
    print(f"[정보] 총 {len(disclosures)}건의 시장 공시 수집 완료. DB 저장 시작...")
    
    # 2. DB 연결 및 순차 저장
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    saved_count = 0
    
    for d in disclosures:
        stock_code = d.get('stock_code')
        if stock_code:
            stock_code = str(stock_code).strip().zfill(6)
        else:
            stock_code = ''
            
        corp_name = d.get('corp_name', '')
        rcept_no = d.get('rcept_no')
        report_nm = d.get('report_nm')
        flr_nm = d.get('flr_nm', '')
        corp_cls = d.get('corp_cls', '')
        rm = d.get('rm', '')
        
        # rcept_dt 형식 변환 (YYYYMMDD -> YYYY-MM-DD)
        raw_dt = d.get('rcept_dt', '')
        rcept_dt = raw_dt
        if len(raw_dt) == 8:
            rcept_dt = f"{raw_dt[:4]}-{raw_dt[4:6]}-{raw_dt[6:]}"
            
        # 이미 등록된 공시인지 확인
        cursor.execute("SELECT id FROM stock_disclosures WHERE rcept_no = ?", (rcept_no,))
        if cursor.fetchone():
            continue
            
        # 기본값 설정 (AI 영향도 점수 및 요약은 분석 단계에서 동적 처리되므로 0.0과 제목으로 기본 적재)
        ai_score = 0.0
        ai_summary = report_nm
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # DB 삽입 (전체 공시 대상)
        try:
            cursor.execute("""
                INSERT INTO stock_disclosures 
                (code, name, rcept_no, rcept_dt, report_nm, flr_nm, corp_cls, rm, ai_impact_score, ai_summary, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (stock_code, corp_name, rcept_no, rcept_dt, report_nm, flr_nm, corp_cls, rm, ai_score, ai_summary, now_str))
            saved_count += 1
        except sqlite3.Error as e:
            print(f"[오류] DB 저장 실패: {e}")
            
    conn.commit()
    conn.close()
    
    print(f"[완료] 전체 수집: {len(disclosures)}건 | 신규 저장: {saved_count}건")

import db_lock_guard

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DART 공시 일괄 수집기')
    parser.add_argument('--days', type=int, default=1, help='수집 대상 과거 일수 (기본값: 1)')
    parser.add_argument('--force-db', action='store_true', help='서버가 실행 중이라도 실행을 강제합니다.')
    args = parser.parse_args()
    
    # DB 락 충돌 방지 가드 체크
    db_lock_guard.check_lock_and_exit("공시 수집 엔진")
    
    collect_disclosures(args.days)
