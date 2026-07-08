# -*- coding: utf-8 -*-
"""
개발2팀 Top 10 추천 종목 저장 유틸리티
기준 문서: trade/Report/audit_logic.md

AI가 분석·결정한 추천 종목을 SQLite tr_audit_recommendations 테이블에 저장.

사용법:
    python trade/audit_save.py recommendations.json
    python trade/audit_save.py recommendations.json --date 2026-05-10
"""
import argparse
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path

SQLITE_PATH = str(Path(__file__).resolve().parent / 'trade.db')


def save(records: list[dict], data_date: str, rec_type: str = 'momentum'):
    """
    records 형식:
    [
        {
            "code":          "005930",
            "name":          "삼성전자",
            "current_price": 268500,
            "target_price":  310800,
            "upside":        15.8,
            "roe":           63.0,
            "debt":          29.9,
            "score":         78.5,
            "reason":        "[반도체] 뉴스:실적·최고 | ROE 63.0% | 수급 외인+/기관-",
            "news_summary":  "...",
            "rec_type":      "value"
        },
        ...
    ]
    """
    if not records:
        print("[오류] 저장할 데이터가 없습니다.")
        return

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print(f"[완료] {len(records)}개 종목 DB 적재 시작 (추천유형: {rec_type})")
    for r in records:
        print(f"   [{r['code']}] {r['name']}  상승여력 {r['upside']:.1f}%")

def _execute_save(conn, db_type, records, data_date, rec_type, now_str):
    cursor = conn.cursor()
    placeholder = '?' if db_type == 'sqlite' else '%s'
    
    # 해당 추천 유형만 삭제
    cursor.execute(f"DELETE FROM tr_audit_recommendations WHERE rec_type = {placeholder}", (rec_type,))
    
    import re
    def _get_sector(rec):
        sector_val = rec.get('sector')
        if not sector_val:
            reason_str = rec.get('reason', '')
            if reason_str and reason_str.startswith('['):
                m = re.match(r'^\[(.*?)\]', reason_str)
                if m:
                    sector_val = m.group(1)
        return sector_val or '기타'

    for r in records:
        item_rec_type = r.get('rec_type', rec_type)
        item_data_date = r.get('data_date', data_date)
        opinion_val = str(r.get('dividend_yield', '')) if item_rec_type == 'dividend' else r.get('opinion', '')
        sector_val = _get_sector(r)
        
        cursor.execute(f"""
            INSERT INTO tr_audit_recommendations
                (code, name, current_price, target_price, buy_target_price, upside, opinion, data_date, created_at,
                 score, roe, debt, reason, news_summary, rec_type, one_liner, disc_json, sector)
            VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})
        """, (
            r['code'], r['name'], float(r['current_price']), float(r['target_price']),
            float(r.get('buy_target_price', 0) or 0), float(r['upside']), opinion_val, item_data_date, now_str, float(r['score']),
            float(r.get('roe', 0)), float(r.get('debt', 0)), r.get('reason', ''),
            r.get('news_summary', '[]'), item_rec_type, r.get('one_liner', ''),
            r.get('disc_json', '[]'), sector_val
        ))
    conn.commit()
    print(f"[완료] {db_type.upper()} tr_audit_recommendations 테이블 적재 성공! (타입: {rec_type})")

def save(records: list[dict], data_date: str, rec_type: str = 'momentum'):
    if not records:
        print("[오류] 저장할 데이터가 없습니다.")
        return

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print(f"[완료] {len(records)}개 종목 DB 적재 시작 (추천유형: {rec_type})")
    for r in records:
        print(f"   [{r['code']}] {r['name']}  상승여력 {r['upside']:.1f}%")

    # 1. SQLite 적재 (기본 개발환경)
    try:
        conn = sqlite3.connect(SQLITE_PATH)
        _execute_save(conn, 'sqlite', records, data_date, rec_type, now_str)
        conn.close()
    except Exception as sqlite_err:
        print(f"[오류] SQLITE tr_audit_recommendations 적재 실패: {sqlite_err}")

    # 2. 외부 DB 적재 (설정된 경우)
    try:
        from dotenv import load_dotenv
        TRADE_DIR = Path(__file__).resolve().parent
        load_dotenv(TRADE_DIR / '.env')
        database_url = os.getenv('DATABASE_URL')

        if database_url:
            if database_url.startswith('postgresql'):
                print("[*] PostgreSQL 데이터베이스 연결 중...")
                import psycopg2
                try:
                    conn = psycopg2.connect(database_url)
                    _execute_save(conn, 'postgres', records, data_date, rec_type, now_str)
                    conn.close()
                except Exception as pg_err:
                    print(f"[오류] POSTGRESQL tr_audit_recommendations 적재 실패: {pg_err}")
            elif database_url.startswith('mysql'):
                print("[*] MySQL 데이터베이스 연결 중...")
                import pymysql
                from urllib.parse import urlparse
                parsed = urlparse(database_url)
                try:
                    conn = pymysql.connect(
                        host=parsed.hostname or '127.0.0.1',
                        port=parsed.port or 3306,
                        user=parsed.username or 'root',
                        password=parsed.password or '',
                        database=parsed.path.lstrip('/') if parsed.path else 'trade',
                        charset='utf8mb4'
                    )
                    _execute_save(conn, 'mysql', records, data_date, rec_type, now_str)
                    conn.close()
                except Exception as mysql_err:
                    print(f"[오류] MYSQL tr_audit_recommendations 적재 실패: {mysql_err}")
    except Exception as env_err:
        print(f"[경고] 외부 DB 연결정보 로드 실패: {env_err}")
    except Exception as e:
        print(f"[오류] {db_type.upper()} audit_recommendations 적재 실패: {e}")


def main():
    parser = argparse.ArgumentParser(description='개발2팀 Top 10 추천 종목 저장')
    parser.add_argument('json_file', help='추천 종목 JSON 파일 경로')
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'),
                        help='기준일 (기본값: 오늘, 형식: YYYY-MM-DD)')
    parser.add_argument('--type', default='sector', choices=['sector', 'momentum', 'value', 'dividend'],
                        help='추천 유형 (sector, momentum, value, dividend)')
    args = parser.parse_args()

    json_path = Path(args.json_file)
    if not json_path.exists():
        print(f"[오류] 파일을 찾을 수 없습니다: {json_path}")
        return

    with open(json_path, encoding='utf-8') as f:
        data = json.load(f)

    # 섹터별 dict 형식 {"섹터명": [...]} → flat list 변환
    if isinstance(data, dict) and 'stocks' not in data:
        records = []
        for sector_stocks in data.values():
            if isinstance(sector_stocks, list):
                for r in sector_stocks:
                    # rank → score 변환 (rank 1 = score 10, rank 10 = score 1)
                    if 'rank' in r and 'score' not in r:
                        r['score'] = max(0.0, 11 - int(r['rank']))
                    records.append(r)
    elif isinstance(data, dict) and 'stocks' in data:
        records = data['stocks']
    else:
        records = data

    save(records, args.date, args.type)

    # DB 적재 성공 후, trade/results/ 디렉토리로 JSON 파일 자동 복사
    try:
        import shutil
        trade_results_dir = Path(__file__).resolve().parent / 'results'
        trade_results_dir.mkdir(parents=True, exist_ok=True)
        
        # 파일명을 추천 타입에 따라 표준화하여 복사 (value_recommendations.json / momentum_recommendations.json)
        dest_filename = f"{args.type}_recommendations.json"
        dest_path = trade_results_dir / dest_filename
        
        shutil.copy2(json_path, dest_path)
        print(f"[완료] 추천 JSON 파일 이관 준비 완료: {json_path.name} -> {dest_path}")
    except Exception as copy_err:
        print(f"[경고] 추천 JSON 파일 복사 실패: {copy_err}")


if __name__ == '__main__':
    main()
