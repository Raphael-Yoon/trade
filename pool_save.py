# -*- coding: utf-8 -*-
"""
개발2팀 투자 후보군(pool) SQLite 저장 유틸리티
기준 문서: trade/Report/audit_logic.md

AI가 결정한 100개 pool을 SQLite tr_stock_pool 테이블에 저장.

사용법:
    python trade/pool_save.py pool.json
    python trade/pool_save.py pool.json --date 2026-06-03
"""
import argparse
import json
import os
import sys
import sqlite3
from datetime import datetime
from pathlib import Path

SQLITE_PATH = str(Path(__file__).resolve().parent / 'trade.db')


def save(records: list[dict], data_date: str):
    """
    records 형식:
    [
        {
            "code":             "005930",
            "name":             "삼성전자",
            "sector":           "반도체와반도체장비",
            "roe":              63.0,
            "pbr":              1.2,
            "per":              10.5,
            "debt_ratio":       29.9,
            "operating_margin": 25.3,
            "target_price":     310800,
            "foreign_net_buy":  1234567890,
            "inst_net_buy":     987654321,
            "pool_score":       85.5
        },
        ...
    ]
    """
    if not records:
        print("[오류] 저장할 데이터가 없습니다.")
        return

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    conn = sqlite3.connect(SQLITE_PATH)
    cur = conn.cursor()

    # 기존 데이터 전체 교체
    cur.execute("DELETE FROM tr_stock_pool")

    for r in records:
        cur.execute("""
            INSERT INTO tr_stock_pool
                (code, name, sector, roe, pbr, per, debt_ratio, operating_margin,
                 target_price, foreign_net_buy, inst_net_buy, pool_score,
                 source_file, data_date, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            r['code'], r['name'], r.get('sector', ''),
            float(r.get('roe', 0)), float(r.get('pbr', 0)), float(r.get('per', 0)),
            float(r.get('debt_ratio', 0)), float(r.get('operating_margin', 0)),
            float(r.get('target_price', 0)),
            float(r.get('foreign_net_buy', 0)), float(r.get('inst_net_buy', 0)),
            float(r.get('pool_score', 0)),
            'manual', data_date, now_str
        ))

    conn.commit()
    conn.close()

    print(f"[완료] {len(records)}개 종목 tr_stock_pool 저장 완료 (data_date={data_date})")
    for r in records[:5]:
        print(f"   [{r['code']}] {r['name']}  pool_score={r.get('pool_score', 0):.1f}")
    if len(records) > 5:
        print(f"   ... 외 {len(records) - 5}개")


def main():
    parser = argparse.ArgumentParser(description='개발2팀 pool SQLite 저장')
    parser.add_argument('json_file', help='pool JSON 파일 경로')
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'),
                        help='기준일 (기본값: 오늘, 형식: YYYY-MM-DD)')
    args = parser.parse_args()

    json_path = Path(args.json_file)
    if not json_path.exists():
        print(f"[오류] 파일을 찾을 수 없습니다: {json_path}")
        sys.exit(1)

    with open(json_path, encoding='utf-8') as f:
        records = json.load(f)

    if isinstance(records, dict) and 'stocks' in records:
        records = records['stocks']

    save(records, args.date)


if __name__ == '__main__':
    main()
