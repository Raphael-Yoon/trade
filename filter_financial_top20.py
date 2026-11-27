# -*- coding: utf-8 -*-
import os
import sys
import io
import argparse
import json
import sqlite3
import re
import time
from datetime import datetime
from pathlib import Path
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TRADE_DIR = PROJECT_ROOT / 'trade'
sys.path.append(str(PROJECT_ROOT))
sys.path.append(str(TRADE_DIR))

load_dotenv(TRADE_DIR / '.env')
DATABASE_URL = os.getenv('DATABASE_URL')
SQLITE_PATH = TRADE_DIR / 'trade.db'


def resolve_market_map(df):
    """각 종목의 시장구분(KOSPI / KOSDAQ)을 판별한다.
    우선순위: (1) 엑셀 '시장구분' 컬럼 -> (2) tr_stocks_master DB -> (3) 네이버 모바일 API 폴백
    """
    market_map = {}

    if '시장구분' in df.columns:
        for _, row in df[['code', '시장구분']].dropna(subset=['시장구분']).iterrows():
            val = str(row['시장구분']).strip().upper()
            if val in ('KOSPI', 'KOSDAQ'):
                market_map[row['code']] = val

    unresolved = [c for c in df['code'].unique() if c not in market_map]

    # 2차: 로컬 SQLite tr_stocks_master 조회
    if unresolved and SQLITE_PATH.exists():
        try:
            conn = sqlite3.connect(SQLITE_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT code, market FROM tr_stocks_master WHERE market IN ('KOSPI', 'KOSDAQ')")
            for code, mkt in cursor.fetchall():
                c_norm = str(code).strip().zfill(6)
                if c_norm not in market_map:
                    market_map[c_norm] = str(mkt).strip().upper()
            conn.close()
        except Exception as e:
            print(f"[!] tr_stocks_master 조회 실패 (무시 후 폴백): {e}")

    unresolved = [c for c in df['code'].unique() if c not in market_map]

    # 3차: 네이버 모바일 API marketValue 조회 (미매핑 종목 보완)
    if unresolved:
        print(f"[*] 시장구분 미매핑 {len(unresolved)}개 종목에 대해 네이버 API 보완 조회 시작...")
        unresolved_set = set(unresolved)
        session = requests.Session()
        session.headers.update({'User-Agent': 'Mozilla/5.0'})
        for mkt in ('KOSPI', 'KOSDAQ'):
            page = 1
            page_size = 100
            while page <= 40 and unresolved_set:
                url = f"https://m.stock.naver.com/api/stocks/marketValue/{mkt}?page={page}&pageSize={page_size}"
                try:
                    res = session.get(url, timeout=5)
                    if res.status_code != 200:
                        break
                    stocks = res.json().get('stocks', [])
                    if not stocks:
                        break
                    for s in stocks:
                        code = str(s.get('itemCode', '')).strip().zfill(6)
                        if code in unresolved_set:
                            market_map[code] = mkt
                            unresolved_set.discard(code)
                    if len(stocks) < page_size:
                        break
                    page += 1
                    time.sleep(0.02)
                except Exception as e:
                    print(f"[!] 네이버 {mkt} 시장목록 조회 중 오류 (page {page}): {e}")
                    break
        print(f"[*] 네이버 보완 조회 완료 (잔여 미매핑: {len(unresolved_set)}개)")

    return market_map


def score_market_pool(pool_df):
    """단일 시장(KOSPI 또는 KOSDAQ) 풀 내부에서 독립 백분위 스코어(0~100)를 산출한다.
    이상치/기저효과 왜곡 방지를 위해 영업이익률(0~60%), ROE(0~60%), 영업이익증가율(-100~300%)에 클램핑을 적용한다.
    """
    pool = pool_df.copy()
    if pool.empty:
        pool['unified_score'] = pd.Series(dtype=float)
        return pool

    op_profit_pct = pool['영업이익_fill'].rank(pct=True)
    roe_pct = pool['ROE_fill'].clip(lower=0.0, upper=60.0).rank(pct=True)
    op_margin_pct = pool['영업이익률_fill'].clip(lower=0.0, upper=60.0).rank(pct=True)

    # 실질부채비율 안전구간 평준화
    is_fin_filtered = pool['업종'].astype(str).str.contains('은행|증권|카드|보험|금융|투자', na=False)
    is_high_prepayment_filtered = pool['업종'].astype(str).str.contains('조선|우주항공|방위|기계|방산', na=False)
    has_row_adj_filtered = pool['실질부채비율'].notna()
    adj_debt_vals = pool['실질부채비율_fill'].clip(lower=0.0)
    health_pct_tiered = np.where(
        is_fin_filtered,
        0.60,
        np.where(
            adj_debt_vals <= 100.0,
            1.0 - (adj_debt_vals / 100.0) * 0.20,
            np.maximum(0.20, 0.80 - ((adj_debt_vals - 100.0) / 50.0) * 0.30)
        )
    )
    health_pct = np.where((~has_row_adj_filtered) & is_high_prepayment_filtered, 0.80, health_pct_tiered)

    valuation_pct = 1 - pool['PBR_fill'].rank(pct=True)
    op_growth_pct = pool['영업이익증가율_fill'].clip(lower=-100.0, upper=300.0).rank(pct=True)

    pool['unified_score'] = (
        op_profit_pct * 0.25 +
        roe_pct * 0.20 +
        op_margin_pct * 0.15 +
        health_pct * 0.10 +
        valuation_pct * 0.10 +
        op_growth_pct * 0.20
    ) * 100
    return pool

def parse_args():
    parser = argparse.ArgumentParser(description="Financial-based stock screener")
    parser.add_argument('--source_file', required=True, help="Original spreadsheet file name")
    parser.add_argument('--id', required=False, help="Google Drive Spreadsheet ID")
    parser.add_argument('--file', required=False, help="Local file path")
    return parser.parse_args()

def main():
    args = parse_args()
    print(f"[*] Starting Step 1 Financial Screener for: {args.source_file}")
    
    df = None
    
    # 1. 데이터 로드 (드라이브 우선, 없으면 로컬 파일)
    if args.id:
        print(f"[*] Downloading from Google Drive ID: {args.id}...")
        try:
            from drive_sync import download_from_drive
            content = download_from_drive(args.id)
            if content:
                df = pd.read_excel(io.BytesIO(content))
                print("[+] Successfully loaded from Drive.")
        except Exception as e:
            print(f"[!] Drive download failed: {e}")
            
    if df is None and args.file:
        print(f"[*] Loading from local file: {args.file}...")
        if os.path.exists(args.file):
            df = pd.read_excel(args.file)
            print("[+] Successfully loaded from local file.")
        else:
            print(f"[!] Local file not found: {args.file}")
            
    # 폴백: 로컬 results 및 trade 폴더에서 검색
    if df is None:
        possible_paths = [
            TRADE_DIR / 'results' / args.source_file,
            TRADE_DIR / args.source_file,
            Path(args.source_file)
        ]
        for p in possible_paths:
            if p.exists():
                print(f"[*] Loading from fallback local path: {p}...")
                df = pd.read_excel(p)
                print("[+] Successfully loaded from fallback path.")
                break
            
    if df is None:
        print("[ERROR] Failed to load any data.")
        sys.exit(1)
        
    required_cols = ['종목코드', '종목명', '업종']
    for col in required_cols:
        if col not in df.columns:
            print(f"[ERROR] Missing required column: {col}")
            sys.exit(1)

    # 2. 감사의견 필터링
    if '회계감사의견' in df.columns:
        df['회계감사의견'] = df['회계감사의견'].apply(lambda x: str(x).strip() if pd.notna(x) else x)
        df = df[df['회계감사의견'].isin(['적정의견', 'N/A']) | df['회계감사의견'].isna()]
        print(f"[*] After Audit Opinion filtering: {len(df)} stocks remaining")

    # 3. 바이오/제약/헬스케어/생물공학 하드 필터링
    before_bio = len(df)
    df = df[~df['업종'].astype(str).str.contains('제약|바이오|건강관리|헬스케어|생물공학', na=False)]
    print(f"[*] After Bio/Healthcare/Biotech filtering: {before_bio} -> {len(df)} stocks remaining")

    # 4. 수치 변환
    numeric_cols = ['PBR', 'PER', 'ROE', '시가총액', '매출액', '영업이익', '당기순이익', 
                    '부채비율', '실질부채비율', '선수금(계약부채)', '선수금증감(%)',
                    '매출액증가율(%)', '영업이익증가율(%)', '순이익증가율(%)', '영업이익률', '순이익률']
    for col in numeric_cols:
        if col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.replace(',', '').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            df[col] = np.nan

    # 결측치 보정
    df['영업이익_fill'] = df['영업이익'].fillna(0.0)
    df['당기순이익_fill'] = df['당기순이익'].fillna(0.0)
    df['ROE_fill'] = df['ROE'].fillna(0.0)
    df['PBR_fill'] = df['PBR'].fillna(df['PBR'].median())
    df['PER_fill'] = df['PER'].fillna(0.0)
    df['영업이익증가율_fill'] = df['영업이익증가율(%)'].fillna(0.0)
    df['매출액증가율_fill'] = df['매출액증가율(%)'].fillna(0.0)
    df['영업이익률_fill'] = df['영업이익률'].fillna(0.0)
    df['부채비율_fill'] = df['부채비율'].fillna(df['부채비율'].median())
    df['실질부채비율_fill'] = np.where(df['실질부채비율'].notna(), df['실질부채비율'], df['부채비율_fill'])
    df['선수금_fill'] = df['선수금(계약부채)'].fillna(0.0)
    df['선수금증감_fill'] = df['선수금증감(%)'].fillna(0.0)
    df['시가총액_fill'] = df['시가총액'].fillna(0.0)
    df['매출액_fill'] = df['매출액'].fillna(0.0)
    df['영업이익_val'] = df['영업이익'].fillna(0.0)

    # 종목코드 6자리 정규화
    def format_code(c):
        c_str = str(c).strip()
        return c_str.zfill(6) if len(c_str) < 6 else c_str

    df['code'] = df['종목코드'].apply(format_code)

    # 우선주 및 상장자산(리츠, ETN, ETF, 스팩) 스코어링 전 선배제
    before_asset = len(df)
    df = df[~df['종목명'].astype(str).str.endswith(('우', '우B', '우C', '우(전환)', '3우B'))]
    df = df[~df['종목명'].astype(str).str.contains('리츠|ETN|ETF|스팩|레버리지|선물|인버스|Koosec', na=False)]
    print(f"[*] After Preferred/ETF/SPAC/REIT exclusion: {before_asset} -> {len(df)} stocks remaining")

    # 영업이익률 결측치 산출 보정 (영업이익 / 매출액)
    calc_op_margin = np.where(df['매출액_fill'] > 0, (df['영업이익_val'] / df['매출액_fill']) * 100, 0.0)
    df['영업이익률_fill'] = np.where((df['영업이익률_fill'] <= 0) & (df['매출액_fill'] > 0), calc_op_margin, df['영업이익률_fill'])

    # 기본 수익성 및 최소 유동성(시가총액 1,000억 이상) 하드 필터링
    before_profit = len(df)
    df = df[
        (df['매출액_fill'] > 0) &
        (df['영업이익_fill'] > 0) &
        (df['당기순이익_fill'] > 0) &
        (df['시가총액_fill'] >= 1000.0)
    ]
    print(f"[*] After Profitability(Sales/OP/NI>0) & Cap(>=1,000억) filtering: {before_profit} -> {len(df)} stocks remaining")

    # PBR 밸류에이션 안전마진(Safety Cap) 하드 필터링:
    # PBR 12.0배 이하 원칙, 단 고ROE(>=25%) & 합리적 PER(0<PER<=25) 성장 우량주는 예외 허용
    before_pbr = len(df)
    high_roe_exception = (df['PER_fill'] > 0) & (df['PER_fill'] <= 25.0) & (df['ROE_fill'] >= 25.0)
    df = df[(df['PBR_fill'] <= 12.0) | high_roe_exception]
    print(f"[*] After PBR Safety Cap (<= 12.0 or High-ROE/Low-PER) filtering: {before_pbr} -> {len(df)} stocks remaining")

    # 자산총계 컬럼 처리
    if '자산총계' in df.columns:
        df['자산총계_fill'] = pd.to_numeric(df['자산총계'], errors='coerce').fillna(0.0)
    else:
        df['자산총계_fill'] = 0.0

    # 5. 부채비율 필터링 (실질부채비율 집계 시 선수금 제외 150% 이하 일괄 적용, 미집계 구형 파일은 수주업종 500% 폴백)
    before_debt = len(df)
    is_financial = df['업종'].astype(str).str.contains('은행|증권|카드|보험|금융|투자', na=False)
    is_high_prepayment = df['업종'].astype(str).str.contains('조선|우주항공|방위|기계|방산', na=False)
    has_row_adj = df['실질부채비율'].notna()
    df = df[
        is_financial
        | (has_row_adj & (df['실질부채비율_fill'] <= 150.0))
        | (~has_row_adj & ((df['부채비율_fill'] <= 150.0) | (is_high_prepayment & (df['부채비율_fill'] <= 500.0))))
    ]
    print(f"[*] After Debt Ratio filtering (Adjusted<=150% or Legacy<=150%/500%): {before_debt} -> {len(df)} stocks remaining")

    # 6. 시장구분(KOSPI / KOSDAQ) 매핑 및 시장별 독립 스코어링
    market_map = resolve_market_map(df)
    df = df.copy()
    df['market'] = df['code'].map(market_map).fillna('UNKNOWN')

    kospi_pool = score_market_pool(df[df['market'] == 'KOSPI'])
    kosdaq_pool = score_market_pool(df[df['market'] == 'KOSDAQ'])
    unknown_cnt = int((df['market'] == 'UNKNOWN').sum())
    print(f"[*] Market Pool Split -> KOSPI: {len(kospi_pool)}개, KOSDAQ: {len(kosdaq_pool)}개 (미매핑 제외: {unknown_cnt}개)")

    # 7. 시장별 상위 30개 종목 추출 (총 60개 후보)
    top_kospi = kospi_pool.sort_values(by='unified_score', ascending=False).head(30).copy()
    top_kosdaq = kosdaq_pool.sort_values(by='unified_score', ascending=False).head(30).copy()

    def to_dict_list(target_df, score_col):
        result = []
        for _, r in target_df.iterrows():
            result.append({
                "code": r['code'],
                "name": str(r['종목명']),
                "market": str(r['market']),
                "sector": str(r['업종']),
                "current_price": 0.0,
                "target_price": float(r['목표주가']) if ('목표주가' in target_df.columns and pd.notna(r['목표주가'])) else 0.0,
                "upside": 0.0,
                "roe": float(r['ROE_fill']),
                "pbr": float(r['PBR_fill']),
                "per": float(r['PER']) if pd.notna(r['PER']) else 0.0,
                "debt": float(r['부채비율_fill']),
                "adjusted_debt": float(r['실질부채비율_fill']),
                "prepayments": float(r['선수금_fill']),
                "prep_growth": float(r['선수금증감_fill']),
                "op_profit": float(r['영업이익_fill']) if pd.notna(r['영업이익_fill']) else 0.0,
                "operating_margin": float(r['영업이익률_fill']) if pd.notna(r['영업이익률_fill']) else 0.0,
                "op_growth": float(r['영업이익증가율_fill']) if pd.notna(r['영업이익증가율_fill']) else 0.0,
                "rev_growth": float(r['매출액증가율(%)']) if pd.notna(r['매출액증가율(%)']) else 0.0,
                "score": float(r[score_col]),
                "market_cap": float(r['시가총액_fill']),
                "source_file": args.source_file,
                "data_date": datetime.now().strftime('%Y-%m-%d')
            })
        return result

    kospi_results = to_dict_list(top_kospi, 'unified_score')
    kosdaq_results = to_dict_list(top_kosdaq, 'unified_score')

    results_dir = TRADE_DIR / 'results'
    results_dir.mkdir(parents=True, exist_ok=True)

    kospi_file = results_dir / 'financial_kospi_top30.json'
    kosdaq_file = results_dir / 'financial_kosdaq_top30.json'

    with open(kospi_file, 'w', encoding='utf-8') as f:
        json.dump(kospi_results, f, ensure_ascii=False, indent=2)

    with open(kosdaq_file, 'w', encoding='utf-8') as f:
        json.dump(kosdaq_results, f, ensure_ascii=False, indent=2)

    print(f"[+] Step 1 Completed. KOSPI Top {len(kospi_results)} saved to: {kospi_file}")
    print(f"[+] Step 1 Completed. KOSDAQ Top {len(kosdaq_results)} saved to: {kosdaq_file}")

if __name__ == '__main__':
    main()
