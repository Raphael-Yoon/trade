# -*- coding: utf-8 -*-
import os
import sys
import io
import argparse
import json
import sqlite3
import re
from datetime import datetime
import pandas as pd
import numpy as np
from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)
sys.path.append(os.path.join(PROJECT_ROOT, 'trade'))

load_dotenv(os.path.join(PROJECT_ROOT, 'trade', '.env'))
DATABASE_URL = os.getenv('DATABASE_URL')
SQLITE_PATH = os.path.join(PROJECT_ROOT, 'trade', 'trade.db')

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
            os.path.join(PROJECT_ROOT, 'trade', 'results', args.source_file),
            os.path.join(PROJECT_ROOT, 'trade', args.source_file),
            args.source_file
        ]
        for p in possible_paths:
            if os.path.exists(p):
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
    df['ROE_fill'] = df['ROE'].fillna(0.0)
    df['PBR_fill'] = df['PBR'].fillna(df['PBR'].median())
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

    # 영업이익률 결측치 산출 보정 (영업이익 / 매출액)
    calc_op_margin = np.where(df['매출액_fill'] > 0, (df['영업이익_val'] / df['매출액_fill']) * 100, 0.0)
    df['영업이익률_fill'] = np.where((df['영업이익률_fill'] <= 0) & (df['매출액_fill'] > 0), calc_op_margin, df['영업이익률_fill'])

    # PBR 밸류에이션 안전마진(Safety Cap) 하드 필터링: PBR 12.0배 초과 투기성 과열주 자동 배제
    before_pbr = len(df)
    df = df[df['PBR_fill'] <= 12.0]
    print(f"[*] After PBR Safety Cap (<= 12.0) filtering: {before_pbr} -> {len(df)} stocks remaining")

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

    # 6. 스코어링 계산
    # 단일 공통 스코어 (Unified Score) 계산 (영업이익 규모 25% + ROE 20% + 영업이익률 15% + 부채비율 안정도 10% + PBR 저평가도 10% + 영업이익 성장률 20%)
    op_profit_pct = df['영업이익_fill'].rank(pct=True)
    roe_pct = df['ROE_fill'].rank(pct=True)
    op_margin_pct = df['영업이익률_fill'].rank(pct=True)
    
    # 실질부채비율 안전구간 평준화:
    # - 100% 이하 안전구간: 0.80 ~ 1.00 (장치/수주/제조업이 10~30% IT기업 대비 과도한 감점을 받지 않도록 평준화)
    # - 100% ~ 150% 허용구간: 0.50 ~ 0.80
    # - 금융업: 레버리지 업종 특성상 중립 기준(0.60) 부여
    # - 미집계 구형 파일 수주업종: 0.80 폴백 유지
    is_fin_filtered = df['업종'].astype(str).str.contains('은행|증권|카드|보험|금융|투자', na=False)
    is_high_prepayment_filtered = df['업종'].astype(str).str.contains('조선|우주항공|방위|기계|방산', na=False)
    has_row_adj_filtered = df['실질부채비율'].notna()
    adj_debt_vals = df['실질부채비율_fill'].clip(lower=0.0)
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
    
    valuation_pct = 1 - df['PBR_fill'].rank(pct=True)
    op_growth_pct = df['영업이익증가율_fill'].rank(pct=True)

    df['unified_score'] = (
        op_profit_pct * 0.25 +
        roe_pct * 0.20 +
        op_margin_pct * 0.15 +
        health_pct * 0.10 +
        valuation_pct * 0.10 +
        op_growth_pct * 0.20
    ) * 100

    # 7. 상위 20개 종목 추출
    def format_code(c):
        c_str = str(c).strip()
        return c_str.zfill(6) if len(c_str) < 6 else c_str

    df['code'] = df['종목코드'].apply(format_code)

    # 우선주 및 상장자산(리츠, ETN, ETF, 스팩) 배제 필터 적용
    df = df[~df['종목명'].astype(str).str.endswith(('우', '우B', '우C', '우(전환)', '3우B'))]
    df = df[~df['종목명'].astype(str).str.contains('리츠|ETN|ETF|스팩|레버리지|선물|인버스|Koosec', na=False)]

    # 대형주(Large Cap) 풀: 자산/매출 2조 원 이상(또는 시총 2조/영업익 5천억 이상) + 최소 시가총액 1조 원 이상 필수 가드
    if (df['자산총계_fill'] > 0).any():
        print("[*] 대형주 선별 기준: 자산/매출 2조 원 이상 및 시총 1조 원 이상 기업 적용")
        large_cap_pool = df[
            ((df['자산총계_fill'] >= 2000000000000.0) | (df['매출액_fill'] >= 2000000000000.0) | (df['시가총액_fill'] >= 20000.0) | (df['영업이익_val'] >= 500000000000.0)) &
            (df['시가총액_fill'] >= 10000.0)
        ]
    else:
        print("[*] 대형주 선별 기준: 매출액 2조 원 이상 (자산총계 누락 폴백 - 시총 1조 원 이상 가드 포함)")
        large_cap_pool = df[
            ((df['매출액_fill'] >= 2000000000000.0) | (df['시가총액_fill'] >= 20000.0) | (df['영업이익_val'] >= 500000000000.0)) &
            (df['시가총액_fill'] >= 10000.0)
        ]
    
    # 중형주(Mid Cap) 풀: 자산총계 5천억~2조 미만(또는 자산 2조 이상이나 시총 3천억~1조 미만 지주사)
    if (df['자산총계_fill'] > 0).any():
        print("[*] 중형주 선별 기준: 자산총계 5천억 원 이상 ~ 2조 원 미만(및 시총 3천억~1조 지주/중견주) 적용")
        mid_cap_pool = df[
            (((df['자산총계_fill'] >= 500000000000.0) & (df['자산총계_fill'] < 2000000000000.0)) |
            ((df['자산총계_fill'] >= 2000000000000.0) & (df['시가총액_fill'] >= 3000.0) & (df['시가총액_fill'] < 10000.0)) |
            (((df['자산총계_fill'] == 0) | df['자산총계_fill'].isna()) & (df['매출액_fill'] >= 500000000000.0) & (df['매출액_fill'] < 2000000000000.0))) &
            (df['시가총액_fill'] < 50000.0)
        ]
    else:
        print("[*] 중형주 선별 기준: 매출액 5천억 원 이상 ~ 2조 원 미만 (자산총계 누락으로 폴백 적용)")
        mid_cap_pool = df[(df['매출액_fill'] >= 500000000000.0) & (df['매출액_fill'] < 2000000000000.0) & (df['시가총액_fill'] < 50000.0)]
    
    # 소형주(Small Cap) 풀: 자산총계 1천억~5천억 미만(또는 자산 5천억 이상이나 시총 3천억 미만 소형주)
    if (df['자산총계_fill'] > 0).any():
        print("[*] 소형주 선별 기준: 자산총계 1천억 원 이상 ~ 5천억 원 미만(및 시총 3천억 미만 소형주) 적용")
        small_cap_pool = df[
            (((df['자산총계_fill'] >= 100000000000.0) & (df['자산총계_fill'] < 500000000000.0)) |
            ((df['자산총계_fill'] >= 500000000000.0) & (df['시가총액_fill'] < 3000.0)) |
            (((df['자산총계_fill'] == 0) | df['자산총계_fill'].isna()) & (df['매출액_fill'] >= 100000000000.0) & (df['매출액_fill'] < 500000000000.0))) &
            (df['시가총액_fill'] < 10000.0) & (df['영업이익_val'] < 300000000000.0)
        ]
    else:
        print("[*] 소형주 선별 기준: 매출액 1천억 원 이상 ~ 5천억 원 미만 (자산총계 누락으로 폴백 적용)")
        small_cap_pool = df[(df['매출액_fill'] >= 100000000000.0) & (df['매출액_fill'] < 500000000000.0) & (df['시가총액_fill'] < 10000.0) & (df['영업이익_val'] < 300000000000.0)]
    
    # 트랙 간 상호 배제 (대형주에 포함된 종목은 중/소형주에서 제외, 중형주는 소형주에서 제외)
    mid_cap_pool = mid_cap_pool[~mid_cap_pool.index.isin(large_cap_pool.index)]
    small_cap_pool = small_cap_pool[~small_cap_pool.index.isin(large_cap_pool.index) & ~small_cap_pool.index.isin(mid_cap_pool.index)]

    top_large_cap = large_cap_pool.sort_values(by='unified_score', ascending=False).head(20).copy()
    top_mid_cap = mid_cap_pool.sort_values(by='unified_score', ascending=False).head(20).copy()
    top_small_cap = small_cap_pool.sort_values(by='unified_score', ascending=False).head(20).copy()

    def to_dict_list(target_df, score_col):
        result = []
        for _, r in target_df.iterrows():
            result.append({
                "code": r['code'],
                "name": str(r['종목명']),
                "sector": str(r['업종']),
                "current_price": 0.0,
                "target_price": float(r['목표주가']) if pd.notna(r['목표주가']) else 0.0,
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

    large_cap_results = to_dict_list(top_large_cap, 'unified_score')
    mid_cap_results = to_dict_list(top_mid_cap, 'unified_score')
    small_cap_results = to_dict_list(top_small_cap, 'unified_score')

    results_dir = os.path.join(PROJECT_ROOT, 'trade', 'results')
    os.makedirs(results_dir, exist_ok=True)

    large_cap_file = os.path.join(results_dir, 'financial_large_cap_top20.json')
    mid_cap_file = os.path.join(results_dir, 'financial_mid_cap_top20.json')
    small_cap_file = os.path.join(results_dir, 'financial_small_cap_top20.json')

    with open(large_cap_file, 'w', encoding='utf-8') as f:
        json.dump(large_cap_results, f, ensure_ascii=False, indent=2)

    with open(mid_cap_file, 'w', encoding='utf-8') as f:
        json.dump(mid_cap_results, f, ensure_ascii=False, indent=2)

    with open(small_cap_file, 'w', encoding='utf-8') as f:
        json.dump(small_cap_results, f, ensure_ascii=False, indent=2)

    print(f"[+] Step 1 Completed. Large-caps saved to: {large_cap_file}")
    print(f"[+] Step 1 Completed. Mid-caps saved to: {mid_cap_file}")
    print(f"[+] Step 1 Completed. Small-caps saved to: {small_cap_file}")

if __name__ == '__main__':
    main()
