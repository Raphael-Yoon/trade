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
            
    # 폴백: 로컬 trade 폴더에서 검색
    if df is None:
        local_path = os.path.join(PROJECT_ROOT, 'trade', args.source_file)
        if os.path.exists(local_path):
            print(f"[*] Loading from fallback local path: {local_path}...")
            df = pd.read_excel(local_path)
            print("[+] Successfully loaded from fallback path.")
            
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

    # 3. 바이오/제약/헬스케어 하드 필터링
    before_bio = len(df)
    df = df[~df['업종'].astype(str).str.contains('제약|바이오|건강관리|헬스케어', na=False)]
    print(f"[*] After Bio/Healthcare filtering: {before_bio} -> {len(df)} stocks remaining")

    # 4. 수치 변환
    numeric_cols = ['PBR', 'PER', 'ROE', '시가총액', '매출액', '영업이익', '당기순이익', 
                    '부채비율', '매출액증가율(%)', '영업이익증가율(%)', '순이익증가율(%)', '영업이익률', '순이익률']
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
    df['시가총액_fill'] = df['시가총액'].fillna(0.0)
    df['매출액_fill'] = df['매출액'].fillna(0.0)
    df['영업이익_val'] = df['영업이익'].fillna(0.0)

    # 자산총계 컬럼 처리
    if '자산총계' in df.columns:
        df['자산총계_fill'] = pd.to_numeric(df['자산총계'], errors='coerce').fillna(0.0)
    else:
        df['자산총계_fill'] = 0.0

    # 5. 부채비율 필터링 (금융업 제외 150% 이하)
    before_debt = len(df)
    is_financial = df['업종'].astype(str).str.contains('은행|증권|카드|보험|금융|투자', na=False)
    is_high_prepayment = df['업종'].astype(str).str.contains('조선|항공우주와국방', na=False)
    df = df[(df['부채비율_fill'] <= 150.0) | is_financial | (is_high_prepayment & (df['부채비율_fill'] <= 500.0))]
    print(f"[*] After Debt Ratio filtering (<=150% except Finance/Shipbuilding/Defense): {before_debt} -> {len(df)} stocks remaining")

    # 6. 스코어링 계산
    # (a) 대형주 (Large Cap) 스코어 계산 (규모 60% + ROE 30% + 부채비율 10%, PBR은 완전히 배제하여 가치주와 차별화)
    # 규모(Size)는 자산총계/매출액/시가총액 백분위의 결합 지표로 산출
    is_mc_valid = (df[df['종목명'].isin(['삼성전자', 'SK하이닉스'])]['시가총액_fill'] > 0).any()
    
    asset_pct = df['자산총계_fill'].rank(pct=True) if (df['자산총계_fill'] > 0).any() else df['매출액_fill'].rank(pct=True)
    rev_pct = df['매출액_fill'].rank(pct=True)
    mc_pct = df['시가총액_fill'].rank(pct=True)
    
    if is_mc_valid:
        size_pct = (asset_pct * 0.4 + rev_pct * 0.3 + mc_pct * 0.3)
    else:
        size_pct = (asset_pct * 0.6 + rev_pct * 0.4)

    roe_pct = df['ROE_fill'].rank(pct=True)
    valuation_pct = 1 - df['PBR_fill'].rank(pct=True)
    health_pct = 1 - df['부채비율_fill'].rank(pct=True)

    df['large_cap_score'] = (
        size_pct * 0.60 +
        roe_pct * 0.30 +
        health_pct * 0.10
    ) * 100

    # (b) 가치주 (Value) 스코어 계산 (저평가/PBR 위주)
    df['value_score'] = (
        valuation_pct * 0.45 +
        roe_pct * 0.30 +
        size_pct * 0.15 +
        health_pct * 0.10
    ) * 100

    # (c) 상승주 (Momentum) 스코어 계산 (Rule of 40 스코어링 가중 30% 반영)
    df['rule_of_40'] = df['매출액증가율_fill'] + df['영업이익률_fill']
    rule_of_40_pct = df['rule_of_40'].rank(pct=True)
    
    op_growth_pct = df['영업이익증가율_fill'].rank(pct=True)
    op_margin_pct = df['영업이익률_fill'].rank(pct=True)
    
    df['momentum_score'] = (
        rule_of_40_pct * 0.30 +
        op_growth_pct * 0.30 +
        op_margin_pct * 0.20 +
        roe_pct * 0.20
    ) * 100

    # 7. 상위 20개 종목 추출
    def format_code(c):
        c_str = str(c).strip()
        return c_str.zfill(6) if len(c_str) < 6 else c_str

    df['code'] = df['종목코드'].apply(format_code)

    # 우선주 및 상장자산(리츠, ETN, ETF, 스팩) 배제 필터 적용
    df = df[~df['종목명'].astype(str).str.endswith(('우', '우B', '우C', '우(전환)', '3우B'))]
    df = df[~df['종목명'].astype(str).str.contains('리츠|ETN|ETF|스팩|레버리지|선물|인버스|Koosec', na=False)]

    # 대형주(Large Cap) 풀: 자산총계 2조 원 또는 매출액 2조 원 이상의 대기업군 결합 필터링
    if (df['자산총계_fill'] > 0).any():
        print("[*] 대형주 선별 기준: 자산총계 2조 원 또는 매출액 2조 원 이상 기업 적용")
        large_cap_pool = df[(df['자산총계_fill'] >= 2000000000000.0) | (df['매출액_fill'] >= 2000000000000.0)]
    else:
        print("[*] 대형주 선별 기준: 매출액 2조 원 이상 (자산총계 누락으로 폴백 적용)")
        large_cap_pool = df[df['매출액_fill'] >= 2000000000000.0]
    
    # 가치주(Value) 풀: 자산총계 5천억 원 또는 매출액 5천억 원 이상의 중견/대기업군 결합 필터링
    if (df['자산총계_fill'] > 0).any():
        print("[*] 가치주 선별 기준: 자산총계 5천억 원 또는 매출액 5천억 원 이상 기업 적용")
        value_pool = df[(df['자산총계_fill'] >= 500000000000.0) | (df['매출액_fill'] >= 500000000000.0)]
    else:
        print("[*] 가치주 선별 기준: 매출액 5천억 원 이상 (자산총계 누락으로 폴백 적용)")
        value_pool = df[df['매출액_fill'] >= 500000000000.0]
    
    # 상승주(Momentum) 풀: 자산총계 1천억 원 또는 매출액 1천억 원 이상 & Rule of 40 >= 30% 결합 필터링
    if (df['자산총계_fill'] > 0).any():
        print("[*] 상승주 선별 기준: 자산총계/매출액 1천억 원 이상 & Rule of 40 >= 30% 적용")
        momentum_pool = df[((df['자산총계_fill'] >= 100000000000.0) | (df['매출액_fill'] >= 100000000000.0)) & (df['rule_of_40'] >= 30.0)]
    else:
        print("[*] 상승주 선별 기준: 매출액 1천억 원 이상 & Rule of 40 >= 30% (자산총계 누락으로 폴백 적용)")
        momentum_pool = df[(df['매출액_fill'] >= 100000000000.0) & (df['rule_of_40'] >= 30.0)]
    
    top_large_cap = large_cap_pool.sort_values(by='large_cap_score', ascending=False).head(20).copy()
    top_value = value_pool.sort_values(by='value_score', ascending=False).head(20).copy()
    top_momentum = momentum_pool.sort_values(by='momentum_score', ascending=False).head(20).copy()

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
                "score": float(r[score_col]),
                "market_cap": float(r['시가총액_fill']),
                "source_file": args.source_file,
                "data_date": datetime.now().strftime('%Y-%m-%d')
            })
        return result

    large_cap_results = to_dict_list(top_large_cap, 'large_cap_score')
    value_results = to_dict_list(top_value, 'value_score')
    momentum_results = to_dict_list(top_momentum, 'momentum_score')

    results_dir = os.path.join(PROJECT_ROOT, 'trade', 'results')
    os.makedirs(results_dir, exist_ok=True)

    large_cap_file = os.path.join(results_dir, 'financial_large_cap_top20.json')
    value_file = os.path.join(results_dir, 'financial_value_top20.json')
    momentum_file = os.path.join(results_dir, 'financial_momentum_top20.json')

    with open(large_cap_file, 'w', encoding='utf-8') as f:
        json.dump(large_cap_results, f, ensure_ascii=False, indent=2)

    with open(value_file, 'w', encoding='utf-8') as f:
        json.dump(value_results, f, ensure_ascii=False, indent=2)

    with open(momentum_file, 'w', encoding='utf-8') as f:
        json.dump(momentum_results, f, ensure_ascii=False, indent=2)

    print(f"[+] Step 1 Completed. Large-caps saved to: {large_cap_file}")
    print(f"[+] Step 1 Completed. Value-stocks saved to: {value_file}")
    print(f"[+] Step 1 Completed. Rising-stocks saved to: {momentum_file}")

if __name__ == '__main__':
    main()
