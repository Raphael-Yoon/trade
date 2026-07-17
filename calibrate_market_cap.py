# -*- coding: utf-8 -*-
"""
시가총액 보정 유틸리티 스크립트
기존에 수집된 엑셀 파일 내 모든 종목의 시가총액을 네이버 실시간 API를 통해 초고속 재수집하여 보정합니다.
사용법: python calibrate_market_cap.py --source_file results/[엑셀파일명].xlsx
"""
import os
import sys
import argparse
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from openpyxl import load_workbook

# Windows 콘솔 UTF-8 설정
if os.name == 'nt':
    sys.stdout.reconfigure(encoding='utf-8')

def fetch_realtime_market_cap(code):
    """네이버 실시간 폴링 API를 이용하여 상장주식수와 현재가를 파싱한 뒤 시가총액(억원)을 계산합니다."""
    code_str = str(code).strip().zfill(6)
    url = f"https://polling.finance.naver.com/api/realtime?query=SERVICE_ITEM:{code_str}"
    try:
        res = requests.get(url, timeout=5)
        if res.status_code == 200:
            data = res.json()
            areas = data.get('result', {}).get('areas', [])
            if areas and areas[0].get('datas'):
                item = areas[0]['datas'][0]
                # 현재가(nv) 또는 기준가(sv) 또는 전일종가(pcv)
                price = item.get('nv') or item.get('sv') or item.get('pcv') or 0
                count_stock = item.get('countOfListedStock') or 0
                if price > 0 and count_stock > 0:
                    # 시가총액 (원 단위) -> 억 단위 환산
                    market_cap_won = count_stock * price
                    market_cap_billion = int(market_cap_won // 100000000)
                    return code_str, market_cap_billion, price
    except Exception as e:
        pass
    return code_str, None, None

def main():
    parser = argparse.ArgumentParser(description="수집 엑셀 데이터 시가총액 보정 도구")
    parser.add_argument('--source_file', required=True, help="보정할 엑셀 파일 경로 (예: results/kospi,kosdaq_all_xxx.xlsx)")
    args = parser.parse_args()
    
    excel_path = args.source_file
    if not os.path.exists(excel_path):
        print(f"[오류] 엑셀 파일이 존재하지 않습니다: {excel_path}")
        sys.exit(1)
        
    print(f"[*] 엑셀 파일 로드 중: {excel_path}")
    df = pd.read_excel(excel_path)
    
    # 종목코드 또는 코드 컬럼 확인
    code_col = None
    for col in ['종목코드', '코드']:
        if col in df.columns:
            code_col = col
            break
            
    if not code_col:
        print("[오류] 엑셀 파일에 '종목코드' 또는 '코드' 컬럼이 없습니다.")
        sys.exit(1)
        
    # 코드 정형화 (6자리 문자열)
    df[code_col] = df[code_col].astype(str).str.strip().str.zfill(6)
    codes = df[code_col].tolist()
    names = df['종목명'].tolist() if '종목명' in df.columns else ["" for _ in codes]
    
    print(f"[*] 총 {len(codes)}개 종목의 실시간 시가총액 정보 수집 시작...")
    
    market_cap_map = {}
    price_map = {}
    success_count = 0
    
    # ThreadPool을 활용한 비동기 초고속 수집 (최대 30개 스레드)
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = {executor.submit(fetch_realtime_market_cap, code): (code, name) for code, name in zip(codes, names)}
        
        for i, future in enumerate(as_completed(futures), 1):
            code, name = futures[future]
            try:
                code_str, cap_billion, price = future.result()
                if cap_billion is not None:
                    market_cap_map[code_str] = cap_billion
                    price_map[code_str] = price
                    success_count += 1
                if i % 100 == 0 or i == len(codes):
                    print(f"    - 진행률: {i}/{len(codes)} 완료 (성공: {success_count}개)")
            except Exception as e:
                pass
                
    print(f"[+] 실시간 시가총액 수집 완료! (성공: {success_count}/{len(codes)} 종목)")
    
    # 엑셀 데이터 업데이트
    print("[*] 엑셀 데이터 보정 및 저장 중...")
    
    # 기존 시가총액/PBR/PER 값을 Naver 실시간 값으로 치환
    updated_count = 0
    for idx, row in df.iterrows():
        code_str = row[code_col]
        if code_str in market_cap_map:
            old_val = row.get('시가총액')
            new_val = market_cap_map[code_str]
            price = price_map[code_str]
            
            df.at[idx, '시가총액'] = new_val
            
            # 실시간 PBR 재계산 (Price / BPS)
            if 'BPS' in df.columns:
                bps = pd.to_numeric(row.get('BPS'), errors='coerce')
                if pd.notna(bps) and bps > 0 and price > 0:
                    df.at[idx, 'PBR'] = round(price / bps, 2)
                    
            # 실시간 PER 재계산 (Price / EPS)
            if 'EPS' in df.columns:
                eps = pd.to_numeric(row.get('EPS'), errors='coerce')
                if pd.notna(eps) and eps > 0 and price > 0:
                    df.at[idx, 'PER'] = round(price / eps, 2)
                elif pd.notna(eps) and eps <= 0:
                    df.at[idx, 'PER'] = 0.0 # 적자 또는 EPS 0 이하
            
            if pd.isna(old_val) or old_val != new_val:
                updated_count += 1
                
    # 파일 오버라이트 저장
    df.to_excel(excel_path, index=False)
    print(f"[+] 보정 완료! 총 {updated_count}개 종목의 시가총액 정보가 갱신되어 저장되었습니다.")
    print(f"    - 파일 경로: {excel_path}")

if __name__ == '__main__':
    main()
