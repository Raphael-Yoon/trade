# -*- coding: utf-8 -*-
"""
STEP 2 — 1단계 선출 종목(대형주/상승주 20위) 대상 뉴스·공시 데이터 수집
역할: 1단계 선출 종목의 실시간 데이터·뉴스·공시를 수집하여 pool_context_top20.json을 생성한다.
      생성된 파일은 Claude AI 프롬프트의 입력으로 사용된다.
"""
import sys
import os
import json
import sqlite3
import OpenDartReader
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TRADE_DIR = PROJECT_ROOT / 'trade'

load_dotenv(TRADE_DIR / '.env')
DART_KEY = os.getenv('DART_API_KEY')
SQLITE_PATH = TRADE_DIR / 'trade.db'

# pool_naver_data 임포트
sys.path.append(str(TRADE_DIR))
try:
    from pool_naver_data import get_all_naver_data
except ImportError:
    print("[오류] pool_naver_data.py를 로드할 수 없습니다.")
    sys.exit(1)

def collect_stock_context(cand, rec_type):
    code = cand['code']
    name = cand['name']
    
    try:
        naver_data = get_all_naver_data(code)
    except Exception as e:
        print(f"  [수집 실패] {name}({code}): {e}")
        return None
        
    current_price = naver_data.get('current_price', 0)
    target_price = cand.get('target_price', 0.0)
    if target_price <= 0:
        target_price = naver_data.get('target_price', 0.0)
        
    if current_price <= 0:
        return None
        
    if target_price <= 0:
        # BPS × min(PBR, 3.0) 추정
        bps = naver_data.get('bps', 0)
        pbr = naver_data.get('pbr', 1.0)
        target_price = min(int(bps * min(pbr, 3.0)), int(current_price * 1.5))
        
    upside = round(((target_price - current_price) / current_price) * 100.0, 1)
    
    # DART 공시
    disclosures = []
    if DART_KEY:
        try:
            dart = OpenDartReader(DART_KEY)
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=30)
            df_dart = dart.list(code, start=start_dt.strftime('%Y%m%d'), end=end_dt.strftime('%Y%m%d'))
            if df_dart is not None and len(df_dart) > 0:
                for _, row in df_dart.iterrows():
                    raw_dt = str(row.get('rcept_dt', ''))
                    rcept_dt = f"{raw_dt[:4]}-{raw_dt[4:6]}-{raw_dt[6:]}" if len(raw_dt) == 8 else raw_dt
                    disclosures.append({
                        'report_nm': row.get('report_nm', ''),
                        'rcept_dt': rcept_dt,
                        'rcept_no': row.get('rcept_no', ''),
                        'flr_nm': row.get('flr_nm', ''),
                        'corp_cls': row.get('corp_cls', ''),
                        'rm': row.get('rm', ''),
                        'link': f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={row.get('rcept_no', '')}"
                    })
        except Exception:
            pass

    # 기술적 이동평균선 격차
    ma5_diff = naver_data.get('ma5_diff', 0.0)
    ma20_diff = naver_data.get('ma20_diff', 0.0)

    # 뉴스 수집
    news = [
        {'title': n.get('title', ''), 'link': n.get('link', ''), 'source': n.get('source', ''), 'date': n.get('date', ''), 'body': n.get('body', '')}
        for n in naver_data.get('news', [])[:10]
    ]

    return {
        'code': code,
        'name': name,
        'sector': cand['sector'],
        'current_price': current_price,
        'target_price': target_price,
        'upside': upside,
        'roe': cand['roe'],
        'pbr': cand['pbr'],
        'per': cand['per'],
        'debt': cand['debt'],
        'score': cand['score'],  # 1단계 정량 스코어
        'ma5_diff': round(ma5_diff, 2),
        'ma20_diff': round(ma20_diff, 2),
        'foreign_5d_net': naver_data.get('foreign_5d_net', 0),
        'inst_5d_net': naver_data.get('inst_5d_net', 0),
        'price_position_52w': naver_data.get('price_position_52w', 50.0),
        'news': news,
        'disclosures': disclosures,
        'rec_type': rec_type,
        'data_date': cand['data_date']
    }

def main():
    print("[*] Starting Step 2 Context Collection...")
    results_dir = TRADE_DIR / 'results'
    large_cap_file = results_dir / 'financial_large_cap_top20.json'
    value_file = results_dir / 'financial_value_top20.json'
    momentum_file = results_dir / 'financial_momentum_top20.json'

    if not large_cap_file.exists() or not value_file.exists() or not momentum_file.exists():
        print("[오류] 1단계 결과 파일 중 누락된 것이 있습니다.")
        sys.exit(1)

    with open(large_cap_file, 'r', encoding='utf-8') as f:
        large_cap_candidates = json.load(f)
    with open(value_file, 'r', encoding='utf-8') as f:
        value_candidates = json.load(f)
    with open(momentum_file, 'r', encoding='utf-8') as f:
        momentum_candidates = json.load(f)

    all_candidates = []
    
    print("[*] Collecting data for Large-caps...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(collect_stock_context, cand, 'large_cap'): cand for cand in large_cap_candidates}
        for future in as_completed(futures):
            res = future.result()
            if res:
                all_candidates.append(res)

    print("[*] Collecting data for Value-stocks...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(collect_stock_context, cand, 'value'): cand for cand in value_candidates}
        for future in as_completed(futures):
            res = future.result()
            if res:
                all_candidates.append(res)

    print("[*] Collecting data for Rising-stocks...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(collect_stock_context, cand, 'momentum'): cand for cand in momentum_candidates}
        for future in as_completed(futures):
            res = future.result()
            if res:
                all_candidates.append(res)

    # 결과를 context JSON으로 저장
    context_file = results_dir / 'pool_context_top20.json'
    with open(context_file, 'w', encoding='utf-8') as f:
        json.dump(all_candidates, f, ensure_ascii=False, indent=2)

    print(f"[+] Step 2 Context Collection Completed. Saved to: {context_file}")

if __name__ == '__main__':
    main()
