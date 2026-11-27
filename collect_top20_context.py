# -*- coding: utf-8 -*-
"""
STEP 2 — 1단계 선출 종목(KOSPI 30 + KOSDAQ 30, 총 60종목) 대상 뉴스·공시·수급 데이터 수집
역할: 1단계 선출 후보군의 실시간 주가·뉴스·공시·수급을 수집하여 pool_context_top20.json을 생성한다.
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

def collect_stock_context(cand, rec_type, dart_client=None):
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
    
    # DART 공시 (1차: 공유 OpenDartReader, 2차 폴백: 네이버 모바일 공시 API)
    disclosures = []
    if dart_client is not None:
        try:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=30)
            df_dart = dart_client.list(code, start=start_dt.strftime('%Y%m%d'), end=end_dt.strftime('%Y%m%d'))
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

    if not disclosures:
        try:
            import requests as _req
            headers = {'User-Agent': 'Mozilla/5.0'}
            disc_url = f"https://m.stock.naver.com/api/stock/{code}/disclosure"
            d_res = _req.get(disc_url, headers=headers, timeout=5)
            if d_res.status_code == 200:
                d_items = d_res.json()
                for item in d_items[:5]:
                    dt_raw = str(item.get('datetime', ''))
                    rcept_dt = dt_raw.split('T')[0] if 'T' in dt_raw else dt_raw
                    disclosures.append({
                        'report_nm': item.get('title', ''),
                        'rcept_dt': rcept_dt,
                        'rcept_no': str(item.get('disclosureId', '')),
                        'flr_nm': item.get('author', ''),
                        'corp_cls': '',
                        'rm': '',
                        'link': f"https://m.stock.naver.com/domestic/stock/{code}/disclosure"
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
        'market': cand.get('market', rec_type.upper()),
        'sector': cand['sector'],
        'current_price': current_price,
        'target_price': target_price,
        'upside': upside,
        'roe': cand['roe'],
        'pbr': cand['pbr'],
        'per': cand['per'],
        'debt': cand['debt'],
        'adjusted_debt': cand.get('adjusted_debt', cand['debt']),
        'prepayments': cand.get('prepayments', 0.0),
        'prep_growth': cand.get('prep_growth', 0.0),
        'operating_margin': cand.get('operating_margin', 0.0),
        'op_profit': cand.get('op_profit', 0.0),
        'op_growth': cand.get('op_growth', 0.0),
        'rev_growth': cand.get('rev_growth', 0.0),
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
    print("[*] Starting Step 2 Context Collection (KOSPI 30 + KOSDAQ 30)...")
    results_dir = TRADE_DIR / 'results'
    kospi_file = results_dir / 'financial_kospi_top30.json'
    kosdaq_file = results_dir / 'financial_kosdaq_top30.json'

    if not kospi_file.exists() or not kosdaq_file.exists():
        print("[오류] 1단계 결과 파일(financial_kospi_top30.json / financial_kosdaq_top30.json)이 누락되었습니다.")
        sys.exit(1)

    with open(kospi_file, 'r', encoding='utf-8') as f:
        kospi_candidates = json.load(f)
    with open(kosdaq_file, 'r', encoding='utf-8') as f:
        kosdaq_candidates = json.load(f)

    # OpenDartReader 1회 초기화 후 전 스레드 공유 (중복 초기화 병목 방지)
    dart_client = None
    if DART_KEY:
        try:
            dart_client = OpenDartReader(DART_KEY)
            print("[*] OpenDartReader initialized once for shared use.")
        except Exception as e:
            print(f"[!] OpenDartReader 초기화 실패 (네이버 공시 폴백 사용): {e}")

    all_candidates = []

    print(f"[*] Collecting data for KOSPI candidates ({len(kospi_candidates)} stocks)...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(collect_stock_context, cand, 'kospi', dart_client): cand for cand in kospi_candidates}
        for future in as_completed(futures):
            res = future.result()
            if res:
                all_candidates.append(res)

    print(f"[*] Collecting data for KOSDAQ candidates ({len(kosdaq_candidates)} stocks)...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(collect_stock_context, cand, 'kosdaq', dart_client): cand for cand in kosdaq_candidates}
        for future in as_completed(futures):
            res = future.result()
            if res:
                all_candidates.append(res)

    # 결과를 context JSON으로 저장
    context_file = results_dir / 'pool_context_top20.json'
    with open(context_file, 'w', encoding='utf-8') as f:
        json.dump(all_candidates, f, ensure_ascii=False, indent=2)

    print(f"[+] Step 2 Context Collection Completed ({len(all_candidates)} stocks). Saved to: {context_file}")

if __name__ == '__main__':
    main()
