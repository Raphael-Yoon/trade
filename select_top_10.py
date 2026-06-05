# -*- coding: utf-8 -*-
import sys
import os
import json
import psycopg2
import psycopg2.extras
import argparse
import OpenDartReader
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Windows 콘솔 UTF-8 설정
if os.name == 'nt':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

# Add trade path to import get_all_naver_data
sys.path.append('c:/Pythons/trade')
from get_all_naver_data import get_all_naver_data

def evaluate_single_candidate(cand, pool, dart_key):
    code = cand['code']
    name = cand['name']
    
    if name.endswith(('우', '우B', '우C', '우(전환)', '3우B')):
        return None
        
    try:
        naver_data = get_all_naver_data(code)
    except Exception as e:
        print(f"Error fetching data for {name} ({code}): {e}")
        return None
        
    current_price = naver_data.get('current_price', 0)
    target_price = naver_data.get('target_price', 0)
    roe = naver_data.get('roe', 0.0)
    debt = naver_data.get('debt_ratio', 0.0)
    
    # Target price fallback
    if target_price == 0:
        for p_item in pool:
            if p_item['code'] == code:
                target_price = p_item.get('target_price', 0)
                break
                
    ma5_diff = naver_data.get('ma5_diff', 0.0)
    ma20_diff = naver_data.get('ma20_diff', 0.0)
    
    # Hard Filter A: Upside must be positive
    if target_price <= 0 or current_price <= 0:
        return None
        
    upside = ((target_price - current_price) / current_price) * 100.0
    if upside <= 0:
        return None
        
    # Hard Filter B: Bad disclosures (audit, internal control, embezzlement/trust breach)
    disclosures = []
    if dart_key:
        try:
            dart = OpenDartReader(dart_key)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            df = dart.list(code, start=start_date.strftime('%Y%m%d'), end=end_date.strftime('%Y%m%d'))
            if df is not None and len(df) > 0:
                for _, row in df.iterrows():
                    raw_dt = str(row.get('rcept_dt', ''))
                    rcept_dt = f"{raw_dt[:4]}-{raw_dt[4:6]}-{raw_dt[6:]}" if len(raw_dt) == 8 else raw_dt
                    disclosures.append((row.get('report_nm', ''), rcept_dt, row.get('rm', '')))
        except Exception as e:
            # 개별 종목 공시 조회 오류는 감수하고 필터링 건너뜀
            pass

    is_filtered = False
    for report_nm, rcept_dt, rm in disclosures:
        if any(term in report_nm for term in ['감사의견', '의견거절', '부적정', '한정', '내부회계', '배임', '횡령']):
            is_filtered = True
            break
    if is_filtered:
        return None
        
    # Hard Filter C: Short-term Trend Filter
    if ma5_diff <= 0 and ma20_diff <= 0:
        return None
        
    # Compute Scoring
    roe_score = (min(100.0, max(0.0, roe)) / 50.0) * 100.0
    
    f_net = naver_data.get('foreign_5d_net', 0)
    i_net = naver_data.get('inst_5d_net', 0)
    
    foreign_subscore = 100.0 if f_net > 0 else (0.0 if f_net == 0 else -10.0)
    inst_subscore = 100.0 if i_net > 0 else 0.0
    supply_score = (foreign_subscore * 0.6) + (inst_subscore * 0.4)
        
    momentum_score = naver_data.get('price_position_52w', 50.0)
    upside_score = (min(100.0, max(0.0, upside)) / 100.0) * 100.0
    
    # 뉴스 헤드라인 수집 및 심리 점수 계산
    news_list = naver_data.get('news', [])
    news_headlines = [n for n in news_list]
    
    pos_keywords = ['수주', '계약', '상승', '호재', '최고', '급등', '돌파', '흑자', '성장', '개선', '대규모', '신기록', '호실적']
    neg_keywords = ['소송', '피소', '하락', '악재', '급락', '적자', '감소', '횡령', '배임', '위기', '우려', '이탈', '손실']
    
    pos_count = 0
    neg_count = 0
    for n in news_headlines[:10]:
        title = n.get('title', '')
        for kw in pos_keywords:
            if kw in title:
                pos_count += 1
                break
        for kw in neg_keywords:
            if kw in title:
                neg_count += 1
                break
                
    news_score = 60.0
    if pos_count > neg_count:
        news_score = min(100.0, 80.0 + (pos_count - neg_count) * 10.0)
    elif neg_count > pos_count:
        news_score = max(0.0, 40.0 - (neg_count - pos_count) * 10.0)

    # 공시 모멘텀 가감점 계산
    disclosure_adjustment = 0.0
    for report_nm, rcept_dt, rm in disclosures:
        if any(term in report_nm for term in ['단일판매', '공급계약', '특허']):
            disclosure_adjustment += 5.0
        if any(term in report_nm for term in ['소송', '피소', '유상증자']):
            disclosure_adjustment -= 5.0
    disclosure_adjustment = max(-5.0, min(5.0, disclosure_adjustment))
 
    # Total score (audit_logic.md: ROE 30%, Supply 20%, Momentum 20%, Upside 20%, News 10% + Disclosure Adj)
    base_score = (roe_score * 0.30) + (supply_score * 0.20) + (momentum_score * 0.20) + (upside_score * 0.20) + (news_score * 0.10)
    final_score = round(min(100.0, max(0.0, base_score + disclosure_adjustment)), 2)
 
    sector = naver_data.get('industry_name', '기타')
    reason = f"[{sector}] ROE {roe:.1f}% | 수급 외인{'유입' if f_net > 0 else '이탈'}/기관{'유입' if i_net > 0 else '이탈'} | 상승여력 {round(upside, 1)}% | 뉴스 {round(news_score, 1)}점 | 공시조정 {disclosure_adjustment:+.1f}점"
 
    news_summary = ""
    if news_headlines:
        news_summary = "\n".join([f"[{n.get('source', '네이버 금융')}] {n.get('title', '')} ({n.get('date', '')}) | {n.get('link', '')}" for n in news_headlines[:4]])
        
    return {
        "code": code,
        "name": name,
        "current_price": current_price,
        "target_price": target_price,
        "upside": round(upside, 1),
        "roe": roe,
        "debt": debt,
        "score": final_score,
        "reason": reason,
        "news_summary": news_summary.strip()
    }

def run_selection():
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
    database_url = os.getenv('DATABASE_URL')
    dart_key = os.getenv('DART_API_KEY')

    conn = psycopg2.connect(database_url, cursor_factory=psycopg2.extras.DictCursor)
    cursor = conn.cursor()

    # Fetch candidates from the database stock_pool table
    cursor.execute("SELECT code, name, target_price, roe, debt_ratio FROM stock_pool")
    rows = cursor.fetchall()

    pool = []
    seen_codes = set()
    candidates = []

    for row in rows:
        code = row['code']
        name = row['name']
        target_price = row['target_price'] or 0
        roe = row['roe'] or 0.0
        debt = row['debt_ratio'] or 0.0

        pool_item = {'code': code, 'name': name, 'target_price': target_price, 'roe': roe, 'debt': debt}
        pool.append(pool_item)

        if code not in seen_codes:
            seen_codes.add(code)
            candidates.append({'code': code, 'name': name})

    conn.close()
    
    results = []
    print(f"Total candidates to evaluate in parallel: {len(candidates)}")
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(evaluate_single_candidate, cand, pool, dart_key): cand for cand in candidates}
        
        completed_count = 0
        for future in as_completed(futures):
            completed_count += 1
            res = future.result()
            if res:
                results.append(res)
            if completed_count % 10 == 0 or completed_count == len(candidates):
                print(f"Progress: {completed_count}/{len(candidates)} candidates evaluated...")
                
    results.sort(key=lambda x: x['score'], reverse=True)
    top_10 = results[:10]

    print(f"\nTop 10 선정 완료. DB 적재를 시작합니다.")
    for idx, r in enumerate(top_10):
        print(f"{idx+1}. [{r['code']}] {r['name']} - Score: {r['score']} - Upside: {r['upside']}% - Reason: {r['reason']}")

    # Neon DB audit_recommendations 저장
    try:
        conn = psycopg2.connect(database_url, cursor_factory=psycopg2.extras.DictCursor)
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE audit_recommendations")
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data_date = datetime.now().strftime('%Y-%m-%d')
        
        for idx, r in enumerate(top_10):
            cursor.execute("""
                INSERT INTO audit_recommendations
                    (code, name, current_price, target_price, upside, opinion, data_date, created_at, score, roe, debt, reason, news_summary)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                r['code'], r['name'], float(r['current_price']), float(r['target_price']),
                float(r['upside']), '', data_date, now_str, float(r['score']),
                float(r.get('roe', 0)), float(r.get('debt', 0)), r.get('reason', ''), r.get('news_summary', '')
            ))
        conn.commit()
        conn.close()
        print("[완료] Neon DB audit_recommendations 테이블 적재 성공!")
    except Exception as e:
        print(f"[오류] Neon DB audit_recommendations 적재 실패: {e}")

    return results

if __name__ == '__main__':
    run_selection()
