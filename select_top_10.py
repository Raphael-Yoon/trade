# -*- coding: utf-8 -*-
import sys
import os
import json
import sqlite3
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Windows 콘솔 UTF-8 설정
if os.name == 'nt':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

# db_lock_guard 임포트
import db_lock_guard

# Add trade path to import get_all_naver_data
sys.path.append('c:/Pythons/trade')
from get_all_naver_data import get_all_naver_data

def evaluate_single_candidate(cand, pool, disclosures_map):
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
    disclosures = disclosures_map.get(code, [])
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
    roe_score = min(100.0, max(0.0, roe)) / 50.0 * 100.0
    
    f_net = naver_data.get('foreign_5d_net', 0)
    i_net = naver_data.get('inst_5d_net', 0)
    supply_score = 0.0
    if f_net > 0:
        supply_score += 60.0
    if i_net > 0:
        supply_score += 40.0
        
    momentum_score = naver_data.get('price_position_52w', 50.0)
    upside_score = min(100.0, max(0.0, upside)) / 100.0 * 100.0
    
    news_list = naver_data.get('news', [])
    news_score = 80.0
    news_headlines = []
    for n in news_list:
        headline = n.get('title', '')
        news_headlines.append(n)
        if any(kwd in headline for kwd in ['수주', '계약', '최고', '실적', '흑자', '호재', '급등', '상승']):
            news_score += 5.0
        if any(kwd in headline for kwd in ['소송', '적자', '감소', '하락', '과징금', '우려']):
            news_score -= 5.0
    news_score = min(100.0, max(0.0, news_score))
    
    # Disclosure Modifier (+5 / -5)
    disc_modifier = 0.0
    for report_nm, rcept_dt, rm in disclosures:
        if any(kwd in report_nm for kwd in ['단일판매', '공급계약', '특허', '수주', 'MOU']):
            disc_modifier += 5.0
        if any(kwd in report_nm for kwd in ['소송', '피소', '유상증자', '전환사채']):
            disc_modifier -= 5.0
            
    disc_modifier = min(10.0, max(-10.0, disc_modifier))
    
    # Total score
    base_score = (roe_score * 0.30) + (supply_score * 0.20) + (momentum_score * 0.20) + (upside_score * 0.20) + (news_score * 0.10)
    final_score = base_score + disc_modifier
    final_score = round(min(100.0, max(0.0, final_score)), 2)
    
    sector = naver_data.get('industry_name', '기타')
    reason = f"[{sector}] 뉴스:{int(news_score)}점 | ROE {roe:.1f}% | 수급 외인{'유입' if f_net > 0 else '이탈'}/기관{'유입' if i_net > 0 else '이탈'}"
    if disc_modifier != 0:
        reason += f" | 공시변동({int(disc_modifier):+d})"
        
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
    candidates_file = 'C:/Users/Yuju/.gemini/antigravity-ide/brain/2b2404c7-070a-4a0c-be5a-eedfc837ee67/scratch/candidates.json'
    if not os.path.exists(candidates_file):
        print(f"[오류] 후보군 파일이 존재하지 않습니다: {candidates_file}")
        sys.exit(1)
        
    with open(candidates_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    fast_track = data['fast_track']
    pool = data['pool']
    
    seen_codes = set()
    candidates = []
    
    for item in fast_track:
        if item['code'] not in seen_codes:
            seen_codes.add(item['code'])
            candidates.append({'code': item['code'], 'name': item['name'], 'is_fast_track': True})
            
    for item in pool:
        if item['code'] not in seen_codes:
            seen_codes.add(item['code'])
            candidates.append({'code': item['code'], 'name': item['name'], 'is_fast_track': False})
            
    conn = sqlite3.connect('c:/Pythons/trade/trade.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT code, report_nm, rcept_dt, rm 
        FROM stock_disclosures
        WHERE rcept_dt >= date('now', '-30 days')
    """)
    disclosures_map = {}
    for code, report_nm, rcept_dt, rm in cursor.fetchall():
        if code not in disclosures_map:
            disclosures_map[code] = []
        disclosures_map[code].append((report_nm, rcept_dt, rm))
        
    conn.close()
    
    results = []
    print(f"Total candidates to evaluate in parallel: {len(candidates)}")
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(evaluate_single_candidate, cand, pool, disclosures_map): cand for cand in candidates}
        
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
    
    output_path = 'c:/Pythons/cowork/recommendations.json'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(top_10, f, ensure_ascii=False, indent=2)
        
    print(f"\nSuccessfully selected Top 10 recommendations and wrote to {output_path}!")
    for idx, r in enumerate(top_10):
        print(f"{idx+1}. [{r['code']}] {r['name']} - Score: {r['score']} - Upside: {r['upside']}% - Reason: {r['reason']}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='감사팀 Top 10 우량 공략주 선정 엔진')
    parser.add_argument('--force-db', action='store_true', help='서버가 실행 중이라도 실행을 강제합니다.')
    args = parser.parse_args()
    
    # DB 락 충돌 방지 가드 체크 (감사팀 작업)
    db_lock_guard.check_lock_and_exit("감사팀 Top 10 공략주 선정")
    
    run_selection()
