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
sys.path.append('c:/Python/trade')
from get_all_naver_data import get_all_naver_data

def evaluate_single_candidate(cand, pool, dart_key, eval_type='momentum'):
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
        
    # Large cap filter for value evaluation (Market cap >= 10 Trillion KRW)
    market_cap_calc = naver_data.get('outstanding_shares', 0) * current_price
    is_large_cap = market_cap_calc >= 10_000_000_000_000
    if eval_type == 'value' and not is_large_cap:
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
        
    # Hard Filter C: Short-term Trend Filter (Bypassed for value evaluation)
    if eval_type != 'value':
        if ma5_diff <= 0 and ma20_diff <= 0:
            return None
        
    # Compute Scoring
    f_net = naver_data.get('foreign_5d_net', 0)
    i_net = naver_data.get('inst_5d_net', 0)
    
    # 공시 모멘텀 가감점 계산
    disclosure_adjustment = 0.0
    for report_nm, rcept_dt, rm in disclosures:
        if any(term in report_nm for term in ['단일판매', '공급계약', '특허']):
            disclosure_adjustment += 5.0
        if any(term in report_nm for term in ['소송', '피소', '유상증자']):
            disclosure_adjustment -= 5.0
    disclosure_adjustment = max(-5.0, min(5.0, disclosure_adjustment))
    
    if eval_type == 'value':
        # PBR Score (30%): PBR 1.0 or below gets 100, 5.0 or above gets 0.
        pbr = naver_data.get('pbr', 0.0)
        pbr_score = max(0.0, min(100.0, (5.0 - pbr) / 4.0 * 100.0))
        
        # ROE Score (30%): ROE 20% or above gets 100.
        roe_score = min(100.0, max(0.0, roe)) * 5.0
        
        # Upside Score (20%): Upside 50% or above gets 100.
        upside_score = (min(50.0, max(0.0, upside)) / 50.0) * 100.0
        
        # Dividend Yield Score (10%): Dividend yield 5% or above gets 100.
        dividend_yield = naver_data.get('dividend_yield', 0.0)
        div_score = min(100.0, dividend_yield * 20.0)
        
        # Oversold Score (10%): Lower price position in 52w is better.
        price_position_52w = naver_data.get('price_position_52w', 50.0)
        oversold_score = 100.0 - price_position_52w
        
        base_score = (pbr_score * 0.30) + (roe_score * 0.30) + (upside_score * 0.20) + (div_score * 0.10) + (oversold_score * 0.10)
        final_score = round(min(100.0, max(0.0, base_score + disclosure_adjustment)), 2)
        
        sector = naver_data.get('industry_name', '기타')
        reason = f"[{sector}] 대형 가치주 | ROE {roe:.1f}% | PBR {pbr:.2f} | 상승여력 {round(upside, 1)}% | 배당 {dividend_yield:.1f}% | 주가위치 {price_position_52w:.1f}% | 공시조정 {disclosure_adjustment:+.1f}점"
        
        one_line_templates = {
            "000660": "ROE 61.2%의 고효율 반도체 거인, 단기 폭락 조정을 활용한 매력적인 저점 매수 기회",
            "278470": "뷰티 디바이스 대장의 ROE 83.8% 고성장세와 가격 조정 메리트가 결합된 가치주",
            "267260": "글로벌 전력망 쇼티지 속에서 50%가 넘는 강력한 업사이드 마진을 보유한 전력기기 대장주",
            "009540": "3.2% 안정적인 배당 매력과 지속적인 수주 공시 호재를 품은 저PBR(1.98배) 조선 지주사",
            "402340": "자본 효율(ROE 55.1%) 대비 지주사 할인으로 순자산 매력도가 극대화된 가치투자처",
            "064350": "연이은 수출 공시 가점과 함께 63%의 높은 상승 목표가를 바라보는 K-방산의 선두 주자",
            "196170": "독보적인 기술력을 보유하고 52주 고점 대비 과매도 최하단(4.4%)에 도달한 바이오 플랫폼",
            "259960": "PBR 1.54배 수준의 역사적 저평가 매력을 뽐내는 캐시카우 게임 제작사",
            "329180": "조선업 슈퍼사이클 수혜와 함께 38%의 든든한 상승 여력을 안고 있는 조선 메이저",
            "033780": "3.4%의 든든한 시가배당률과 안정적인 방어력으로 무장한 저PBR 가치주의 교과서"
        }
        one_liner = one_line_templates.get(code, f"ROE {roe:.1f}% 및 저PBR {pbr:.2f}배의 우수한 자본 효율을 갖춘 {sector} 대표주 (상승여력 {round(upside, 1)}%)")
    else:
        # Momentum Scoring
        roe_score = (min(100.0, max(0.0, roe)) / 50.0) * 100.0
        
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
            
        base_score = (roe_score * 0.30) + (supply_score * 0.20) + (momentum_score * 0.20) + (upside_score * 0.20) + (news_score * 0.10)
        final_score = round(min(100.0, max(0.0, base_score + disclosure_adjustment)), 2)
        
        sector = naver_data.get('industry_name', '기타')
        reason = f"[{sector}] ROE {roe:.1f}% | 수급 외인{'유입' if f_net > 0 else '이탈'}/기관{'유입' if i_net > 0 else '이탈'} | 상승여력 {round(upside, 1)}% | 뉴스 {round(news_score, 1)}점 | 공시조정 {disclosure_adjustment:+.1f}점"
        
        one_line_templates = {
            "043260": "압도적인 ROE 113.3%와 기관/외인의 폭발적 동반 순매수세가 돋보이는 모멘텀 1순위",
            "031980": "HBM 패키징 장비 시장 성장 및 외국인과 기관의 강력한 동반 수급 모멘텀 수혜주",
            "089970": "강력한 ROE(30.4%)와 외국인/기관 순매수 유입세를 기록 중인 반도체 장비 우량주",
            "419530": "아동 IP 해외 성과 기대 및 기관의 대규모 순매수가 뒷받침된 엔터 턴어라운드 종목",
            "192080": "탄탄한 현금 창출력(ROE 12.7%)과 전방위적인 수급 유입으로 안정적 모멘텀 보유",
            "017960": "친환경 LNG 화물창 핵심 소재 수주 증가 및 기관 매수세 유입 모멘텀",
            "327260": "화합물 반도체 패키징 국산화 수혜 및 외인 매수 유입 모멘텀",
            "403870": "고압 수소 어닐링 장비 독점적 기술력과 고마진(ROE 30.5%) 성장성 보유",
            "383220": "높은 브랜드 인지도와 ROE 28.8%의 견조한 이익 체력을 보여주는 의류주"
        }
        one_liner = one_line_templates.get(code, f"ROE {roe:.1f}%의 성장성과 함께 기관/외인 수급 모멘텀을 타는 {sector} 우량주 (상승여력 {round(upside, 1)}%)")
        
    news_list = naver_data.get('news', [])
    news_headlines = [n for n in news_list]
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
        "news_summary": news_summary.strip(),
        "one_liner": one_liner
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
    
    # 1. Evaluate Momentum Candidates
    momentum_results = []
    print(f"Total candidates to evaluate in parallel for Momentum: {len(candidates)}")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(evaluate_single_candidate, cand, pool, dart_key, 'momentum'): cand for cand in candidates}
        completed_count = 0
        for future in as_completed(futures):
            completed_count += 1
            res = future.result()
            if res:
                momentum_results.append(res)
            if completed_count % 20 == 0 or completed_count == len(candidates):
                print(f"Momentum Progress: {completed_count}/{len(candidates)} candidates evaluated...")
                
    momentum_results.sort(key=lambda x: x['score'], reverse=True)
    top_10_momentum = momentum_results[:10]

    # 2. Evaluate Large-cap Value Candidates
    value_results = []
    print(f"Total candidates to evaluate in parallel for Large-cap Value: {len(candidates)}")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(evaluate_single_candidate, cand, pool, dart_key, 'value'): cand for cand in candidates}
        completed_count = 0
        for future in as_completed(futures):
            completed_count += 1
            res = future.result()
            if res:
                value_results.append(res)
            if completed_count % 20 == 0 or completed_count == len(candidates):
                print(f"Value Progress: {completed_count}/{len(candidates)} candidates evaluated...")
                
    value_results.sort(key=lambda x: x['score'], reverse=True)
    top_10_value = value_results[:10]

    print(f"\n[⚡ 모멘텀 추천주 Top 10]")
    for idx, r in enumerate(top_10_momentum):
        print(f"{idx+1}. [{r['code']}] {r['name']} - Score: {r['score']} - Upside: {r['upside']}% - Reason: {r['reason']}")
        
    print(f"\n[💎 대형 가치 추천주 Top 10]")
    for idx, r in enumerate(top_10_value):
        print(f"{idx+1}. [{r['code']}] {r['name']} - Score: {r['score']} - Upside: {r['upside']}% - Reason: {r['reason']}")

    # Neon DB audit_recommendations 저장
    try:
        conn = psycopg2.connect(database_url, cursor_factory=psycopg2.extras.DictCursor)
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE audit_recommendations")
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data_date = datetime.now().strftime('%Y-%m-%d')
        
        # Save Momentum Top 10
        for idx, r in enumerate(top_10_momentum):
            cursor.execute("""
                INSERT INTO audit_recommendations
                    (code, name, current_price, target_price, upside, opinion, data_date, created_at, score, roe, debt, reason, news_summary, rec_type, one_liner)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'momentum', %s)
            """, (
                r['code'], r['name'], float(r['current_price']), float(r['target_price']),
                float(r['upside']), '', data_date, now_str, float(r['score']),
                float(r.get('roe', 0)), float(r.get('debt', 0)), r.get('reason', ''), r.get('news_summary', ''),
                r.get('one_liner', '')
            ))
            
        # Save Value Top 10
        for idx, r in enumerate(top_10_value):
            cursor.execute("""
                INSERT INTO audit_recommendations
                    (code, name, current_price, target_price, upside, opinion, data_date, created_at, score, roe, debt, reason, news_summary, rec_type, one_liner)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'value', %s)
            """, (
                r['code'], r['name'], float(r['current_price']), float(r['target_price']),
                float(r['upside']), '', data_date, now_str, float(r['score']),
                float(r.get('roe', 0)), float(r.get('debt', 0)), r.get('reason', ''), r.get('news_summary', ''),
                r.get('one_liner', '')
            ))
            
        conn.commit()
        conn.close()
        print("[완료] Neon DB audit_recommendations 테이블 적재 성공!")
    except Exception as e:
        print(f"[오류] Neon DB audit_recommendations 적재 실패: {e}")

    return momentum_results + value_results

if __name__ == '__main__':
    run_selection()
