# -*- coding: utf-8 -*-
"""
STEP 3 & 4 — 정성 평가 및 최종 1~10위 선발 & DB 적재
역할: pool_context_top20.json 데이터를 로드하여 뉴스 감성 및 공시 가감점을 반영하고,
      대형주(large_cap), 가치주(value), 상승주(momentum) 각각 최종 상위 10개 종목을 선발해 DB 및 sector_recommendations.json에 저장한다.
"""
import sys
import os
import json
import sqlite3
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TRADE_DIR = PROJECT_ROOT / 'trade'
SQLITE_PATH = TRADE_DIR / 'trade.db'

def analyze_news_sentiment(news_list):
    pos_words = ['호재', '수주', '흑자', '성장', '계약', '최고', '상승', '신제품', '개발', '전환', '협약', '인수', '진입', '독점', '상위', '우수', '돌파', '강세', 'M&A']
    neg_words = ['악재', '소송', '적자', '하락', '횡령', '배임', '감소', '감원', '피소', '위험', '논란', '위기', '부진', '약세', '우려', '손실', '취소']
    
    news_score = 60.0
    enriched_news = []
    
    for n in news_list:
        title = n.get('title', '')
        score_change = 0
        sentiment = 'neutral'
        reason = '특이 사항 없는 중립 기사입니다.'
        
        found_pos = [w for w in pos_words if w in title]
        found_neg = [w for w in neg_words if w in title]
        
        if found_pos and not found_neg:
            score_change = 5
            sentiment = 'positive'
            reason = f"긍정 키워드({', '.join(found_pos)})가 감지되었습니다."
        elif found_neg and not found_pos:
            score_change = -5
            sentiment = 'negative'
            reason = f"부정 키워드({', '.join(found_neg)})가 감지되었습니다."
        elif found_pos and found_neg:
            sentiment = 'neutral'
            reason = "긍정/부정 키워드가 동시에 감지되었습니다."
            
        news_score += score_change
        enriched_news.append({
            'title': title,
            'link': n.get('link', ''),
            'source': n.get('source', '네이버 금융'),
            'date': n.get('date', ''),
            'sentiment': sentiment,
            'reason': reason
        })
        
    news_score = max(0.0, min(100.0, news_score))
    return enriched_news, news_score

def analyze_disclosures(disc_list):
    pos_keywords = ['공급계약', '단일판매', '특허', '수익', '흑자', '취득', '합병', '양수', '무상증자']
    neg_keywords = ['소송', '피소', '재해', '파산', '영업정지', '배임', '횡령', '부적정', '의견거절', '범위제한', '벌금', '제재', '유상증자']
    
    impact = 0.0
    enriched_disc = []
    
    for d in disc_list:
        name = d.get('report_nm', '')
        sentiment = 'neutral'
        reason = '일반 규정 준수 또는 정기 공시입니다.'
        
        found_pos = [w for w in pos_keywords if w in name]
        found_neg = [w for w in neg_keywords if w in name]
        
        # 기재정정, 단순 발행결과 등은 노이즈로 보고 영향도를 최소화
        is_minor = any(w in name for w in ['기재정정', '발행결과', '결과보고서', '소유상황'])
        
        if found_pos and not found_neg:
            val = 0.5 if is_minor else 1.5
            impact += val
            sentiment = 'positive'
            reason = f"호재성 공시({', '.join(found_pos)})가 확인되었습니다."
        elif found_neg and not found_pos:
            val = -0.5 if is_minor else -1.5
            impact -= val
            sentiment = 'negative'
            reason = f"악재성 공시({', '.join(found_neg)})가 확인되었습니다."
            
        enriched_disc.append({
            'report_nm': name,
            'rcept_dt': d.get('rcept_dt', ''),
            'rcept_no': d.get('rcept_no', ''),
            'flr_nm': d.get('flr_nm', ''),
            'corp_cls': d.get('corp_cls', ''),
            'rm': d.get('rm', ''),
            'sentiment': sentiment,
            'reason': reason
        })
        
    impact = max(-1.5, min(1.5, impact))
    return enriched_disc, impact

def main():
    print("[*] Starting Step 3 & 4 Qualitative Re-ranking...")
    results_dir = TRADE_DIR / 'results'
    context_file = results_dir / 'pool_context_top20.json'
    
    if not context_file.exists():
        print(f"[오류] 컨텍스트 파일({context_file})이 존재하지 않습니다.")
        sys.exit(1)
        
    with open(context_file, 'r', encoding='utf-8') as f:
        candidates = json.load(f)
        
    large_caps = []
    mid_caps = []
    small_caps = []
    
    for c in candidates:
        name = c['name']
        code = c['code']
        rec_type = c['rec_type']
        
        # Hard Filter: 중대 공시
        has_critical_risk = False
        for d in c.get('disclosures', []):
            if any(t in d['report_nm'] for t in ['의견거절', '부적정', '한정', '내부회계', '배임', '횡령']):
                has_critical_risk = True
                break
        if has_critical_risk:
            print(f"  [제외] {name}({code}): 중대 공시 리스크 감지")
            continue
            
        # 뉴스 및 공시 감성 분석
        enriched_news, news_score = analyze_news_sentiment(c.get('news', []))
        enriched_disc, disc_impact = analyze_disclosures(c.get('disclosures', []))
        
        # 최종 점수 계산 (단일 공식 통일 및 정성 평가 스케일 다운)
        step1_score = c['score']
        final_score = step1_score + ((news_score - 50) * 0.06) + disc_impact
            
        opinion = "적극매수" if final_score >= 80 else ("매수" if final_score >= 60 else "보유")
        kor_type = {'large_cap': '대형주', 'mid_cap': '중형주', 'small_cap': '소형주'}.get(rec_type, rec_type)
        
        # 최근 공시 및 뉴스 정보 추출 (중복 템플릿 제거)
        recent_discs = [d['report_nm'].strip() for d in c.get('disclosures', []) if d.get('report_nm')]
        recent_news = [n['title'].strip() for n in c.get('news', []) if n.get('title')]
        
        if recent_discs:
            one_liner = f"최근 공시: {recent_discs[0]}"
        elif recent_news:
            one_liner = f"최근 뉴스: {recent_news[0]}"
        else:
            one_liner = f"[{c['sector']}] 재무 밸런스가 돋보이는 {kor_type} 우량 추천주"
            
        desc_parts = []
        if recent_discs:
            desc_parts.append(f"최근 공시로 '{recent_discs[0]}'" + (f" 외 {len(recent_discs)-1}건" if len(recent_discs) > 1 else "") + "이 발표되었습니다.")
        if recent_news:
            desc_parts.append(f"최근 관련 보도로 '{recent_news[0]}'" + (f" 외 {len(recent_news)-1}건" if len(recent_news) > 1 else "") + " 뉴스 등이 확인됩니다.")
            
        if desc_parts:
            reason = f"[{c['sector']}] " + " ".join(desc_parts)
        else:
            reason = f"[{c['sector']}] 특이 공시 및 시장 노이즈가 최소화된 안정적인 펀더멘털 집중 구간입니다."
        
        result_item = {
            'code': code,
            'name': name,
            'sector': c['sector'],
            'current_price': c['current_price'],
            'target_price': c['target_price'],
            'upside': c['upside'],
            'roe': c['roe'],
            'debt': c['debt'],
            'pbr': c.get('pbr', 0.0),
            'op_profit': c.get('op_profit', 0.0),
            'operating_growth': c.get('op_growth', 0.0),
            'revenue_growth': c.get('rev_growth', 0.0),
            'score': round(final_score, 2),
            'reason': reason,
            'news_summary': json.dumps(enriched_news, ensure_ascii=False),
            'disc_json': json.dumps(enriched_disc, ensure_ascii=False),
            'rec_type': rec_type,
            'one_liner': one_liner,
            'opinion': opinion,
            'data_date': c['data_date']
        }
        
        if rec_type == 'large_cap':
            large_caps.append(result_item)
        elif rec_type == 'mid_cap':
            mid_caps.append(result_item)
        elif rec_type == 'small_cap':
            small_caps.append(result_item)
            
    # 정렬 및 Top 10 선정
    large_caps.sort(key=lambda x: x['score'], reverse=True)
    mid_caps.sort(key=lambda x: x['score'], reverse=True)
    small_caps.sort(key=lambda x: x['score'], reverse=True)
    
    top10_large_cap = large_caps[:10]
    top10_mid_cap = mid_caps[:10]
    top10_small_cap = small_caps[:10]
    
    print(f"[+] Selected Large-cap Top 10: {[s['name'] for s in top10_large_cap]}")
    print(f"[+] Selected Mid-cap Top 10: {[s['name'] for s in top10_mid_cap]}")
    print(f"[+] Selected Small-cap Top 10: {[s['name'] for s in top10_small_cap]}")
    
    combined = top10_large_cap + top10_mid_cap + top10_small_cap
    
    # 1. 파일 저장
    rec_json_file = results_dir / 'sector_recommendations.json'
    with open(rec_json_file, 'w', encoding='utf-8') as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)
    print(f"[+] Saved sector_recommendations.json")
    
    # 2. SQLite DB 적재
    print(f"[*] Writing to local SQLite DB: {SQLITE_PATH}")
    conn = sqlite3.connect(SQLITE_PATH)
    cursor = conn.cursor()
    
    # tr_audit_recommendations 테이블에 컬럼이 없을 경우를 대비해 ALTER TABLE 자동 실행
    for col in ['operating_growth', 'revenue_growth', 'pbr', 'op_profit']:
        try:
            cursor.execute(f"ALTER TABLE tr_audit_recommendations ADD COLUMN {col} REAL")
        except sqlite3.OperationalError:
            pass
    
    # 기존 대형주, 중형주, 소형주 및 구 가치주/상승주 데이터 삭제 후 재적재
    cursor.execute("DELETE FROM tr_audit_recommendations WHERE rec_type IN ('large_cap', 'mid_cap', 'small_cap', 'value', 'momentum')")
    
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for r in combined:
        cursor.execute("""
            INSERT INTO tr_audit_recommendations
            (code, name, sector, current_price, target_price, upside, opinion, data_date, created_at,
             score, roe, debt, reason, news_summary, rec_type, one_liner, disc_json, operating_growth, revenue_growth, pbr, op_profit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            r['code'], r['name'], r['sector'], r['current_price'], r['target_price'], r['upside'],
            r['opinion'], r['data_date'], now_str, r['score'], r['roe'], r['debt'], r['reason'],
            r['news_summary'], r['rec_type'], r['one_liner'], r['disc_json'], r['operating_growth'], r['revenue_growth'],
            r['pbr'], r['op_profit']
        ))
    conn.commit()
    conn.close()
    print("[+] DB Load Successful!")

if __name__ == '__main__':
    main()
