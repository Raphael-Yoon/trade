# -*- coding: utf-8 -*-
import os
import sys
import json
import sqlite3
import re
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

def analyze_sentiment_news(news_list):
    """
    뉴스 목록에 대해 단순 규칙 기반 감성 분석을 수행합니다.
    뉴스 개별 항목에 sentiment 및 reason 필드를 주입하고, 종합 뉴스 심리 점수(0~100)를 반환합니다.
    """
    pos_words = ['호재', '수주', '흑자', '성장', '계약', '최고', '상승', '신제품', '개발', '전환', '협약', '인수', '진입', '독점', '상위', '우수', '돌파', '강세']
    neg_words = ['악재', '소송', '적자', '하락', '횡령', '배임', '감소', '감원', '피소', '위험', '논란', '위기', '부진', '약세', '우려', '손실']
    
    total_score = 60.0  # 기본 중립 60점
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
            # 혼조세인 경우
            sentiment = 'neutral'
            reason = "긍정/부정 키워드가 동시에 감지되었습니다."
            
        total_score += score_change
        
        enriched_news.append({
            'title': title,
            'link': n.get('link', ''),
            'source': n.get('source', '네이버 금융'),
            'date': n.get('date', ''),
            'sentiment': sentiment,
            'reason': reason
        })
        
    total_score = max(0.0, min(100.0, total_score))
    return enriched_news, total_score

def analyze_disclosures(disc_list):
    """
    공시 목록에 대해 감성 분석 및 가감점(+5 / -5 / 0)을 계산합니다.
    """
    pos_keywords = ['공급계약', '단일판매', '특허', '수익', '흑자', '취득', '합병', '양수', '무상증자']
    neg_keywords = ['소송', '피소', '재해', '파산', '영업정지', '배임', '횡령', '부적정', '의견거절', '범위제한', '벌금', '제재', '유상증자']
    
    impact = 0
    enriched_disc = []
    
    for d in disc_list:
        name = d.get('report_nm', '')
        sentiment = 'neutral'
        reason = '일반 규정 준수 또는 정기 공시입니다.'
        
        found_pos = [w for w in pos_keywords if w in name]
        found_neg = [w for w in neg_keywords if w in name]
        
        if found_pos and not found_neg:
            impact += 5
            sentiment = 'positive'
            reason = f"호재성 공시({', '.join(found_pos)})가 확인되었습니다."
        elif found_neg and not found_pos:
            impact -= 5
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
        
    impact = max(-5, min(5, impact))
    return enriched_disc, impact

def collect_stock_full_data(cand, rec_type):
    """
    1종목의 실시간 데이터 및 뉴스/공시를 수집하고 점수화합니다.
    """
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
        # 자체 추정 목표주가
        bps = naver_data.get('bps', 0)
        pbr = naver_data.get('pbr', 1.0)
        target_price = min(int(bps * min(pbr, 3.0)), int(current_price * 1.5))
        
    upside = round(((target_price - current_price) / current_price) * 100.0, 1)
    
    # DART 공시 수집
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
                    })
        except Exception:
            pass

    # Hard Filter: 중대 공시
    for d in disclosures:
        if any(t in d['report_nm'] for t in ['의견거절', '부적정', '한정', '내부회계', '배임', '횡령']):
            print(f"  [제외] {name}({code}): 중대 공시 리스크 감지")
            return None

    # Hard Filter: Momentum용 역배열 제외
    ma5_diff = naver_data.get('ma5_diff', 0.0)
    ma20_diff = naver_data.get('ma20_diff', 0.0)
    if rec_type == 'momentum':
        if ma5_diff <= 0 or ma20_diff <= 0:
            print(f"  [제외] {name}({code}): 단기 하락 추세(MA5: {ma5_diff}%, MA20: {ma20_diff}%)")
            return None

    # 뉴스 수집
    raw_news = [
        {'title': n.get('title', ''), 'link': n.get('link', ''), 'source': n.get('source', ''), 'date': n.get('date', '')}
        for n in naver_data.get('news', [])[:10]
    ]

    # 정성 평가
    enriched_news, news_score = analyze_sentiment_news(raw_news)
    enriched_disc, disc_impact = analyze_disclosures(disclosures)

    # 최종 점수 계산
    step1_score = cand['score']
    if rec_type == 'value':
        final_score = step1_score + (news_score * 0.20) + disc_impact
    else: # momentum
        final_score = step1_score + (news_score * 0.25) + disc_impact

    # 투자의견 및 한줄 코멘트 작성
    opinion = "적극매수" if final_score >= 80 else ("매수" if final_score >= 60 else "보유")
    one_liner = f"재무 정량점수 {step1_score:.1f}점에 뉴스/공시 모멘텀을 결합한 {rec_type.upper()} 공략주"

    return {
        'code': code,
        'name': name,
        'sector': cand['sector'],
        'current_price': current_price,
        'target_price': target_price,
        'upside': upside,
        'roe': cand['roe'],
        'debt': cand['debt'],
        'score': round(final_score, 2),
        'reason': f"[{cand['sector']}] 뉴스심리 {news_score:.1f}점 및 공시가감점 {disc_impact:+}점 반영. ROE {cand['roe']}% / 부채비율 {cand['debt']}%",
        'news_summary': json.dumps(enriched_news, ensure_ascii=False),
        'disc_json': json.dumps(enriched_disc, ensure_ascii=False),
        'rec_type': rec_type,
        'one_liner': one_liner,
        'opinion': opinion,
        'data_date': cand['data_date']
    }

def main():
    print("[*] Starting Step 2 Qualitative Scoring & Re-ranking...")
    results_dir = TRADE_DIR / 'results'
    value_file = results_dir / 'financial_value_top20.json'
    momentum_file = results_dir / 'financial_momentum_top20.json'

    if not value_file.exists() or not momentum_file.exists():
        print("[오류] 1단계 결과 파일이 존재하지 않습니다. filter_financial_top20.py를 먼저 실행해 주세요.")
        sys.exit(1)

    with open(value_file, 'r', encoding='utf-8') as f:
        value_candidates = json.load(f)
    with open(momentum_file, 'r', encoding='utf-8') as f:
        momentum_candidates = json.load(f)

    # 대형주 수집 및 점수화
    print(f"[*] Processing Large-caps ({len(value_candidates)} candidates)...")
    final_value_list = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(collect_stock_full_data, cand, 'value'): cand for cand in value_candidates}
        for future in as_completed(futures):
            res = future.result()
            if res:
                final_value_list.append(res)

    # 상승주 수집 및 점수화
    print(f"[*] Processing Rising-stocks ({len(momentum_candidates)} candidates)...")
    final_momentum_list = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(collect_stock_full_data, cand, 'momentum'): cand for cand in momentum_candidates}
        for future in as_completed(futures):
            res = future.result()
            if res:
                final_momentum_list.append(res)

    # 정렬 및 Top 10 선정
    final_value_list.sort(key=lambda x: x['score'], reverse=True)
    final_momentum_list.sort(key=lambda x: x['score'], reverse=True)

    top10_value = final_value_list[:10]
    top10_momentum = final_momentum_list[:10]

    print(f"[+] Selected Value Top 10: {[s['name'] for s in top10_value]}")
    print(f"[+] Selected Momentum Top 10: {[s['name'] for s in top10_momentum]}")

    # 두 목록을 하나로 결합
    combined_recommendations = top10_value + top10_momentum

    # 로컬 파일 저장
    rec_json_file = results_dir / 'sector_recommendations.json'
    with open(rec_json_file, 'w', encoding='utf-8') as f:
        json.dump(combined_recommendations, f, ensure_ascii=False, indent=2)
    print(f"[+] Saved sector_recommendations.json to {rec_json_file}")

    # 로컬 SQLite DB 적재
    print(f"[*] Writing to local SQLite DB: {SQLITE_PATH}")
    conn = sqlite3.connect(SQLITE_PATH)
    cursor = conn.cursor()

    # 기존 데이터 삭제 후 일괄 재적재
    cursor.execute("DELETE FROM tr_audit_recommendations WHERE rec_type IN ('value', 'momentum')")
    
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for r in combined_recommendations:
        cursor.execute("""
            INSERT INTO tr_audit_recommendations
            (code, name, sector, current_price, target_price, upside, opinion, data_date, created_at,
             score, roe, debt, reason, news_summary, rec_type, one_liner, disc_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            r['code'], r['name'], r['sector'], r['current_price'], r['target_price'], r['upside'],
            r['opinion'], r['data_date'], now_str, r['score'], r['roe'], r['debt'], r['reason'],
            r['news_summary'], r['rec_type'], r['one_liner'], r['disc_json']
        ))
    conn.commit()
    conn.close()
    print("[+] DB Load Successful!")

if __name__ == '__main__':
    main()
