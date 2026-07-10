# -*- coding: utf-8 -*-
"""
STEP 2 — 뉴스·공시 데이터 수집 (개발2팀 전담)
기준 문서: trade/Report/audit_logic.md

역할: Pool 종목의 시장 데이터·뉴스·공시를 수집하여 pool_context.json을 생성한다.
      생성된 파일은 Claude AI 프롬프트의 입력으로 사용된다.

사용법:
    python trade/select_top_10.py                             # 최신 Pool 자동 선택
    python trade/select_top_10.py --source 파일명.xlsx        # 특정 Pool 지정
"""
import sys
import os
import json
import sqlite3
import OpenDartReader
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

TRADE_DIR = Path(__file__).resolve().parent

# 최종 추천에서 제외할 업종 키워드 (바이오·제약·헬스케어)
EXCLUDED_SECTORS = ['제약', '바이오', '건강관리', '헬스케어']

def is_excluded_sector(sector: str) -> bool:
    return any(k in (sector or '') for k in EXCLUDED_SECTORS)

if os.name == 'nt':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

# pool_naver_data.py는 trade/ 내부에 있으므로 직접 임포트합니다.
# (trade.py가 쓰는 get_all_naver_data.py와는 별개의 확장판이라 이름을 분리해 두었습니다.)
try:
    from pool_naver_data import get_all_naver_data
except ImportError:
    print("[오류] trade/pool_naver_data.py 모듈을 찾을 수 없습니다.")
    sys.exit(1)



def parse_market_cap(cap_str):
    if not cap_str or cap_str == 'N/A':
        return 0.0
    import re
    s = str(cap_str).replace(',', '').strip()
    val = 0.0
    m_cho = re.search(r'(\d+)조', s)
    m_uk = re.search(r'(\d+)억', s)
    if m_cho:
        val += float(m_cho.group(1)) * 1000000000000
    if m_uk:
        val += float(m_uk.group(1)) * 100000000
    if not m_cho and not m_uk:
        nums = re.findall(r'\d+', s)
        if nums:
            val = float(nums[0])
    return val


def collect_candidate(cand, pool, dart_key, sector_avg_pbr=None):
    """종목 1개의 시장 데이터·뉴스·공시를 수집하여 반환."""
    code = cand['code']
    name = cand['name']

    if name.endswith(('우', '우B', '우C', '우(전환)', '3우B')):
        return None

    try:
        naver_data = get_all_naver_data(code)
    except Exception as e:
        print(f"  [수집 오류] {name}({code}): {e}", file=sys.stderr)
        return None

    current_price = naver_data.get('current_price', 0)
    target_price = naver_data.get('target_price', 0)
    roe = naver_data.get('roe', 0.0)
    debt = naver_data.get('debt_ratio', 0.0)

    if current_price <= 0:
        return None

    is_estimated_tp = False
    if target_price <= 0:
        # 자체 목표주가 추정: BPS × min(섹터 평균 PBR, 3.0) (현재가 대비 최대 1.5배로 제한)
        bps = naver_data.get('bps', 0)
        sector = cand.get('sector', '기타')
        avg_pbr = (sector_avg_pbr or {}).get(sector, 0.0)
        if bps > 0 and avg_pbr > 0:
            raw_target = int(bps * min(avg_pbr, 3.0))
            target_price = min(raw_target, int(current_price * 1.5))
            is_estimated_tp = True
        if target_price <= 0:
            return None

    upside = round(((target_price - current_price) / current_price) * 100.0, 1)
    if upside <= 0:
        return None

    ma5_diff  = naver_data.get('ma5_diff', 0.0)
    ma20_diff = naver_data.get('ma20_diff', 0.0)

    # DART 공시 수집 (최근 30일)
    disclosures = []
    if dart_key:
        try:
            dart = OpenDartReader(dart_key)
            end_dt   = datetime.now()
            start_dt = end_dt - timedelta(days=30)
            df = dart.list(code, start=start_dt.strftime('%Y%m%d'), end=end_dt.strftime('%Y%m%d'))
            if df is not None and len(df) > 0:
                for _, row in df.iterrows():
                    raw_dt   = str(row.get('rcept_dt', ''))
                    rcept_dt = f"{raw_dt[:4]}-{raw_dt[4:6]}-{raw_dt[6:]}" if len(raw_dt) == 8 else raw_dt
                    disclosures.append({
                        'report_nm': row.get('report_nm', ''),
                        'rcept_dt':  rcept_dt,
                        'rcept_no':  row.get('rcept_no', ''),
                        'flr_nm':    row.get('flr_nm', ''),
                        'corp_cls':  row.get('corp_cls', ''),
                        'rm':        row.get('rm', ''),
                        'link':      f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={row.get('rcept_no', '')}",
                    })
        except Exception:
            pass

    # Hard Filter: 중대 공시 (감사의견 비적정, 배임·횡령)
    for d in disclosures:
        if any(t in d['report_nm'] for t in ['감사의견', '의견거절', '부적정', '한정', '내부회계', '배임', '횡령']):
            return None

    # 뉴스 수집
    news = [
        {'title': n.get('title', ''), 'link': n.get('link', ''),
         'source': n.get('source', '네이버 금융'), 'date': n.get('date', ''),
         'body': n.get('body', '')}
        for n in naver_data.get('news', [])[:10]
    ]

    return {
        'code':          code,
        'name':          name,
        'sector':        naver_data.get('industry_name', '기타'),
        'current_price': current_price,
        'target_price':  target_price,
        'upside':        upside,
        'is_estimated_tp': is_estimated_tp,
        'roe':           roe,
        'debt':          debt,
        'ma5_diff':      round(ma5_diff, 2),
        'ma20_diff':     round(ma20_diff, 2),
        'foreign_5d_net': naver_data.get('foreign_5d_net', 0),
        'inst_5d_net':    naver_data.get('inst_5d_net', 0),
        'foreign_5d_weighted': naver_data.get('foreign_5d_weighted', 0.0),
        'inst_5d_weighted':    naver_data.get('inst_5d_weighted', 0.0),
        'foreign_today_net':   naver_data.get('foreign_today_net', 0),
        'inst_today_net':      naver_data.get('inst_today_net', 0),
        'price_position_52w': naver_data.get('price_position_52w', 50.0),
        'pbr':           naver_data.get('pbr', 0.0),
        'dividend_yield': naver_data.get('dividend_yield', 0.0),
        'news':          news,
        'disclosures':   disclosures,
        'is_sector_leader': cand.get('is_sector_leader', False),
        'market_cap':    parse_market_cap(naver_data.get('market_cap', 'N/A')),
        'data_date':     cand.get('data_date'),
        'source_file':   cand.get('source_file'),
        'dps_history':    naver_data.get('dps_history', []),
        'payout_history':  naver_data.get('payout_history', []),
    }


def _enrich_items(items: list, evals: list, key: str) -> list:
    """
    items(공시 또는 뉴스 목록)의 각 항목에 sentiment/reason을 주입한다.
    evals는 ai_evaluations.json의 disc_evals 또는 news_evals 배열.
    keyword가 items[key] 필드에 포함될 경우 매칭으로 판단.
    """
    if not evals:
        return items
    enriched = []
    for item in items:
        text = item.get(key, '')
        matched = next((e for e in evals if e.get('keyword', '') in text), None)
        if matched:
            item = dict(item)
            item['sentiment'] = matched['sentiment']
            item['reason'] = matched['reason']
        enriched.append(item)
    return enriched


def run_collection(source_file=None):
    from dotenv import load_dotenv
    load_dotenv(TRADE_DIR / '.env')
    database_url = os.getenv('DATABASE_URL')
    dart_key     = os.getenv('DART_API_KEY')

    db_type = 'sqlite'
    conn = None
    if database_url and database_url.startswith('postgresql'):
        try:
            import psycopg2
            import psycopg2.extras
            conn = psycopg2.connect(database_url, cursor_factory=psycopg2.extras.DictCursor)
            cursor = conn.cursor()
            db_type = 'postgres'
            print("[+] PostgreSQL 데이터베이스 연결 성공")
        except Exception as pg_err:
            print(f"[경고] PostgreSQL 연결 실패: {pg_err}. SQLite로 대체하여 계속 진행합니다.")
    elif database_url and database_url.startswith('mysql'):
        try:
            import pymysql
            from urllib.parse import urlparse
            parsed = urlparse(database_url)
            conn = pymysql.connect(
                host=parsed.hostname or '127.0.0.1',
                port=parsed.port or 3306,
                user=parsed.username or 'root',
                password=parsed.password or '',
                database=parsed.path.lstrip('/') if parsed.path else 'trade',
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            cursor = conn.cursor()
            db_type = 'mysql'
            print("[+] MySQL 데이터베이스 연결 성공")
        except Exception as mysql_err:
            print(f"[경고] MySQL 연결 실패: {mysql_err}. SQLite로 대체하여 계속 진행합니다.")

    if conn is None:
        import sqlite3
        SQLITE_PATH = TRADE_DIR / 'trade.db'
        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        db_type = 'sqlite'
        print("[+] SQLite 데이터베이스 연결 성공")

    if not source_file:
        cursor.execute("""
            SELECT DISTINCT source_file, data_date FROM tr_stock_pool 
            ORDER BY data_date DESC
        """)
        pools = cursor.fetchall()
        if pools:
            print("\n=== 사용 가능한 Pool 목록 ===")
            for idx, p in enumerate(pools, 1):
                print(f"[{idx}] 파일명: {p['source_file']} (기준일: {p['data_date']})")

            try:
                if sys.stdin.isatty():
                    print(f"\n작업할 Pool의 번호를 입력하세요 (기본값 [1]): ", end="")
                    sys.stdout.flush()
                    sel = sys.stdin.readline().strip()
                    if sel:
                        choice = int(sel) - 1
                        if 0 <= choice < len(pools):
                            source_file = pools[choice]['source_file']
                        else:
                            print("잘못된 번호입니다. 최신 Pool을 사용합니다.")
                            source_file = pools[0]['source_file']
                    else:
                        source_file = pools[0]['source_file']
                else:
                    source_file = pools[0]['source_file']
            except Exception:
                source_file = pools[0]['source_file']
        else:
            print("최근 적재된 소스 파일이 없어 전체 조회합니다...")

    placeholder = '?' if db_type == 'sqlite' else '%s'
    if source_file:
        print(f"\n[선택된 Pool] 소스 파일({source_file}) 기준 tr_stock_pool 조회 중...")
        cursor.execute(f"""
            SELECT code, name, sector, roe, debt_ratio, pbr, per, is_sector_leader, market_cap, data_date, source_file
            FROM tr_stock_pool
            WHERE source_file = {placeholder}
        """, (source_file,))
    else:
        cursor.execute("SELECT code, name, sector, roe, debt_ratio, pbr, per, is_sector_leader, market_cap, data_date, source_file FROM tr_stock_pool")

    rows = cursor.fetchall()

    conn.close()

    # 섹터별 평균 PBR 계산 (자체 목표주가 추정용)
    from collections import defaultdict
    sector_pbr_map = defaultdict(list)
    for r in rows:
        pbr_val = r['pbr'] if r['pbr'] else 0.0
        if pbr_val > 0:
            sector_pbr_map[r['sector'] or '기타'].append(float(pbr_val))
    sector_avg_pbr = {s: sum(vals) / len(vals) for s, vals in sector_pbr_map.items() if vals}

    pool       = [{'code': r['code'], 'name': r['name'],
                   'roe': r['roe'] or 0.0, 'debt': r['debt_ratio'] or 0.0,
                   'is_sector_leader': bool(r['is_sector_leader']),
                   'market_cap': r['market_cap'] or 0.0,
                   'data_date': r['data_date'],
                   'source_file': r['source_file']} for r in rows]
    candidates = [{'code': r['code'], 'name': r['name'],
                   'sector': r['sector'] or '기타',
                   'is_sector_leader': bool(r['is_sector_leader']),
                   'market_cap': r['market_cap'] or 0.0,
                   'data_date': r['data_date'],
                   'source_file': r['source_file']} for r in rows]

    # 중복 제거
    seen = set()
    unique = []
    for c in candidates:
        if c['code'] not in seen:
            seen.add(c['code'])
            unique.append(c)
    candidates = unique
    
    print(f"수집 대상: {len(candidates)}개 종목\n")

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(collect_candidate, c, pool, dart_key, sector_avg_pbr): c for c in candidates}
        done = 0
        for future in as_completed(futures):
            done += 1
            res = future.result()
            if res:
                results.append(res)
            if done % 20 == 0 or done == len(candidates):
                print(f"  진행: {done}/{len(candidates)}", file=sys.stderr)

    # 상승여력 기준 정렬 (AI 검토 편의)
    results.sort(key=lambda x: x['upside'], reverse=True)

    print(f"\n{'='*60}")
    print(f"STEP 2 수집 완료 — {len(results)}개 후보 (Hard Filter 통과)")
    print(f"{'='*60}\n")

    for i, r in enumerate(results, 1):
        f_net = r['foreign_5d_net']
        i_net = r['inst_5d_net']
        trend = "역배열" if r['ma5_diff'] <= 0 and r['ma20_diff'] <= 0 else "정배열"
        print(f"[{i:>3}] {r['name']} ({r['code']}) | {r['sector']}")
        print(f"       현재가 {r['current_price']:,} | 목표가 {r['target_price']:,} | 상승여력 {r['upside']}%")
        print(f"       ROE {r['roe']:.1f}% | 부채 {r['debt']:.1f}% | PBR {r['pbr']:.2f} | 배당 {r['dividend_yield']:.1f}%")
        print(f"       수급 외인 {'▲' if f_net > 0 else '▼'}{abs(f_net):,} / 기관 {'▲' if i_net > 0 else '▼'}{abs(i_net):,} | 추세 {trend}")
        if r['news']:
            print(f"       뉴스:")
            for n in r['news']:
                print(f"         - [{n['source']}] {n['title']} ({n['date']})")
        if r['disclosures']:
            print(f"       공시 (최근 30일):")
            for d in r['disclosures']:
                print(f"         - [{d['rcept_dt']}] {d['report_nm']}")
        print()

    return results


def generate_fallback_oneliner(r):
    is_leader = r.get("is_sector_leader", False)
    roe = r.get("roe", 0.0)
    upside = r.get("upside", 0.0)
    sector = r.get("sector", "기타")
    debt = r.get("debt", 0.0)
    
    if is_leader:
        if roe >= 30:
            return f"ROE {roe:.1f}%의 압도적인 자본효율성을 자랑하는 {sector} 업종 대표 대장주"
        else:
            return f"안정적인 수급 흐름과 높은 업종 대표성을 지닌 {sector} 섹터 대장주"
    else:
        if upside >= 50:
            return f"목표주가 대비 {upside}%의 우수한 상승 여력(마진)을 보유한 {sector} 저평가주"
        elif roe >= 25:
            return f"자기자본이익률(ROE) {roe:.1f}%로 강력한 수익성을 입증한 {sector} 알짜 우량주"
        else:
            return f"부채비율 {debt:.1f}% 수준의 우수한 재무 건전성을 유지하고 있는 {sector} 우량 기업"


if __name__ == '__main__':
    import argparse
    from collections import defaultdict

    parser = argparse.ArgumentParser(description="Pool 종목 뉴스·공시 수집 → pool_context.json 생성")
    parser.add_argument('--source', help='사용할 Pool의 source_file명 (미지정 시 최신 Pool 자동 선택)')
    args = parser.parse_args()

    results = run_collection(source_file=args.source)

    # 섹터별 버킷 — AI 판단용 컨텍스트만 구성
    sector_dict = defaultdict(list)

    for r in results:
        f_flow = (r.get("foreign_5d_weighted", 0.0) > 0) or (r.get("foreign_today_net", 0) > 0)
        i_flow = (r.get("inst_5d_weighted", 0.0) > 0) or (r.get("inst_today_net", 0) > 0)
        if f_flow and i_flow:
            supply_text = "외인+/기관+"
        elif f_flow and not i_flow:
            supply_text = "외인+/기관-"
        elif not f_flow and i_flow:
            supply_text = "외인-/기관+"
        else:
            supply_text = "외인-/기관-"

        sector_dict[r["sector"]].append({
            "code":          r["code"],
            "name":          r["name"],
            "sector":        r["sector"],
            "current_price": r["current_price"],
            "target_price":  r["target_price"],
            "upside":        r["upside"],
            "roe":           r["roe"],
            "debt":          r["debt"],
            "pbr":           r["pbr"],
            "market_cap":    r["market_cap"],
            "supply":        supply_text,
            "is_sector_leader": r.get("is_sector_leader", False),
            "is_estimated_tp":  r.get("is_estimated_tp", False),
            "news":          r.get("news", []),
            "disclosures":   r.get("disclosures", []),
            "data_date":     r.get("data_date"),
            "source_file":   r.get("source_file"),
            "dividend_yield": r.get("dividend_yield", 0.0),
        })

    # pool_context.json 저장 — AI 프롬프트 입력용
    pool_context = dict(sector_dict)
    with open(TRADE_DIR / "pool_context.json", "w", encoding="utf-8") as f:
        json.dump(pool_context, f, ensure_ascii=False, indent=2)

    # 결과 요약 출력
    print(f"\n{'='*60}")
    print("데이터 수집 완료 — pool_context.json 저장됨")
    print(f"{'='*60}")
    total = sum(len(v) for v in pool_context.values())
    for sector, stocks in pool_context.items():
        print(f"  [{sector}] {len(stocks)}종목")
    print(f"\n  총 {total}종목 | 다음 단계: AI 프롬프트로 섹터별 순위 결정")
    print(f"{'='*60}\n")
