# -*- coding: utf-8 -*-
import sys
import os

# Windows 콘솔 UTF-8 설정
if os.name == 'nt':
    os.system('chcp 65001 > nul')
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

from flask import Flask, render_template, jsonify, send_file, request, g
import threading
import uuid
from datetime import datetime, timedelta
import subprocess
import json
import psutil
import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from ai_analysis import analyze_stock_data, analyze_portfolio
from get_all_naver_data import get_all_naver_data
import time

app = Flask(__name__)

# 작업 상태 저장
tasks = {}

# 결과 파일 저장 디렉토리
RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')
if not os.path.exists(RESULTS_DIR):
    os.makedirs(RESULTS_DIR)

# PostgreSQL(Neon) 연결 문자열
from dotenv import load_dotenv as _load_dotenv
_load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
DATABASE_URL = os.getenv('DATABASE_URL')


class _AdaptedCursor:
    """sqlite3 ? 플레이스홀더를 psycopg2 %s로 변환하는 커서 래퍼."""

    def __init__(self, cursor):
        self._cur = cursor

    @staticmethod
    def _adapt(query, has_params):
        if has_params:
            return query.replace('%', '%%').replace('?', '%s')
        return query

    def execute(self, query, params=None):
        if query.strip().upper().startswith('PRAGMA'):
            return self
        adapted = self._adapt(query, params is not None)
        if params is not None:
            self._cur.execute(adapted, params)
        else:
            self._cur.execute(adapted)
        return self

    def executemany(self, query, seq_of_params):
        adapted = self._adapt(query, True)
        self._cur.executemany(adapted, seq_of_params)
        return self

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    @property
    def rowcount(self):
        return self._cur.rowcount

    @property
    def description(self):
        return self._cur.description


class _PsycopgAdapter:
    """sqlite3 Connection 인터페이스를 흉내내는 psycopg2 연결 래퍼."""

    def __init__(self, dsn):
        self._conn = psycopg2.connect(dsn, cursor_factory=psycopg2.extras.DictCursor)

    @property
    def row_factory(self):
        return None

    @row_factory.setter
    def row_factory(self, value):
        pass

    def cursor(self):
        return _AdaptedCursor(self._conn.cursor())

    def execute(self, query, params=None):
        if query.strip().upper().startswith('PRAGMA'):
            return _AdaptedCursor(self._conn.cursor())
        cur = self.cursor()
        cur.execute(query, params)
        return cur

    def executemany(self, query, seq_of_params):
        cur = self.cursor()
        cur.executemany(query, seq_of_params)
        return cur

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()


def _new_db_conn():
    """백그라운드 스레드용 새 DB 연결 생성."""
    return _PsycopgAdapter(DATABASE_URL)


def get_db():
    """Flask 요청별 DB 연결 관리."""
    if 'db' not in g:
        g.db = _PsycopgAdapter(DATABASE_URL)
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    """요청 종료 시 DB 연결 닫기."""
    db = g.pop('db', None)
    if db is not None:
        db.close()

def init_db():
    """데이터베이스 초기화 및 테이블 생성 (PostgreSQL)"""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    # 종목 통합 테이블 (보유 종목 + 관심 종목)
    # type = 'portfolio' (보유) / 'watchlist' (관심)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS my_stocks (
            code TEXT PRIMARY KEY,
            name TEXT,
            added_at TEXT,
            purchase_price REAL DEFAULT 0,
            quantity INTEGER DEFAULT 0,
            stop_loss_ratio REAL DEFAULT 0,
            is_favorite INTEGER DEFAULT 0,
            peak_price REAL DEFAULT 0,
            owner TEXT DEFAULT '나',
            type TEXT DEFAULT 'portfolio'
        )
    ''')
    for col_name, col_def in [
        ('purchase_price', 'REAL DEFAULT 0'),
        ('quantity', 'INTEGER DEFAULT 0'),
        ('stop_loss_ratio', 'REAL DEFAULT 0'),
        ('is_favorite', 'INTEGER DEFAULT 0'),
        ('peak_price', 'REAL DEFAULT 0'),
        ("owner", "TEXT DEFAULT '나'"),
        ("type", "TEXT DEFAULT 'portfolio'"),
    ]:
        cursor.execute(f"ALTER TABLE my_stocks ADD COLUMN IF NOT EXISTS {col_name} {col_def}")

    # 종목별 일자별 히스토리 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_daily_history (
            id SERIAL PRIMARY KEY,
            date TEXT,
            code TEXT,
            name TEXT,
            purchase_price REAL,
            current_price REAL,
            quantity INTEGER,
            owner TEXT,
            recorded_at TEXT,
            day_profit REAL DEFAULT 0,
            cumulative_profit REAL DEFAULT 0,
            change_rate REAL DEFAULT 0
        )
    ''')
    for col_name, col_def in [
        ('day_profit', 'REAL DEFAULT 0'),
        ('cumulative_profit', 'REAL DEFAULT 0'),
        ('change_rate', 'REAL DEFAULT 0'),
    ]:
        cursor.execute(f"ALTER TABLE stock_daily_history ADD COLUMN IF NOT EXISTS {col_name} {col_def}")

    # 매도 거래 이력 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sell_history (
            id SERIAL PRIMARY KEY,
            code TEXT,
            name TEXT,
            owner TEXT,
            sell_price REAL,
            sell_qty INTEGER,
            purchase_price REAL,
            profit REAL,
            profit_rate REAL,
            sell_date TEXT,
            created_at TEXT
        )
    ''')

    # 종목 마스터 테이블 (검색용)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stocks_master (
            code TEXT PRIMARY KEY,
            name TEXT,
            market TEXT
        )
    ''')

    # 포트폴리오 AI 분석 캐시 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS portfolio_ai_cache (
            cache_key TEXT PRIMARY KEY,
            ai_result TEXT,
            created_at TEXT
        )
    ''')

    # 투자 풀 테이블 (감사팀 협업)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_pool (
            code TEXT PRIMARY KEY,
            name TEXT,
            sector TEXT,
            roe REAL,
            pbr REAL,
            per REAL,
            debt_ratio REAL,
            operating_margin REAL,
            target_price REAL,
            foreign_net_buy REAL,
            inst_net_buy REAL,
            pool_score REAL,
            data_date TEXT,
            updated_at TEXT
        )
    ''')

    # 감사팀 추천 종목 테이블 복원
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS audit_recommendations (
            id SERIAL PRIMARY KEY,
            code TEXT,
            name TEXT,
            current_price REAL,
            target_price REAL,
            upside REAL,
            opinion TEXT,
            data_date TEXT,
            created_at TEXT,
            score REAL DEFAULT 0.0,
            roe REAL DEFAULT 0.0,
            debt REAL DEFAULT 0.0,
            reason TEXT,
            news_summary TEXT
        )
    ''')

    # 조회 성능 인덱스
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_daily_history_date ON stock_daily_history(date)")

    conn.commit()
    conn.close()

# DB 초기화 실행
init_db()

# 실시간 모니터링 관리 (정규장/시간외 분리)
monitor_active_market = False
monitor_active_ah = False
monitor_thread_market = None
monitor_thread_ah = None
# [김선화] 정규장 및 시간외 설정 분리
monitor_threshold = 7.0 # 정규장 임계치
monitor_min_volume = 50000 # 정규장 최소 거래량
monitor_threshold_ah = 2.0 # 시간외 임계치 (낮게 설정)
monitor_min_volume_ah = 1000 # 시간외 최소 거래량 (낮게 설정)

# [김선화] ETF 필터링용 캐시
etf_cache = {'codes': [], 'last_updated': 0}

# [김정음] 급등주 알림 메모리 저장소 (DB 미사용 — 세션 내 실시간 데이터만 유지)
# 구조: {code: {code, name, type, change_rate, price, volume, industry, ...}}
price_alerts_store = {}

# [김선화] 감사팀 재무 데이터 캐시
financial_cache = {}
industry_cache = {}

# [김선화] 보유 기간 중 최고가(Trailing Stop 기준) 캐시
# 형식: {code: {'high': 0, 'high_date': '', 'high_kospi': 0, 'date': ''}}
holding_high_cache = {}

# [김정음] 종목 시가총액 및 코스피 전체 시총 캐시 (24h TTL)
market_cap_cache = {}           # {code: {'cap_억': float, 'ts': float}}
kospi_total_cap_cache = {'cap_억': 0, 'ts': 0}

# [김정음] 일별 시세 캐시 — 1시간 TTL, 과거 데이터라 빈번한 갱신 불필요
# 형식: {code: {'data': [...], 'ts': float}}
daily_prices_cache = {}

# [김정음] KOSPI 일별 시세 캐시 — 30분 TTL
kospi_daily_cache = {'data': [], 'ts': 0.0}

def load_financial_health(force=False):
    """[김선화] 감사팀의 재무 보고서(Excel)를 구글 드라이브 또는 로컬에서 로드하여 주요 지표를 캐싱합니다."""
    global financial_cache
    if not force and financial_cache: return financial_cache
    
    # [김선화] 강제 로드 시 기존 캐시 초기화
    if force: financial_cache = {}
    
    # 1. 구글 드라이브에서 최신 데이터 시도 (개발2팀 연결 방식 적용)
    try:
        from drive_sync import list_files_in_folder, download_from_drive
        import io
        import pandas as pd
        
        print("🔍 구글 드라이브에서 최신 재무 데이터 검색 중...")
        files = list_files_in_folder("Stock_Analysis_Results")
        if files:
            # 구글 시트(spreadsheet) 타입의 최신 파일 검색
            latest_file = None
            for f in files:
                if f['mimeType'] == 'application/vnd.google-apps.spreadsheet':
                    latest_file = f
                    break
            
            if latest_file:
                print(f"📥 구글 드라이브 최신 파일 발견: {latest_file['name']}")
                file_content = download_from_drive(latest_file['id'])
                if file_content:
                    df = pd.read_excel(io.BytesIO(file_content))
                    # 종목코드를 6자리 문자열로 변환 (0 채우기)
                    df['종목코드'] = df['종목코드'].astype(str).str.zfill(6)
                    
                    for _, row in df.iterrows():
                        code = row['종목코드']
                        financial_cache[code] = {
                            'audit': str(row.get('회계감사의견', 'N/A')),
                            'internal': str(row.get('내부통제의견', 'N/A')),
                            'roe': float(row.get('ROE', 0)),
                            'debt_ratio': float(row.get('부채비율', 0))
                        }
                    
                    print(f"✅ 구글 드라이브 재무 데이터 로드 완료 ({len(financial_cache)} 종목)")
                    return financial_cache
    except Exception as e:
        print(f"⚠️ 구글 드라이브 데이터 로드 실패: {e}. 로컬 데이터로 전환합니다.")

    # 2. 로컬 데이터 폴백 (가장 최신 파일 탐색)
    try:
        # [김선화] Linux/Windows 호환 경로 설정
        base_dir = os.path.dirname(os.path.dirname(__file__))
        report_dir = os.path.join(base_dir, 'cowork', 'Report')
        
        if os.path.exists(report_dir):
            local_files = [os.path.join(report_dir, f) for f in os.listdir(report_dir) if f.endswith('.xlsx')]
            if local_files:
                local_files.sort(key=os.path.getmtime, reverse=True)
                file_path = local_files[0]
                print(f"📂 로컬 최신 데이터 사용: {file_path}")
                
                df = pd.read_excel(file_path)
                df['종목코드'] = df['종목코드'].astype(str).str.zfill(6)
                for _, row in df.iterrows():
                    code = row['종목코드']
                    financial_cache[code] = {
                        'audit': str(row.get('회계감사의견', 'N/A')),
                        'internal': str(row.get('내부통제의견', 'N/A')),
                        'roe': float(row.get('ROE', 0)),
                        'debt_ratio': float(row.get('부채비율', 0))
                    }
                
                print(f"✅ 로컬 재무 데이터 로드 완료 ({len(financial_cache)} 종목)")
                return financial_cache
            else:
                print(f"⚠️ {report_dir} 폴더에 Excel 파일이 없습니다.")
        else:
            print(f"⚠️ 로컬 보고서 경로가 존재하지 않습니다: {report_dir}")
    except Exception as e:
        print(f"⚠️ 로컬 데이터 로드 실패: {e}")
    return financial_cache

def get_etf_codes():
    """네이버 API에서 전체 ETF 리스트 가져오기 (1시간 캐싱)"""
    global etf_cache
    import time
    if time.time() - etf_cache['last_updated'] > 3600:
        try:
            url = "https://finance.naver.com/api/sise/etfItemList.nhn"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            data = res.json()
            etf_cache['codes'] = [item['itemcode'] for item in data['result']['etfItemList']]
            etf_cache['last_updated'] = time.time()
        except: pass
    return etf_cache['codes']

def get_current_price_naver(code):
    """네이버 금융에서 현재가 가져오기"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        price_tag = soup.select_one(".no_today .blind")
        if price_tag:
            return int(price_tag.text.replace(',', ''))
    except Exception as e:
        print(f"가격 수집 오류 ({code}): {e}")
    return None

def get_industry_naver(code):
    """[김선화] 네이버 금융에서 해당 종목의 업종(Sector) 정보를 가져옵니다."""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 업종 정보 추출 (상세 페이지 내 '업종' 텍스트 뒤의 em 태그)
        industry_el = soup.select_one(".description em a")
        if not industry_el:
            # 다른 패턴 시도
            for th in soup.find_all("th", scope="row"):
                if "업종" in th.text:
                    industry_el = th.find_next("td").find("a")
                    break
        
        if industry_el:
            return industry_el.text.strip()
    except Exception as e:
        print(f"업종 수집 오류 ({code}): {e}")
    return "기타"

def get_industry_leaders(industry_id):
    """[김선화] 해당 업종의 상위 등락률 종목 3개를 가져옵니다."""
    if not industry_id: return "[]"
    try:
        url = f"https://finance.naver.com/sise/sise_group_detail.naver?type=upjong&no={industry_id}"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        leaders = []
        table = soup.select_one("table.type_5")
        if table:
            rows = table.select("tbody tr")
            for row in rows:
                name_tag = row.select_one("td a")
                if not name_tag: continue
                
                name = name_tag.get_text(strip=True)
                code = name_tag['href'].split('=')[-1]
                
                # 등락률 추출
                rate_tag = row.select_one("td.number span")
                rate = rate_tag.get_text(strip=True).replace('%', '') if rate_tag else "0.0"
                
                leaders.append({'name': name, 'code': code, 'rate': rate})
                if len(leaders) >= 3: break
        
        return json.dumps(leaders, ensure_ascii=False)
    except Exception as e:
        print(f"업종 리더 수집 오류: {e}")
        return "[]"

def get_market_movers():
    """시장 전체에서 급등/급락 종목 가져오기 (네이버 금융 상위 종목)"""
    movers = []
    try:
        # [김선화] 시간대에 따라 스캔 대상 조정 (16:00~18:00은 시간외 단일가 스캔)
        now = datetime.now()
        is_after_hours = now.hour >= 16 and now.hour < 18
        
        if is_after_hours:
            targets = [f'sise_low_up.naver?menu=danjiga&sosok={sosok}' for sosok in [0, 1]]
        else:
            targets = [
                'sise_quant.naver?sosok=0', 'sise_quant.naver?sosok=1', # 거래량 상위
                'sise_rise.naver?sosok=0', 'sise_rise.naver?sosok=1',   # 상승 상위
                'sise_market_sum.naver?sosok=0', 'sise_market_sum.naver?sosok=1' # 시가총액 상위
            ]
        
        etf_codes = get_etf_codes()
        for t_url in targets:
            try:
                url = f"https://finance.naver.com/sise/{t_url}"
                res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, 'html.parser')
                rows = soup.select("table.type_2 tr")

                for row in rows:
                    cols = row.select("td")
                    if len(cols) < 6: continue

                    # [김선화] 시간대별 컬럼 인덱스 대응
                    if is_after_hours:
                        # sise_low_up.naver?menu=danjiga 기준: 1:시간외등락률, 2:종목명, 3:시간외단가, 8:기준가(당일종가), 9:거래량
                        # cols[1]=기준가대비 시간외 전용 등락률, cols[5]=전일대비 등락률(정규장 포함) → cols[1] 사용
                        name = cols[2].text.strip()
                        code_el = cols[2].find('a')
                        price_str = cols[3].text.strip().replace(",", "")
                        change_rate_str = cols[1].text.strip().replace("%", "").replace("+", "")
                        volume_str = cols[9].text.strip().replace(",", "") if len(cols) > 9 else "0"
                    else:
                        # sise_quant.naver 등 일반 페이지 기준: 1:종목명, 2:현재가, 4:등락률, 5:거래량
                        name = cols[1].text.strip()
                        code_el = cols[1].find('a')
                        price_str = cols[2].text.strip().replace(",", "")
                        change_rate_str = cols[4].text.strip().replace("%", "").replace("+", "")
                        volume_str = cols[5].text.strip().replace(",", "")

                    if not code_el: continue
                    code = code_el['href'].split('=')[-1]

                    # [김선화] ETF, ETN, 스팩 제외 로직 강화 (노이즈 제거)
                    if code in etf_codes or any(x in name.upper() for x in ['ETF', 'ETN', '스팩', 'SPAC']):
                        continue

                    try:
                        price = int(price_str)
                        change_rate = float(change_rate_str)
                        volume = int(volume_str)

                        # [김선화] 시간대에 맞는 설정값 적용
                        current_threshold = monitor_threshold_ah if is_after_hours else monitor_threshold
                        current_min_volume = monitor_min_volume_ah if is_after_hours else monitor_min_volume

                        # [김선화] 임계치 이상 && 최소 거래량 이상일 때만 포착
                        if change_rate >= current_threshold and volume >= current_min_volume:
                            movers.append({
                                'code': code,
                                'name': name,
                                'price': price,
                                'change_rate': change_rate,
                                'volume': volume,
                                'type': 'spike'
                            })
                    except ValueError as e:
                        print(f"파싱 오류 ({t_url} / {name}): {e}")
                        continue
            except Exception as e:
                print(f"시장 모니터링 오류 ({t_url}): {e}")
                continue
    except Exception as e:
        print(f"시장 모니터링 오류: {e}")
    return movers

def is_market_open():
    """장 운영 시간 확인 (정규장 09:00~15:30 + 시간외 16:00~18:00)"""
    now = datetime.now()
    if now.weekday() >= 5: # 토, 일
        return False
    start_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
    end_time = now.replace(hour=18, minute=0, second=0, microsecond=0) # [김선화] 시간외 단일가 종료까지 연장
    return start_time <= now <= end_time

def run_market_monitor():
    """[김선화] 정규장 실시간 급등주 탐지 엔진 (09:00 - 15:30)"""
    global monitor_active_market, industry_cache
    print("🚀 정규장 모니터링 엔진 가동 시작...")
    
    while monitor_active_market:
        try:
            now = datetime.now()
            if now.hour < 9 or (now.hour >= 15 and now.minute > 30) or now.hour >= 16:
                if now.minute % 10 == 0 and now.second < 10:
                    print(f"💤 장 운영 시간이 아닙니다. (현재 {now.strftime('%H:%M')})")
                time.sleep(10)
                continue

            # 1. 시장 전체 급등주 스캔
            movers = get_market_movers()
            mover_codes = {m['code'] for m in movers}
            
            # 2. 오늘 이미 포착된 종목들 목록 가져오기
            # 메모리에서 오늘 포착된 종목 로드
            today_alerts = list(price_alerts_store.values())

            # 캐시 업데이트
            for alert in today_alerts:
                code, ind = alert['code'], alert.get('industry', '기타')
                if code not in industry_cache: industry_cache[code] = ind

            # 포착된 종목 중 Movers에 없는 종목 업데이트 대상 포함
            for alert in today_alerts:
                alert_code, alert_name = alert['code'], alert['name']
                if alert_code not in mover_codes:
                    det = get_detailed_price(alert_code)
                    if det['current_price'] > 0:
                        movers.append({
                            'code': alert_code, 'name': alert_name, 'price': det['current_price'],
                            'change_rate': det['change_rate'], 'volume': 0, 'is_update_only': True
                        })

            print(f"📊 [SCAN] 정규장 스캔 중... (대상: {len(movers)} 종목)")

            # 3. 상세 정보 수집 (병렬 처리)
            def fetch_market_detail(m):
                details = get_all_naver_data(m['code'])
                industry = details.get('industry_name', industry_cache.get(m['code'], '기타'))
                industry_cache[m['code']] = industry
                return {
                    'm': m, 'industry': industry,
                    'intensity': details.get('intensity', 0.0),
                    'prev_change_rate': details.get('prev_change_rate', 0.0),
                    'foreign_net_buy': details.get('foreign_net_buy_today', 0)
                }

            with ThreadPoolExecutor(max_workers=8) as executor:
                update_data = list(executor.map(fetch_market_detail, movers))

            # 4. 메모리 업데이트
            for item in update_data:
                m = item['m']
                fin = load_financial_health().get(m['code'], {'audit': 'N/A'})
                score = 10.0
                if item['prev_change_rate'] < 0: score += 5
                if item['intensity'] > 100: score += min((item['intensity'] - 100) / 5, 10)
                if fin['audit'] == '적정의견': score += 5
                f_buy = item.get('foreign_net_buy', 0)
                if f_buy > 0: score += 5
                if f_buy > 100000000: score += 5
                recommend_score = round(score, 2)

                if m['code'] in price_alerts_store:
                    a = price_alerts_store[m['code']]
                    a['change_rate'] = m['change_rate']
                    a['price'] = m['price']
                    if m['volume'] > 0: a['volume'] = m['volume']
                    a['intensity'] = item['intensity']
                    a['recommend_score'] = recommend_score
                    a['prev_change_rate'] = item['prev_change_rate']
                    a['foreign_net_buy'] = f_buy
                    a['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                elif not m.get('is_update_only'):
                    print(f"📡 [SPIKE] {m['name']}({m['code']}) [{item['industry']}] {m['price']:,}원 {m['change_rate']}% (외인: {f_buy:,}원) 탐지!")
                    price_alerts_store[m['code']] = {
                        'code': m['code'], 'name': m['name'], 'type': 'spike',
                        'change_rate': m['change_rate'], 'price': m['price'], 'volume': m['volume'],
                        'industry': item['industry'], 'intensity': item['intensity'],
                        'recommend_score': recommend_score, 'prev_change_rate': item['prev_change_rate'],
                        'foreign_net_buy': f_buy, 'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    }
            time.sleep(180)
        except Exception as e:
            print(f"정규장 모니터링 오류: {e}")
            time.sleep(30)

def run_ah_monitor():
    """[김선화] 시간외 단일가 급등주 탐지 엔진 (16:00 - 18:00)"""
    global monitor_active_ah, industry_cache
    print("🌙 시간외 단일가 탐지 엔진 가동 시작...")
    
    while monitor_active_ah:
        try:
            now = datetime.now()
            if now.hour < 16 or now.hour >= 18:
                if now.minute % 10 == 0 and now.second < 10:
                    print(f"💤 시간외 운영 시간이 아닙니다. (현재 {now.strftime('%H:%M')})")
                time.sleep(10)
                continue

            print(f"🔍 [SCAN] 시간외 시장 스캔 시작... ({now.strftime('%H:%M:%S')})")

            # 1. 시간외 급등주 스캔
            movers = get_market_movers_filtered(is_after_hours=True)
            mover_codes = {m['code'] for m in movers}

            # 2. 오늘 이미 포착된 종목들 목록 가져오기
            # 메모리에서 오늘 시간외 포착 종목 로드
            today_alerts = [a for a in price_alerts_store.values() if a.get('type') == 'after_hours']

            # 오늘 포착된 종목 중 Movers에 없는 종목 업데이트 대상 포함
            for alert in today_alerts:
                alert_code, alert_name = alert['code'], alert['name']
                ind = alert.get('industry', '기타')
                if alert_code not in industry_cache: industry_cache[alert_code] = ind
                if alert_code not in mover_codes:
                    det = get_detailed_price(alert_code)
                    if det['current_price'] > 0:
                        movers.append({
                            'code': alert_code, 'name': alert_name, 'price': det['current_price'],
                            'change_rate': det['change_rate'], 'volume': 0, 'is_update_only': True
                        })

            print(f"📊 [DATA] 상세 정보 수집 중... (대상: {len(movers)} 종목)")

            # 3. 상세 정보 수집 (병렬 처리)
            def fetch_ah_detail(m):
                details = get_all_naver_data(m['code'])
                industry = details.get('industry_name', industry_cache.get(m['code'], '기타'))
                industry_cache[m['code']] = industry
                return {
                    'm': m, 'industry': industry,
                    'intensity': details.get('intensity', 0.0),
                    'prev_change_rate': details.get('prev_change_rate', 0.0),
                    'foreign_net_buy': details.get('foreign_net_buy_today', 0)
                }

            with ThreadPoolExecutor(max_workers=8) as executor:
                update_data = list(executor.map(fetch_ah_detail, movers))

            # 4. 메모리 업데이트
            for item in update_data:
                m = item['m']
                fin = load_financial_health().get(m['code'], {'audit': 'N/A'})
                score = 10.0
                if item['prev_change_rate'] < 0: score += 3
                if fin['audit'] == '적정의견': score += 2
                f_buy = item.get('foreign_net_buy', 0)
                if f_buy > 0: score += 3
                recommend_score = round(score, 2)

                if m['code'] in price_alerts_store:
                    a = price_alerts_store[m['code']]
                    a['change_rate'] = m['change_rate']
                    a['price'] = m['price']
                    if m['volume'] > 0: a['volume'] = m['volume']
                    a['intensity'] = item['intensity']
                    a['recommend_score'] = recommend_score
                    a['prev_change_rate'] = item['prev_change_rate']
                    a['foreign_net_buy'] = f_buy
                    a['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                elif not m.get('is_update_only'):
                    print(f"📡 [AFTER_HOURS] {m['name']}({m['code']}) [{item['industry']}] {m['price']:,}원 {m['change_rate']}% (외인: {f_buy:,}원) 탐지!")
                    price_alerts_store[m['code']] = {
                        'code': m['code'], 'name': m['name'], 'type': 'after_hours',
                        'change_rate': m['change_rate'], 'price': m['price'], 'volume': m['volume'],
                        'industry': item['industry'], 'intensity': item['intensity'],
                        'recommend_score': recommend_score, 'prev_change_rate': item['prev_change_rate'],
                        'foreign_net_buy': f_buy, 'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    }
            print(f"✅ [DONE] 시간외 업데이트 완료. 3분 뒤 재스캔합니다.")
            time.sleep(180)
        except Exception as e:
            print(f"시간외 모니터링 오류: {e}")
            time.sleep(30)

def get_market_movers_filtered(is_after_hours=False):
    """[김선화] 조건에 맞는 시장 급등주 목록 수집"""
    movers = []
    try:
        if is_after_hours:
            targets = [f'sise_low_up.naver?menu=danjiga&sosok={sosok}' for sosok in [0, 1]]
        else:
            targets = ['sise_quant.naver?sosok=0', 'sise_quant.naver?sosok=1', 'sise_rise.naver?sosok=0', 'sise_rise.naver?sosok=1']

        etf_codes = get_etf_codes()
        for t_url in targets:
            try:
                url = f"https://finance.naver.com/sise/{t_url}"
                res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, 'html.parser')
                rows = soup.select("table.type_2 tr")

                for row in rows:
                    cols = row.select("td")
                    if len(cols) < 6: continue

                    if is_after_hours:
                        name = cols[2].text.strip()
                        code_el = cols[2].find('a')
                        price_str = cols[3].text.strip().replace(",", "")
                        change_rate_str = cols[1].text.strip().replace("%", "").replace("+", "")
                        volume_str = cols[9].text.strip().replace(",", "") if len(cols) > 9 else "0"
                    else:
                        name = cols[1].text.strip()
                        code_el = cols[1].find('a')
                        price_str = cols[2].text.strip().replace(",", "")
                        change_rate_str = cols[4].text.strip().replace("%", "").replace("+", "")
                        volume_str = cols[5].text.strip().replace(",", "")

                    if not code_el: continue
                    code = code_el['href'].split('=')[-1]
                    if code in etf_codes or any(x in name.upper() for x in ['ETF', 'ETN', '스팩', 'SPAC']): continue

                    try:
                        price = int(price_str)
                        change_rate = float(change_rate_str)
                        volume = int(volume_str)

                        threshold = monitor_threshold_ah if is_after_hours else monitor_threshold
                        min_vol = monitor_min_volume_ah if is_after_hours else monitor_min_volume

                        if change_rate >= threshold and volume >= min_vol:
                            movers.append({'code': code, 'name': name, 'price': price, 'change_rate': change_rate, 'volume': volume})
                    except ValueError as e:
                        print(f"파싱 오류 ({t_url} / {name}): {e}")
                        continue
            except Exception as e:
                print(f"시장 모니터링(filtered) 오류 ({t_url}): {e}")
                continue
    except Exception as e:
        print(f"시장 모니터링(filtered) 오류: {e}")
    return movers

@app.route('/api/monitor/toggle', methods=['POST'])
def api_toggle_monitor():
    """모니터링 토글 (target: market/ah)"""
    global monitor_active_market, monitor_active_ah, monitor_thread_market, monitor_thread_ah
    data = request.get_json() or {}
    target = data.get('target', 'market')
    
    if target == 'market':
        monitor_active_market = not monitor_active_market
        if monitor_active_market:
            # [김선화] 즉시 응답을 위해 DB 초기화 및 쓰레드 시작을 비동기화 고민했으나, 
            # 쓰레드 내부에서 초기화하도록 구조 변경하여 API 지연 해소
            monitor_thread_market = threading.Thread(target=run_market_monitor, daemon=True)
            monitor_thread_market.start()
        return jsonify({"active": monitor_active_market, "target": "market"})
    else:
        monitor_active_ah = not monitor_active_ah
        if monitor_active_ah:
            monitor_thread_ah = threading.Thread(target=run_ah_monitor, daemon=True)
            monitor_thread_ah.start()
        return jsonify({"active": monitor_active_ah, "target": "ah"})

@app.route('/api/monitor/status')
def api_monitor_status():
    return jsonify({
        "market_active": monitor_active_market,
        "ah_active": monitor_active_ah
    })

@app.route('/api/monitor/threshold', methods=['GET', 'POST'])
def api_monitor_threshold():
    """[김선화] 탐지 임계치 조회 및 설정 (정규장/시간외 분리)"""
    global monitor_threshold, monitor_threshold_ah
    if request.method == 'POST':
        try:
            data = request.get_json()
            if 'threshold' in data: monitor_threshold = float(data['threshold'])
            if 'threshold_ah' in data: monitor_threshold_ah = float(data['threshold_ah'])
            return jsonify({
                "status": "success", 
                "message": "임계치 설정이 변경되었습니다.",
                "threshold": monitor_threshold,
                "threshold_ah": monitor_threshold_ah
            })
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 400
    return jsonify({"threshold": monitor_threshold, "threshold_ah": monitor_threshold_ah})

@app.route('/api/monitor/min_volume', methods=['GET', 'POST'])
def api_monitor_min_volume():
    """[김선화] 최소 거래량 필터 조회 및 설정 (정규장/시간외 분리)"""
    global monitor_min_volume, monitor_min_volume_ah
    if request.method == 'POST':
        try:
            data = request.get_json()
            if 'min_volume' in data: monitor_min_volume = int(data['min_volume'])
            if 'min_volume_ah' in data: monitor_min_volume_ah = int(data['min_volume_ah'])
            return jsonify({
                "status": "success", 
                "message": "거래량 필터 설정이 변경되었습니다.",
                "min_volume": monitor_min_volume,
                "min_volume_ah": monitor_min_volume_ah
            })
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 400
    return jsonify({"min_volume": monitor_min_volume, "min_volume_ah": monitor_min_volume_ah})


@app.route('/api/alerts')
def get_alerts():
    """최근 알림 목록 조회 (필터링 조건 최적화 및 안정화)"""
    sort_by = request.args.get('sort', 'change_rate')
    try:
        now = datetime.now()
        is_after_hours = now.hour >= 16 and now.hour < 18
        current_threshold = float(monitor_threshold_ah if is_after_hours else monitor_threshold)
        current_min_volume = int(monitor_min_volume_ah if is_after_hours else monitor_min_volume)

        alerts = [
            a for a in price_alerts_store.values()
            if a['change_rate'] >= current_threshold and a.get('volume', 0) >= current_min_volume
        ]

        sort_key = {
            'recommend': lambda x: x.get('recommend_score', 0),
            'foreign_net_buy': lambda x: x.get('foreign_net_buy', 0),
            'time': lambda x: x.get('created_at', ''),
        }.get(sort_by, lambda x: x.get('change_rate', 0))

        alerts.sort(key=sort_key, reverse=True)
        return jsonify(alerts[:100])
    except Exception as e:
        print(f"API 오류(get_alerts): {e}")
        return jsonify([])

@app.route('/api/stock/<code>/disclosures')
def get_stock_disclosures(code):
    """특정 종목의 최근 공시 목록 — DART API 직접 조회."""
    try:
        from dotenv import load_dotenv
        import OpenDartReader
        load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
        dart = OpenDartReader(os.getenv('DART_API_KEY'))

        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=90)

        # corp_code DataFrame에서 stock_code 매핑
        corp_df = dart.corp_code
        matched = corp_df[corp_df['stock_code'] == code]
        if matched.empty:
            return jsonify([])

        corp_code = matched.iloc[0]['corp_code']
        df = dart.list(corp_code, start=start_dt.strftime('%Y%m%d'), end=end_dt.strftime('%Y%m%d'))
        if df is None or len(df) == 0:
            return jsonify([])

        keywords = ['단일판매','공급계약','특허','증자','감소','소각','소송',
                    '횡령','배임','영업정지','의견','인수','합병','양수','공개매수']
        result = []
        for _, row in df.iterrows():
            report_nm = row.get('report_nm', '')
            if any(kw in report_nm for kw in keywords):
                raw_dt = str(row.get('rcept_dt', ''))
                rcept_dt = f"{raw_dt[:4]}-{raw_dt[4:6]}-{raw_dt[6:]}" if len(raw_dt) == 8 else raw_dt
                result.append({
                    'code': code, 'name': row.get('corp_name', ''),
                    'rcept_no': row.get('rcept_no', ''), 'rcept_dt': rcept_dt,
                    'report_nm': report_nm, 'flr_nm': row.get('flr_nm', ''),
                    'corp_cls': row.get('corp_cls', ''), 'rm': row.get('rm', ''),
                })
        return jsonify(result[:15])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/targets/live-prices')
def get_live_prices():
    """[김정음] 종목코드 목록의 실시간 현재가·등락률을 병렬 조회하여 반환합니다."""
    codes_str = request.args.get('codes', '')
    if not codes_str:
        return jsonify({})
    codes = [c.strip() for c in codes_str.split(',') if c.strip()]

    def fetch_one(code):
        d = get_detailed_price(code)
        return code, d

    result = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_one, code): code for code in codes}
        for future in as_completed(futures):
            code, d = future.result()
            if d.get('current_price', 0) > 0:
                result[code] = {'price': d['current_price'], 'change_rate': d['change_rate'], 'change': d['change']}
    return jsonify(result)

@app.route('/api/pool')
def get_stock_pool():
    """투자적격 종목 풀 조회 (audit_recommendations Top 10 우선, 그 외 pool_score 순)"""
    try:
        conn = _new_db_conn()
        cursor = conn.cursor()
        
        # 1. 최신 추천 일자 조회
        cursor.execute("SELECT MAX(data_date) FROM audit_recommendations")
        max_date_row = cursor.fetchone()
        max_date = max_date_row[0] if max_date_row and max_date_row[0] else None
        
        if max_date:
            # 2. 최신 추천 종목(10개) 및 나머지 종목 결합 쿼리 수행
            cursor.execute("""
                SELECT * FROM (
                    SELECT 
                        a.code, a.name, p.sector, 
                        COALESCE(a.roe, p.roe) as roe,
                        p.pbr, p.per,
                        COALESCE(a.debt, p.debt_ratio) as debt_ratio,
                        p.operating_margin,
                        a.target_price, p.pool_score,
                        a.score AS priority_score, a.reason AS ai_summary,
                        a.news_summary,
                        a.upside, a.current_price,
                        0 as is_rec
                    FROM audit_recommendations a
                    LEFT JOIN stock_pool p ON a.code = p.code
                    WHERE a.data_date = %s
                    
                    UNION ALL
                    
                    SELECT 
                        p.code, p.name, p.sector, p.roe, p.pbr, p.per, p.debt_ratio, p.operating_margin,
                        p.target_price, p.pool_score,
                        NULL as priority_score, NULL as ai_summary,
                        NULL as news_summary,
                        NULL as upside, NULL as current_price,
                        1 as is_rec
                    FROM stock_pool p
                    WHERE p.code NOT IN (
                        SELECT code FROM audit_recommendations 
                        WHERE data_date = %s
                    )
                ) combined
                ORDER BY is_rec, priority_score DESC, pool_score DESC
            """, (max_date, max_date))
        else:
            # 추천 데이터가 없을 경우 재무 점수 순 정렬
            cursor.execute("""
                SELECT 
                    code, name, sector, roe, pbr, per, debt_ratio, operating_margin,
                    target_price, pool_score, pool_score AS priority_score, 
                    NULL AS ai_summary, NULL AS news_summary, NULL AS upside, 
                    NULL AS current_price, 1 AS is_rec
                FROM stock_pool 
                ORDER BY pool_score DESC
            """)
            
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        
        ranked_by = "ai" if max_date else "score"
        return jsonify({"ranked_by": ranked_by, "stocks": rows})
    except Exception as e:
        print(f"pool 조회 오류: {e}")
        return jsonify({"ranked_by": "score", "stocks": []})


def run_data_collection(task_id, stock_count=100, fields=None, market='KOSPI', year=None, report_types=None):
    """백그라운드에서 데이터 수집 실행"""
    try:
        tasks[task_id]['status'] = 'running'
        tasks[task_id]['progress'] = 0
        tasks[task_id]['message'] = f'{market} 데이터 수집 시작...'
        tasks[task_id]['logs'] = []

        script_path = os.path.join(os.path.dirname(__file__), 'data_collect.py')
        python_cmd = sys.executable
        if 'uwsgi' in python_cmd.lower():
            python_cmd = 'python'

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        count_label = 'all' if stock_count == 0 else f'top{stock_count}'
        result_filename = f'{market.lower()}_{count_label}_{timestamp}.xlsx'
        result_path = os.path.join(RESULTS_DIR, result_filename)

        cmd = [python_cmd, script_path, '--count', str(stock_count), '--market', market, '--output', result_path]
        
        if year:
            cmd.extend(['--year', str(year)])
        if report_types:
            cmd.extend(['--report_types', ','.join(report_types)])

        if fields:
            cmd.extend(['--fields', ','.join(fields)])

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            cwd=os.path.dirname(__file__)
        )
        
        tasks[task_id]['process'] = process

        for line in process.stdout:
            line = line.strip()
            if line:
                tasks[task_id]['message'] = line
                tasks[task_id]['logs'].append(line)
                if len(tasks[task_id]['logs']) > 100:
                    tasks[task_id]['logs'].pop(0)
                
                if '진행률:' in line:
                    try:
                        start_idx = line.find('[')
                        end_idx = line.find(']')
                        if start_idx != -1 and end_idx != -1:
                            bracket_content = line[start_idx+1:end_idx]
                            if '/' in bracket_content:
                                current, total = map(int, bracket_content.split('/'))
                                tasks[task_id]['progress'] = int((current / total) * 100)
                        elif '%' in line:
                            percent_val = line.split('%')[0].split()[-1]
                            tasks[task_id]['progress'] = int(percent_val)
                    except:
                        pass

        process.wait()

        if 'process' in tasks[task_id]:
            del tasks[task_id]['process']

        if tasks[task_id].get('status') == 'cancelled':
            return

        if process.returncode == 0:
            if os.path.exists(result_path):
                tasks[task_id]['status'] = 'completed'
                tasks[task_id]['progress'] = 100
                tasks[task_id]['message'] = '데이터 수집 완료!'
                tasks[task_id]['result_file'] = result_filename
                
                try:
                    from drive_sync import upload_to_drive
                    drive_data = upload_to_drive(result_path)
                    if drive_data:
                        tasks[task_id]['message'] += f' (구글 드라이브 업로드 완료)'
                        tasks[task_id]['drive_link'] = drive_data['link']
                        # Drive-Native: 업로드 완료 후 로컬 파일 즉시 삭제, DB 저장 없음
                        if os.path.exists(result_path):
                            os.remove(result_path)
                        json_path = result_path.replace('.xlsx', '.json')
                        if os.path.exists(json_path):
                            os.remove(json_path)
                except Exception as drive_err:
                    print(f"드라이브 업로드 실패: {drive_err}")
                
                cleanup_old_results()
            else:
                tasks[task_id]['status'] = 'error'
                tasks[task_id]['message'] = '결과 파일을 찾을 수 없습니다.'
        else:
            error_msg = process.stderr.read()
            tasks[task_id]['status'] = 'error'
            tasks[task_id]['message'] = f'오류 발생: {error_msg}'

    except Exception as e:
        tasks[task_id]['status'] = 'error'
        tasks[task_id]['message'] = f'오류 발생: {str(e)}'

def check_is_local():
    return os.name == 'nt' or 'PYTHONANYWHERE_DOMAIN' not in os.environ

@app.route('/')
def index():
    return render_template('index.html', is_local=check_is_local())

@app.route('/api/collect', methods=['POST'])
def start_collection():
    if not check_is_local():
        return jsonify({'success': False, 'message': '서버 환경에서는 데이터 수집 기능을 사용할 수 없습니다.'}), 403
        
    data = request.get_json() or {}
    stock_count = data.get('stock_count', 100)
    fields = data.get('fields', [])
    market = data.get('market', 'KOSPI')
    tickers = data.get('tickers', [])
    year = data.get('year')
    report_types = data.get('report_types', [])

    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        'status': 'pending',
        'progress': 0,
        'message': '대기 중...',
        'stock_count': stock_count,
        'market': market,
        'tickers': tickers,
        'created_at': datetime.now().isoformat()
    }

    thread = threading.Thread(target=run_data_collection, args=(task_id, stock_count, fields, market, year, report_types))
    thread.start()

    return jsonify({
        'success': True,
        'task_id': task_id,
        'message': '데이터 수집이 시작되었습니다.'
    })

@app.route('/api/status/<task_id>', methods=['GET'])
def get_status(task_id):
    if task_id not in tasks:
        return jsonify({'error': '작업을 찾을 수 없습니다.'}), 404
    task_info = {k: v for k, v in tasks[task_id].items() if k != 'process'}
    return jsonify(task_info)

@app.route('/api/cancel/<task_id>', methods=['POST'])
def cancel_collection(task_id):
    if task_id not in tasks:
        return jsonify({'error': '작업을 찾을 수 없습니다.'}), 404
    
    task = tasks[task_id]
    if task['status'] == 'running' and 'process' in task:
        try:
            process = task['process']
            parent = psutil.Process(process.pid)
            for child in parent.children(recursive=True):
                child.terminate()
            parent.terminate()
            task['status'] = 'cancelled'
            return jsonify({'success': True})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500
    return jsonify({'success': False, 'message': '취소할 수 없습니다.'})

def get_portfolio_details(ticker):
    """
    네이버 금융에서 모든 가능한 데이터를 수집합니다.

    get_all_naver_data 함수를 래핑하여 기존 인터페이스 유지 + 추가 데이터 제공
    """
    # 새로운 전체 데이터 수집 함수 사용
    all_data = get_all_naver_data(ticker)

    # 기존 코드 호환성을 위한 필드 매핑
    data = {
        'code': all_data.get('code', ticker),
        'name': all_data.get('name', ''),
        'current_price': all_data.get('current_price', 0),
        'market_cap': all_data.get('market_cap', 'N/A'),
        'opinion': all_data.get('opinion', 'N/A'),
        'target_price': all_data.get('target_price', 0),
        'high_52w': all_data.get('high_52w', 0),
        'low_52w': all_data.get('low_52w', 0),
        'per': all_data.get('per', 0),
        'pbr': all_data.get('pbr', 0),
        'dividend_yield': all_data.get('dividend_yield', 0),
        'revenue_growth': all_data.get('revenue_growth', 'N/A'),
        'profit_growth': all_data.get('profit_growth', 'N/A'),
        'foreign_net_buy': all_data.get('foreign_net_buy_today', 0),
        'inst_net_buy': all_data.get('inst_net_buy_today', 0),
        'rsi': all_data.get('rsi', 0),
    }

    # 새로 추가된 데이터도 포함
    data.update({
        # 기본 시세
        'prev_price': all_data.get('prev_price', 0),
        'open_price': all_data.get('open_price', 0),
        'high_price': all_data.get('high_price', 0),
        'low_price': all_data.get('low_price', 0),
        'volume': all_data.get('volume', 0),
        'trading_value': all_data.get('trading_value', 0),

        # 시가총액 상세
        'market_cap_rank': all_data.get('market_cap_rank', 'N/A'),
        'outstanding_shares': all_data.get('outstanding_shares', 0),

        # 외국인
        'foreign_ownership_ratio': all_data.get('foreign_ownership_ratio', 0),
        'foreign_exhaustion_ratio': all_data.get('foreign_exhaustion_ratio', 0),

        # 투자의견 상세
        'opinion_score': all_data.get('opinion_score', 0),

        # PER/PBR 상세
        'eps': all_data.get('eps', 0),
        'estimated_per': all_data.get('estimated_per', 0),
        'estimated_eps': all_data.get('estimated_eps', 0),
        'bps': all_data.get('bps', 0),

        # 업종 비교
        'sector_per': all_data.get('sector_per', 0),
        'sector_change_rate': all_data.get('sector_change_rate', 0),

        # 재무 상세
        'revenue': all_data.get('revenue', 'N/A'),
        'operating_profit': all_data.get('operating_profit', 'N/A'),
        'net_profit': all_data.get('net_profit', 'N/A'),
        'roe': all_data.get('roe', 0),
        'debt_ratio': all_data.get('debt_ratio', 0),
        'current_ratio': all_data.get('current_ratio', 0),

        # 기술적
         'price_position_52w': all_data.get('price_position_52w', 0),
         
         # 추가 데이터 (뉴스, 수급 추세)
         'news': all_data.get('news', []),
         'foreign_5d_net': all_data.get('foreign_5d_net', 0),
         'foreign_20d_net': all_data.get('foreign_20d_net', 0),
         'inst_5d_net': all_data.get('inst_5d_net', 0),
         'inst_20d_net': all_data.get('inst_20d_net', 0),
         'ma5': all_data.get('ma5', 0),
         'ma20': all_data.get('ma20', 0),
         'treasury_shares': all_data.get('treasury_shares', 0),
         'treasury_ratio': all_data.get('treasury_ratio', 0),
     })

    return data


# ===== 기존 get_portfolio_details 함수는 주석 처리 (백업용) =====
def get_portfolio_details_old(ticker):
    """[DEPRECATED] 기존 함수 - get_all_naver_data로 대체됨"""
    # 1. 메인 페이지 데이터 (가격, 목표주가, 재무지표)
    main_url = f"https://finance.naver.com/item/main.naver?code={ticker}"
    # 2. 투자자별 매매동향 (수급)
    investor_url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    data = {
        'code': ticker,
        'current_price': 0,
        'market_cap': 'N/A',
        'opinion': 'N/A',
        'target_price': 0,
        'high_52w': 0,
        'low_52w': 0,
        'per': 0,
        'pbr': 0,
        'dividend_yield': 0,
        'revenue_growth': 'N/A',
        'profit_growth': 'N/A',
        'foreign_net_buy': 0,
        'inst_net_buy': 0,
        'rsi': 0
    }
    
    try:
        # --- 메인 페이지 파싱 ---
        response = requests.get(main_url, headers=headers, timeout=5)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 현재가
        new_totalinfo = soup.find('div', class_='new_totalinfo')
        if new_totalinfo:
            blind = new_totalinfo.find('dl', class_='blind')
            if blind:
                dd_list = blind.find_all('dd')
                if len(dd_list) >= 4:
                    price_text = dd_list[3].text.split()[1].replace(',', '')
                    data['current_price'] = int(price_text)
                
        # 시가총액 (첫 번째 테이블에서 찾기)
        first_tbody = soup.find('table', class_='tb_type1')
        if first_tbody:
            tbody = first_tbody.find('tbody')
            if tbody:
                for tr in tbody.find_all('tr'):
                    th = tr.find('th')
                    if th and '시가총액' in th.get_text():
                        td = tr.find('td')
                        if td:
                            data['market_cap'] = td.get_text(strip=True)

        # 투자의견/목표주가 (전체 페이지에서 검색 - HTML 구조 변경에 대응)
        # 새로운 네이버 금융 구조: <th>투자의견l목표주가</th> <td><span>매수</span><em>166,385</em></td>
        for tr in soup.find_all('tr'):
            th = tr.find('th')
            if th and '투자의견' in th.get_text() and '목표주가' in th.get_text():
                td = tr.find('td')
                if td:
                    # 투자의견 (span 태그에서)
                    opinion_span = td.find('span', class_='f_up') or td.find('span')
                    if opinion_span:
                        opinion_text = opinion_span.get_text(strip=True)
                        # 숫자 제거 (예: "4.00매수" -> "매수")
                        data['opinion'] = re.sub(r'^[\d.]+', '', opinion_text).strip()

                    # 목표주가 (em 태그에서)
                    ems = td.find_all('em')
                    for em in ems:
                        text = em.get_text(strip=True).replace(',', '')
                        # 숫자만 있는 em 태그 찾기 (목표주가)
                        if text.isdigit() and len(text) >= 4:  # 최소 4자리 (만원 이상)
                            data['target_price'] = int(text)
                            break
                break

        # 재무 지표 (성장성 포함)
        section = soup.find('div', class_='section cop_analysis')
        if section:
            # 클래스가 tb_type1과 tb_num을 포함하는 테이블 찾기 (HTML 구조 변경 대응)
            table = section.find('table', class_=lambda c: c and 'tb_type1' in c and 'tb_num' in c)
            if table:
                trs = table.find_all('tr')
                
                # 수집할 데이터 맵
                finance_data = {
                    '매출액': [],
                    '영업이익': [],
                    '매출액증가율': 'N/A',
                    '영업이익증가율': 'N/A'
                }
                
                for tr in trs:
                    th = tr.find('th')
                    if not th: continue
                    th_text = th.get_text(strip=True)
                    tds = tr.find_all('td')
                    if not tds: continue
                    
                    # -2: 최근 확정 연도 실적, -1: 올해 전망치(보통)
                    # 만약 전망치가 있으면 -2를 쓰고, 없으면 -1을 쓰는 유연함이 필요하지만 
                    # 우선 -2를 기준으로 하되 N/A인 경우 앞쪽으로 탐색
                    
                    def get_last_valid_val(td_list):
                        # 뒤에서부터 (전망치 제외하고) 유효한 값 찾기
                        for i in range(len(td_list)-2, -1, -1):
                            val = td_list[i].get_text(strip=True).replace(',', '')
                            if val and val != '-' and val != 'N/A':
                                return val
                        return None

                    if '매출액증가율' in th_text:
                        val = get_last_valid_val(tds)
                        if val: finance_data['매출액증가율'] = val
                    elif '영업이익증가율' in th_text:
                        val = get_last_valid_val(tds)
                        if val: finance_data['영업이익증가율'] = val
                    elif th_text == '매출액':
                        finance_data['매출액'] = [t.get_text(strip=True).replace(',', '') for t in tds]
                    elif th_text == '영업이익':
                        finance_data['영업이익'] = [t.get_text(strip=True).replace(',', '') for t in tds]

                # 직접 계산 (성장성 지표가 명시적으로 없는 경우)
                if finance_data['매출액증가율'] == 'N/A' and len(finance_data['매출액']) >= 3:
                    try:
                        # 최근 2년 데이터 비교 (보통 인덱스 1, 2 또는 2, 3)
                        # thead에서 확정 연도 위치를 파악하는 것이 정확하나 간이로 진행
                        curr = float(finance_data['매출액'][-2]) # 최근 확정
                        prev = float(finance_data['매출액'][-3]) # 전년
                        if prev > 0:
                            growth = round((curr - prev) / prev * 100, 1)
                            finance_data['매출액증가율'] = str(growth)
                    except: pass
                
                if finance_data['영업이익증가율'] == 'N/A' and len(finance_data['영업이익']) >= 3:
                    try:
                        curr = float(finance_data['영업이익'][-2])
                        prev = float(finance_data['영업이익'][-3])
                        if prev > 0:
                            growth = round((curr - prev) / prev * 100, 1)
                            finance_data['영업이익증가율'] = str(growth)
                    except: pass
                
                data['revenue_growth'] = finance_data['매출액증가율']
                data['profit_growth'] = finance_data['영업이익증가율']

        # 52주 고점/저점 및 PER/PBR (전체 페이지에서 검색 - 구조 변경 대응)
        all_trs = soup.find_all('tr')
        for tr in all_trs:
            tr_text = tr.get_text()
            # 52주 고/저
            if '52주' in tr_text and ('최고' in tr_text or '고가' in tr_text):
                ems = tr.find_all('em')
                if len(ems) >= 2:
                    try:
                        high_text = ems[0].get_text(strip=True).replace(',', '')
                        low_text = ems[1].get_text(strip=True).replace(',', '')
                        if high_text.isdigit():
                            data['high_52w'] = int(high_text)
                        if low_text.isdigit():
                            data['low_52w'] = int(low_text)
                    except:
                        pass

            # PER (ID 기반 검색이 더 안정적)
            if 'PER' in tr_text and '배당' not in tr_text:
                per_em = tr.find('em', id='_per')
                if per_em:
                    val = per_em.get_text(strip=True).replace(',', '')
                    if val and val != '-' and val != 'N/A':
                        try:
                            data['per'] = float(val)
                        except:
                            pass

            # PBR
            if 'PBR' in tr_text:
                pbr_em = tr.find('em', id='_pbr')
                if pbr_em:
                    val = pbr_em.get_text(strip=True).replace(',', '')
                    if val and val != '-' and val != 'N/A':
                        try:
                            data['pbr'] = float(val)
                        except:
                            pass

            # 배당수익률
            if '배당수익률' in tr_text:
                d_em = tr.find('em', id='_dvr')
                if d_em:
                    val = d_em.get_text(strip=True).replace(',', '').replace('%', '')
                    if val and val != '-' and val != 'N/A':
                        try:
                            data['dividend_yield'] = float(val)
                        except:
                            pass

        # --- 수급 현황 (일별 매매동향) 파싱 ---
        frgn_response = requests.get(investor_url, headers=headers, timeout=5)
        frgn_soup = BeautifulSoup(frgn_response.text, 'html.parser')
        frgn_table = frgn_soup.find('table', class_='type2')
        if frgn_table:
            rows = frgn_table.find_all('tr')
            f_total = 0
            i_total = 0
            count = 0
            for r in rows:
                if count >= 5: break # 최근 5일치 합산
                tds = r.find_all('td')
                # 날짜가 있는 데이터 행인지 확인 (클래스 tc 가 보통 날짜를 포함함)
                if len(tds) >= 7 and '.' in tds[0].get_text():
                    try:
                        # 숫자와 부호만 추출
                        i_text = re.sub(r'[^0-9\-]', '', tds[5].get_text(strip=True))
                        f_text = re.sub(r'[^0-9\-]', '', tds[6].get_text(strip=True))
                        if i_text: i_total += int(i_text)
                        if f_text: f_total += int(f_text)
                        count += 1
                    except: continue
            data['foreign_net_buy'] = f_total
            data['inst_net_buy'] = i_total

        # --- 기술적 지표 (RSI) 약식 계산 또는 외부 연동 ---
        if data['high_52w'] > data['low_52w']:
            data['rsi'] = round((data['current_price'] - data['low_52w']) / (data['high_52w'] - data['low_52w']) * 100, 1)

        return data
    except Exception as e:
        print(f"Error collecting data for {ticker}: {e}")
        return data

def get_detailed_price(ticker):
    """[김정음] 네이버 금융에서 현재가, 전일종가, 등락 정보를 상세히 가져옵니다."""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        # Naver Finance는 EUC-KR을 사용하므로 명시적 처리
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': f'https://finance.naver.com/item/main.naver?code={ticker}'
        }
        res = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='euc-kr')
        
        # 1. 현재가 추출
        today_area = soup.select_one('.no_today')
        current_price = 0
        if today_area:
            price_elem = today_area.select_one('.blind')
            if price_elem:
                current_price = int(re.sub(r'[^0-9]', '', price_elem.text))

        # 2. 전일종가 추출 (다양한 패턴 대응)
        prev_close = 0
        
        # 패턴 A: .no_info 테이블 (일반 주식)
        info_area = soup.select_one('.no_info')
        if info_area:
            for td in info_area.select('td'):
                if '전일' in td.text:
                    val_elem = td.select_one('.blind')
                    if val_elem:
                        prev_close = int(re.sub(r'[^0-9]', '', val_elem.text))
                        break
        
        # 패턴 B: .rate_info 영역 (ETF 등)
        if prev_close == 0:
            rate_info = soup.select_one('.rate_info')
            if rate_info:
                # '전일' 텍스트를 포함한 td나 th를 찾음
                target = rate_info.find(string=re.compile('전일'))
                if target:
                    parent = target.find_parent(['td', 'th', 'div'])
                    # 인접한 곳에서 숫자 추출
                    val_elem = parent.find_next_sibling() if parent else None
                    if not val_elem:
                         val_elem = parent # 자기 자신일 수도 있음
                    
                    # blind 클래스 혹은 텍스트에서 숫자 추출
                    text_to_search = val_elem.text if val_elem else ""
                    nums = re.findall(r'[0-9,]+', text_to_search)
                    if nums:
                        prev_close = int(nums[0].replace(',', ''))

        # 3. 고가/저가 추출
        high_price = 0
        low_price = 0
        if info_area:
            for td in info_area.select('td'):
                if '고가' in td.text and '52주' not in td.text:
                    val_elem = td.select_one('.blind')
                    if val_elem: high_price = int(re.sub(r'[^0-9]', '', val_elem.text))
                elif '저가' in td.text and '52주' not in td.text:
                    val_elem = td.select_one('.blind')
                    if val_elem: low_price = int(re.sub(r'[^0-9]', '', val_elem.text))

        # 4. 등락액, 등락률 계산
        change = current_price - prev_close if prev_close > 0 else 0
        change_rate = (change / prev_close * 100) if prev_close > 0 else 0
        
        return {
            'current_price': current_price,
            'prev_close': prev_close,
            'high_price': high_price,
            'low_price': low_price,
            'change': change,
            'change_rate': round(change_rate, 2)
        }
    except Exception as e:
        print(f"Detailed scraping error for {ticker}: {e}")
    return {'current_price': 0, 'prev_close': 0, 'change': 0, 'change_rate': 0}

def get_market_index(ticker):
    """[김정음] 네이버 금융에서 코스피, 코스닥 지수를 가져옵니다."""
    try:
        url = f"https://finance.naver.com/sise/sise_index.naver?code={ticker}"
        res = requests.get(url, timeout=5)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='euc-kr')
        
        now_value = soup.select_one('#now_value')
        change_area = soup.select_one('#change_value_and_rate')
        
        if now_value and change_area:
            price = float(now_value.text.replace(',', ''))
            
            # 등락 및 등락률 파싱
            change_text = change_area.text.strip()
            # "상승 10.00 +0.40%" 형태 또는 "하락 10.00 -0.40%"
            nums = re.findall(r'[0-9.]+', change_text)
            
            change = float(nums[0]) if len(nums) > 0 else 0
            rate = float(nums[1]) if len(nums) > 1 else 0
            
            if '하락' in change_text or '-' in change_text:
                change = -change
                rate = -rate
                
            return {
                'name': '코스피' if ticker == 'KOSPI' else '코스닥',
                'code': ticker,
                'price': price,
                'change': change,
                'rate': rate
            }
    except Exception as e:
        print(f"Error fetching index {ticker}: {e}")
    return {'name': ticker, 'code': ticker, 'price': 0, 'change': 0, 'rate': 0}

@app.route('/api/market/indices', methods=['GET'])
def get_market_indices_api():
    """[김정음] 주요 시장 지수를 반환합니다."""
    indices = []
    for ticker in ['KOSPI', 'KOSDAQ']:
        indices.append(get_market_index(ticker))
    return jsonify(indices)

@app.route('/api/my_stocks', methods=['GET'])
def get_my_stocks():
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("SELECT code, name, added_at, purchase_price, quantity, stop_loss_ratio, owner FROM my_stocks WHERE type = 'portfolio' ORDER BY added_at DESC")
        stocks = [dict(row) for row in cursor.fetchall()]
        return jsonify(stocks)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_daily_prices(code, pages=3):
    """네이버 금융에서 일별 시세를 가져옵니다. (고가 정보 포함)"""
    global daily_prices_cache
    cache_key = f"{code}_{pages}"
    entry = daily_prices_cache.get(cache_key)
    if entry and time.time() - entry['ts'] < 3600:
        return entry['data']

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': f'https://finance.naver.com/item/main.naver?code={code}'
    }
    prices = []
    try:
        for page in range(1, pages + 1):
            url = f"https://finance.naver.com/item/sise_day.naver?code={code}&page={page}"
            res = requests.get(url, headers=headers, timeout=5)
            soup = BeautifulSoup(res.text, 'html.parser')
            rows = soup.select('tr[onmouseover]')
            for row in rows:
                tds = row.find_all('td')
                if len(tds) >= 7:
                    try:
                        date = tds[0].get_text(strip=True).replace('.', '-')
                        close = int(tds[1].get_text(strip=True).replace(',', ''))
                        high = int(tds[4].get_text(strip=True).replace(',', ''))
                        prices.append({'date': date, 'close': close, 'high': high})
                    except: continue
        daily_prices_cache[cache_key] = {'data': prices, 'ts': time.time()}
        return prices
    except:
        return []

def parse_market_cap_to_억(text):
    """'435조 5,730억' → 4355730, '5,230억' → 5230"""
    text = text.replace(',', '').strip()
    total = 0
    m = re.search(r'(\d+)조', text)
    if m:
        total += int(m.group(1)) * 10000
    m = re.search(r'(\d+)억', text)
    if m:
        total += int(m.group(1))
    return total

def get_stock_market_cap_억(code):
    """종목 시가총액(억원) 반환. 24시간 캐시."""
    global market_cap_cache
    now = time.time()
    if code in market_cap_cache and now - market_cap_cache[code]['ts'] < 86400:
        return market_cap_cache[code]['cap_억']
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': f'https://finance.naver.com/item/main.naver?code={code}'
        }
        res = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='euc-kr')
        table = soup.find('table', class_='tb_type1')
        if table:
            for tr in table.find_all('tr'):
                th = tr.find('th')
                if th and '시가총액' in th.get_text():
                    td = tr.find('td')
                    if td:
                        cap = parse_market_cap_to_억(td.get_text(strip=True))
                        if cap > 0:
                            market_cap_cache[code] = {'cap_억': cap, 'ts': now}
                            return cap
    except Exception:
        pass
    return 0

def get_kospi_total_cap_억():
    """KOSPI 전체 시가총액(억원) 반환. 24시간 캐시."""
    global kospi_total_cap_cache
    now = time.time()
    if now - kospi_total_cap_cache['ts'] < 86400 and kospi_total_cap_cache['cap_억'] > 0:
        return kospi_total_cap_cache['cap_억']
    try:
        url = "https://finance.naver.com/sise/sise_index.naver?code=KOSPI"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://finance.naver.com/sise/sise_index.naver?code=KOSPI'
        }
        res = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='euc-kr')
        for tr in soup.find_all('tr'):
            th = tr.find('th')
            if th and '시가총액' in th.get_text():
                td = tr.find('td')
                if td:
                    cap = parse_market_cap_to_억(td.get_text(strip=True))
                    if cap > 0:
                        kospi_total_cap_cache = {'cap_억': cap, 'ts': now}
                        return cap
    except Exception:
        pass
    return 0

def get_kospi_weight(code):
    """종목의 코스피 비중(0.0~1.0) 반환."""
    stock_cap = get_stock_market_cap_억(code)
    total_cap = get_kospi_total_cap_억()
    if stock_cap > 0 and total_cap > 0:
        return min(stock_cap / total_cap, 1.0)
    return 0.0

def get_holding_high(code, added_at, current_price, purchase_price, kospi_data=None):
    """[김선화] 보유 시점(added_at) 이후의 최고 종가와 해당 시점 코스피를 계산하거나 캐시에서 가져옵니다."""
    global holding_high_cache

    today_str = datetime.now().strftime('%Y-%m-%d')
    if code in holding_high_cache and holding_high_cache[code]['date'] == today_str:
        return holding_high_cache[code]

    daily_prices = get_daily_prices(code, pages=3)

    added_date = added_at[:10]
    relevant = [(p['close'], p['date']) for p in daily_prices if added_date <= p['date'] < today_str]

    if relevant:
        max_close, max_date = max(relevant, key=lambda x: x[0])
    else:
        max_close, max_date = 0, added_date

    # [김선화] 취득가가 실제 최고가인 경우 → 기준 날짜는 매수일
    if purchase_price >= max_close:
        max_price = purchase_price
        max_date = added_date
    else:
        max_price = max_close

    # 최고가 시점 코스피 탐색
    high_kospi = 0
    if kospi_data:
        for k in kospi_data:
            if k['date'] <= max_date:
                high_kospi = k['index']
                break

    holding_high_cache[code] = {
        'high': max_price,
        'high_date': max_date,
        'high_kospi': high_kospi,
        'date': today_str
    }
    return holding_high_cache[code]

def get_kospi_daily(pages=2):
    """네이버 금융에서 코스피 일별 시세를 가져옵니다."""
    global kospi_daily_cache
    if time.time() - kospi_daily_cache['ts'] < 1800 and kospi_daily_cache['data']:
        return kospi_daily_cache['data']

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://finance.naver.com/sise/sise_index.naver?code=KOSPI'
    }
    data = []
    try:
        for page in range(1, pages + 1):
            url = f"https://finance.naver.com/sise/sise_index_day.naver?code=KOSPI&page={page}"
            res = requests.get(url, headers=headers, timeout=5)
            soup = BeautifulSoup(res.text, 'html.parser')
            rows = soup.find_all('tr')
            for row in rows:
                date_td = row.find('td', class_='date')
                price_td = row.find('td', class_='number_1')
                if date_td and price_td:
                    date = date_td.get_text(strip=True).replace('.', '-')
                    price = float(price_td.get_text(strip=True).replace(',', ''))
                    data.append({'date': date, 'index': price})
        kospi_daily_cache = {'data': data, 'ts': time.time()}
        return data
    except:
        return []

def calculate_stop_loss_status(stock, current_price, daily_prices, kospi_data, kospi_current):
    """5가지 손절 원칙에 따라 상태를 진단합니다."""
    sl_ratio = (stock.get('stop_loss_ratio', 0) or 0) / 100
    if sl_ratio <= 0:
        return {'signal': 'KEEP', 'reasons': []}

    purchase_price = stock.get('purchase_price', 0)
    added_at = stock.get('added_at', '')[:10]  # YYYY-MM-DD
    
    signals = []
    
    # 1. 취득가 대비 손절 (Legacy)
    if current_price <= purchase_price * (1 - sl_ratio):
        signals.append(f"취득가({purchase_price:,}원) 대비 {stock['stop_loss_ratio']}% 하락")

    # 2. 최근 최고가 대비 손절 (Trailing - KOSPI 초과 하락 기준)
    if daily_prices:
        relevant = [(p['close'], p['date']) for p in daily_prices if p['date'] >= added_at]
        if not relevant:
            relevant = [(p['close'], p['date']) for p in daily_prices]

        if relevant:
            max_close, max_date = max(relevant, key=lambda x: x[0])
            if purchase_price >= max_close:
                max_price, max_date = purchase_price, added_at
            else:
                max_price = max_close

            if kospi_data and kospi_current > 0:
                high_kospi = next((k['index'] for k in kospi_data if k['date'] <= max_date), 0)
                if high_kospi > 0:
                    kospi_weight = get_kospi_weight(stock['code'])
                    stock_drop = (current_price - max_price) / max_price
                    kospi_drop = (kospi_current - high_kospi) / high_kospi
                    adjusted_kospi_drop = kospi_drop * (1 - kospi_weight)
                    excess_drop = stock_drop - adjusted_kospi_drop
                    if excess_drop <= -sl_ratio:
                        signals.append(
                            f"최근 최고가({max_price:,}원) 대비 코스피 초과 하락 "
                            f"({round(excess_drop*100,1)}% / 종목 {round(stock_drop*100,1)}%, "
                            f"코스피 보정 {round(adjusted_kospi_drop*100,1)}% [비중 {round(kospi_weight*100,1)}% 차감])"
                        )
                else:
                    if current_price <= max_price * (1 - sl_ratio):
                        signals.append(f"최근 최고가({max_price:,}원) 대비 {stock['stop_loss_ratio']}% 하락")
            else:
                if current_price <= max_price * (1 - sl_ratio):
                    signals.append(f"최근 최고가({max_price:,}원) 대비 {stock['stop_loss_ratio']}% 하락")

    # 3. 코스피 대비 상대 손절
    if purchase_price > 0 and kospi_data and kospi_current > 0:
        # 매수일의 코스피 지수 찾기
        purchase_kospi = None
        for k in kospi_data:
            if k['date'] <= added_at:
                purchase_kospi = k['index']
                break
        if not purchase_kospi: purchase_kospi = kospi_data[-1]['index']
        
        stock_return = (current_price - purchase_price) / purchase_price
        kospi_return = (kospi_current - purchase_kospi) / purchase_kospi
        relative_return = stock_return - kospi_return
        
        if relative_return <= -sl_ratio:
            signals.append(f"지수 대비 상대 수익률({round(relative_return*100, 1)}%)이 손절 포인트 도달")

    if signals:
        return {'signal': 'SELL', 'reasons': signals}
    return {'signal': 'KEEP', 'reasons': []}

@app.route('/api/my_stocks/status', methods=['GET'])
def get_my_stocks_status():
    try:
        db = get_db()
        cursor = db.cursor()
        audit_data = load_financial_health()
        
        # 보유 종목 + 관심 종목 통합 조회
        cursor.execute("SELECT code, name, purchase_price, quantity, stop_loss_ratio, added_at, type FROM my_stocks")
        all_rows = [dict(row) for row in cursor.fetchall()]
        portfolio_stocks = [s for s in all_rows if s.get('type') == 'portfolio']
        watchlist_stocks = [s for s in all_rows if s.get('type') == 'watchlist']
        stocks = portfolio_stocks + watchlist_stocks
        
        # 상세 데이터 수집 (병렬 처리)
        with ThreadPoolExecutor(max_workers=10) as executor:
            details = list(executor.map(lambda s: get_portfolio_details(s['code']), stocks))
        
        # 코스피 지수 데이터 가져오기 (상대 매도 포인트 계산용)
        kospi_data = get_kospi_daily(pages=3)
        kospi_current = 0
        if kospi_data:
            kospi_current = kospi_data[0]['index']

        results = []
        for i, stock in enumerate(stocks):
            detail = details[i] if details[i] else {}
            price = detail.get('current_price', 0)
            purchase_price = stock.get('purchase_price') or 0
            qty = stock.get('quantity') or 0
            profit = (price - purchase_price) * qty if purchase_price > 0 else 0
            profit_rate = ((price - purchase_price) / purchase_price * 100) if purchase_price > 0 else 0
            
            # [김선화] 당일 변동 계산 (오늘 신규 매입한 종목인 경우 매수 단가 대비로 계산)
            today_str = datetime.now().strftime('%Y-%m-%d')
            prev_price = detail.get('prev_price', 0)
            is_today_buy = False
            if stock.get('added_at'):
                try:
                    added_date = stock['added_at'][:10]
                    if added_date == today_str:
                        is_today_buy = True
                except Exception:
                    pass
            
            if is_today_buy:
                change = price - purchase_price
                change_rate = (change / purchase_price * 100) if purchase_price > 0 else 0.0
            else:
                change = price - prev_price if prev_price > 0 else 0
                change_rate = (change / prev_price * 100) if prev_price > 0 else 0.0
            
            # 손절 상태 진단 (보유 종목만)
            sl_diagnosis = {'signal': 'KEEP', 'reasons': []}
            if stock['type'] == 'portfolio':
                daily_prices = get_daily_prices(stock['code'], pages=3)
                sl_diagnosis = calculate_stop_loss_status(stock, price, daily_prices, kospi_data, kospi_current)

            results.append({
                'code': stock['code'],
                'name': stock['name'],
                'type': stock['type'],
                'current_price': price,
                'purchase_price': purchase_price,
                'quantity': qty,
                'stop_loss_ratio': stock.get('stop_loss_ratio', 0),
                'sl_diagnosis': sl_diagnosis, # 손절 진단 추가
                'profit': profit,
                'profit_rate': round(profit_rate, 2),
                'change': change,
                'change_rate': round(change_rate, 2),
                'market_cap': detail.get('market_cap', 'N/A'),
                'opinion': detail.get('opinion', 'N/A'),
                'target_price': detail.get('target_price', 0),
                'high_52w': detail.get('high_52w', 0),
                'low_52w': detail.get('low_52w', 0),
                'per': detail.get('per', 0),
                'pbr': detail.get('pbr', 0),
                'eps': detail.get('eps', 0),
                'bps': detail.get('bps', 0),
                'sector_per': detail.get('sector_per', 0),
                'dividend_yield': detail.get('dividend_yield', 0),
                'revenue_growth': detail.get('revenue_growth', 'N/A'),
                'profit_growth': detail.get('profit_growth', 'N/A'),
                'roe': detail.get('roe', 0),
                'debt_ratio': detail.get('debt_ratio', 0),
                'revenue': detail.get('revenue', 'N/A'),
                'operating_profit': detail.get('operating_profit', 'N/A'),
                'net_profit': detail.get('net_profit', 'N/A'),
                'foreign_net_buy': detail.get('foreign_net_buy', 0),
                'inst_net_buy': detail.get('inst_net_buy', 0),
                'foreign_5d_net': detail.get('foreign_5d_net', 0),
                'foreign_20d_net': detail.get('foreign_20d_net', 0),
                'inst_5d_net': detail.get('inst_5d_net', 0),
                'inst_20d_net': detail.get('inst_20d_net', 0),
                'foreign_ownership_ratio': detail.get('foreign_ownership_ratio', 0),
                'rsi_pos': detail.get('rsi', 0),
                'news': detail.get('news', []),
                'ma5': detail.get('ma5', 0),
                'ma20': detail.get('ma20', 0),
                'ma5_diff': detail.get('ma5_diff', 0),
                'audit_opinion_team': audit_data.get(stock['code'], {}).get('audit', 'N/A'),
                'internal_control_team': audit_data.get(stock['code'], {}).get('internal', 'N/A'),
                'roe_team': audit_data.get(stock['code'], {}).get('roe', 0),
                'debt_ratio_team': audit_data.get(stock['code'], {}).get('debt_ratio', 0),
                'ma20_diff': detail.get('ma20_diff', 0),
                'owner': stock.get('owner', '나')
            })
            
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/my_stocks', methods=['POST'])
def add_my_stock():
    data = request.get_json() or {}
    code = data.get('code')
    name = data.get('name', '')
    purchase_price = data.get('purchase_price', 0)
    quantity = data.get('quantity', 0)
    stop_loss_ratio = data.get('stop_loss_ratio', 0)
    owner = data.get('owner', '나')
    if not code:
        return jsonify({'success': False, 'message': '종목 코드가 필요합니다.'}), 400
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("""
            INSERT INTO my_stocks (code, name, added_at, purchase_price, quantity, stop_loss_ratio, owner, type)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'portfolio')
            ON CONFLICT (code) DO UPDATE SET
                name=EXCLUDED.name, added_at=EXCLUDED.added_at,
                purchase_price=EXCLUDED.purchase_price, quantity=EXCLUDED.quantity,
                stop_loss_ratio=EXCLUDED.stop_loss_ratio, owner=EXCLUDED.owner,
                type='portfolio'
        """, (code, name, datetime.now().isoformat(), purchase_price, quantity, stop_loss_ratio, owner))
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/my_stocks/<code_val>', methods=['DELETE'])
def delete_my_stock(code_val):
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("DELETE FROM my_stocks WHERE code = ?", (code_val,))
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/my_stocks/<code_val>/sell', methods=['POST'])
def sell_my_stock(code_val):
    """[김선화] 보유 종목 매도 처리 - 수량 차감, 전량 시 포트폴리오 제거, sell_history 기록"""
    try:
        data = request.get_json() or {}
        sell_price = float(data.get('sell_price', 0))
        sell_qty = int(data.get('sell_qty', 0))
        if sell_price <= 0 or sell_qty <= 0:
            return jsonify({'success': False, 'message': '매도가와 수량을 올바르게 입력해주세요.'}), 400

        db = get_db()
        cursor = db.cursor()
        cursor.execute("SELECT code, name, purchase_price, quantity, owner FROM my_stocks WHERE code = ?", (code_val,))
        row = cursor.fetchone()
        if not row:
            return jsonify({'success': False, 'message': '보유 종목을 찾을 수 없습니다.'}), 404

        stock = dict(row)
        current_qty = stock['quantity'] or 0
        purchase_price = stock['purchase_price'] or 0

        if sell_qty > current_qty:
            return jsonify({'success': False, 'message': f'매도 수량({sell_qty:,}주)이 보유 수량({current_qty:,}주)을 초과합니다.'}), 400

        profit = (sell_price - purchase_price) * sell_qty
        profit_rate = ((sell_price - purchase_price) / purchase_price * 100) if purchase_price else 0
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sell_date = datetime.now().strftime('%Y-%m-%d')

        cursor.execute("""
            INSERT INTO sell_history (code, name, owner, sell_price, sell_qty, purchase_price, profit, profit_rate, sell_date, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (code_val, stock['name'], stock.get('owner', '나'), sell_price, sell_qty, purchase_price, profit, profit_rate, sell_date, now))

        new_qty = current_qty - sell_qty
        if new_qty <= 0:
            cursor.execute("DELETE FROM my_stocks WHERE code = ?", (code_val,))
            fully_sold = True
        else:
            cursor.execute("UPDATE my_stocks SET quantity = ? WHERE code = ?", (new_qty, code_val))
            fully_sold = False

        db.commit()

        profit_str = f"{profit:+,.0f}원 ({profit_rate:+.1f}%)"
        msg = f"{stock['name']} {'전량' if fully_sold else f'{sell_qty:,}주'} 매도 완료 | 수익: {profit_str}"
        return jsonify({'success': True, 'message': msg, 'profit': profit, 'profit_rate': profit_rate,
                        'remaining_qty': new_qty, 'fully_sold': fully_sold})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/my_stocks/<code_val>', methods=['PATCH'])
def update_my_stock(code_val):
    data = request.get_json() or {}
    purchase_price = data.get('purchase_price')
    quantity = data.get('quantity')
    stop_loss_ratio = data.get('stop_loss_ratio')
    owner = data.get('owner')
    
    try:
        db = get_db()
        cursor = db.cursor()
        
        updates = []
        params = []
        if purchase_price is not None:
            updates.append("purchase_price = %s")
            params.append(purchase_price)
        if quantity is not None:
            updates.append("quantity = %s")
            params.append(quantity)
        if stop_loss_ratio is not None:
            updates.append("stop_loss_ratio = %s")
            params.append(stop_loss_ratio)
        if owner is not None:
            updates.append("owner = %s")
            params.append(owner)

        if not updates:
            return jsonify({'success': False, 'message': '수정할 데이터가 없습니다.'}), 400

        params.append(code_val)
        query = f"UPDATE my_stocks SET {', '.join(updates)} WHERE code = %s"
        cursor.execute(query, params)
        
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/search_stock', methods=['GET'])
def search_stock():
    query = request.args.get('q', '')
    if len(query) < 2:
        return jsonify([])
    
    try:
        db = get_db()
        cursor = db.cursor()
        # 이름으로 검색 (부분 일치)
        cursor.execute("SELECT code, name FROM stocks_master WHERE name LIKE ? LIMIT 10", (f'%{query}%',))
        results = [dict(row) for row in cursor.fetchall()]
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/update_master', methods=['POST'])
def update_master():
    """종목 마스터 리스트 업데이트 (백그라운드)"""
    def run_update():
        try:
            import requests
            from bs4 import BeautifulSoup
            
            all_stocks = []
            session = requests.Session()
            # KOSPI (sosok=0), KOSDAQ (sosok=1)
            for sosok in [0, 1]:
                market_name = 'KOSPI' if sosok == 0 else 'KOSDAQ'
                page = 1
                while True:
                    url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
                    res = session.get(url)
                    soup = BeautifulSoup(res.text, 'html.parser')
                    table = soup.find('table', {'class': 'type_2'})
                    if not table: break
                    
                    found = False
                    for a in table.find_all('a', {'class': 'tltle'}):
                        code = a.get('href').split('code=')[1]
                        name = a.text.strip()
                        all_stocks.append((code, name, market_name))
                        found = True
                    
                    if not found or page > 40: break # 대략 40페이지까지
                    page += 1

            # [김정음] ETF 리스트 추가 수집
            try:
                etf_url = "https://finance.naver.com/api/sise/etfItemList.nhn"
                etf_res = session.get(etf_url, timeout=10)
                etf_data = etf_res.json()
                if etf_data.get('resultCode') == 'success':
                    etf_items = etf_data.get('result', {}).get('etfItemList', [])
                    for item in etf_items:
                        if 'itemcode' in item:
                            all_stocks.append((item['itemcode'], item['itemname'], 'ETF'))
            except Exception as etf_e:
                print(f"ETF 마스터 수집 중 오류: {etf_e}")
            
            if all_stocks:
                conn = _new_db_conn()
                cursor = conn.cursor()
                cursor.executemany("""
                    INSERT INTO stocks_master (code, name, market) VALUES (?, ?, ?)
                    ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, market=EXCLUDED.market
                """, all_stocks)
                conn.commit()
                conn.close()
                print(f"종목 마스터 업데이트 완료: {len(all_stocks)}개 종목")
        except Exception as e:
            print(f"마스터 업데이트 중 오류: {e}")

    threading.Thread(target=run_update).start()
    return jsonify({'success': True, 'message': '업데이트가 시작되었습니다.'})


@app.route('/api/results', methods=['GET'])
def get_results():
    try:
        from drive_sync import list_files_in_folder
        drive_files = list_files_in_folder()
        
        results = []
        for df in drive_files:
            if df.get('mimeType') == 'application/vnd.google-apps.spreadsheet':
                name = df['name']
                # .xlsx 확장자 보정
                if not name.endswith('.xlsx'):
                    name += '.xlsx'
                
                # 파일명에서 시장 및 종목수 파싱
                parts = name.replace('.xlsx', '').split('_')
                market_val = parts[0].upper() if len(parts) > 0 else 'UNKNOWN'
                count_val = parts[1] if len(parts) > 1 else '0'
                
                results.append({
                    'filename': name,
                    'market': market_val,
                    'stock_count': count_val,
                    'created_at': df.get('createdTime'),
                    'size': int(df.get('size', 0)) if df.get('size') else 0,
                    'spreadsheet_id': df['id'],
                    'drive_link': df.get('webViewLink'),
                    'ai_result': None # 실시간 조회시 AI 결과는 별도 API로 처리
                })
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': '구글 드라이브 연결에 실패했습니다.'}), 503

@app.route('/api/download/<filename>')
def download_file(filename):
    # 1. 로컬에 있으면 로컬 파일 제공
    file_path = os.path.join(RESULTS_DIR, filename)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    
    # 2. 로컬에 없으면 드라이브에서 실시간 다운로드
    try:
        from drive_sync import list_files_in_folder, download_from_drive
        drive_files = list_files_in_folder()
        spreadsheet_id = None
        
        # 파일명으로 ID 찾기
        target_name = filename.replace('.xlsx', '')
        for df in drive_files:
            if df['name'] == target_name or df['name'] == filename:
                spreadsheet_id = df['id']
                break
        
        if spreadsheet_id:
            content = download_from_drive(spreadsheet_id)
            if content:
                import io
                return send_file(io.BytesIO(content), as_attachment=True, download_name=filename)
    except Exception as e:
        print(f"드라이브 다운로드 중 오류: {e}")
        
    return jsonify({'error': '파일을 찾을 수 없습니다.'}), 404

@app.route('/api/delete/<filename>', methods=['DELETE'])
def delete_result(filename):
    try:
        from drive_sync import delete_from_drive, list_files_in_folder, find_ai_report
        
        # 1. 드라이브에서 파일 ID 조회
        drive_files = list_files_in_folder()
        spreadsheet_id = None
        target_name_base = filename.replace('.xlsx', '')
        
        for df in drive_files:
            if df['name'] == target_name_base or df['name'] == filename:
                spreadsheet_id = df['id']
                break
        
        # 2. 구글 드라이브 파일 삭제
        if spreadsheet_id:
            delete_from_drive(spreadsheet_id)
            
        # 3. 연관된 AI 리포트 문서 삭제
        existing_report = find_ai_report(target_name_base)
        if existing_report:
            delete_from_drive(existing_report['id'])
            
        for ext in ['.xlsx', '.json']:
            path = os.path.join(RESULTS_DIR, target_name_base + ext)
            if os.path.exists(path):
                os.remove(path)
            
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/ai_report_check/<filename>', methods=['GET'])
def ai_report_check(filename):
    """기존 AI 리포트가 있는지 확인만 (캐시 체크용)"""
    try:
        from drive_sync import find_ai_report, get_doc_content
        base_name = os.path.splitext(filename)[0]
        existing_report = find_ai_report(base_name)
        if existing_report:
            cached_content = get_doc_content(existing_report['id'])
            if cached_content and len(cached_content.strip()) > 100:
                return jsonify({'success': True, 'result': cached_content, 'cached': True})
        return jsonify({'success': True, 'cached': False})
    except Exception as e:
        return jsonify({'success': False, 'cached': False, 'message': str(e)})

@app.route('/api/save_report_to_drive', methods=['POST'])
def save_report_to_drive():
    """포트폴리오 분석 리포트를 구글 드라이브에 저장"""
    try:
        from drive_sync import create_google_doc

        data = request.get_json()
        filename = data.get('filename', '').strip()
        content = data.get('content', '')

        if not filename:
            return jsonify({'success': False, 'message': '파일명이 필요합니다.'})
        if not content:
            return jsonify({'success': False, 'message': '저장할 내용이 없습니다.'})

        # 구글 드라이브에 문서 저장 (Portfolio_Reports 폴더에 저장)
        result = create_google_doc(filename, content, folder_name="Portfolio_Reports")

        if result:
            return jsonify({'success': True, 'link': result.get('link')})
        else:
            return jsonify({'success': False, 'message': '구글 드라이브 저장 실패'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/ai_analyze/<filename>', methods=['POST'])
def ai_analyze(filename):
    try:
        from drive_sync import find_ai_report, get_doc_content, create_google_doc, download_from_drive

        # 파일명에서 확장자 제거 (AI 리포트 검색용)
        base_name = os.path.splitext(filename)[0]

        # 1. 구글 드라이브에서 기존 AI 리포트 확인 (혹시 직접 호출된 경우 대비)
        existing_report = find_ai_report(base_name)
        if existing_report:
            cached_content = get_doc_content(existing_report['id'])
            if cached_content and len(cached_content.strip()) > 100:
                return jsonify({'success': True, 'result': cached_content, 'cached': True})

        # 2. 원본 데이터 파일 확인 (Drive-Native: Drive에서 직접 ID 조회)
        file_path = os.path.join(RESULTS_DIR, filename)
        if not os.path.exists(file_path):
            from drive_sync import list_files_in_folder
            drive_files = list_files_in_folder()
            target_name = filename.replace('.xlsx', '')
            spreadsheet_id = None
            for df in drive_files:
                if df['name'] == target_name or df['name'] == filename:
                    spreadsheet_id = df['id']
                    break
            if spreadsheet_id:
                content = download_from_drive(spreadsheet_id)
                if content:
                    with open(file_path, 'wb') as f:
                        f.write(content)
                else:
                    return jsonify({'success': False, 'message': '드라이브에서 파일을 다운로드할 수 없습니다.'}), 404
            else:
                return jsonify({'success': False, 'message': '드라이브에서 파일을 찾을 수 없습니다.'}), 404

        # 3. AI 분석 수행
        try:
            result_text = analyze_stock_data(file_path)
        finally:
            # Drive-Native: 분석용 임시 파일 즉시 삭제
            if os.path.exists(file_path):
                os.remove(file_path)

        # 4. 결과를 구글 문서로 저장 (유효한 경우만)
        if "오류" not in result_text and "제한" not in result_text:
            report_title = f"AI 분석 리포트 - {base_name}"
            create_google_doc(report_title, result_text)

        return jsonify({'success': True, 'result': result_text, 'cached': False})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/ai_analyze_portfolio', methods=['POST'])
def ai_analyze_portfolio():
    try:
        data = request.get_json() or {}
        portfolio_data = data.get('portfolio_data', [])
        force_refresh = data.get('refresh', False)
        
        if not portfolio_data:
            return jsonify({'success': False, 'message': '분석할 데이터가 없습니다.'}), 400
            
        # 1. 캐시 키 생성 (종목코드, 평단가, 수량의 조합 + 오늘 날짜)
        import hashlib
        sorted_portfolio = sorted(portfolio_data, key=lambda x: x['code'])
        # 평단가와 수량이 같으면 같은 포트폴리오로 간주 (시장가는 수시로 변하므로 캐시 효율을 위해 제외)
        portfolio_str = "|".join([f"{s['code']}:{s.get('purchase_price',0)}:{s.get('quantity',0)}" for s in sorted_portfolio])
        today = datetime.now().strftime('%Y-%m-%d')
        cache_key = hashlib.md5(f"{portfolio_str}_{today}".encode()).hexdigest()
        
        db = get_db()
        cursor = db.cursor()
        
        # 2. 오래된 캐시 삭제 (오늘 이전 데이터)
        cursor.execute("DELETE FROM portfolio_ai_cache WHERE created_at < ?", (today,))
        db.commit()
        
        # 3. 캐시 확인 (강제 새로고침이 아닌 경우)
        if not force_refresh:
            cursor.execute("SELECT ai_result FROM portfolio_ai_cache WHERE cache_key = ?", (cache_key,))
            row = cursor.fetchone()
            if row and row['ai_result']:
                return jsonify({'success': True, 'result': row['ai_result'], 'cached': True})
            
        # 4. AI 분석 수행
        result_text = analyze_portfolio(portfolio_data)
        
        # 5. 결과 저장 (유효한 경우만)
        if "오류" not in result_text and "제한" not in result_text:
            cursor.execute("""
                INSERT INTO portfolio_ai_cache (cache_key, ai_result, created_at) VALUES (?, ?, ?)
                ON CONFLICT (cache_key) DO UPDATE SET ai_result=EXCLUDED.ai_result, created_at=EXCLUDED.created_at
            """, (cache_key, result_text, datetime.now().isoformat()))
            db.commit()
            
        return jsonify({'success': True, 'result': result_text, 'cached': False})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/news_search', methods=['GET'])
def news_search():
    query = request.args.get('q', '')
    if not query:
        return jsonify([])
    
    try:
        import requests
        from bs4 import BeautifulSoup
        import urllib.parse
        
        encoded_query = urllib.parse.quote(query.encode('euc-kr'))
        url = f"https://finance.naver.com/news/news_search.naver?q={encoded_query}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers)
        # Naver news search uses euc-kr
        soup = BeautifulSoup(res.content.decode('euc-kr', 'replace'), 'html.parser')
        
        news_list = []
        # Naver Finance news search result structure
        items = soup.select('.newsList dt.articleSubject, .newsList dd.articleSubject')
        # Sometimes it's just .newsList dl
        if not items:
            items = soup.select('.newsList dl dt a')
            
        # Let's try a more robust selector
        articles = soup.select('.newsList dl')
        for art in articles:
            subject_a = art.select_one('dt.articleSubject a, dd.articleSubject a, dt a')
            if not subject_a: continue
            
            title = subject_a.get_text(strip=True)
            link = "https://finance.naver.com" + subject_a['href']
            
            summary = ""
            summary_el = art.select_one('dd.articleSummary')
            if summary_el:
                # Remove span (source/date) from summary
                for span in summary_el.find_all('span'):
                    span.decompose()
                summary = summary_el.get_text(strip=True)
            
            source = ""
            date = ""
            info_el = art.select_one('.press, .wdate')
            # Naver search results have press and date in different spans usually
            press_el = art.select_one('.press')
            date_el = art.select_one('.wdate')
            if press_el: source = press_el.get_text(strip=True)
            if date_el: date = date_el.get_text(strip=True)
            
            news_list.append({
                'title': title,
                'link': link,
                'summary': summary,
                'source': source,
                'date': date
            })
            
        return jsonify(news_list[:20])
    except Exception as e:
        print(f"News search error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/watchlist', methods=['GET'])
def get_watchlist():
    """관심 종목 리스트."""
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("SELECT code, name, added_at, owner FROM my_stocks WHERE type = 'watchlist' ORDER BY added_at DESC")
        stocks = [dict(row) for row in cursor.fetchall()]
        return jsonify(stocks)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/watchlist', methods=['POST'])
def add_to_watchlist():
    """관심 종목 추가."""
    try:
        data = request.get_json()
        code = data.get('code')
        name = data.get('name', '')
        owner = data.get('owner', '나')
        if not code:
            return jsonify({'success': False, 'message': '코드가 누락되었습니다.'}), 400

        db = get_db()
        cursor = db.cursor()
        cursor.execute("""
            INSERT INTO my_stocks (code, name, added_at, type, owner, purchase_price, quantity, stop_loss_ratio)
            VALUES (?, ?, ?, 'watchlist', ?, 0, 0, 0)
            ON CONFLICT (code) DO NOTHING
        """, (code, name, datetime.now().isoformat(), owner))
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/watchlist/<code>', methods=['DELETE'])
def delete_from_watchlist(code):
    """관심 종목 삭제."""
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("DELETE FROM my_stocks WHERE code = ? AND type = 'watchlist'", (code,))
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/watchlist/promote', methods=['POST'])
def promote_to_portfolio():
    """관심 종목 → 보유 종목 승격 (type 변경)."""
    try:
        data = request.get_json()
        code = data.get('code')
        name = data.get('name', '')
        price = data.get('purchase_price', 0)
        qty = data.get('quantity', 0)
        stop_loss = data.get('stop_loss_ratio', 0)
        owner = data.get('owner', '나')

        db = get_db()
        cursor = db.cursor()
        cursor.execute("""
            INSERT INTO my_stocks (code, name, added_at, purchase_price, quantity, stop_loss_ratio, owner, type)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'portfolio')
            ON CONFLICT (code) DO UPDATE SET
                type='portfolio', added_at=EXCLUDED.added_at,
                purchase_price=EXCLUDED.purchase_price, quantity=EXCLUDED.quantity,
                stop_loss_ratio=EXCLUDED.stop_loss_ratio, owner=EXCLUDED.owner
        """, (code, name, datetime.now().isoformat(), price, qty, stop_loss, owner))
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/stocks/owner', methods=['POST'])
def update_stock_owner():
    """[김선화] 종목의 소유주(나, 경미, 유주) 상태를 업데이트합니다."""
    try:
        data = request.get_json()
        code = data.get('code')
        owner = data.get('owner', '나')
        
        db = get_db()
        cursor = db.cursor()
        
        cursor.execute("UPDATE my_stocks SET owner = ? WHERE code = ?", (owner, code))
        
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/realtime/prices', methods=['GET'])
def get_realtime_prices():
    """[김정음] 내 종목 및 관심 종목의 실시간 주가 정보를 상세히 반환합니다."""
    try:
        kospi_data = get_kospi_daily(pages=2)
        kospi_current = kospi_data[0]['index'] if kospi_data else 0
        
        # [김선화] 스레드 간 중복 호출 방지를 위해 코스피 시가총액을 미리 구하여 캐싱
        get_kospi_total_cap_억()

        db = get_db()
        cursor = db.cursor()

        # [김선화] 효율적 실시간 모니터링을 위해 손절 설정치, 소유주, 추가일자 조회
        cursor.execute("SELECT code, name, purchase_price, quantity, stop_loss_ratio, owner, added_at, type FROM my_stocks")
        all_stocks = [dict(s) for s in cursor.fetchall()]
        if not all_stocks:
            return jsonify([])
            
        results = []
        
        def fetch_stock_data(stock):
            try:
                price_info = get_detailed_price(stock['code'])
                current_price = price_info['current_price']
                purchase_price = stock['purchase_price']
                quantity = stock.get('quantity', 0)
                stop_loss_ratio = stock.get('stop_loss_ratio', 0)
                
                profit_rate = 0
                profit = 0
                is_stop_loss = False
                holding_high = 0
                effective_stop_loss_price = 0
                original_stop_loss_price = 0
                kospi_drop_pct = 0
                kospi_weight_pct = 0

                if stock['type'] == 'portfolio' and purchase_price > 0:
                    profit_rate = round(((current_price - purchase_price) / purchase_price) * 100, 2)
                    profit = (current_price - purchase_price) * quantity

                    # [김선화] 보유 기간 최고가 기반 Trailing Stop-Loss 계산 (KOSPI 초과 하락 기준)
                    high_info = get_holding_high(stock['code'], stock['added_at'], current_price, purchase_price, kospi_data)
                    holding_high = high_info['high']

                    if stop_loss_ratio > 0 and holding_high > 0:
                        sl_ratio = stop_loss_ratio / 100
                        original_stop_loss_price = round(holding_high * (1 - sl_ratio))
                        high_kospi = high_info.get('high_kospi', 0)
                        if high_kospi > 0 and kospi_current > 0:
                            kospi_weight = get_kospi_weight(stock['code'])
                            kospi_drop = (kospi_current - high_kospi) / high_kospi
                            adjusted_kospi_drop = kospi_drop * (1 - kospi_weight)
                            stock_drop = (current_price - holding_high) / holding_high
                            is_stop_loss = (stock_drop - adjusted_kospi_drop) <= -sl_ratio
                            effective_stop_loss_price = round(holding_high * (1 - sl_ratio + adjusted_kospi_drop))
                            kospi_drop_pct = round(adjusted_kospi_drop * 100, 2)
                            kospi_weight_pct = round(kospi_weight * 100, 1)
                        else:
                            is_stop_loss = current_price <= holding_high * (1 - sl_ratio)
                            effective_stop_loss_price = original_stop_loss_price
                            kospi_drop_pct = 0
                            kospi_weight_pct = 0

                return {
                    'code': stock['code'],
                    'name': stock['name'],
                    'price': current_price,
                    'prev_close': price_info['prev_close'],
                    'holding_high': holding_high,
                    'original_stop_loss_price': original_stop_loss_price,
                    'effective_stop_loss_price': effective_stop_loss_price,
                    'kospi_drop_pct': kospi_drop_pct,
                    'kospi_weight_pct': kospi_weight_pct,
                    'change': price_info['change'],
                    'change_rate': price_info['change_rate'],
                    'purchase_price': purchase_price,
                    'quantity': quantity,
                    'profit_rate': profit_rate,
                    'profit': profit,
                    'stop_loss_ratio': stop_loss_ratio,
                    'owner': stock.get('owner', '나'),
                    'is_stop_loss': is_stop_loss,
                    'type': stock['type']
                }
            except Exception:
                return {
                    'code': stock['code'],
                    'name': stock['name'],
                    'price': 0,
                    'prev_close': 0,
                    'holding_high': 0,
                    'original_stop_loss_price': 0,
                    'effective_stop_loss_price': 0,
                    'kospi_drop_pct': 0,
                    'kospi_weight_pct': 0,
                    'change': 'EVEN',
                    'change_rate': 0,
                    'purchase_price': stock['purchase_price'],
                    'quantity': stock.get('quantity', 0),
                    'profit_rate': 0,
                    'profit': 0,
                    'stop_loss_ratio': stock.get('stop_loss_ratio', 0),
                    'owner': stock.get('owner', '나'),
                    'is_stop_loss': False,
                    'type': stock['type']
                }

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_stock = {executor.submit(fetch_stock_data, s): s for s in all_stocks}
            for future in as_completed(future_to_stock):
                try:
                    res_data = future.result()
                    results.append(res_data)
                except Exception:
                    stock = future_to_stock[future]
                    results.append({
                        'code': stock['code'],
                        'name': stock['name'],
                        'price': 0,
                        'prev_close': 0,
                        'holding_high': 0,
                        'original_stop_loss_price': 0,
                        'effective_stop_loss_price': 0,
                        'kospi_drop_pct': 0,
                        'kospi_weight_pct': 0,
                        'change': 'EVEN',
                        'change_rate': 0,
                        'purchase_price': stock['purchase_price'],
                        'quantity': stock.get('quantity', 0),
                        'profit_rate': 0,
                        'profit': 0,
                        'stop_loss_ratio': stock.get('stop_loss_ratio', 0),
                        'owner': stock.get('owner', '나'),
                        'is_stop_loss': False,
                        'type': stock['type']
                    })
                    
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/history/snapshot', methods=['POST'])
def record_daily_snapshot():
    """[김선화] 프론트에서 넘긴 실시간 시세 데이터를 그대로 히스토리에 저장"""
    try:
        body = request.get_json()
        if not body:
            return jsonify({'success': False, 'message': '저장할 데이터가 없습니다.'}), 400

        # force 플래그와 stocks 배열 분리 (구버전 호환: 배열 직접 전송도 허용)
        if isinstance(body, list):
            stocks = body
            force = False
        else:
            stocks = body.get('stocks', [])
            force = body.get('force', False)

        if not stocks:
            return jsonify({'success': False, 'message': '저장할 데이터가 없습니다.'}), 400

        portfolio = [s for s in stocks if s.get('type') == 'portfolio']
        if not portfolio:
            return jsonify({'success': False, 'message': '보유 종목 데이터가 없습니다.'}), 400

        db = get_db()
        cursor = db.cursor()
        today = datetime.now().strftime('%Y-%m-%d')

        # 오늘 데이터 이미 존재 여부 확인
        cursor.execute("SELECT COUNT(*) AS cnt FROM stock_daily_history WHERE date = ?", (today,))
        existing_count = cursor.fetchone()['cnt']
        if existing_count > 0 and not force:
            return jsonify({'success': False, 'exists': True,
                            'message': f'{today} 날짜의 데이터가 이미 존재합니다. 덮어쓰시겠습니까?'})

        recorded_at = datetime.now().isoformat()

        # 덮어쓰기: 기존 날짜 데이터 전체 삭제 후 재삽입
        if existing_count > 0:
            cursor.execute("DELETE FROM stock_daily_history WHERE date = ?", (today,))

        for s in portfolio:
            day_profit = s.get('change', 0) * s.get('quantity', 0)
            change_rate = s.get('change_rate', 0)
            cumulative_profit = s.get('profit', 0)
            cursor.execute('''
                INSERT INTO stock_daily_history
                (date, code, name, purchase_price, current_price, quantity, owner, recorded_at, day_profit, change_rate, cumulative_profit)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (today, s['code'], s['name'], s['purchase_price'], s['price'],
                  s['quantity'], s.get('owner', '나'), recorded_at, day_profit, change_rate, cumulative_profit))

        db.commit()
        action = '덮어쓰기' if existing_count > 0 else '기록'
        return jsonify({'success': True, 'message': f'{today} 기준 {len(portfolio)}개 종목 데이터가 {action}되었습니다.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/history/delete', methods=['DELETE'])
def delete_history():
    """[김정음] 특정 날짜의 히스토리 데이터 전체 삭제"""
    try:
        date = request.args.get('date')
        if not date:
            return jsonify({'success': False, 'message': '날짜를 지정해주세요.'}), 400
        db = get_db()
        cursor = db.cursor()
        cursor.execute("DELETE FROM stock_daily_history WHERE date = ?", (date,))
        db.commit()
        deleted = cursor.rowcount
        if deleted == 0:
            return jsonify({'success': False, 'message': f'{date} 날짜의 데이터가 없습니다.'})
        return jsonify({'success': True, 'message': f'{date} 날짜의 데이터 {deleted}건이 삭제되었습니다.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/history/dates', methods=['GET'])
def get_history_dates():
    """[김선화] 데이터가 기록된 날짜 목록 조회 (최신순)"""
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("""
            SELECT DISTINCT date
            FROM stock_daily_history
            WHERE date IS NOT NULL
            ORDER BY date DESC
        """)
        dates = [row['date'] for row in cursor.fetchall()]
        return jsonify({'success': True, 'dates': dates})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/history/data', methods=['GET'])
def get_history_data():
    """[김선화] 특정 날짜의 종목별 상세 데이터 조회"""
    date = request.args.get('date')
    if not date:
        return jsonify({'success': False, 'message': '날짜를 지정해주세요.'}), 400
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("SELECT * FROM stock_daily_history WHERE date = ? ORDER BY owner, name", (date,))
        history = [dict(row) for row in cursor.fetchall()]
        return jsonify({'success': True, 'data': history})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/history/backfill', methods=['POST'])
def backfill_history():
    """[김정음] 최근 30일치 혹은 특정 일자의 누락된 종가 데이터를 네이버 금융에서 소급 저장"""
    try:
        body = request.get_json(silent=True) or {}
        req_date = request.args.get('date') or body.get('date')
        force = request.args.get('force', '').lower() == 'true' or body.get('force', False)

        db = get_db()
        cursor = db.cursor()

        cursor.execute("SELECT code, name, purchase_price, quantity, owner, added_at FROM my_stocks WHERE type = 'portfolio'")
        stocks = [dict(row) for row in cursor.fetchall()]
        if not stocks:
            return jsonify({'success': False, 'message': '등록된 종목이 없습니다.'})

        today = datetime.now().date()
        recorded_at = datetime.now().isoformat()
        inserted_total = 0
        skipped_total = 0
        errors = []

        target_date_obj = None
        since = today - timedelta(days=30)
        req_pages = 2

        if req_date:
            try:
                target_date_obj = datetime.strptime(req_date, '%Y-%m-%d').date()
                if target_date_obj > today:
                    return jsonify({'success': False, 'message': '미래 날짜는 소급할 수 없습니다.'}), 400
                days_diff = (today - target_date_obj).days
                req_pages = max(2, (days_diff // 7) + 1)
                if req_pages > 10:
                    req_pages = 10
            except ValueError:
                return jsonify({'success': False, 'message': '날짜 형식이 올바르지 않습니다. (YYYY-MM-DD)'}), 400

        for stock in stocks:
            code = stock['code']
            name = stock['name']
            purchase_price = stock['purchase_price'] or 0
            quantity = stock['quantity'] or 0
            owner = stock['owner'] or '나'

            prices = get_daily_prices(code, pages=req_pages)
            if not prices:
                errors.append(f"{name}({code}): 데이터 조회 실패")
                continue

            prices_sorted = sorted(prices, key=lambda x: x['date'])

            if target_date_obj:
                cursor.execute(
                    "SELECT DISTINCT date FROM stock_daily_history WHERE code = ? AND date = ?",
                    (code, req_date)
                )
            else:
                cursor.execute(
                    "SELECT DISTINCT date FROM stock_daily_history WHERE code = ? AND date >= ?",
                    (code, since.strftime('%Y-%m-%d'))
                )
            existing_dates = {row['date'] for row in cursor.fetchall()}

            for i, p in enumerate(prices_sorted):
                date_str = p['date']
                try:
                    price_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                except Exception:
                    continue

                if target_date_obj:
                    if price_date != target_date_obj:
                        continue
                else:
                    if price_date < since or price_date > today:
                        continue

                if date_str in existing_dates:
                    if force:
                        cursor.execute("DELETE FROM stock_daily_history WHERE code = ? AND date = ?", (code, date_str))
                    else:
                        skipped_total += 1
                        continue

                close = p['close']
                prev_close = prices_sorted[i - 1]['close'] if i > 0 else None
                
                # [김선화] 매수 당일인 경우 전일 종가가 아닌 매수 단가 대비로 당일 수익 계산
                is_buy_date = False
                if stock.get('added_at'):
                    try:
                        added_date = stock['added_at'][:10]
                        if added_date == date_str:
                            is_buy_date = True
                    except Exception:
                        pass
                
                if is_buy_date:
                    change_rate = round((close - purchase_price) / purchase_price * 100, 2) if purchase_price > 0 else 0.0
                    day_profit = (close - purchase_price) * quantity if purchase_price > 0 else 0.0
                else:
                    change_rate = round((close - prev_close) / prev_close * 100, 2) if prev_close else 0.0
                    day_profit = (close - prev_close) * quantity if prev_close else 0.0
                    
                cumulative_profit = (close - purchase_price) * quantity

                cursor.execute('''
                    INSERT INTO stock_daily_history
                    (date, code, name, purchase_price, current_price, quantity, owner,
                     recorded_at, day_profit, change_rate, cumulative_profit)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (date_str, code, name, purchase_price, close, quantity, owner,
                      recorded_at, day_profit, change_rate, cumulative_profit))
                inserted_total += 1

        db.commit()
        target_desc = f"{req_date} 자" if req_date else "최근 30일"
        msg = f"{target_desc} 소급 완료: {inserted_total}건 저장, {skipped_total}건 기존 데이터 유지"
        if errors:
            msg += f" / 조회 실패: {', '.join(errors)}"
        return jsonify({'success': True, 'message': msg,
                        'inserted': inserted_total, 'skipped': skipped_total, 'errors': errors})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/history/report', methods=['GET'])
def get_history_report():
    """[김정음] 월/분기/연 단위 히스토리 집계 리포트"""
    period = request.args.get('period', 'month')
    if period == 'month':
        period_expr = "LEFT(date, 7)"
    elif period == 'quarter':
        period_expr = "LEFT(date, 4) || '-Q' || ((CAST(SUBSTRING(date, 6, 2) AS INTEGER) - 1) / 3 + 1)"
    else:
        period_expr = "LEFT(date, 4)"
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute(f"""
            WITH period_data AS (
                SELECT {period_expr} AS period_key, date, day_profit,
                       cumulative_profit, current_price * quantity AS portfolio_value,
                       MAX(date) OVER (PARTITION BY {period_expr}) AS last_date
                FROM stock_daily_history
            )
            SELECT period_key,
                   MIN(date) AS period_start, MAX(date) AS period_end,
                   COUNT(DISTINCT date) AS trading_days,
                   ROUND(SUM(day_profit), 0) AS day_profit_sum,
                   ROUND(SUM(CASE WHEN date = last_date THEN cumulative_profit ELSE 0 END), 0) AS end_cumulative_profit,
                   ROUND(SUM(CASE WHEN date = last_date THEN portfolio_value ELSE 0 END), 0) AS end_portfolio_value
            FROM period_data GROUP BY period_key ORDER BY period_key DESC
        """)
        periods = [dict(row) for row in cursor.fetchall()]
        cursor.execute(f"""
            WITH period_data AS (
                SELECT {period_expr} AS period_key, owner, date, day_profit,
                       cumulative_profit, current_price * quantity AS portfolio_value,
                       MAX(date) OVER (PARTITION BY {period_expr}) AS last_date
                FROM stock_daily_history
            )
            SELECT period_key, owner,
                   ROUND(SUM(day_profit), 0) AS day_profit_sum,
                   ROUND(SUM(CASE WHEN date = last_date THEN cumulative_profit ELSE 0 END), 0) AS end_cumulative_profit,
                   ROUND(SUM(CASE WHEN date = last_date THEN portfolio_value ELSE 0 END), 0) AS end_portfolio_value,
                   COUNT(DISTINCT date) AS trading_days
            FROM period_data GROUP BY period_key, owner ORDER BY period_key DESC, owner
        """)
        by_owner = [dict(row) for row in cursor.fetchall()]
        return jsonify({'success': True, 'periods': periods, 'by_owner': by_owner})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/history/daily-chart', methods=['GET'])
def get_daily_chart():
    """[유병욱] 특정 월의 일자별 손익 집계 (일별 차트용)"""
    month = request.args.get('month')  # "2026-05"
    if not month:
        return jsonify({'success': False, 'message': 'month 파라미터 필요'}), 400
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("""
            SELECT date, owner,
                   ROUND(SUM(day_profit), 0) AS day_profit_sum,
                   ROUND(SUM(cumulative_profit), 0) AS cum_profit,
                   ROUND(SUM(current_price * quantity), 0) AS portfolio_value
            FROM stock_daily_history
            WHERE LEFT(date, 7) = ?
            GROUP BY date, owner
            ORDER BY date, owner
        """, (month,))
        rows = [dict(r) for r in cursor.fetchall()]
        dates = sorted({r['date'] for r in rows})
        return jsonify({'success': True, 'dates': dates, 'by_owner': rows})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/sync', methods=['POST'])
def sync_data():
    try:
        from drive_sync import sync_results_with_drive
        added, removed = sync_results_with_drive(RESULTS_DIR)
        # DB 동기화 로직 (단순화)
        init_db() 
        return jsonify({'success': True, 'added': added, 'removed': removed})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500 

def auto_snapshot_scheduler():
    """[김선화] 매일 장 마감(15:40) 후 자동 스냅샷 기록 스케줄러"""
    print("[김선화] 자동 스냅샷 스케줄러 가동 중 (평일 15:40 예정)")
    while True:
        try:
            now = datetime.now()
            # 평일(0-4: 월-금)이고 15:40분인 경우 실행
            if now.weekday() < 5 and now.hour == 15 and now.minute == 40:
                print(f"[김선화] {now.strftime('%Y-%m-%d')} 장 마감 자동 스냅샷 실행 중...")
                with app.app_context():
                    # 내부 함수 호출 (API 로직 재사용)
                    try:
                        conn = _new_db_conn()
                        cursor = conn.cursor()
                        cursor.execute("SELECT code, name, purchase_price, quantity, owner, added_at FROM my_stocks WHERE type = 'portfolio'")
                        stocks = [dict(row) for row in cursor.fetchall()]
                        
                        if stocks:
                            with ThreadPoolExecutor(max_workers=10) as executor:
                                details = list(executor.map(lambda s: get_portfolio_details(s['code']), stocks))
                            
                            today = now.strftime('%Y-%m-%d')
                            recorded_at = now.isoformat()
                            
                            for i, stock in enumerate(stocks):
                                detail = details[i] if details[i] else {}
                                current_price = detail.get('current_price', 0)
                                prev_price = detail.get('prev_price', 0)
                                quantity = stock['quantity']
                                purchase_price = stock['purchase_price']

                                # [김선화] 매수 당일인 경우 전일 종가가 아닌 매수 단가 대비로 당일 수익 계산
                                is_today_buy = False
                                if stock.get('added_at'):
                                    try:
                                        added_date = stock['added_at'][:10]
                                        if added_date == today:
                                            is_today_buy = True
                                    except Exception:
                                        pass

                                if is_today_buy:
                                    change = current_price - purchase_price
                                    change_rate = round((change / purchase_price * 100), 2) if purchase_price > 0 else 0.0
                                    day_profit = change * quantity
                                else:
                                    change = current_price - prev_price if prev_price > 0 else 0
                                    change_rate = round((change / prev_price * 100), 2) if prev_price > 0 else 0.0
                                    day_profit = change * quantity
                                    
                                cumulative_profit = (current_price - purchase_price) * quantity if purchase_price > 0 else 0

                                cursor.execute("SELECT id FROM stock_daily_history WHERE date = ? AND code = ?", (today, stock['code']))
                                existing = cursor.fetchone()
                                if existing:
                                    cursor.execute(
                                        "UPDATE stock_daily_history SET purchase_price=?, current_price=?, quantity=?, owner=?, recorded_at=?, day_profit=?, change_rate=?, cumulative_profit=? WHERE id=?",
                                        (purchase_price, current_price, quantity, stock['owner'], recorded_at, day_profit, change_rate, cumulative_profit, existing['id'])
                                    )
                                else:
                                    cursor.execute(
                                        "INSERT INTO stock_daily_history (date, code, name, purchase_price, current_price, quantity, owner, recorded_at, day_profit, change_rate, cumulative_profit) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                                        (today, stock['code'], stock['name'], purchase_price, current_price, quantity, stock['owner'], recorded_at, day_profit, change_rate, cumulative_profit)
                                    )
                            conn.commit()
                            print(f"[김선화] {today} 자동 스냅샷 기록 완료 ({len(stocks)}개 종목)")
                        conn.close()
                    except Exception as ex:
                        print(f"[김선화] 자동 스냅샷 실패: {ex}")
                
                # 1분간 대기하여 중복 실행 방지
                time.sleep(65)
            
            # 30초마다 체크
            time.sleep(30)
        except Exception as e:
            print(f"[김선화] 스케줄러 루프 오류: {e}")
            time.sleep(60)

if __name__ == '__main__':
    init_db()  # [김정음] 스타트업 시 DB 초기화 보장
    
    # [김선화] 자동 스케줄러 쓰레드 시작
    scheduler_thread = threading.Thread(target=auto_snapshot_scheduler, daemon=True)
    scheduler_thread.start()
    
    app.run(debug=False, threaded=True, host='0.0.0.0', port=5000)