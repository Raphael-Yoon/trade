# -*- coding: utf-8 -*-
import sys
import os

# Windows 콘솔 UTF-8 설정
if os.name == 'nt':
    os.system('chcp 65001 > nul')
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

from flask import Flask, render_template, jsonify, send_file, request, g, session, redirect, url_for
import threading
import uuid
from datetime import datetime, timedelta
import subprocess
import json
import psutil
import pymysql
import pymysql.cursors
import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from ai_analysis import analyze_stock_data, analyze_portfolio
from get_all_naver_data import get_all_naver_data
from db_init import init_db

import time

app = Flask(__name__)

# Flask 앱의 모든 응답에 ngrok 경고창 패스 헤더를 강제로 심어주는 코드
@app.after_request
def bypass_ngrok_warning(response):
    response.headers['ngrok-skip-browser-warning'] = 'true'
    return response

@app.before_request
def check_auth():
    if request.endpoint in ('login', 'logout', 'static'):
        return
    if not session.get('authenticated'):
        if request.path.startswith('/api/'):
            return jsonify({'error': 'Unauthorized'}), 401
        return redirect(url_for('login'))
    login_time = session.get('login_time')
    if login_time and datetime.fromisoformat(login_time) + timedelta(days=1) < datetime.utcnow():
        session.clear()
        if request.path.startswith('/api/'):
            return jsonify({'error': 'Unauthorized'}), 401
        return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form.get('password') == APP_PASSWORD:
            session.permanent = False
            session['authenticated'] = True
            session['login_time'] = datetime.utcnow().isoformat()
            return redirect(url_for('index'))
        error = '비밀번호가 올바르지 않습니다.'
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    session.pop('authenticated', None)
    return redirect(url_for('login'))

# 작업 상태 저장
tasks = {}

# 결과 파일 저장 디렉토리
RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')
if not os.path.exists(RESULTS_DIR):
    os.makedirs(RESULTS_DIR)

# MySQL(Local Docker) 연결 문자열
from dotenv import load_dotenv as _load_dotenv
from urllib.parse import urlparse
_load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
DATABASE_URL = os.getenv('DATABASE_URL')
_IS_POSTGRES = bool(DATABASE_URL and DATABASE_URL.startswith('postgresql'))
app.secret_key = os.getenv('SECRET_KEY', 'fallback-secret-key')
app.permanent_session_lifetime = timedelta(hours=12)
APP_PASSWORD = os.getenv('APP_PASSWORD', '')


class _DictRow(dict):
    """딕셔너리를 SQLite Row처럼 사용 가능하게 하는 래퍼"""
    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


class _AdaptedCursor:
    """sqlite3 ? 플레이스홀더를 pymysql %s로 변환하는 커서 래퍼."""

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
            if not isinstance(params, (tuple, list, dict)):
                params = (params,)
            self._cur.execute(adapted, params)
        else:
            self._cur.execute(adapted)
        return self

    def executemany(self, query, seq_of_params):
        adapted = self._adapt(query, True)
        self._cur.executemany(adapted, seq_of_params)
        return self

    @staticmethod
    def _conv_row(row):
        from decimal import Decimal
        return _DictRow({k: float(v) if isinstance(v, Decimal) else v for k, v in row.items()})

    def fetchone(self):
        row = self._cur.fetchone()
        return self._conv_row(row) if row else None

    def fetchall(self):
        rows = self._cur.fetchall()
        return [self._conv_row(r) for r in rows] if rows else []

    @property
    def rowcount(self):
        return self._cur.rowcount

    @property
    def description(self):
        return self._cur.description


class _PyMySQLAdapter:
    """sqlite3 Connection 인터페이스를 흉내내는 pymysql 연결 래퍼."""

    def __init__(self, dsn):
        self._dsn = dsn
        parsed = urlparse(dsn)
        db_opts = {
            'host': parsed.hostname or '127.0.0.1',
            'port': parsed.port or 3306,
            'user': parsed.username or 'root',
            'password': parsed.password or '150606',
            'database': parsed.path.lstrip('/') if parsed.path else 'trade',
            'charset': 'utf8mb4',
        }
        self._conn = pymysql.connect(**db_opts, cursorclass=pymysql.cursors.DictCursor)

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

    def ping(self):
        try:
            self._conn.ping(reconnect=True)
            return True
        except Exception as e:
            print(f"[DB Pool] MySQL ping failed: {e}")
            return False

    def close(self):
        if hasattr(self, '_pool') and self._pool is not None:
            self._pool.release(self)
        else:
            self.real_close()

    def real_close(self):
        try:
            self._conn.close()
        except:
            pass


class _PsycopgAdapter:
    """sqlite3 Connection 인터페이스를 흉내내는 psycopg2 연결 래퍼."""

    # Neon DB 서버리스 특성상 idle 연결을 SSL 수준에서 강제 종료함.
    # TCP keepalive로 주기적 패킷을 보내 연결을 유지.
    _CONNECT_KWARGS = dict(
        cursor_factory=psycopg2.extras.DictCursor,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
        connect_timeout=10,
    )

    def __init__(self, dsn):
        self._dsn = dsn
        self._conn = psycopg2.connect(dsn, **self._CONNECT_KWARGS)

    def _reconnect(self):
        try:
            self._conn.close()
        except Exception:
            pass
        self._conn = psycopg2.connect(self._dsn, **self._CONNECT_KWARGS)

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

    def ping(self):
        # self._conn.closed: 0=open, 1=closed, 2=broken(fatal error)
        if self._conn.closed:
            try:
                self._reconnect()
                return True
            except Exception as e:
                print(f"[DB Pool] PostgreSQL reconnect failed: {e}")
                return False
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception as e:
            try:
                self._reconnect()
                return True
            except Exception as e2:
                print(f"[DB Pool] PostgreSQL ping failed ({e}). Reconnect failed: {e2}")
                return False

    def close(self):
        if hasattr(self, '_pool') and self._pool is not None:
            self._pool.release(self)
        else:
            self.real_close()

    def real_close(self):
        try:
            self._conn.close()
        except:
            pass


from queue import Queue, Empty

class _DbConnectionPool:
    """스레드 안전한 데이터베이스 커넥션 풀 (MySQL / PostgreSQL 겸용)"""
    def __init__(self, creator_fn, minconn=3, maxconn=15):
        self._creator = creator_fn
        self._pool = Queue(maxsize=maxconn)
        self._lock = threading.Lock()
        self._active_connections = 0
        
        # 최소 커넥션 수만큼 미리 생성하여 적재
        for _ in range(minconn):
            try:
                conn = self._create_connection()
                self._pool.put(conn)
            except Exception as e:
                print(f"[DB Pool] 초기 커넥션 생성 실패: {e}")

    def _create_connection(self):
        conn = self._creator()
        conn._pool = self  # 커넥션 객체에 풀 참조 주입
        with self._lock:
            self._active_connections += 1
        return conn

    def acquire(self):
        """커넥션 대여 및 헬스 체크"""
        conn = None
        # 1. 풀에서 우선 획득 시도
        try:
            conn = self._pool.get_nowait()
        except Empty:
            # 2. 풀이 비어있고 최대 커넥션에 도달하지 않은 경우 새로 생성
            with self._lock:
                can_create = self._active_connections < self._pool.maxsize
            if can_create:
                try:
                    conn = self._create_connection()
                except Exception as e:
                    print(f"[DB Pool] 신규 커넥션 생성 실패: {e}")
            # 3. 새로 생성하지 못했다면 대기 (최대 5초)
            if conn is None:
                try:
                    conn = self._pool.get(timeout=5)
                except Empty:
                    raise Exception("[DB Pool] 커넥션 풀 대여 시간 초과 (Timeout)")

        # 헬스 체크: 실패 시 풀 카운트 차감 후 신규 커넥션으로 교체
        if conn and not conn.ping():
            with self._lock:
                self._active_connections -= 1
            try:
                conn = self._create_connection()
            except Exception as e:
                print(f"[DB Pool] 커넥션 교체 실패: {e}")
                raise

        return conn

    def release(self, conn):
        """커넥션을 풀로 반환"""
        if conn is None:
            return
        try:
            self._pool.put_nowait(conn)
        except Exception:
            # 풀이 가득 차 반환 불가능한 경우 소켓을 완전히 닫음
            try:
                conn.real_close()
            except:
                pass
            with self._lock:
                self._active_connections -= 1


_DB_POOL = None

def _make_db_conn():
    """DATABASE_URL 스킴에 따라 적절한 DB 어댑터 반환."""
    if DATABASE_URL and DATABASE_URL.startswith('postgresql'):
        return _PsycopgAdapter(DATABASE_URL)
    return _PyMySQLAdapter(DATABASE_URL)


def init_db_pool():
    global _DB_POOL
    if _DB_POOL is None:
        _DB_POOL = _DbConnectionPool(_make_db_conn, minconn=3, maxconn=15)


def _new_db_conn():
    """백그라운드 스레드용 DB 연결 (커넥션 풀에서 대여)"""
    return _DB_POOL.acquire()


def get_db():
    """Flask 요청별 DB 연결 관리 (커넥션 풀 적용)"""
    if 'db' not in g:
        g.db = _DB_POOL.acquire()
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    """요청 종료 시 DB 연결을 풀로 반환"""
    db = g.pop('db', None)
    if db is not None:
        db.close()  # 커넥션 객체의 close()는 자동으로 풀에 반환합니다.

# DB 초기화 실행
init_db()

# 커넥션 풀 초기화
init_db_pool()

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
holding_high_cache_file = os.path.join(RESULTS_DIR, 'holding_high_cache.json')
try:
    if os.path.exists(holding_high_cache_file):
        with open(holding_high_cache_file, 'r', encoding='utf-8') as f:
            holding_high_cache = json.load(f)
    else:
        holding_high_cache = {}
except Exception as e:
    print(f"Error loading holding_high_cache: {e}")
    holding_high_cache = {}

# [김정음] 종목 시가총액 및 코스피 전체 시총 캐시 (24h TTL)
market_cap_cache = {}           # {code: {'cap_억': float, 'ts': float}}
kospi_total_cap_cache = {'cap_억': 0, 'ts': 0}

# [김정음] 일별 시세 캐시 — 1시간 TTL, 과거 데이터라 빈번한 갱신 불필요
# 형식: {code: {'data': [...], 'ts': float}}
daily_prices_cache = {}

# [김정음] KOSPI 일별 시세 캐시 — 30분 TTL
kospi_daily_cache = {'data': [], 'ts': 0.0}

# [김정음] 투자자별 순매수 캐시 — 60초 TTL
investor_trend_cache = {}

def load_financial_health(force=False):
    """[김선화] 감사팀의 재무 보고서(Excel)를 구글 드라이브 또는 로컬에서 로드하여 주요 지표를 캐싱합니다."""
    global financial_cache
    if not force and financial_cache: return financial_cache
    
    # [김선화] 강제 로드 시 기존 캐시 초기화
    if force: financial_cache = {}
    
    # 1. 구글 드라이브에서 최신 데이터 시도 (gspread 직접 읽기)
    try:
        from drive_sync import list_files_in_folder, read_sheet_as_df
        import re, pandas as pd

        print("🔍 구글 드라이브에서 최신 재무 데이터 검색 중...")
        files = list_files_in_folder("Stock_Analysis_Results")
        sheets = [f for f in files if f['mimeType'] == 'application/vnd.google-apps.spreadsheet']
        if sheets:
            def _ts(f):
                m = re.search(r'(\d{8}_\d{6})', f['name'])
                if m: return m.group(1)
                m = re.search(r'(\d{8})', f['name'])
                if m: return m.group(1) + '_000000'
                return '00000000_000000'
            latest_file = max(sheets, key=_ts)
            print(f"📥 구글 드라이브 최신 파일 발견: {latest_file['name']}")
            df = read_sheet_as_df(latest_file['id'])
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
    """특정 종목의 공시 목록 — 스코어링 시점에 저장된 DB 데이터 반환."""
    try:
        import json as _json
        conn = _new_db_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT disc_json FROM tr_audit_recommendations WHERE code = ? LIMIT 1", (code,))
        row = cursor.fetchone()
        conn.close()
        if row and row['disc_json']:
            return jsonify(_json.loads(row['disc_json']))
        return jsonify([])
    except Exception as e:
        app.logger.warning(f"Disclosures DB fetch failed ({code}): {e}")
        return jsonify([])

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

@app.route('/api/targets/migrate', methods=['POST'])
def migrate_dev_data():
    """[김정음] Neon DB(개발 환경)의 추천 종목 및 종목 풀 데이터를 운영 DB로 이관합니다."""
    is_prod = (os.getenv('RUN_MODE') == 'prod') or (os.getenv('FLASK_ENV') == 'production') or (os.getenv('IS_PROD') == 'true')
    if not is_prod:
        return jsonify({
            'success': False,
            'message': '이 기능은 운영서버(IS_PROD=true 또는 RUN_MODE=prod)에서만 실행 가능합니다.'
        }), 403

    neon_url = os.getenv('NEON_DATABASE_URL')
    
    # NEON_DATABASE_URL이 명시적으로 주어지지 않았다면 에러 처리 (단, 개발 환경과 운영 환경 분리 시 필수)
    if not neon_url:
        return jsonify({
            'success': False,
            'message': 'NEON_DATABASE_URL 환경 변수가 구성되지 않았습니다. 운영서버의 환경설정을 확인해 주세요.'
        }), 400
        
    prod_url = DATABASE_URL
    if not prod_url:
        return jsonify({
            'success': False,
            'message': '운영 DB 연결 설정(DATABASE_URL)이 유효하지 않습니다.'
        }), 400

    if neon_url == prod_url:
        return jsonify({
            'success': True,
            'message': '현재 개발 환경(Neon DB와 운영 DB가 동일)이므로 데이터 이관 단계를 건너뛰었습니다.'
        })

    try:
        # 1. Neon DB (Source) 연결 및 데이터 조회
        src_conn = psycopg2.connect(neon_url, cursor_factory=psycopg2.extras.DictCursor)
        src_cursor = src_conn.cursor()
        
        # tr_audit_recommendations 데이터 조회
        src_cursor.execute("""
            SELECT code, name, current_price, target_price, upside, opinion, data_date, created_at, score, roe, debt, reason, news_summary, rec_type, one_liner, disc_json
            FROM tr_audit_recommendations
        """)
        rec_rows = [dict(r) for r in src_cursor.fetchall()]
        
        # tr_stock_pool 데이터 조회
        src_cursor.execute("""
            SELECT code, name, sector, roe, pbr, per, debt_ratio, operating_margin, target_price, pool_score, is_sector_leader, market_cap
            FROM tr_stock_pool
        """)
        pool_rows = [dict(r) for r in src_cursor.fetchall()]
        src_conn.close()
        
        # 2. 운영 DB (Target) 연결 및 데이터 적재
        dest_conn = _make_db_conn()
        
        # a. tr_audit_recommendations 이관 (기존 데이터 비운 후 신규 적재)
        dest_conn.execute("DELETE FROM tr_audit_recommendations")
        for r in rec_rows:
            dest_conn.execute("""
                INSERT INTO tr_audit_recommendations
                (code, name, current_price, target_price, upside, opinion, data_date, created_at, score, roe, debt, reason, news_summary, rec_type, one_liner, disc_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                r['code'], r['name'], float(r['current_price']), float(r['target_price']),
                float(r['upside']), r['opinion'], r['data_date'], r['created_at'], float(r['score']),
                float(r['roe'] or 0), float(r['debt'] or 0), r['reason'], r['news_summary'],
                r['rec_type'], r['one_liner'], r['disc_json']
            ))
            
        # b. tr_stock_pool 이관 (기존 데이터 비운 후 신규 적재)
        dest_conn.execute("DELETE FROM tr_stock_pool")
        for p in pool_rows:
            dest_conn.execute("""
                INSERT INTO tr_stock_pool
                (code, name, sector, roe, pbr, per, debt_ratio, operating_margin, target_price, pool_score, is_sector_leader, market_cap)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                p['code'], p['name'], p['sector'], float(p['roe'] or 0), float(p['pbr'] or 0), float(p['per'] or 0),
                float(p['debt_ratio'] or 0), float(p['operating_margin'] or 0), float(p['target_price'] or 0),
                float(p['pool_score'] or 0), bool(p['is_sector_leader']), float(p['market_cap'] or 0)
            ))
            
        dest_conn.commit()
        dest_conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Neon DB -> 운영 DB 이관이 성공적으로 완료되었습니다. (추천: {len(rec_rows)}건, 풀: {len(pool_rows)}건)'
        })
        
    except Exception as e:
        app.logger.error(f"Migration error: {e}")
        return jsonify({
            'success': False,
            'message': f'이관 중 오류 발생: {str(e)}'
        }), 500

@app.route('/api/pool')
def get_stock_pool():
    """투자적격 종목 풀 조회 (Neon DB tr_audit_recommendations Top 10 우선, 그 외 pool_score 순)"""
    try:
        conn = _new_db_conn()
        cursor = conn.cursor()
        
        # 1. tr_audit_recommendations 테이블에서 추천 종목 조회
        cursor.execute("""
            SELECT code, name, current_price, target_price, upside, score, roe, debt, reason, news_summary, rec_type, one_liner, disc_json
            FROM tr_audit_recommendations
        """)
        rec_rows = [dict(r) for r in cursor.fetchall()]
        
        # 2. tr_stock_pool 조회 (source_file 파라미터 지원, 없으면 최신 소스 파일 기준)
        source_file = request.args.get('source_file')
        if source_file:
            cursor.execute("""
                SELECT code, name, sector, roe, pbr, per, debt_ratio, operating_margin, target_price, pool_score, data_date, source_file
                FROM tr_stock_pool
                WHERE source_file = ?
            """, (source_file,))
        else:
            # 가장 최근의 source_file 조회
            cursor.execute("""
                SELECT source_file FROM tr_stock_pool
                ORDER BY data_date DESC, updated_at DESC LIMIT 1
            """)
            latest_row = cursor.fetchone()
            if latest_row and latest_row['source_file']:
                cursor.execute("""
                    SELECT code, name, sector, roe, pbr, per, debt_ratio, operating_margin, target_price, pool_score, data_date, source_file
                    FROM tr_stock_pool
                    WHERE source_file = ?
                """, (latest_row['source_file'],))
            else:
                cursor.execute("""
                    SELECT code, name, sector, roe, pbr, per, debt_ratio, operating_margin, target_price, pool_score, data_date, source_file
                    FROM tr_stock_pool
                """)
        pool_rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        
        pool_dict = {r['code']: r for r in pool_rows}
        
        results = []
        rec_codes = set()
        
        # 추천 종목 우선 매핑
        for rec in rec_rows:
            code = rec.get('code')
            if not code:
                continue
            rec_codes.add(code)
            
            pool_info = pool_dict.get(code, {})
            
            rec_type = rec.get('rec_type', 'momentum')
            is_rec_val = 2 if rec_type == 'value' else 0
            
            results.append({
                "code": code,
                "name": rec.get('name') or pool_info.get('name', ''),
                "sector": pool_info.get('sector', '기타'),
                "roe": rec.get('roe') or pool_info.get('roe', 0.0),
                "pbr": pool_info.get('pbr'),
                "per": pool_info.get('per'),
                "debt_ratio": rec.get('debt') or pool_info.get('debt_ratio', 0.0),
                "operating_margin": pool_info.get('operating_margin'),
                "target_price": rec.get('target_price') or pool_info.get('target_price', 0.0),
                "pool_score": pool_info.get('pool_score', 0.0),
                "priority_score": rec.get('score', 0.0),
                "ai_summary": rec.get('reason', ''),
                "news_summary": rec.get('news_summary', '[]'),
                "disc_json": rec.get('disc_json', '[]'),
                "upside": rec.get('upside', 0.0),
                "current_price": rec.get('current_price', 0.0),
                "is_rec": is_rec_val,
                "rec_type": rec_type,
                "one_liner": rec.get('one_liner', '')
            })
            
        # 추천 종목이 아닌 나머지 종목 추가
        other_stocks = []
        for r in pool_rows:
            if r['code'] not in rec_codes:
                other_stocks.append({
                    "code": r['code'],
                    "name": r['name'],
                    "sector": r['sector'],
                    "roe": r['roe'],
                    "pbr": r['pbr'],
                    "per": r['per'],
                    "debt_ratio": r['debt_ratio'],
                    "operating_margin": r['operating_margin'],
                    "target_price": r['target_price'],
                    "pool_score": r['pool_score'],
                    "priority_score": None,
                    "ai_summary": None,
                    "news_summary": None,
                    "upside": None,
                    "current_price": None,
                    "is_rec": 1,
                    "rec_type": None
                })
                
        # 나머지 종목 정렬 (pool_score DESC)
        other_stocks.sort(key=lambda x: x['pool_score'] or 0.0, reverse=True)
        
        # 추천 종목 정렬 (priority_score DESC)
        results.sort(key=lambda x: x['priority_score'] or 0.0, reverse=True)
        
        # 합산
        combined = results + other_stocks
        
        ranked_by = "ai" if rec_rows else "score"
        return jsonify({"ranked_by": ranked_by, "stocks": combined})
        
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
    is_prod = (os.getenv('RUN_MODE') == 'prod') or (os.getenv('FLASK_ENV') == 'production') or (os.getenv('IS_PROD') == 'true')
    return render_template('index.html', is_local=check_is_local(), is_prod=is_prod)

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

def get_market_investor_trend(ticker):
    """[김정음] 네이버 모바일 API에서 투자자별 순매수를 가져옵니다. (단위: 억원)"""
    global investor_trend_cache
    entry = investor_trend_cache.get(ticker)
    if entry and time.time() - entry['ts'] < 60:
        return entry['data']
    empty = {'foreign': None, 'institution': None, 'individual': None}
    try:
        url = f"https://m.stock.naver.com/api/index/{ticker}/trend"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
        d = res.json()
        def _parse(s):
            try:
                return int(str(s).replace(',', '').replace('+', ''))
            except Exception:
                return None
        result = {
            'foreign':     _parse(d.get('foreignValue')),
            'institution': _parse(d.get('institutionalValue')),
            'individual':  _parse(d.get('personalValue')),
        }
        investor_trend_cache[ticker] = {'data': result, 'ts': time.time()}
        return result
    except Exception as e:
        print(f"Error fetching investor trend {ticker}: {e}")
    return empty


@app.route('/api/market/indices', methods=['GET'])
def get_market_indices_api():
    """[김정음] 주요 시장 지수를 반환합니다."""
    indices = []
    for ticker in ['KOSPI', 'KOSDAQ']:
        data = get_market_index(ticker)
        data['investor'] = get_market_investor_trend(ticker)
        indices.append(data)
    return jsonify(indices)

@app.route('/api/my_stocks', methods=['GET'])
def get_my_stocks():
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("SELECT code, name, added_at, purchase_price, quantity, stop_loss_ratio, owner FROM tr_my_stocks WHERE type = 'portfolio' ORDER BY added_at DESC")
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
    
    # 1. 오늘 이미 캐시가 최신화되었다면 바로 반환
    if code in holding_high_cache and holding_high_cache[code]['date'] == today_str:
        return holding_high_cache[code]

    kospi_current = kospi_data[0]['index'] if kospi_data else 0

    # 2. 오늘 날짜가 아니지만 기존 캐시가 있는 경우: 과거 최고가와 오늘 현재가를 비교하여 갱신 (네트워크 요청 제거)
    if code in holding_high_cache:
        cached = holding_high_cache[code]
        max_price = max(cached['high'], current_price)
        max_date = today_str if current_price > cached['high'] else cached['high_date']
        
        # 코스피 정보 업데이트
        high_kospi = cached.get('high_kospi', 0)
        if current_price > cached['high'] and kospi_current > 0:
            high_kospi = kospi_current
            
        holding_high_cache[code] = {
            'high': max_price,
            'high_date': max_date,
            'high_kospi': high_kospi,
            'date': today_str
        }
        
        try:
            with open(holding_high_cache_file, 'w', encoding='utf-8') as f:
                json.dump(holding_high_cache, f, ensure_ascii=False, indent=2)
        except:
            pass
            
        return holding_high_cache[code]

    # 3. 오늘 등록된 종목인 경우: 네트워크 요청 없이 바로 캐시 생성
    added_date = added_at[:10]
    if added_date >= today_str:
        holding_high_cache[code] = {
            'high': max(purchase_price, current_price),
            'high_date': today_str,
            'high_kospi': kospi_current,
            'date': today_str
        }
        try:
            with open(holding_high_cache_file, 'w', encoding='utf-8') as f:
                json.dump(holding_high_cache, f, ensure_ascii=False, indent=2)
        except:
            pass
        return holding_high_cache[code]

    # 4. 과거에 등록되었으나 캐시가 아예 없는 경우: 최초 1회만 일별 시세 조회
    daily_prices = get_daily_prices(code, pages=3)
    relevant = [(p['close'], p['date']) for p in daily_prices if added_date <= p['date'] < today_str]

    if relevant:
        max_close, max_date = max(relevant, key=lambda x: x[0])
    else:
        max_close, max_date = 0, added_date

    # 취득가가 실제 최고가인 경우 → 기준 날짜는 매수일
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
        'high': max(max_price, current_price),
        'high_date': today_str if current_price > max_price else max_date,
        'high_kospi': kospi_current if current_price > max_price else high_kospi,
        'date': today_str
    }
    
    try:
        with open(holding_high_cache_file, 'w', encoding='utf-8') as f:
            json.dump(holding_high_cache, f, ensure_ascii=False, indent=2)
    except:
        pass
        
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
        cursor.execute("SELECT code, name, purchase_price, quantity, stop_loss_ratio, added_at, type FROM tr_my_stocks")
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
        _sql = ("""
            INSERT INTO tr_my_stocks (code, name, added_at, purchase_price, quantity, stop_loss_ratio, owner, type)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'portfolio')
            ON CONFLICT (code) DO UPDATE SET
                name=EXCLUDED.name, added_at=EXCLUDED.added_at,
                purchase_price=EXCLUDED.purchase_price, quantity=EXCLUDED.quantity,
                stop_loss_ratio=EXCLUDED.stop_loss_ratio, owner=EXCLUDED.owner,
                type='portfolio'
        """ if _IS_POSTGRES else """
            INSERT INTO tr_my_stocks (code, name, added_at, purchase_price, quantity, stop_loss_ratio, owner, type)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'portfolio')
            ON DUPLICATE KEY UPDATE
                name=VALUES(name), added_at=VALUES(added_at),
                purchase_price=VALUES(purchase_price), quantity=VALUES(quantity),
                stop_loss_ratio=VALUES(stop_loss_ratio), owner=VALUES(owner),
                type='portfolio'
        """)
        cursor.execute(_sql, (code, name, datetime.now().isoformat(), purchase_price, quantity, stop_loss_ratio, owner))
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/my_stocks/<code_val>', methods=['DELETE'])
def delete_my_stock(code_val):
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("DELETE FROM tr_my_stocks WHERE code = ?", (code_val,))
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/my_stocks/<code_val>/sell', methods=['POST'])
def sell_my_stock(code_val):
    """[김선화] 보유 종목 매도 처리 - 수량 차감, 전량 시 포트폴리오 제거, tr_sell_history 기록"""
    try:
        data = request.get_json() or {}
        sell_price = float(data.get('sell_price', 0))
        sell_qty = int(data.get('sell_qty', 0))
        if sell_price <= 0 or sell_qty <= 0:
            return jsonify({'success': False, 'message': '매도가와 수량을 올바르게 입력해주세요.'}), 400

        db = get_db()
        cursor = db.cursor()
        cursor.execute("SELECT code, name, purchase_price, quantity, owner FROM tr_my_stocks WHERE code = ?", (code_val,))
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
            INSERT INTO tr_sell_history (code, name, owner, sell_price, sell_qty, purchase_price, profit, profit_rate, sell_date, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (code_val, stock['name'], stock.get('owner', '나'), sell_price, sell_qty, purchase_price, profit, profit_rate, sell_date, now))

        new_qty = current_qty - sell_qty
        if new_qty <= 0:
            cursor.execute("DELETE FROM tr_my_stocks WHERE code = ?", (code_val,))
            fully_sold = True
        else:
            cursor.execute("UPDATE tr_my_stocks SET quantity = ? WHERE code = ?", (new_qty, code_val))
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
            updates.append("purchase_price = ?")
            params.append(purchase_price)
        if quantity is not None:
            updates.append("quantity = ?")
            params.append(quantity)
        if stop_loss_ratio is not None:
            updates.append("stop_loss_ratio = ?")
            params.append(stop_loss_ratio)
        if owner is not None:
            updates.append("owner = ?")
            params.append(owner)

        if not updates:
            return jsonify({'success': False, 'message': '수정할 데이터가 없습니다.'}), 400

        params.append(code_val)
        query = f"UPDATE tr_my_stocks SET {', '.join(updates)} WHERE code = ?"
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
        cursor.execute("SELECT code, name FROM tr_stocks_master WHERE name LIKE ? LIMIT 10", (f'%{query}%',))
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
                _master_sql = (
                    "INSERT INTO tr_stocks_master (code, name, market) VALUES (?, ?, ?) "
                    "ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, market=EXCLUDED.market"
                    if _IS_POSTGRES else
                    "INSERT INTO tr_stocks_master (code, name, market) VALUES (?, ?, ?) "
                    "ON DUPLICATE KEY UPDATE name=VALUES(name), market=VALUES(market)"
                )
                cursor.executemany(_master_sql, all_stocks)
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
        
        # Neon DB에서 이미 Pool 구성 완료된 소스 파일 목록 조회
        composed_files = set()
        try:
            conn = _new_db_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT source_file FROM tr_stock_pool")
            composed_files = {row['source_file'] for row in cursor.fetchall() if row['source_file']}
            conn.close()
        except Exception as db_err:
            print(f"Error checking composed pools: {db_err}")

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
                
                # filename 또는 Google Drive에 저장된 본래 name이 DB의 source_file 컬럼에 있는지 확인
                has_pool = (name in composed_files) or (df['name'] in composed_files)

                results.append({
                    'filename': name,
                    'market': market_val,
                    'stock_count': count_val,
                    'created_at': df.get('createdTime'),
                    'size': int(df.get('size', 0)) if df.get('size') else 0,
                    'spreadsheet_id': df['id'],
                    'drive_link': df.get('webViewLink'),
                    'ai_result': None, # 실시간 조회시 AI 결과는 별도 API로 처리
                    'has_pool': has_pool
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

@app.route('/api/results/collect_pool', methods=['POST'])
def collect_pool_from_result():
    if not check_is_local():
        return jsonify({'success': False, 'message': '서버 환경에서는 이 기능을 실행할 수 없습니다.'}), 403

    data = request.get_json() or {}
    spreadsheet_id = data.get('spreadsheet_id')
    filename = data.get('filename')

    if not spreadsheet_id and not filename:
        return jsonify({'success': False, 'message': 'Spreadsheet ID 또는 파일명이 누락되었습니다.'}), 400

    script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'cowork', 'pool_collect.py')
    python_cmd = sys.executable
    if 'uwsgi' in python_cmd.lower():
        python_cmd = 'python'

    import subprocess
    cmd = [python_cmd, script_path]
    if filename:
        cmd.extend(['--source_file', filename])

    # 1. 드라이브 ID가 있는 경우 직접 드라이브에서 데이터 로드
    if spreadsheet_id:
        cmd.extend(['--id', spreadsheet_id])
        try:
            process = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                cwd=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'cowork')
            )
            if process.returncode == 0:
                return jsonify({
                    'success': True,
                    'message': '감사 Pool 구성이 성공적으로 완료되었습니다. (Neon DB 직접 적재 완료)',
                    'output': process.stdout
                })
            else:
                return jsonify({
                    'success': False,
                    'message': f'Pool 구성 중 오류 발생: {process.stderr}',
                    'output': process.stdout
                }), 500
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500

    # 2. 로컬 파일로 폴백 진행
    file_path = os.path.join(RESULTS_DIR, filename)
    downloaded_temp = False
    try:
        # 로컬에 없으면 드라이브에서 다운로드 시도
        if not os.path.exists(file_path):
            from drive_sync import list_files_in_folder, download_from_drive
            drive_files = list_files_in_folder()
            target_name_base = filename.replace('.xlsx', '')
            for df in drive_files:
                if df['name'] == target_name_base or df['name'] == filename:
                    spreadsheet_id = df['id']
                    break
            if spreadsheet_id:
                content = download_from_drive(spreadsheet_id)
                if content:
                    with open(file_path, 'wb') as f:
                        f.write(content)
                    downloaded_temp = True
            
        if not os.path.exists(file_path):
            return jsonify({'success': False, 'message': '엑셀 파일을 찾을 수 없습니다.'}), 404

        cmd.extend(['--file', file_path])
        process = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            cwd=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'cowork')
        )

        if downloaded_temp and os.path.exists(file_path):
            os.remove(file_path)

        if process.returncode == 0:
            return jsonify({
                'success': True,
                'message': '감사 Pool 구성이 성공적으로 완료되었습니다. (Neon DB 적재 완료)',
                'output': process.stdout
            })
        else:
            return jsonify({
                'success': False,
                'message': f'Pool 구성 중 오류 발생: {process.stderr}',
                'output': process.stdout
            }), 500

    except Exception as e:
        if downloaded_temp and os.path.exists(file_path):
            os.remove(file_path)
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
        from drive_sync import find_ai_report, get_doc_content, create_google_doc, list_files_in_folder, read_sheet_as_df

        # 파일명에서 확장자 제거 (AI 리포트 검색용)
        base_name = os.path.splitext(filename)[0]

        # 1. 구글 드라이브에서 기존 AI 리포트 확인 (혹시 직접 호출된 경우 대비)
        existing_report = find_ai_report(base_name)
        if existing_report:
            cached_content = get_doc_content(existing_report['id'])
            if cached_content and len(cached_content.strip()) > 100:
                return jsonify({'success': True, 'result': cached_content, 'cached': True})

        # 2. 원본 데이터 파일 확인 (Drive-Native: gspread 직접 읽기 후 로컬 저장)
        file_path = os.path.join(RESULTS_DIR, filename)
        if not os.path.exists(file_path):
            drive_files = list_files_in_folder()
            target_name = filename.replace('.xlsx', '')
            spreadsheet_id = None
            for df in drive_files:
                if df['name'] == target_name or df['name'] == filename:
                    spreadsheet_id = df['id']
                    break
            if spreadsheet_id:
                df_data = read_sheet_as_df(spreadsheet_id)
                df_data.to_excel(file_path, index=False)
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
        cursor.execute("DELETE FROM tr_portfolio_ai_cache WHERE created_at < ?", (today,))
        db.commit()
        
        # 3. 캐시 확인 (강제 새로고침이 아닌 경우)
        if not force_refresh:
            cursor.execute("SELECT ai_result FROM tr_portfolio_ai_cache WHERE cache_key = ?", (cache_key,))
            row = cursor.fetchone()
            if row and row['ai_result']:
                return jsonify({'success': True, 'result': row['ai_result'], 'cached': True})
            
        # 4. AI 분석 수행
        result_text = analyze_portfolio(portfolio_data)
        
        # 5. 결과 저장 (유효한 경우만)
        if "오류" not in result_text and "제한" not in result_text:
            _cache_sql = (
                "INSERT INTO tr_portfolio_ai_cache (cache_key, ai_result, created_at) VALUES (?, ?, ?) "
                "ON CONFLICT (cache_key) DO UPDATE SET ai_result=EXCLUDED.ai_result, created_at=EXCLUDED.created_at"
                if _IS_POSTGRES else
                "INSERT INTO tr_portfolio_ai_cache (cache_key, ai_result, created_at) VALUES (?, ?, ?) "
                "ON DUPLICATE KEY UPDATE ai_result=VALUES(ai_result), created_at=VALUES(created_at)"
            )
            cursor.execute(_cache_sql, (cache_key, result_text, datetime.now().isoformat()))
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
        cursor.execute("SELECT code, name, added_at, owner FROM tr_my_stocks WHERE type = 'watchlist' ORDER BY added_at DESC")
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
        _watch_sql = (
            "INSERT INTO tr_my_stocks (code, name, added_at, type, owner, purchase_price, quantity, stop_loss_ratio) "
            "VALUES (?, ?, ?, 'watchlist', ?, 0, 0, 0) ON CONFLICT (code) DO NOTHING"
            if _IS_POSTGRES else
            "INSERT IGNORE INTO tr_my_stocks (code, name, added_at, type, owner, purchase_price, quantity, stop_loss_ratio) "
            "VALUES (?, ?, ?, 'watchlist', ?, 0, 0, 0)"
        )
        cursor.execute(_watch_sql, (code, name, datetime.now().isoformat(), owner))
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
        cursor.execute("DELETE FROM tr_my_stocks WHERE code = ? AND type = 'watchlist'", (code,))
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
        _move_sql = ("""
            INSERT INTO tr_my_stocks (code, name, added_at, purchase_price, quantity, stop_loss_ratio, owner, type)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'portfolio')
            ON CONFLICT (code) DO UPDATE SET
                type='portfolio', added_at=EXCLUDED.added_at,
                purchase_price=EXCLUDED.purchase_price, quantity=EXCLUDED.quantity,
                stop_loss_ratio=EXCLUDED.stop_loss_ratio, owner=EXCLUDED.owner
        """ if _IS_POSTGRES else """
            INSERT INTO tr_my_stocks (code, name, added_at, purchase_price, quantity, stop_loss_ratio, owner, type)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'portfolio')
            ON DUPLICATE KEY UPDATE
                type='portfolio', added_at=VALUES(added_at),
                purchase_price=VALUES(purchase_price), quantity=VALUES(quantity),
                stop_loss_ratio=VALUES(stop_loss_ratio), owner=VALUES(owner)
        """)
        cursor.execute(_move_sql, (code, name, datetime.now().isoformat(), price, qty, stop_loss, owner))
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
        
        cursor.execute("UPDATE tr_my_stocks SET owner = ? WHERE code = ?", (owner, code))
        
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
        cursor.execute("SELECT code, name, purchase_price, quantity, stop_loss_ratio, owner, added_at, type FROM tr_my_stocks")
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
        cursor.execute("SELECT COUNT(*) AS cnt FROM tr_stock_daily_history WHERE date = ?", (today,))
        existing_count = cursor.fetchone()['cnt']
        if existing_count > 0 and not force:
            return jsonify({'success': False, 'exists': True,
                            'message': f'{today} 날짜의 데이터가 이미 존재합니다. 덮어쓰시겠습니까?'})

        recorded_at = datetime.now().isoformat()

        # 덮어쓰기: 기존 날짜 데이터 전체 삭제 후 재삽입
        if existing_count > 0:
            cursor.execute("DELETE FROM tr_stock_daily_history WHERE date = ?", (today,))

        for s in portfolio:
            day_profit = s.get('change', 0) * s.get('quantity', 0)
            change_rate = s.get('change_rate', 0)
            cumulative_profit = s.get('profit', 0)
            cursor.execute('''
                INSERT INTO tr_stock_daily_history
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
        cursor.execute("DELETE FROM tr_stock_daily_history WHERE date = ?", (date,))
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
            FROM tr_stock_daily_history
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
        cursor.execute("SELECT * FROM tr_stock_daily_history WHERE date = ? ORDER BY owner, name", (date,))
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

        cursor.execute("SELECT code, name, purchase_price, quantity, owner, added_at FROM tr_my_stocks WHERE type = 'portfolio'")
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
                    "SELECT DISTINCT date FROM tr_stock_daily_history WHERE code = ? AND date = ?",
                    (code, req_date)
                )
            else:
                cursor.execute(
                    "SELECT DISTINCT date FROM tr_stock_daily_history WHERE code = ? AND date >= ?",
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
                        cursor.execute("DELETE FROM tr_stock_daily_history WHERE code = ? AND date = ?", (code, date_str))
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
                    INSERT INTO tr_stock_daily_history
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
        period_expr = "CONCAT(LEFT(date, 4), '-Q', FLOOR((CAST(SUBSTRING(date, 6, 2) AS DECIMAL) - 1) / 3) + 1)"
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
                FROM tr_stock_daily_history
            )
            SELECT period_key,
                   MIN(date) AS period_start, MAX(date) AS period_end,
                   COUNT(DISTINCT date) AS trading_days,
                   ROUND(CAST(SUM(day_profit) AS DECIMAL(20,4)), 0) AS day_profit_sum,
                   ROUND(CAST(SUM(CASE WHEN date = last_date THEN cumulative_profit ELSE 0 END) AS DECIMAL(20,4)), 0) AS end_cumulative_profit,
                   ROUND(CAST(SUM(CASE WHEN date = last_date THEN portfolio_value ELSE 0 END) AS DECIMAL(20,4)), 0) AS end_portfolio_value
            FROM period_data GROUP BY period_key ORDER BY period_key DESC
        """)
        periods = [dict(row) for row in cursor.fetchall()]
        cursor.execute(f"""
            WITH period_data AS (
                SELECT {period_expr} AS period_key, owner, date, day_profit,
                       cumulative_profit, current_price * quantity AS portfolio_value,
                       MAX(date) OVER (PARTITION BY {period_expr}) AS last_date
                FROM tr_stock_daily_history
            )
            SELECT period_key, owner,
                   ROUND(CAST(SUM(day_profit) AS DECIMAL(20,4)), 0) AS day_profit_sum,
                   ROUND(CAST(SUM(CASE WHEN date = last_date THEN cumulative_profit ELSE 0 END) AS DECIMAL(20,4)), 0) AS end_cumulative_profit,
                   ROUND(CAST(SUM(CASE WHEN date = last_date THEN portfolio_value ELSE 0 END) AS DECIMAL(20,4)), 0) AS end_portfolio_value,
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
                   ROUND(CAST(SUM(day_profit) AS DECIMAL(20,4)), 0) AS day_profit_sum,
                   ROUND(CAST(SUM(cumulative_profit) AS DECIMAL(20,4)), 0) AS cum_profit,
                   ROUND(CAST(SUM(current_price * quantity) AS DECIMAL(20,4)), 0) AS portfolio_value
            FROM tr_stock_daily_history
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
                        cursor.execute("SELECT code, name, purchase_price, quantity, owner, added_at FROM tr_my_stocks WHERE type = 'portfolio'")
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

                                cursor.execute("SELECT id FROM tr_stock_daily_history WHERE date = ? AND code = ?", (today, stock['code']))
                                existing = cursor.fetchone()
                                if existing:
                                    cursor.execute(
                                        "UPDATE tr_stock_daily_history SET purchase_price=?, current_price=?, quantity=?, owner=?, recorded_at=?, day_profit=?, change_rate=?, cumulative_profit=? WHERE id=?",
                                        (purchase_price, current_price, quantity, stock['owner'], recorded_at, day_profit, change_rate, cumulative_profit, existing['id'])
                                    )
                                else:
                                    cursor.execute(
                                        "INSERT INTO tr_stock_daily_history (date, code, name, purchase_price, current_price, quantity, owner, recorded_at, day_profit, change_rate, cumulative_profit) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
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
