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
from datetime import datetime
import subprocess
import json
import psutil
import sqlite3
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

# 데이터베이스 파일
DB_FILE = os.path.join(os.path.dirname(__file__), 'trade.db')

def get_db():
    """요청별 DB 연결 관리"""
    if 'db' not in g:
        g.db = sqlite3.connect(DB_FILE)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    """요청 종료 시 DB 연결 닫기"""
    db = g.pop('db', None)
    if db is not None:
        db.close()

def init_db():
    """데이터베이스 초기화 및 테이블 생성"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # 내 종목 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS my_stocks (
            code TEXT PRIMARY KEY,
            name TEXT,
            added_at TEXT,
            purchase_price REAL DEFAULT 0,
            quantity INTEGER DEFAULT 0,
            stop_loss_ratio REAL DEFAULT 0
        )
    ''')
    
    # 기존 테이블에 컬럼이 없는 경우 추가 (스키마 업데이트)
    try:
        cursor.execute("ALTER TABLE my_stocks ADD COLUMN purchase_price REAL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
        
    try:
        cursor.execute("ALTER TABLE my_stocks ADD COLUMN quantity INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute("ALTER TABLE my_stocks ADD COLUMN stop_loss_ratio REAL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    
    # 분석 결과 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS analysis_results (
            filename TEXT PRIMARY KEY,
            market TEXT,
            stock_count TEXT,
            created_at TEXT,
            size INTEGER,
            spreadsheet_id TEXT,
            drive_link TEXT,
            ai_result TEXT
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

    # 실시간 알림 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS price_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT,
            name TEXT,
            type TEXT, -- 'spike' or 'drop'
            change_rate REAL,
            price REAL,
            volume INTEGER DEFAULT 0, -- [김선화] 거래량 추가
            industry TEXT, -- [김선화] 업종 추가
            created_at TEXT
        )
    ''')
    
    # 스키마 업데이트 (기존 테이블에 volume, industry 컬럼 추가)
    try:
        cursor.execute("ALTER TABLE price_alerts ADD COLUMN volume INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE price_alerts ADD COLUMN industry TEXT")
    except sqlite3.OperationalError:
        pass
    
    conn.commit()
    
    conn.commit()
    conn.close()

# DB 초기화 실행
init_db()

# 실시간 모니터링 관리
monitor_running = False
monitor_thread = None
monitor_threshold = 7.0 # [김선화] 급등 탐지 임계치 (기본 7.0%)
monitor_min_volume = 50000 # [김선화] 최소 거래량 필터 (기본 5만 주)

# [김선화] ETF 필터링용 캐시
etf_cache = {'codes': [], 'last_updated': 0}

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

def get_market_movers():
    """시장 전체에서 급등/급락 종목 가져오기 (네이버 금융 상위 종목)"""
    movers = []
    try:
        # [김선화] 탐지 정밀도를 높이기 위해 '거래량 상위', '상승 상위', '시가총액 상위' 페이지를 모두 스캔합니다.
        targets = [
            'sise_quant.naver?sosok=0', 'sise_quant.naver?sosok=1', # 거래량 상위
            'sise_rise.naver?sosok=0', 'sise_rise.naver?sosok=1',   # 상승 상위
            'sise_market_sum.naver?sosok=0', 'sise_market_sum.naver?sosok=1' # 시가총액 상위
        ]
        
        for t_url in targets:
            url = f"https://finance.naver.com/sise/{t_url}"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            soup = BeautifulSoup(res.text, 'html.parser')
            rows = soup.select("table.type_2 tr")
            
            etf_codes = get_etf_codes() # [김선화] ETF 필터링용 리스트
            
            for row in rows:
                cols = row.select("td")
                if len(cols) < 6: continue
                name = cols[1].text.strip()
                code_el = cols[1].find('a')
                if not code_el: continue
                code = code_el['href'].split('=')[-1]
                
                # [김선화] ETF, ETN, 스팩 제외 로직 강화 (노이즈 제거)
                if code in etf_codes or any(x in name.upper() for x in ['ETF', 'ETN', '스팩', 'SPAC']):
                    continue
                
                # [김선화] 등락률 컬럼 인덱스 수정 (5 -> 4) 및 파싱 로직 보완
                price_str = cols[2].text.strip().replace(',', '') # [김선화] 현재가 (Index 2)
                change_rate_str = cols[4].text.strip().replace('%', '').replace('+', '').replace(',', '')
                volume_str = cols[5].text.strip().replace(',', '') # [김선화] 거래량 (Index 5)
                try:
                    price = int(price_str)
                    change_rate = float(change_rate_str)
                    volume = int(volume_str)
                    
                    # [김선화] 임계치 이상 && 최소 거래량 이상일 때만 포착
                    if change_rate >= monitor_threshold and volume >= monitor_min_volume:
                        movers.append({
                            'code': code, 
                            'name': name, 
                            'price': price, # 현재가 추가
                            'change_rate': change_rate, 
                            'volume': volume, 
                            'type': 'spike'
                        })
                except: continue
    except Exception as e:
        print(f"시장 모니터링 오류: {e}")
    return movers

def is_market_open():
    """장 운영 시간 확인 (09:00 ~ 15:30, 주말 제외)"""
    now = datetime.now()
    if now.weekday() >= 5: # 토, 일
        return False
    start_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
    end_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return start_time <= now <= end_time

def run_realtime_monitor():
    """실시간 급등주 모니터링 백그라운드 태스크 (전시장 스캔)"""
    global monitor_running
    print("🚀 시장 전수 모니터링 스레드 시작")
    
    while monitor_running:
        try:
            # 장 시간이 아니면 대기
            if not is_market_open():
                print("💤 장 운영 시간이 아닙니다. 대기 중...")
                time.sleep(300) # 5분 대기
                continue

            # 1. 시장 전체 급등주 스캔 (Top Movers)
            movers = get_market_movers()
            
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            
            for m in movers:
                # 이미 오늘 알림이 울린 종목은 중복 방지 (최근 1시간 내)
                cursor.execute("""
                    SELECT id FROM price_alerts 
                    WHERE code = ? AND created_at > datetime('now', '-1 hour')
                """, (m['code'],))
                if cursor.fetchone(): continue
                
                # [김선화] 업종 정보 수집 (상세 페이지 1회 조회)
                industry = get_industry_naver(m['code'])
                
                print(f"📡 [전시장 포착] {m['name']}({m['code']}) [{industry}] {m['price']:,}원 {m['change_rate']}% 급등 중! (거래량: {m['volume']:,})")
                cursor.execute("""
                    INSERT INTO price_alerts (code, name, type, change_rate, price, volume, industry, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (m['code'], m['name'], m['type'], m['change_rate'], m['price'], m['volume'], industry, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            
            conn.commit()
            conn.close()
            
            time.sleep(180) # 시장 전수 스캔은 3분 간격
            
        except Exception as e:
            print(f"시장 모니터링 루프 오류: {e}")
            time.sleep(30)

def start_monitor():
    global monitor_running, monitor_thread
    if not monitor_running:
        monitor_running = True
        monitor_thread = threading.Thread(target=run_realtime_monitor, daemon=True)
        monitor_thread.start()

@app.route('/api/monitor/start', methods=['POST'])
def api_start_monitor():
    """모니터링 시작 ([김선화] 시작 시 기존 알림 초기화 및 즉시 재조회)"""
    global monitor_running, monitor_thread
    if not monitor_running:
        # [김선화] 재조회 요청에 따라 기존 알림 내역 초기화 (Clean Start)
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM price_alerts")
            conn.commit()
            conn.close()
        except: pass
        
        monitor_running = True
        monitor_thread = threading.Thread(target=run_realtime_monitor, daemon=True)
        monitor_thread.start()
        return jsonify({"status": "success", "message": "모니터링이 시작되었습니다. 시장을 재조회합니다."})
    return jsonify({"status": "info", "message": "이미 모니터링이 실행 중입니다."})

@app.route('/api/monitor/stop', methods=['POST'])
def api_stop_monitor():
    """모니터링 중지"""
    global monitor_running
    monitor_running = False
    return jsonify({"status": "success", "message": "모니터링이 중지되었습니다."})

@app.route('/api/monitor/threshold', methods=['GET', 'POST'])
def api_monitor_threshold():
    """[김선화] 탐지 임계치 조회 및 설정"""
    global monitor_threshold
    if request.method == 'POST':
        try:
            data = request.get_json()
            val = float(data.get('threshold', 7.0))
            if val <= 0:
                return jsonify({"status": "error", "message": "임계치는 0보다 커야 합니다."}), 400
            monitor_threshold = val
            return jsonify({"status": "success", "message": f"임계치가 {val}%로 변경되었습니다.", "threshold": monitor_threshold})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 400
    return jsonify({"threshold": monitor_threshold})

@app.route('/api/monitor/min_volume', methods=['GET', 'POST'])
def api_monitor_min_volume():
    """[김선화] 최소 거래량 필터 조회 및 설정"""
    global monitor_min_volume
    if request.method == 'POST':
        try:
            data = request.get_json()
            val = int(data.get('min_volume', 50000))
            if val < 0:
                return jsonify({"status": "error", "message": "거래량은 0 이상이어야 합니다."}), 400
            monitor_min_volume = val
            return jsonify({"status": "success", "message": f"최소 거래량이 {val:,}주로 변경되었습니다.", "min_volume": monitor_min_volume})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 400
    return jsonify({"min_volume": monitor_min_volume})

@app.route('/api/monitor/status')
def api_monitor_status():
    """모니터링 상태 조회"""
    return jsonify({"running": monitor_running})

@app.route('/api/alerts')
def get_alerts():
    """최근 알림 목록 조회 (현재 설정값으로 실시간 필터링 및 정렬)"""
    sort_by = request.args.get('sort', 'change_rate') # 기본값: 상승률순
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # 정렬 기준 설정
    order_clause = "change_rate DESC" if sort_by == 'change_rate' else "created_at DESC"
    
    # [김선화] 사용자가 설정한 현재 임계치와 최소 거래량을 만족하는 알림만 반환
    query = f"""
        SELECT * FROM price_alerts 
        WHERE change_rate >= ? AND volume >= ?
        ORDER BY {order_clause} LIMIT 50
    """
    cursor.execute(query, (monitor_threshold, monitor_min_volume))
    alerts = [dict(zip([column[0] for column in cursor.description], row)) for row in cursor.fetchall()]
    conn.close()
    return jsonify(alerts)

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
        res = requests.get(url, timeout=5)
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

        # 3. 등락액, 등락률 계산
        change = current_price - prev_close if prev_close > 0 else 0
        change_rate = (change / prev_close * 100) if prev_close > 0 else 0
        
        return {
            'current_price': current_price,
            'prev_close': prev_close,
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
        cursor.execute("SELECT code, name, added_at, purchase_price, quantity, stop_loss_ratio FROM my_stocks ORDER BY added_at DESC")
        stocks = [dict(row) for row in cursor.fetchall()]
        return jsonify(stocks)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_daily_prices(code, pages=2):
    """네이버 금융에서 일별 시세를 가져옵니다."""
    headers = {'User-Agent': 'Mozilla/5.0'}
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
                        prices.append({'date': date, 'close': close})
                    except: continue
        return prices
    except:
        return []

def get_kospi_daily(pages=2):
    """네이버 금융에서 코스피 일별 시세를 가져옵니다."""
    headers = {'User-Agent': 'Mozilla/5.0'}
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

    # 2. 최근 최고가 대비 손절 (Trailing)
    if daily_prices:
        # added_at 이후의 가격들 중 최고가 찾기 (없으면 전체 기간 중)
        relevant_prices = [p['close'] for p in daily_prices if p['date'] >= added_at]
        if not relevant_prices: relevant_prices = [p['close'] for p in daily_prices]
        
        if relevant_prices:
            max_price = max(relevant_prices)
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
        
        # 1. 보유 종목 가져오기 (added_at 필드 추가)
        cursor.execute("SELECT code, name, purchase_price, quantity, stop_loss_ratio, added_at FROM my_stocks")
        portfolio_stocks = [dict(row) for row in cursor.fetchall()]
        portfolio_codes = {s['code'] for s in portfolio_stocks}
        for s in portfolio_stocks:
            s['type'] = 'portfolio'
            
        # 2. 관심 종목 가져오기
        cursor.execute("SELECT code, name FROM watchlist")
        watchlist_stocks = []
        for row in cursor.fetchall():
            s = dict(row)
            if s['code'] not in portfolio_codes:
                s['type'] = 'watchlist'
                s['purchase_price'] = 0
                s['quantity'] = 0
                s['stop_loss_ratio'] = 0
                s['added_at'] = datetime.now().isoformat()
                watchlist_stocks.append(s)
            
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
                'ma20_diff': detail.get('ma20_diff', 0)
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
    if not code:
        return jsonify({'success': False, 'message': '종목 코드가 필요합니다.'}), 400
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("INSERT OR REPLACE INTO my_stocks (code, name, added_at, purchase_price, quantity, stop_loss_ratio) VALUES (?, ?, ?, ?, ?, ?)", 
                       (code, name, datetime.now().isoformat(), purchase_price, quantity, stop_loss_ratio))
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

@app.route('/api/my_stocks/<code_val>', methods=['PATCH'])
def update_my_stock(code_val):
    data = request.get_json() or {}
    purchase_price = data.get('purchase_price')
    quantity = data.get('quantity')
    stop_loss_ratio = data.get('stop_loss_ratio')
    
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
            
        if not updates:
            return jsonify({'success': False, 'message': '수정할 데이터가 없습니다.'}), 400
            
        params.append(code_val)
        query = f"UPDATE my_stocks SET {', '.join(updates)} WHERE code = ?"
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
                conn = sqlite3.connect(DB_FILE)
                cursor = conn.cursor()
                cursor.executemany("INSERT OR REPLACE INTO stocks_master (code, name, market) VALUES (?, ?, ?)", all_stocks)
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
            
        # 4. 로컬 및 DB 잔재 청소 (보조적 정리)
        try:
            db = get_db()
            cursor = db.cursor()
            cursor.execute("DELETE FROM analysis_results WHERE filename = ?", (filename,))
            db.commit()
        except: pass
        
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
            cursor.execute("INSERT OR REPLACE INTO portfolio_ai_cache (cache_key, ai_result, created_at) VALUES (?, ?, ?)", 
                           (cache_key, result_text, datetime.now().isoformat()))
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
    """[김정음] 관심 종목 리스트를 가져옵니다."""
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("SELECT code, name, added_at FROM watchlist ORDER BY added_at DESC")
        stocks = [dict(row) for row in cursor.fetchall()]
        return jsonify(stocks)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/watchlist', methods=['POST'])
def add_to_watchlist():
    """[김정음] 관심 종목을 추가합니다."""
    try:
        data = request.get_json()
        code = data.get('code')
        name = data.get('name', '')
        if not code:
            return jsonify({'success': False, 'message': '코드가 누락되었습니다.'}), 400
            
        db = get_db()
        cursor = db.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO watchlist (code, name, added_at) VALUES (?, ?, ?)",
            (code, name, datetime.now().isoformat())
        )
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/watchlist/<code>', methods=['DELETE'])
def delete_from_watchlist(code):
    """[김정음] 관심 종목을 삭제합니다."""
    try:
        db = get_db()
        cursor = db.cursor()
        cursor.execute("DELETE FROM watchlist WHERE code = ?", (code,))
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/watchlist/promote', methods=['POST'])
def promote_to_portfolio():
    """[김정음] 관심 종목을 내 포트폴리오(보유 종목)로 승격시킵니다."""
    try:
        data = request.get_json()
        code = data.get('code')
        name = data.get('name', '')
        price = data.get('purchase_price', 0)
        qty = data.get('quantity', 0)
        stop_loss = data.get('stop_loss_ratio', 0)
        
        db = get_db()
        cursor = db.cursor()
        # 1. 포트폴리오에 추가
        cursor.execute(
            "INSERT OR REPLACE INTO my_stocks (code, name, added_at, purchase_price, quantity, stop_loss_ratio) VALUES (?, ?, ?, ?, ?, ?)",
            (code, name, datetime.now().isoformat(), price, qty, stop_loss)
        )
        # 2. 관심 종목에서 삭제
        cursor.execute("DELETE FROM watchlist WHERE code = ?", (code,))
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/realtime/prices', methods=['GET'])
def get_realtime_prices():
    """[김정음] 내 종목 및 관심 종목의 실시간 주가 정보를 상세히 반환합니다."""
    try:
        db = get_db()
        cursor = db.cursor()
        
        # 보유 종목 가져오기
        cursor.execute("SELECT code, name FROM my_stocks")
        portfolio_stocks = [{'code': s['code'], 'name': s['name'], 'type': 'portfolio'} for s in cursor.fetchall()]
        
        # 관심 종목 가져오기
        cursor.execute("SELECT code, name FROM watchlist")
        watchlist_stocks = [{'code': s['code'], 'name': s['name'], 'type': 'watchlist'} for s in cursor.fetchall()]
        
        all_stocks = portfolio_stocks + watchlist_stocks
        if not all_stocks:
            return jsonify([])
            
        results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_stock = {executor.submit(get_detailed_price, s['code']): s for s in all_stocks}
            for future in as_completed(future_to_stock):
                stock = future_to_stock[future]
                try:
                    price_info = future.result()
                    results.append({
                        'code': stock['code'],
                        'name': stock['name'],
                        'price': price_info['current_price'],
                        'prev_close': price_info['prev_close'],
                        'change': price_info['change'],
                        'change_rate': price_info['change_rate'],
                        'type': stock['type']
                    })
                except Exception:
                    results.append({
                        'code': stock['code'], 
                        'name': stock['name'], 
                        'price': 0, 
                        'prev_close': 0, 
                        'change': 0, 
                        'change_rate': 0, 
                        'type': stock['type']
                    })
                    
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

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
if __name__ == '__main__':
    init_db()  # [김정음] 스타트업 시 DB 초기화 보장
    app.run(debug=True, host='0.0.0.0', port=5000)
        