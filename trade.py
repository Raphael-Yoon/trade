# -*- coding: utf-8 -*-
import sys
import os

# Windows 콘솔 UTF-8 설정
if os.name == 'nt':
    os.system('chcp 65001 > nul')
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

from flask import Flask, render_template, jsonify, send_file, request
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
from concurrent.futures import ThreadPoolExecutor
from ai_analysis import analyze_stock_data

app = Flask(__name__)

# 작업 상태 저장
tasks = {}

# 결과 파일 저장 디렉토리
RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')
if not os.path.exists(RESULTS_DIR):
    os.makedirs(RESULTS_DIR)

# 데이터베이스 파일
DB_FILE = os.path.join(os.path.dirname(__file__), 'trade.db')

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
            quantity INTEGER DEFAULT 0
        )
    ''')
    
    # 기존 테이블에 컬럼이 없는 경우 추가 (스키마 업데이트)
    try:
        cursor.execute("ALTER TABLE my_stocks ADD COLUMN purchase_price REAL DEFAULT 0")
    except sqlite3.OperationalError:
        pass # 이미 존재함
        
    try:
        cursor.execute("ALTER TABLE my_stocks ADD COLUMN quantity INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass # 이미 존재함
    
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
    
    conn.commit()
    
    # 기존 JSON 데이터 마이그레이션 (한 번만 실행)
    json_file = os.path.join(os.path.dirname(__file__), 'my_stocks.json')
    if os.path.exists(json_file):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                stocks = json.load(f)
                for s in stocks:
                    cursor.execute(
                        "INSERT OR IGNORE INTO my_stocks (code, name, added_at) VALUES (?, ?, ?)",
                        (s['code'], s.get('name', ''), s.get('added_at', datetime.now().isoformat()))
                    )
            conn.commit()
            os.rename(json_file, json_file + '.bak')
            print("JSON 데이터를 SQLite로 마이그레이션 완료했습니다.")
        except Exception as e:
            print(f"마이그레이션 중 오류: {e}")

    # 기존 분석 결과 파일들 마이그레이션
    try:
        for filename in os.listdir(RESULTS_DIR):
            if filename.endswith('.json') and not filename.endswith('_ai.json') and not filename.endswith('.bak'):
                xlsx_name = filename.replace('.json', '.xlsx')
                json_path = os.path.join(RESULTS_DIR, filename)
                
                # 이미 DB에 있는지 확인
                cursor.execute("SELECT filename FROM analysis_results WHERE filename = ?", (xlsx_name,))
                if not cursor.fetchone():
                    try:
                        with open(json_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            
                            # 파일명 파싱
                            parts = xlsx_name.replace('.xlsx', '').split('_')
                            market_val = parts[0].upper() if len(parts) > 0 else 'UNKNOWN'
                            count_val = parts[1] if len(parts) > 1 else '0'
                            
                            cursor.execute('''
                                INSERT OR IGNORE INTO analysis_results 
                                (filename, market, stock_count, created_at, size, spreadsheet_id, drive_link)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                xlsx_name,
                                market_val,
                                count_val,
                                data.get('created_at', datetime.now().isoformat()),
                                data.get('size', 0),
                                data.get('spreadsheet_id'),
                                data.get('drive_link')
                            ))
                    except Exception as e:
                        print(f"파일 마이그레이션 중 오류 ({filename}): {e}")
        conn.commit()
    except Exception as e:
        print(f"결과 목록 마이그레이션 중 오류: {e}")
            
    # 종목 마스터가 비어있으면 업데이트 트리거
    cursor.execute("SELECT COUNT(*) FROM stocks_master")
    if cursor.fetchone()[0] == 0:
        print("종목 마스터가 비어있습니다. 백그라운드 업데이트를 시작합니다...")
        def background_update():
            try:
                import requests
                from bs4 import BeautifulSoup
                import sqlite3 as sqlite
                
                all_stocks = []
                session = requests.Session()
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
                        
                        if not found or page > 40: break
                        page += 1
                
                if all_stocks:
                    conn_bg = sqlite.connect(DB_FILE)
                    cursor_bg = conn_bg.cursor()
                    cursor_bg.executemany("INSERT OR REPLACE INTO stocks_master (code, name, market) VALUES (?, ?, ?)", all_stocks)
                    conn_bg.commit()
                    conn_bg.close()
                    print(f"종목 마스터 초기 업데이트 완료: {len(all_stocks)}개 종목")
            except Exception as e:
                print(f"초기 마스터 업데이트 중 오류: {e}")

        threading.Thread(target=background_update, daemon=True).start()
    
    conn.close()

# DB 초기화 실행
init_db()

def cleanup_old_results(max_files=20):
    """오래된 결과 파일 자동 삭제"""
    try:
        files = []
        for filename in os.listdir(RESULTS_DIR):
            if filename.endswith('.xlsx'):
                file_path = os.path.join(RESULTS_DIR, filename)
                files.append((file_path, os.path.getctime(file_path)))
        
        files.sort(key=lambda x: x[1])
        
        if len(files) > max_files:
            for i in range(len(files) - max_files):
                os.remove(files[i][0])
                print(f"자동 삭제됨: {files[i][0]}")
    except Exception as e:
        print(f"파일 정리 중 오류: {e}")

def run_data_collection(task_id, stock_count=100, fields=None, market='KOSPI'):
    """백그라운드에서 데이터 수집 실행"""
    try:
        tasks[task_id]['status'] = 'running'
        tasks[task_id]['progress'] = 0
        tasks[task_id]['message'] = f'{market} 데이터 수집 시작...'

        script_path = os.path.join(os.path.dirname(__file__), 'data_collect.py')
        python_cmd = sys.executable
        if 'uwsgi' in python_cmd.lower():
            python_cmd = 'python'

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if market == 'MY_STOCKS':
            result_filename = f'mystocks_{timestamp}.xlsx'
        else:
            count_label = 'all' if stock_count == 0 else f'top{stock_count}'
            result_filename = f'{market.lower()}_{count_label}_{timestamp}.xlsx'
        result_path = os.path.join(RESULTS_DIR, result_filename)

        cmd = [python_cmd, script_path, '--count', str(stock_count), '--market', market, '--output', result_path]
        
        if market == 'MY_STOCKS' and 'tickers' in tasks[task_id]:
            cmd.extend(['--tickers', ','.join(tasks[task_id]['tickers'])])

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
                
                drive_link = None
                spreadsheet_id = None
                try:
                    from drive_sync import upload_to_drive
                    drive_data = upload_to_drive(result_path)
                    if drive_data:
                        tasks[task_id]['message'] += f' (구글 드라이브 업로드 완료)'
                        drive_link = drive_data['link']
                        spreadsheet_id = drive_data['id']
                        tasks[task_id]['drive_link'] = drive_link
                        os.remove(result_path)
                except Exception as drive_err:
                    print(f"드라이브 업로드 실패: {drive_err}")

                try:
                    conn = sqlite3.connect(DB_FILE)
                    cursor = conn.cursor()
                    parts = result_filename.replace('.xlsx', '').split('_')
                    market_val = parts[0].upper() if len(parts) > 0 else market
                    count_val = parts[1] if len(parts) > 1 else str(stock_count)
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO analysis_results 
                        (filename, market, stock_count, created_at, size, spreadsheet_id, drive_link)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        result_filename,
                        market_val,
                        count_val,
                        datetime.now().isoformat(),
                        os.path.getsize(result_path) if os.path.exists(result_path) else 0,
                        spreadsheet_id,
                        drive_link
                    ))
                    conn.commit()
                    conn.close()
                except Exception as db_err:
                    print(f"DB 저장 실패: {db_err}")
                
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

    thread = threading.Thread(target=run_data_collection, args=(task_id, stock_count, fields, market))
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

def get_current_price(ticker):
    """네이버 금융에서 현재가를 가져옵니다."""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = requests.get(url, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        price_area = soup.select_one('.no_today .no_up .blind, .no_today .no_down .blind, .no_today .no_steady .blind')
        if price_area:
            return int(price_area.text.strip().replace(',', ''))
    except:
        pass
    return 0

@app.route('/api/my_stocks', methods=['GET'])
def get_my_stocks():
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT code, name, added_at, purchase_price, quantity FROM my_stocks ORDER BY added_at DESC")
        stocks = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify(stocks)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/my_stocks/status', methods=['GET'])
def get_my_stocks_status():
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT code, name, purchase_price, quantity FROM my_stocks")
        stocks = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        # 병렬로 현재가 가져오기
        with ThreadPoolExecutor(max_workers=10) as executor:
            prices = list(executor.map(lambda s: get_current_price(s['code']), stocks))
        
        results = []
        for i, stock in enumerate(stocks):
            price = prices[i]
            purchase_price = stock['purchase_price'] or 0
            qty = stock['quantity'] or 0
            profit = (price - purchase_price) * qty if purchase_price > 0 else 0
            profit_rate = ((price - purchase_price) / purchase_price * 100) if purchase_price > 0 else 0
            
            results.append({
                'code': stock['code'],
                'name': stock['name'],
                'current_price': price,
                'purchase_price': purchase_price,
                'quantity': qty,
                'profit': profit,
                'profit_rate': round(profit_rate, 2)
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
    if not code:
        return jsonify({'success': False, 'message': '종목 코드가 필요합니다.'}), 400
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO my_stocks (code, name, added_at, purchase_price, quantity) VALUES (?, ?, ?, ?, ?)", 
                       (code, name, datetime.now().isoformat(), purchase_price, quantity))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/my_stocks/<code_val>', methods=['DELETE'])
def delete_my_stock(code_val):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM my_stocks WHERE code = ?", (code_val,))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/my_stocks/<code_val>', methods=['PATCH'])
def update_my_stock(code_val):
    data = request.get_json() or {}
    purchase_price = data.get('purchase_price')
    quantity = data.get('quantity')
    
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        if purchase_price is not None and quantity is not None:
            cursor.execute("UPDATE my_stocks SET purchase_price = ?, quantity = ? WHERE code = ?", 
                           (purchase_price, quantity, code_val))
        elif purchase_price is not None:
            cursor.execute("UPDATE my_stocks SET purchase_price = ? WHERE code = ?", 
                           (purchase_price, code_val))
        elif quantity is not None:
            cursor.execute("UPDATE my_stocks SET quantity = ? WHERE code = ?", 
                           (quantity, code_val))
        
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/search_stock', methods=['GET'])
def search_stock():
    query = request.args.get('q', '')
    if len(query) < 2:
        return jsonify([])
    
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        # 이름으로 검색 (부분 일치)
        cursor.execute("SELECT code, name FROM stocks_master WHERE name LIKE ? LIMIT 10", (f'%{query}%',))
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
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
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM analysis_results ORDER BY created_at DESC")
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/<filename>')
def download_file(filename):
    file_path = os.path.join(RESULTS_DIR, filename)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    
    # 드라이브에서 다운로드 시도
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT spreadsheet_id FROM analysis_results WHERE filename = ?", (filename,))
        row = cursor.fetchone()
        conn.close()
        if row and row[0]:
            from drive_sync import download_from_drive
            content = download_from_drive(row[0])
            if content:
                import io
                return send_file(io.BytesIO(content), as_attachment=True, download_name=filename)
    except: pass
    return jsonify({'error': '파일을 찾을 수 없습니다.'}), 404

@app.route('/api/delete/<filename>', methods=['DELETE'])
def delete_result(filename):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM analysis_results WHERE filename = ?", (filename,))
        conn.commit()
        conn.close()
        file_path = os.path.join(RESULTS_DIR, filename)
        if os.path.exists(file_path):
            os.remove(file_path)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/ai_analyze/<filename>', methods=['POST'])
def ai_analyze(filename):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT ai_result FROM analysis_results WHERE filename = ?", (filename,))
        row = cursor.fetchone()
        if row and row[0]:
            conn.close()
            return jsonify({'success': True, 'result': row[0], 'cached': True})
            
        file_path = os.path.join(RESULTS_DIR, filename)
        if not os.path.exists(file_path):
            # 드라이브에서 임시 다운로드 시도
            cursor.execute("SELECT spreadsheet_id FROM analysis_results WHERE filename = ?", (filename,))
            row_id = cursor.fetchone()
            if row_id and row_id[0]:
                from drive_sync import download_from_drive
                content = download_from_drive(row_id[0])
                if content:
                    with open(file_path, 'wb') as f:
                        f.write(content)
                else:
                    return jsonify({'success': False, 'message': '파일을 찾을 수 없습니다.'}), 404
            else:
                return jsonify({'success': False, 'message': '파일을 찾을 수 없습니다.'}), 404
            
        result_text = analyze_stock_data(file_path)
        cursor.execute("UPDATE analysis_results SET ai_result = ? WHERE filename = ?", (result_text, filename))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'result': result_text, 'cached': False})
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
