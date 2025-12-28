# -*- coding: utf-8 -*-
import sys
import os

# Windows 콘솔 UTF-8 설정
if os.name == 'nt':
    os.system('chcp 65001 > nul')
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# sys.path.append(r'C:\Users\newsi\AppData\Roaming\Python\Python313\site-packages')

from flask import Flask, render_template, jsonify, send_file, request
import threading
import uuid
from datetime import datetime
import subprocess
import json

app = Flask(__name__)

# 작업 상태 저장 (실제 운영 환경에서는 Redis 등 사용 권장)
tasks = {}

# 결과 파일 저장 디렉토리
RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')
if not os.path.exists(RESULTS_DIR):
    os.makedirs(RESULTS_DIR)

def cleanup_old_results(max_files=20):
    """오래된 결과 파일 자동 삭제"""
    try:
        files = []
        for filename in os.listdir(RESULTS_DIR):
            if filename.endswith('.xlsx'):
                file_path = os.path.join(RESULTS_DIR, filename)
                files.append((file_path, os.path.getctime(file_path)))
        
        # 생성 시간순 정렬 (오래된 순)
        files.sort(key=lambda x: x[1])
        
        # max_files를 초과하는 파일 삭제
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
        tasks[task_id]['stock_count'] = stock_count

        # data_collect.py 실행
        script_path = os.path.join(os.path.dirname(__file__), 'data_collect.py')
        # uWSGI 환경 대응: sys.executable이 uwsgi일 경우 python 명령어로 대체
        python_cmd = sys.executable
        if 'uwsgi' in python_cmd.lower():
            python_cmd = 'python'

        cmd = [python_cmd, script_path, '--count', str(stock_count), '--market', market]

        # 선택된 필드가 있으면 추가
        if fields:
            cmd.extend(['--fields', ','.join(fields)])

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            cwd=os.path.dirname(__file__)  # 작업 디렉토리 고정
        )

        # 실시간 출력 읽기
        for line in process.stdout:
            line = line.strip()
            if line:
                tasks[task_id]['message'] = line

                # 진행률 파싱 (진행률: [10/100] 10% 완료)
                if '진행률:' in line and '%' in line:
                    try:
                        # [10/100] 형태에서 숫자 추출
                        if '[' in line and ']' in line:
                            bracket_content = line[line.find('[')+1:line.find(']')]
                            current, total = map(int, bracket_content.split('/'))
                            tasks[task_id]['progress'] = int((current / total) * 100)
                    except:
                        pass

        process.wait()

        if process.returncode == 0:
            # 결과 파일 이동
            source_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'result.xlsx')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            count_label = 'all' if stock_count == 0 else f'top{stock_count}'
            result_filename = f'{market.lower()}_{count_label}_{timestamp}.xlsx'
            result_path = os.path.join(RESULTS_DIR, result_filename)

            if os.path.exists(source_file):
                os.rename(source_file, result_path)
                tasks[task_id]['status'] = 'completed'
                tasks[task_id]['progress'] = 100
                tasks[task_id]['message'] = '데이터 수집 완료!'
                tasks[task_id]['result_file'] = result_filename
                
                # 오래된 파일 정리
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

@app.route('/')
def index():
    """메인 페이지"""
    return render_template('index.html')

@app.route('/api/collect', methods=['POST'])
def start_collection():
    """데이터 수집 시작"""
    data = request.get_json() or {}
    stock_count = data.get('stock_count', 100)
    fields = data.get('fields', [])
    market = data.get('market', 'KOSPI')

    # 유효성 검사
    if not isinstance(stock_count, int) or stock_count < 0 or stock_count > 10000:
        return jsonify({
            'success': False,
            'message': '종목 수는 0(전체) 또는 1~10000 사이의 숫자여야 합니다.'
        }), 400

    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        'status': 'pending',
        'progress': 0,
        'message': '대기 중...',
        'stock_count': stock_count,
        'market': market,
        'created_at': datetime.now().isoformat()
    }

    # 백그라운드 스레드로 실행
    thread = threading.Thread(target=run_data_collection, args=(task_id, stock_count, fields, market))
    thread.start()

    message = f'{market} 전체 종목 데이터 수집이 시작되었습니다.' if stock_count == 0 else f'{market} {stock_count}개 종목 데이터 수집이 시작되었습니다.'

    return jsonify({
        'success': True,
        'task_id': task_id,
        'message': message
    })

@app.route('/api/status/<task_id>', methods=['GET'])
def get_status(task_id):
    """작업 상태 조회"""
    if task_id not in tasks:
        return jsonify({'error': '작업을 찾을 수 없습니다.'}), 404

    return jsonify(tasks[task_id])

@app.route('/api/download/<filename>', methods=['GET'])
def download_file(filename):
    """결과 파일 다운로드"""
    file_path = os.path.join(RESULTS_DIR, filename)

    if not os.path.exists(file_path):
        return jsonify({'error': '파일을 찾을 수 없습니다.'}), 404

    return send_file(file_path, as_attachment=True, download_name=filename)

@app.route('/api/delete/<filename>', methods=['DELETE'])
def delete_file(filename):
    """결과 파일 삭제"""
    file_path = os.path.join(RESULTS_DIR, filename)
    
    if not os.path.exists(file_path):
        return jsonify({'success': False, 'message': '파일을 찾을 수 없습니다.'}), 404
        
    try:
        os.remove(file_path)
        return jsonify({'success': True, 'message': '파일이 삭제되었습니다.'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'삭제 실패: {str(e)}'}), 500

@app.route('/api/results', methods=['GET'])
def list_results():
    """저장된 결과 파일 목록"""
    files = []
    if os.path.exists(RESULTS_DIR):
        for filename in os.listdir(RESULTS_DIR):
            if filename.endswith('.xlsx'):
                file_path = os.path.join(RESULTS_DIR, filename)
                stat = os.stat(file_path)
                files.append({
                    'filename': filename,
                    'size': stat.st_size,
                    'created_at': datetime.fromtimestamp(stat.st_ctime).isoformat()
                })

    # 최신순 정렬
    files.sort(key=lambda x: x['created_at'], reverse=True)
    return jsonify(files)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
