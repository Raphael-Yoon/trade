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

app = Flask(__name__)

# 작업 상태 저장 (실제 운영 환경에서는 Redis 등 사용 권장)
tasks = {}

# 결과 파일 저장 디렉토리
RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')
if not os.path.exists(RESULTS_DIR):
    os.makedirs(RESULTS_DIR)

def run_data_collection(task_id, stock_count=100, fields=None):
    """백그라운드에서 데이터 수집 실행"""
    try:
        tasks[task_id]['status'] = 'running'
        tasks[task_id]['progress'] = 0
        tasks[task_id]['message'] = '데이터 수집 시작...'
        tasks[task_id]['stock_count'] = stock_count

        # data_collect.py 실행
        script_path = os.path.join(os.path.dirname(__file__), 'data_collect.py')
        cmd = [sys.executable, script_path, '--count', str(stock_count)]

        # 선택된 필드가 있으면 추가
        if fields:
            cmd.extend(['--fields', ','.join(fields)])

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8'
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
            source_file = os.path.join(os.path.dirname(__file__), 'result.xlsx')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            count_label = 'all' if stock_count == 0 else f'top{stock_count}'
            result_filename = f'kospi_{count_label}_{timestamp}.xlsx'
            result_path = os.path.join(RESULTS_DIR, result_filename)

            if os.path.exists(source_file):
                os.rename(source_file, result_path)
                tasks[task_id]['status'] = 'completed'
                tasks[task_id]['progress'] = 100
                tasks[task_id]['message'] = '데이터 수집 완료!'
                tasks[task_id]['result_file'] = result_filename
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
        'created_at': datetime.now().isoformat()
    }

    # 백그라운드 스레드로 실행
    thread = threading.Thread(target=run_data_collection, args=(task_id, stock_count, fields))
    thread.start()

    message = '전체 종목 데이터 수집이 시작되었습니다.' if stock_count == 0 else f'{stock_count}개 종목 데이터 수집이 시작되었습니다.'

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
    print("=" * 80)
    print("KOSPI 데이터 수집 웹 서버 시작")
    print("브라우저에서 http://localhost:5000 접속")
    print("=" * 80)
    app.run(debug=True, host='0.0.0.0', port=5000)
