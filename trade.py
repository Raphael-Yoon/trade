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
import markdown
from ai_analysis import analyze_stock_data

app = Flask(__name__)

# 작업 상태 저장
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
        python_cmd = sys.executable
        if 'uwsgi' in python_cmd.lower():
            python_cmd = 'python'

        # 결과 파일명 미리 생성
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        count_label = 'all' if stock_count == 0 else f'top{stock_count}'
        result_filename = f'{market.lower()}_{count_label}_{timestamp}.xlsx'
        result_path = os.path.join(RESULTS_DIR, result_filename)

        cmd = [python_cmd, script_path, '--count', str(stock_count), '--market', market, '--output', result_path]

        # 선택된 필드가 있으면 추가
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

        # 실시간 출력 읽기
        for line in process.stdout:
            line = line.strip()
            if line:
                tasks[task_id]['message'] = line

                # 진행률 파싱 (진행률: [10/100] 10% 완료)
                if '진행률:' in line:
                    try:
                        # [current/total] 형태에서 숫자 추출
                        start_idx = line.find('[')
                        end_idx = line.find(']')
                        if start_idx != -1 and end_idx != -1:
                            bracket_content = line[start_idx+1:end_idx]
                            if '/' in bracket_content:
                                current, total = map(int, bracket_content.split('/'))
                                tasks[task_id]['progress'] = int((current / total) * 100)
                        
                        # % 기호로도 백업 파싱
                        elif '%' in line:
                            percent_val = line.split('%')[0].split()[-1]
                            tasks[task_id]['progress'] = int(percent_val)
                    except:
                        pass

        process.wait()

        # 프로세스 참조 제거
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
                
                # 구글 드라이브 업로드 시도
                try:
                    from drive_sync import upload_to_drive
                    drive_data = upload_to_drive(result_path)
                    if drive_data:
                        tasks[task_id]['message'] += f' (구글 드라이브 업로드 완료)'
                        tasks[task_id]['drive_link'] = drive_data['link']
                        
                        # 캐시 및 메타데이터 저장
                        cache_path = os.path.join(RESULTS_DIR, result_filename.replace('.xlsx', '.json'))
                        stat = os.stat(result_path)
                        cache_data = {
                            'filename': result_filename,
                            'size': stat.st_size,
                            'spreadsheet_id': drive_data['id'],
                            'drive_link': drive_data['link'],
                            'created_at': datetime.now().isoformat()
                        }
                        with open(cache_path, 'w', encoding='utf-8') as f:
                            json.dump(cache_data, f, ensure_ascii=False, indent=4)
                        
                        # 로컬 엑셀 파일 삭제 (드라이브에만 보관)
                        os.remove(result_path)
                        print(f"로컬 파일 삭제됨 (드라이브 업로드 완료): {result_path}")
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

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/collect', methods=['POST'])
def start_collection():
    data = request.get_json() or {}
    stock_count = data.get('stock_count', 100)
    fields = data.get('fields', [])
    market = data.get('market', 'KOSPI')

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
    if task_id not in tasks:
        return jsonify({'error': '작업을 찾을 수 없습니다.'}), 404
    
    # 프로세스 객체는 JSON 직렬화가 안 되므로 복사하여 제외
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
            task['message'] = '사용자에 의해 수집이 취소되었습니다.'
            return jsonify({'success': True, 'message': '수집이 취소되었습니다.'})
        except Exception as e:
            return jsonify({'success': False, 'message': f'취소 중 오류 발생: {str(e)}'}), 500
    
    return jsonify({'success': False, 'message': '취소할 수 있는 상태가 아닙니다.'})

@app.route('/api/download/<filename>', methods=['GET'])
def download_file(filename):
    cache_path = os.path.join(RESULTS_DIR, filename.replace('.xlsx', '.json'))
    if not os.path.exists(cache_path):
        return jsonify({'error': '파일 정보를 찾을 수 없습니다.'}), 404
    
    try:
        with open(cache_path, 'r', encoding='utf-8') as f:
            cache_data = json.load(f)
        
        spreadsheet_id = cache_data.get('spreadsheet_id')
        if not spreadsheet_id:
            return jsonify({'error': '구글 드라이브 ID가 없습니다.'}), 404
            
        from drive_sync import download_from_drive
        file_content = download_from_drive(spreadsheet_id)
        
        if file_content:
            import io
            return send_file(
                io.BytesIO(file_content),
                as_attachment=True,
                download_name=filename,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        else:
            # 드라이브에 파일이 없으면 캐시 삭제 (Self-healing)
            if os.path.exists(cache_path):
                os.remove(cache_path)
            return jsonify({'error': '드라이브에서 파일을 찾을 수 없습니다. 목록에서 제거되었습니다.'}), 404
    except Exception as e:
        return jsonify({'error': f'다운로드 중 오류: {str(e)}'}), 500

@app.route('/api/delete/<filename>', methods=['DELETE'])
def delete_file(filename):
    file_path = os.path.join(RESULTS_DIR, filename)
    cache_path = os.path.join(RESULTS_DIR, filename.replace('.xlsx', '.json'))
    
    try:
        # 1. 구글 드라이브 파일 삭제 시도
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                
                from drive_sync import delete_from_drive
                # 엑셀(스프레드시트) 삭제
                if cache_data.get('spreadsheet_id'):
                    delete_from_drive(cache_data['spreadsheet_id'])
                # AI 리포트(구글 문서) 삭제
                if cache_data.get('report_id'):
                    delete_from_drive(cache_data['report_id'])
            except Exception as e:
                print(f"드라이브 파일 삭제 중 오류 (무시하고 진행): {e}")

        # 2. 로컬 파일 삭제
        if os.path.exists(file_path):
            os.remove(file_path)
        if os.path.exists(cache_path):
            os.remove(cache_path)
            
        return jsonify({'success': True, 'message': '로컬 및 드라이브 파일이 모두 삭제되었습니다.'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'삭제 실패: {str(e)}'}), 500

@app.route('/api/upload_drive/<filename>', methods=['POST'])
def upload_existing_file(filename):
    file_path = os.path.join(RESULTS_DIR, filename)
    if not os.path.exists(file_path):
        return jsonify({'success': False, 'message': '파일을 찾을 수 없습니다.'}), 404
    
    try:
        from drive_sync import upload_to_drive
        drive_link = upload_to_drive(file_path)
        if drive_link:
            return jsonify({
                'success': True, 
                'message': '구글 드라이브 업로드 완료!',
                'drive_link': drive_link
            })
        else:
            return jsonify({'success': False, 'message': '업로드에 실패했습니다.'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'오류 발생: {str(e)}'}), 500

@app.route('/api/sync', methods=['POST'])
def sync_with_drive():
    """구글 드라이브와 로컬 캐시 동기화"""
    if not os.path.exists(RESULTS_DIR):
        os.makedirs(RESULTS_DIR)
    
    removed_count = 0
    added_count = 0
    try:
        from drive_sync import get_drive_service, list_files_in_folder
        service = get_drive_service()
        drive_files = list_files_in_folder()
        
        # 1. 드라이브에 있는 파일을 기반으로 로컬 캐시 생성/업데이트
        drive_ids = set()
        for df in drive_files:
            drive_ids.add(df['id'])
            
            # 스프레드시트 파일만 결과 목록의 주 파일로 처리
            if df['mimeType'] == 'application/vnd.google-apps.spreadsheet':
                filename = df['name']
                if not filename.endswith('.xlsx'):
                    filename += '.xlsx'
                
                cache_filename = filename.replace('.xlsx', '.json')
                cache_path = os.path.join(RESULTS_DIR, cache_filename)
                
                # 캐시가 없으면 생성
                if not os.path.exists(cache_path):
                    cache_data = {
                        'filename': filename,
                        'size': int(df.get('size', 0)) if df.get('size') else 0,
                        'spreadsheet_id': df['id'],
                        'drive_link': df.get('webViewLink'),
                        'created_at': df.get('createdTime')
                    }
                    with open(cache_path, 'w', encoding='utf-8') as f:
                        json.dump(cache_data, f, ensure_ascii=False, indent=4)
                    added_count += 1
                    print(f"동기화: 새로운 캐시 생성됨: {cache_filename}")

        # 2. 로컬 캐시 중 드라이브에 없는 것 삭제
        for filename in os.listdir(RESULTS_DIR):
            if filename.endswith('.json'):
                cache_path = os.path.join(RESULTS_DIR, filename)
                try:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    spreadsheet_id = data.get('spreadsheet_id')
                    if not spreadsheet_id or spreadsheet_id not in drive_ids:
                        os.remove(cache_path)
                        removed_count += 1
                        print(f"동기화: 드라이브에 없는 캐시 삭제됨: {filename}")
                except Exception as e:
                    print(f"동기화 중 파일 처리 오류 ({filename}): {e}")
                    continue
                    
        return jsonify({
            'success': True, 
            'removed': removed_count,
            'added': added_count
        })
    except Exception as e:
        print(f"동기화 전체 오류: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/results', methods=['GET'])
def list_results():
    files = []
    if os.path.exists(RESULTS_DIR):
        for filename in os.listdir(RESULTS_DIR):
            if filename.endswith('.json'):
                try:
                    cache_path = os.path.join(RESULTS_DIR, filename)
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # 메타데이터가 있는 경우만 추가
                    if 'filename' in data:
                        files.append({
                            'filename': data['filename'],
                            'size': data.get('size', 0),
                            'created_at': data.get('created_at'),
                            'drive_link': data.get('drive_link')
                        })
                except:
                    continue
    # 최신순(내림차순) 정렬: created_at이 없으면 빈 문자열 대신 아주 오래된 날짜를 기본값으로 사용
    files.sort(key=lambda x: x.get('created_at') or '0000-00-00', reverse=True)
    return jsonify(files)

@app.route('/api/ai_analyze/<filename>', methods=['POST'])
def ai_analyze(filename):
    file_path = os.path.join(RESULTS_DIR, filename)
    cache_path = os.path.join(RESULTS_DIR, filename.replace('.xlsx', '.json'))
    
    # 1. 캐시된 결과가 있는지 확인 + 드라이브 실시간 확인
    cache_data = {}
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            report_id = cache_data.get('report_id')
            if cache_data.get('result') and report_id:
                # 드라이브에 실제 리포트 파일이 있는지 확인
                try:
                    from drive_sync import get_drive_service
                    service = get_drive_service()
                    file_info = service.files().get(fileId=report_id, fields='id, trashed').execute()
                    
                    if not file_info.get('trashed'):
                        return jsonify({
                            'success': True, 
                            'result': cache_data.get('result'),
                            'drive_link': cache_data.get('drive_link'),
                            'cached': True
                        })
                    else:
                        print(f"AI 리포트가 휴지통에 있음. 재분석 진행: {filename}")
                except:
                    print(f"AI 리포트를 드라이브에서 찾을 수 없음. 재분석 진행: {filename}")
        except Exception as e:
            print(f"캐시 읽기 오류: {e}")

    # 2. 로컬에 엑셀 파일이 없으면 드라이브에서 임시로 다운로드
    temp_file_created = False
    if not os.path.exists(file_path):
        spreadsheet_id = cache_data.get('spreadsheet_id')
        if not spreadsheet_id:
            return jsonify({'success': False, 'message': '분석할 파일 데이터가 없습니다.'})
        
        try:
            from drive_sync import download_from_drive
            content = download_from_drive(spreadsheet_id)
            if content:
                with open(file_path, 'wb') as f:
                    f.write(content)
                temp_file_created = True
            else:
                # 드라이브에 파일이 없으면 캐시 삭제 (Self-healing)
                if os.path.exists(cache_path):
                    os.remove(cache_path)
                return jsonify({'success': False, 'message': '드라이브에서 파일을 찾을 수 없습니다. 목록에서 제거되었습니다.'})
        except Exception as e:
            return jsonify({'success': False, 'message': f'파일 다운로드 중 오류: {e}'})

    # 3. AI 분석 실행
    result = analyze_stock_data(file_path)
    
    # 분석 후 임시 파일 삭제
    if temp_file_created and os.path.exists(file_path):
        os.remove(file_path)

    if "오류" in result or "설정되지 않았습니다" in result:
        return jsonify({'success': False, 'message': result})
    
    # 4. 마크다운을 HTML로 변환하여 구글 문서 서식 적용
    try:
        # 표(tables) 확장 기능 포함하여 변환
        html_result = markdown.markdown(result, extensions=['tables', 'fenced_code'])
    except Exception as e:
        print(f"마크다운 변환 오류: {e}")
        html_result = result.replace('\n', '<br>')

    # 5. 구글 문서로 자동 저장
    drive_data = None
    try:
        from drive_sync import create_google_doc
        title = f"AI 분석 리포트 - {filename.replace('.xlsx', '')}"
        drive_data = create_google_doc(title, html_result)
    except Exception as e:
        print(f"자동 구글 문서 저장 실패: {e}")
    
    # 6. 결과 캐시 저장
    try:
        # 기존 캐시 데이터가 있으면 읽어옴 (spreadsheet_id 보존을 위해)
        cache_data = {}
        if os.path.exists(cache_path):
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
        
        cache_data.update({
            'result': result,
            'report_id': drive_data['id'] if drive_data else cache_data.get('report_id'),
            'drive_link': drive_data['link'] if drive_data else cache_data.get('drive_link'),
            'created_at': datetime.now().isoformat()
        })
        
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"캐시 저장 오류: {e}")

    return jsonify({
        'success': True, 
        'result': result,
        'drive_link': cache_data.get('drive_link'),
        'cached': False
    })

@app.route('/api/save_ai_report', methods=['POST'])
def save_ai_report():
    data = request.json
    title = data.get('title', 'AI 분석 리포트')
    content = data.get('content', '')
    
    if not content:
        return jsonify({'success': False, 'message': '저장할 내용이 없습니다.'})
        
    try:
        html_content = markdown.markdown(content, extensions=['tables', 'fenced_code'])
    except:
        html_content = content.replace('\n', '<br>')

    from drive_sync import create_google_doc
    drive_data = create_google_doc(title, html_content)
    
    if drive_data:
        return jsonify({'success': True, 'drive_link': drive_data['link']})
    else:
        return jsonify({'success': False, 'message': '구글 문서 생성에 실패했습니다.'})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
