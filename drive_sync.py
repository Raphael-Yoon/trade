import os
import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload
import re

# 구글 드라이브 API 권한 범위 (파일 읽기/쓰기/생성)
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def get_drive_service():
    """구글 드라이브 서비스 객체 생성 및 인증"""
    creds = None
    # token.pickle 파일에 사용자 인증 정보 저장
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
            
    # 인증 정보가 없거나 유효하지 않으면 새로 인증
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists('credentials.json'):
                raise FileNotFoundError("credentials.json 파일이 없습니다. 구글 클라우드 콘솔에서 다운로드하여 프로젝트 루트에 저장해주세요.")
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # 인증 정보 저장
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)

def get_or_create_folder(service, folder_name):
    """구글 드라이브에서 특정 이름의 폴더를 찾거나 생성"""
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])

    if items:
        return items[0]['id']
    else:
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        folder = service.files().create(body=folder_metadata, fields='id').execute()
        return folder.get('id')

def upload_to_drive(file_path, folder_name="Stock_Analysis_Results"):
    """파일을 구글 드라이브 특정 폴더에 업로드"""
    try:
        service = get_drive_service()
        folder_id = get_or_create_folder(service, folder_name)
        
        file_name = os.path.basename(file_path)
        # .xlsx 확장자 제거 (구글 시트 변환 시 깔끔하게 보이기 위함)
        display_name = os.path.splitext(file_name)[0]
        
        file_metadata = {
            'name': display_name,
            'parents': [folder_id],
            'mimeType': 'application/vnd.google-apps.spreadsheet'  # 구글 시트로 변환 설정
        }
        
        media = MediaFileUpload(
            file_path,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            resumable=True
        )
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, webViewLink'
        ).execute()
        
        print(f"파일 업로드 완료: {file_name} (ID: {file.get('id')})")
        return {
            'id': file.get('id'),
            'link': file.get('webViewLink')
        }
        
    except Exception as e:
        print(f"구글 드라이브 업로드 중 오류 발생: {e}")
        return None

def create_google_doc(title, content, folder_name="Stock_Analysis_Results"):
    """마크다운 또는 HTML 내용을 구글 문서(Google Docs)로 생성 (서식 보존)"""
    try:
        service = get_drive_service()
        folder_id = get_or_create_folder(service, folder_name)
        
        # 마크다운 변환 시도 (ImportError 발생 시 fallback)
        try:
            import markdown
            # tables: 테이블 지원, nl2br: 줄바꿈 지원, sane_lists: 리스트 개선
            html_body = markdown.markdown(content, extensions=['tables', 'nl2br', 'sane_lists', 'fenced_code'])
        except ImportError:
            print("markdown 라이브러리가 없어 plain text 방식으로 처리합니다.")
            html_body = content.replace('\n', '<br>')
        except Exception as e:
            print(f"마크다운 변환 중 오류: {e}")
            html_body = content.replace('\n', '<br>')

        # 구글 문서 변환 시 표 너비를 문서 폭에 맞게 강제하기 위해 고정 픽셀(700px) 사용
        # 첫 번째 컬럼(180px), 두 번째 컬럼(100px) 지정을 위해 table-layout: fixed 적용
        html_body = html_body.replace('<table>', '<table width="700" style="width: 700px; border-collapse: collapse; border: 1px solid #cbd5e1; margin: 20px 0; table-layout: fixed;">')
        html_body = html_body.replace('<thead>', '<thead style="background-color: #f8fafc;">')
        html_body = html_body.replace('<th>', '<th style="background-color: #f8fafc; color: #1e293b; font-weight: bold; padding: 8px 10px; border: 1px solid #cbd5e1; text-align: center;">')
        html_body = html_body.replace('<td>', '<td style="padding: 6px 10px; border: 1px solid #cbd5e1; text-align: left; vertical-align: top; word-wrap: break-word;">')

        # [추가] 헤더(th) 내용에 포함된 강제 줄바꿈 태그 제거 (데이터 차원의 수정)
        # 이미 스타일이 적용된 <th style="..."> 태그 내부의 내용물에서 <br>과 \n을 공백으로 치환
        html_body = re.sub(r'(<th[^>]*>)(.*?)(</th>)', 
                          lambda m: m.group(1) + m.group(2).replace('<br>', ' ').replace('<br/>', ' ').replace('\n', ' ') + m.group(3), 
                          html_body, 
                          flags=re.DOTALL | re.IGNORECASE)

        file_metadata = {
            'name': title,
            'parents': [folder_id],
            'mimeType': 'application/vnd.google-apps.document'
        }
        
        import io
        from googleapiclient.http import MediaIoBaseUpload
        
        # UTF-8 BOM을 추가하여 한글 깨짐 방지 및 HTML 선언
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{ 
                    font-family: 'Nanum Gothic', 'Malgun Gothic', sans-serif; 
                    line-height: 1.6; 
                    color: #333; 
                    padding: 20px; 
                    width: 700px;
                    margin: 0 auto;
                }}
                h1 {{ color: #1e293b; border-bottom: 2px solid #6366f1; padding-bottom: 10px; text-align: center; }}
                h2 {{ color: #4338ca; margin-top: 30px; border-left: 5px solid #6366f1; padding-left: 10px; background-color: #f1f5f9; padding: 8px 10px; }}
                h3 {{ color: #1e40af; margin-top: 20px; }}
                
                table {{ 
                    width: 700px !important; 
                    border-collapse: collapse; 
                    margin: 20px 0; 
                    table-layout: fixed;
                }}
                th, td {{
                    border: 1px solid #cbd5e1;
                    padding: 6px 10px;
                    text-align: left;
                    font-size: 10pt;
                    word-wrap: break-word;
                    line-height: 1.4;
                }}
                th {{
                    background-color: #f8fafc;
                    color: #1e293b;
                    font-weight: bold;
                    text-align: center;
                    padding: 8px 10px;
                    line-height: 1.3;
                }}
                
                /* 첫 번째 컬럼 (종목명 등) - 기존 60px에서 3배인 180px로 확대 */
                th:first-child, td:first-child {{ 
                    width: 180px; 
                    text-align: center; 
                }}
                
                /* 두 번째 컬럼 (업종 등) - 너비 축소 (약 100px) */
                th:nth-child(2), td:nth-child(2) {{ 
                    width: 100px; 
                    text-align: center;
                }}

                /* 세 번째 컬럼 (추천 요약 등) - 나머지 모든 폭 사용 */
                th:nth-child(3), td:nth-child(3) {{ 
                    width: auto; 
                }}
                
                blockquote {{ 
                    border-left: 4px solid #e2e8f0; 
                    padding-left: 15px; 
                    color: #64748b; 
                    font-style: italic; 
                    background-color: #f8fafc; 
                    padding: 10px 15px; 
                }}
                .highlight {{ background-color: #fef9c3; padding: 2px 5px; border-radius: 3px; }}
            </style>
        </head>
        <body>
            {html_body}
        </body>
        </html>
        """
        
        fh = io.BytesIO(html_content.encode('utf-8'))
        media = MediaIoBaseUpload(fh, mimetype='text/html', resumable=True)
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, webViewLink'
        ).execute()
        
        print(f"구글 문서 생성 완료: {title} (ID: {file.get('id')})")
        return {
            'id': file.get('id'),
            'link': file.get('webViewLink')
        }
        
    except Exception as e:
        print(f"구글 문서 생성 중 오류 발생: {e}")
        return None

def delete_from_drive(file_id):
    """구글 드라이브에서 파일 삭제"""
    if not file_id:
        return False
    try:
        service = get_drive_service()
        service.files().delete(fileId=file_id).execute()
        print(f"구글 드라이브 파일 삭제 완료 (ID: {file_id})")
        return True
    except Exception as e:
        print(f"구글 드라이브 파일 삭제 중 오류 발생: {e}")
        return False

def download_from_drive(file_id):
    """구글 드라이브에서 파일을 엑셀 형식으로 다운로드"""
    try:
        service = get_drive_service()
        # 구글 시트를 엑셀로 내보내기
        request = service.files().export_media(
            fileId=file_id,
            mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        return request.execute()
    except Exception as e:
        print(f"구글 드라이브 다운로드 중 오류 발생: {e}")
        return None

def get_doc_content(file_id):
    """구글 문서(Google Docs)의 HTML 내용을 읽어오기"""
    try:
        service = get_drive_service()
        # 구글 문서를 HTML로 내보내기 (서식 유지)
        request = service.files().export_media(
            fileId=file_id,
            mimeType='text/html'
        )
        content = request.execute()
        return content.decode('utf-8')
    except Exception as e:
        print(f"구글 문서 읽기 중 오류 발생: {e}")
        return None

def find_ai_report(base_filename, folder_name="Stock_Analysis_Results"):
    """특정 분석 파일에 대한 AI 리포트 문서 찾기"""
    try:
        service = get_drive_service()
        folder_id = get_or_create_folder(service, folder_name)

        # AI 리포트 파일명 패턴
        report_name = f"AI 분석 리포트 - {base_filename}"

        query = f"name = '{report_name}' and '{folder_id}' in parents and mimeType = 'application/vnd.google-apps.document' and trashed = false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])

        if items:
            return items[0]  # {'id': ..., 'name': ...}
        return None
    except Exception as e:
        print(f"AI 리포트 검색 중 오류 발생: {e}")
        return None

def sync_results_with_drive(results_dir, folder_name="Stock_Analysis_Results"):
    """구글 드라이브와 로컬 파일 목록 동기화"""
    try:
        service = get_drive_service()
        drive_files = list_files_in_folder(folder_name)
        
        added = 0
        removed = 0
        
        # 드라이브에 있는 파일들을 로컬 메타데이터(.json)로 생성
        drive_filenames = set()
        for df in drive_files:
            if df.get('mimeType') == 'application/vnd.google-apps.spreadsheet':
                name = df['name']
                if not name.endswith('.xlsx'):
                    name += '.xlsx'
                drive_filenames.add(name)
                
                json_path = os.path.join(results_dir, name.replace('.xlsx', '.json'))
                # 로컬에 메타데이터가 없으면 생성
                if not os.path.exists(json_path):
                    import json
                    from datetime import datetime
                    
                    # createdTime 파싱 (2024-01-24T08:14:56.000Z)
                    created_at = df.get('createdTime', datetime.now().isoformat())
                    
                    metadata = {
                        'filename': name,
                        'size': int(df.get('size', 0)) if df.get('size') else 0,
                        'spreadsheet_id': df['id'],
                        'drive_link': df.get('webViewLink'),
                        'created_at': created_at
                    }
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(metadata, f, ensure_ascii=False, indent=4)
                    added += 1
        
        # 로컬에만 있고 드라이브에 없는 메타데이터 삭제 (선택 사항)
        # 여기서는 안전을 위해 삭제하지 않거나, 명시적으로 드라이브 기반 동기화임을 알림
        
        return added, removed
    except Exception as e:
        print(f"동기화 중 오류 발생: {e}")
        raise e

def list_files_in_folder(folder_name="Stock_Analysis_Results"):
    """구글 드라이브 특정 폴더의 파일 목록 가져오기"""
    try:
        service = get_drive_service()
        folder_id = get_or_create_folder(service, folder_name)
        
        query = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(
            q=query, 
            fields="files(id, name, mimeType, createdTime, webViewLink, size)",
            orderBy="createdTime desc"
        ).execute()
        return results.get('files', [])
    except Exception as e:
        print(f"구글 드라이브 목록 조회 중 오류 발생: {e}")
        return []

if __name__ == "__main__":
    # 테스트 코드
    # upload_to_drive("test.xlsx")
    pass
