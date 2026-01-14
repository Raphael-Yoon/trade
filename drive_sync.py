import os
import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload

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
    """HTML 내용을 구글 문서(Google Docs)로 생성 (서식 보존)"""
    try:
        service = get_drive_service()
        folder_id = get_or_create_folder(service, folder_name)
        
        # 텍스트가 마크다운인 경우 HTML로 변환을 위해 trade.py에서 처리하지만,
        # 여기서는 전달받은 content가 HTML이라고 가정하고 업로드합니다.
        
        file_metadata = {
            'name': title,
            'parents': [folder_id],
            'mimeType': 'application/vnd.google-apps.document'
        }
        
        import io
        from googleapiclient.http import MediaIoBaseUpload
        
        # UTF-8 BOM을 추가하여 한글 깨짐 방지 및 HTML 선언
        html_content = f"""
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{ font-family: 'Nanum Gothic', 'Malgun Gothic', sans-serif; line-height: 1.6; color: #333; padding: 20px; }}
                h1 {{ color: #1e293b; border-bottom: 2px solid #6366f1; padding-bottom: 10px; text-align: center; }}
                h2 {{ color: #4338ca; margin-top: 30px; border-left: 5px solid #6366f1; padding-left: 10px; background-color: #f1f5f9; padding: 8px 10px; }}
                h3 {{ color: #1e40af; margin-top: 20px; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; table-layout: fixed; }}
                th, td {{ border: 1px solid #cbd5e1; padding: 10px; text-align: left; word-break: break-all; font-size: 10pt; }}
                th {{ background-color: #f8fafc; color: #1e293b; font-weight: bold; text-align: center; }}
                /* 첫 번째 열(순위/번호) 너비 제한 */
                th:first-child, td:first-child {{ width: 40px; text-align: center; }}
                /* 종목명 열은 조금 더 넓게 */
                th:nth-child(2), td:nth-child(2) {{ width: 120px; }}
                blockquote {{ border-left: 4px solid #e2e8f0; padding-left: 15px; color: #64748b; font-style: italic; background-color: #f8fafc; padding: 10px 15px; }}
                .highlight {{ background-color: #fef9c3; padding: 2px 5px; border-radius: 3px; }}
            </style>
        </head>
        <body>
            {content}
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
