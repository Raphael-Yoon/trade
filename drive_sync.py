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
    """텍스트 내용을 구글 문서(Google Docs)로 생성"""
    try:
        service = get_drive_service()
        folder_id = get_or_create_folder(service, folder_name)
        
        file_metadata = {
            'name': title,
            'parents': [folder_id],
            'mimeType': 'application/vnd.google-apps.document'
        }
        
        import io
        from googleapiclient.http import MediaIoBaseUpload
        
        fh = io.BytesIO(content.encode('utf-8'))
        media = MediaIoBaseUpload(fh, mimetype='text/plain', resumable=True)
        
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
