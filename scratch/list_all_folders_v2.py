
import os
import pickle
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

# 1. 환경 설정 (trade 폴더 기준)
TRADE_DIR = r'c:\Python\trade'
TOKEN_PATH = os.path.join(TRADE_DIR, 'token.pickle')

def get_drive_service():
    creds = None
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            return None
        with open(TOKEN_PATH, 'wb') as token:
            pickle.dump(creds, token)
    return build('drive', 'v3', credentials=creds)

def list_all_folders():
    service = get_drive_service()
    if not service: return
    
    # 더 많은 정보를 가져오기 위해 pageSize 증가 및 필드 추가
    results = service.files().list(
        q="mimeType = 'application/vnd.google-apps.folder' and trashed = false",
        fields="files(id, name, parents)",
        pageSize=100
    ).execute()
    folders = results.get('files', [])
    
    print("All Folders on Google Drive:")
    for f in folders:
        print(f"- {f['name']} (ID: {f['id']}, Parents: {f.get('parents')})")

if __name__ == "__main__":
    list_all_folders()
