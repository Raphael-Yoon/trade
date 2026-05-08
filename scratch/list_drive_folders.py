
import os
import pickle
import sys
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

# 1. 환경 설정 (trade 폴더 기준)
TRADE_DIR = r'c:\Python\trade'
TOKEN_PATH = os.path.join(TRADE_DIR, 'token.pickle')
CREDENTIALS_PATH = os.path.join(TRADE_DIR, 'credentials.json')

def get_drive_service():
    creds = None
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, 'rb') as token:
            creds = pickle.load(token)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            print("Credentials invalid and cannot refresh. Run manual auth.")
            return None
        with open(TOKEN_PATH, 'wb') as token:
            pickle.dump(creds, token)
    
    return build('drive', 'v3', credentials=creds)

def list_all_folders():
    service = get_drive_service()
    if not service: return
    
    query = "mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    folders = results.get('files', [])
    
    print("Available Folders on Google Drive:")
    for f in folders:
        print(f"- {f['name']} (ID: {f['id']})")

if __name__ == "__main__":
    list_all_folders()
