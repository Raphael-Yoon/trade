
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

def list_files_in_results():
    service = get_drive_service()
    if not service: return
    
    folder_id = "1dZ1v4XbuR7ieJd3rx42wq1TRZRztO0b8" # Stock_Analysis_Results
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name, mimeType, createdTime)").execute()
    files = results.get('files', [])
    
    print(f"Files in Stock_Analysis_Results:")
    for f in files:
        print(f"- {f['name']} ({f['mimeType']}) [Created: {f['createdTime']}] (ID: {f['id']})")

if __name__ == "__main__":
    list_files_in_results()
