
import os
import pickle
import io
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd

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

def check_file_columns(file_id):
    service = get_drive_service()
    if not service: return
    
    request = service.files().export_media(
        fileId=file_id,
        mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    
    fh.seek(0)
    df = pd.read_excel(fh)
    print("Column names:")
    for i, col in enumerate(df.columns):
        print(f"{i}: {col}")

if __name__ == "__main__":
    check_file_columns("1Isc7NK9dVSZM4bPNeSUYOdoA09GUgXBiGk1QxizMTGQ")
