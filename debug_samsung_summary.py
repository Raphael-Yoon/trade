import OpenDartReader
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("DART_API_KEY")

def test_samsung_summary():
    dart = OpenDartReader(API_KEY)
    ticker = '005930'
    print("Fetching Samsung 2024 Annual Summary (finstate)...")
    try:
        # 주요재무사항 (지정된 항목만 가져옴: 매출액, 영업이익 등)
        df = dart.finstate(ticker, 2024, '11011')
        if df is not None and not df.empty:
            cols = [c for c in ['account_nm', 'thstrm_amount', 'frmtrm_amount', 'fs_div'] if c in df.columns]
            print(df[cols].to_string(index=False))
        else:
            print("No summary data.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_samsung_summary()
