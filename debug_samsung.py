import OpenDartReader
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("DART_API_KEY")

def test_samsung_detailed():
    dart = OpenDartReader(API_KEY)
    ticker = '005930' # 삼성전자
    
    # 사업보고서 (2024년 결산)
    print("Fetching Samsung 2024 Annual (11011)...")
    try:
        df = dart.finstate_all(ticker, 2024, '11011')
        if df is not None and not df.empty:
            # 매출액 관련 행 모두 출력
            rev_rows = df[df['account_nm'].str.contains('매출액|수익', na=False)]
            print("\nPotential Revenue Rows:")
            cols = [c for c in ['account_nm', 'account_id', 'thstrm_amount', 'frmtrm_amount', 'sj_div'] if c in df.columns]
            print(rev_rows[cols].to_string(index=False))
            
            # 전체 계정명 일부 출력
            print("\nFirst 10 accounts:")
            print(df['account_nm'].head(10).tolist())
        else:
            print("No data.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_samsung_detailed()
