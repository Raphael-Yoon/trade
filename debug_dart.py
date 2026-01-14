import OpenDartReader
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("DART_API_KEY")

def test_samsung():
    dart = OpenDartReader(API_KEY)
    # 삼성전자 (005930) 2025년 3분기 보고서 (11014)
    print("Fetching Samsung 2025 Q3...")
    df = dart.finstate_all('005930', 2025, '11014')
    if df is not None and not df.empty:
        print("Columns:", df.columns.tolist())
        # 매출액 찾기
        revenue_df = df[df['account_nm'].str.contains('매출액', na=False)]
        print("\nRevenue Rows:")
        print(revenue_df[['account_nm', 'thstrm_amount', 'frmtrm_amount']].to_string())
    else:
        print("No data found for 2025 Q3. Trying 2024 Annual...")
        df = dart.finstate_all('005930', 2024, '11011')
        if df is not None:
             print("Columns:", df.columns.tolist())
             revenue_df = df[df['account_nm'].str.contains('매출액', na=False)]
             print("\nRevenue Rows:")
             print(revenue_df[['account_nm', 'thstrm_amount', 'frmtrm_amount']].to_string())

if __name__ == "__main__":
    test_samsung()
