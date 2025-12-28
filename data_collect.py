# -*- coding: utf-8 -*-
import sys
import os

# Windows 콘솔 UTF-8 설정
if os.name == 'nt':
    os.system('chcp 65001 > nul')
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

sys.path.append(r'C:\Users\newsi\AppData\Roaming\Python\Python313\site-packages')

import OpenDartReader
import pandas as pd
import time
from pykrx import stock
from datetime import datetime, timedelta
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
import warnings
import argparse
warnings.filterwarnings('ignore')

# DART API KEY (환경변수로 관리 권장)
API_KEY = "08e04530eea4ba322907021334794e4164002525"

def get_latest_business_day():
    """가장 최근의 영업일을 반환합니다."""
    try:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=10)).strftime("%Y%m%d")
        ohlcv = stock.get_market_ohlcv(start_date, end_date, "005930")
        if ohlcv.empty:
            return (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        return ohlcv.index[-1].strftime("%Y%m%d")
    except Exception as e:
        print(f"영업일 조회 오류: {e}. 어제 날짜 사용")
        return (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

def get_sector_from_pykrx(ticker, sector_df):
    """pykrx 업종 분류 데이터에서 업종 정보를 추출합니다."""
    try:
        if ticker in sector_df.index:
            # 두 번째 컬럼(인덱스 1)이 업종 정보
            sector = sector_df.loc[ticker].iloc[1]
            return sector if pd.notna(sector) else 'N/A'
        return 'N/A'
    except:
        return 'N/A'

def get_dart_financials(dart, ticker, year):
    """OpenDARTReader를 사용하여 재무 데이터를 추출합니다."""
    try:
        # finstate_all 사용 (연결재무제표 우선)
        df = dart.finstate_all(ticker, year)

        if df is None or df.empty:
            return 0, 0, 0, 0, 0, 0, 0, 0, 0

        revenue = 0
        op = 0
        re = 0
        cash = 0
        liabilities = 0
        equity = 0
        ocf = 0
        capex = 0
        da = 0

        # XBRL 표준 계정 ID 매칭
        for _, row in df.iterrows():
            acc_id = str(row.get('account_id', ''))
            acc_name = str(row['account_nm']).replace(" ", "")
            val = pd.to_numeric(row['thstrm_amount'], errors='coerce')
            if pd.isna(val):
                val = 0

            # 1. 매출액
            if acc_id == 'ifrs-full_Revenue' or acc_name == '매출액' or acc_name == '수익(매출액)':
                if revenue == 0 or acc_id == 'ifrs-full_Revenue':
                    revenue = val
            
            # 2. 영업이익
            elif acc_id == 'dart_OperatingIncomeLoss' or acc_name == '영업이익' or acc_name == '영업이익(손실)':
                if op == 0 or acc_id == 'dart_OperatingIncomeLoss':
                    op = val
            
            # 3. 이익잉여금
            elif acc_id == 'ifrs-full_RetainedEarnings' or (re == 0 and '이익잉여금' in acc_name and '기타' not in acc_name):
                if re == 0 or acc_id == 'ifrs-full_RetainedEarnings':
                    re = val
            
            # 4. 현금및현금성자산
            elif acc_id == 'ifrs-full_CashAndCashEquivalents' or (cash == 0 and '현금및현금성자산' in acc_name):
                if cash == 0 or acc_id == 'ifrs-full_CashAndCashEquivalents':
                    cash = val

            # 5. 부채총계
            elif acc_id == 'ifrs-full_Liabilities' or acc_name == '부채총계':
                liabilities = val

            # 6. 자본총계
            elif acc_id == 'ifrs-full_Equity' or acc_name == '자본총계':
                if equity == 0 or acc_id == 'ifrs-full_Equity':
                    equity = val

            # 7. 영업활동현금흐름 (FCF 계산용)
            elif acc_id == 'ifrs-full_CashFlowsFromUsedInOperatingActivities' or acc_name == '영업활동현금흐름':
                ocf = val

            # 8. 유형/무형자산 취득 (CapEx 계산용)
            elif 'PurchaseOfPropertyPlantAndEquipment' in acc_id or 'PurchaseOfIntangibleAssets' in acc_id:
                capex += val
            elif acc_name in ['유형자산의취득', '무형자산의취득']:
                capex += val

            # 9. 감가상각비 (EBITDA 계산용)
            if 'Depreciation' in acc_id or 'Amortisation' in acc_id or '감가상각' in acc_name:
                da += val

        return revenue, op, re, cash, liabilities, equity, ocf, capex, da
    except Exception as e:
        print(f"[DART] {ticker} 재무제표 조회 실패: {e}")
        return 0, 0, 0, 0, 0, 0, 0, 0, 0

def main(stock_count=100, selected_fields=None):
    try:
        print("=" * 80)
        if stock_count == 0:
            print("KOSPI 전체 종목 데이터 수집 시작")
        else:
            print(f"KOSPI 상위 {stock_count}개 종목 데이터 수집 시작")
        print("수집 우선순위: pykrx > DART API")
        print("=" * 80)

        dart = OpenDartReader(API_KEY)

        # 1. 최근 영업일 조회
        latest_date = get_latest_business_day()
        print(f"\n최근 영업일: {latest_date}")

        # 2. pykrx로 시가총액 상위 100개 종목 조회
        print("\n[1단계] pykrx로 시가총액 및 기본 데이터 수집 중...")
        df_cap = stock.get_market_cap_by_ticker(latest_date, market="KOSPI")
        df_fundamental = stock.get_market_fundamental(latest_date, market="KOSPI")
        df_sector = stock.get_market_sector_classifications(latest_date, market="KOSPI")

        # 상위 N개 종목 (0이면 전체)
        if stock_count == 0:
            df_top100 = df_cap.sort_values(by='시가총액', ascending=False)
            print(f"대상 종목 수: 전체 {len(df_top100)}개")
        else:
            df_top100 = df_cap.sort_values(by='시가총액', ascending=False).head(stock_count)
            print(f"대상 종목 수: {len(df_top100)}개")

        # 3. 업종별 평균 PBR, PER 계산 (PBR, PER > 0 인 종목만 대상)
        print("\n[2단계] 업종별 평균 PBR, PER 계산 중...")
        # pykrx.stock.get_market_sector_classifications 결과는 ['종목명', '업종명', '종가', '대비', '등락률', '시가총액'] 컬럼을 가짐
        df_merged = pd.concat([df_fundamental, df_sector[['업종명']]], axis=1)
        
        # PBR, PER이 0보다 큰 데이터만 필터링
        df_valid = df_merged[(df_merged['PBR'] > 0) & (df_merged['PER'] > 0)]
        
        # 업종별 평균 계산
        industry_avg = df_valid.groupby('업종명')[['PBR', 'PER']].mean()
        industry_avg_dict = industry_avg.to_dict('index')

        # 4. 종목별 상세 데이터 수집 중...
        current_year = datetime.now().year - 1
        results = []
        print(f"\n[3단계] 종목별 상세 데이터 수집 중...")

        # 52주 데이터 계산을 위한 날짜 설정
        end_date = latest_date
        start_date = (datetime.strptime(latest_date, "%Y%m%d") - timedelta(days=365)).strftime("%Y%m%d")

        # 진행 상황 표시
        total = len(df_top100)
        for idx, ticker in enumerate(df_top100.index, 1):
            name = stock.get_market_ticker_name(ticker)

            # 진행률 표시 (같은 줄에 덮어쓰기)
            print(f"\r진행률: [{idx}/{total}] {idx*100//total}% 완료", end='', flush=True)

            # (1) pykrx 데이터 (최우선)
            pbr = df_fundamental.loc[ticker, 'PBR'] if ticker in df_fundamental.index else 0.0
            per = df_fundamental.loc[ticker, 'PER'] if ticker in df_fundamental.index else 0.0
            eps = df_fundamental.loc[ticker, 'EPS'] if ticker in df_fundamental.index else 0.0
            bps = df_fundamental.loc[ticker, 'BPS'] if ticker in df_fundamental.index else 0.0
            div_yield = df_fundamental.loc[ticker, 'DIV'] if ticker in df_fundamental.index else 0.0

            # ROE 계산 (ROE = EPS / BPS * 100)
            roe = (eps / bps * 100) if bps > 0 else 0.0

            # (2) 52주 최고가/최저가 수집
            high_52w = 0
            low_52w = 0
            try:
                df_ohlcv = stock.get_market_ohlcv_by_date(start_date, end_date, ticker)
                if not df_ohlcv.empty:
                    high_52w = int(df_ohlcv['고가'].max())
                    low_52w = int(df_ohlcv['저가'].min())
            except:
                pass

            # (3) pykrx에서 업종 정보
            sector = get_sector_from_pykrx(ticker, df_sector)
            
            # 업종 평균 데이터 가져오기
            avg_pbr = 0.0
            avg_per = 0.0
            if sector in industry_avg_dict:
                avg_pbr = industry_avg_dict[sector]['PBR']
                avg_per = industry_avg_dict[sector]['PER']

            # (4) DART API 데이터 (매출액, 영업이익, 이익잉여금, 현금, 부채, 자본, 현금흐름, CapEx, D/A)
            revenue, op, re, cash, liabilities, equity, ocf, capex, da = get_dart_financials(dart, ticker, current_year)

            # (5) 추가 지표 계산
            debt_ratio = (liabilities / equity * 100) if equity > 0 else 0.0
            fcf = ocf - capex
            ebitda = op + da

            # 데이터 저장
            results.append({
                '종목코드': ticker,
                '종목명': name,
                '업종': sector,
                'PBR': round(pbr, 2),
                '업종평균PBR': round(avg_pbr, 2),
                'PER': round(per, 2),
                '업종평균PER': round(avg_per, 2),
                'ROE': round(roe, 2),
                'EPS': int(eps),
                'BPS': int(bps),
                '배당수익률': round(div_yield, 2),
                '매출액': int(revenue),
                '영업이익': int(op),
                '이익잉여금': int(re),
                '현금및현금성자산': int(cash),
                '52주최고가': int(high_52w),
                '52주최저가': int(low_52w),
                '부채비율': round(debt_ratio, 2),
                'FCF': int(fcf),
                'EBITDA': int(ebitda)
            })

            time.sleep(0.05)  # API 부하 방지

        print()  # 진행률 표시 후 줄바꿈
        df_result = pd.DataFrame(results)

        # 선택된 필드만 필터링
        if selected_fields:
            # 종목코드와 종목명은 항상 포함
            required_fields = ['종목코드', '종목명']
            fields_to_include = required_fields + [f for f in selected_fields if f not in required_fields]
            
            # PBR이나 PER이 선택된 경우 업종 평균도 포함
            if 'PBR' in fields_to_include and '업종평균PBR' not in fields_to_include:
                idx = fields_to_include.index('PBR')
                fields_to_include.insert(idx + 1, '업종평균PBR')
            if 'PER' in fields_to_include and '업종평균PER' not in fields_to_include:
                idx = fields_to_include.index('PER')
                fields_to_include.insert(idx + 1, '업종평균PER')
                
            # DataFrame에 실제 존재하는 컬럼만 선택
            fields_to_include = [f for f in fields_to_include if f in df_result.columns]
            df_result = df_result[fields_to_include]

        # 4. 엑셀 저장 먼저 수행
        output_file = "result.xlsx"
        df_result.to_excel(output_file, index=False, engine='openpyxl')

        # 엑셀 포맷팅
        wb = load_workbook(output_file)
        ws = wb.active
        ws.auto_filter.ref = ws.dimensions

        # 컬럼 너비 자동 조정
        for col in ws.columns:
            max_length = 0
            column = col[0].column_letter
            for cell in col:
                try:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))
                except:
                    pass
            ws.column_dimensions[column].width = min(max_length + 2, 50)

        wb.save(output_file)

        # 5. 결과 출력 (인코딩 안전 처리)
        print("\n" + "=" * 80)
        print("[Data Collection Completed]")
        print("=" * 80)
        try:
            print(df_result.head(10).to_string(index=False))
        except:
            pass  # 인코딩 오류 무시

        print(f"\nData saved: {output_file}")
        print(f"Total stocks: {len(df_result)}")

    except Exception as e:
        print(f"\nError occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='KOSPI 상위 종목 데이터 수집')
    parser.add_argument('--count', type=int, default=100,
                       help='수집할 종목 수 (기본값: 100)')
    parser.add_argument('--fields', type=str, default=None,
                       help='수집할 필드 (쉼표로 구분, 예: PBR,PER,ROE)')
    args = parser.parse_args()

    # 필드를 리스트로 변환
    selected_fields = args.fields.split(',') if args.fields else None

    main(stock_count=args.count, selected_fields=selected_fields)
