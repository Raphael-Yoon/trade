# -*- coding: utf-8 -*-
import sys
import os

# Windows 콘솔 UTF-8 설정
if os.name == 'nt':
    os.system('chcp 65001 > nul')
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

sys.path.append(r'C:\Users\newsi\AppData\Roaming\Python\Python313\site-packages')

# [System Config] 불필요한 프록시 설정 제거 (서버 환경 호환성)
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
    os.environ.pop(key, None)

import OpenDartReader
import pandas as pd
import time
from pykrx import stock
from datetime import datetime, timedelta
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
import requests
from bs4 import BeautifulSoup
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

def get_dart_order_backlog(dart, corp_name, year):
    """dart.report를 사용하여 수주상황(수주잔고)을 가져옵니다."""
    try:
        # 수주상황 리포트 조회
        ds = dart.report(corp_name, "수주상황", year)
        if ds is None or ds.empty:
            return 0
            
        # 수주잔고(수주총계) 관련 컬럼 찾기 (표준화가 덜 되어 있어 여러 키워드 탐색)
        target_col = None
        for col in ds.columns:
            if "잔고" in col or "수주잔고" in col or "잔액" in col:
                target_col = col
                break
        
        if target_col:
            # 수치 변환 (문자열 제거 후 합계)
            total_backlog = 0
            for val in ds[target_col]:
                try:
                    # 쉼표 제거, 괄호 등 제거
                    clean_val = str(val).replace(',', '').replace('-', '0').strip()
                    # 숫자만 추출
                    import re
                    match = re.search(r'(\d+)', clean_val)
                    if match:
                        total_backlog += int(match.group(1))
                except:
                    pass
            return total_backlog
            
        return 0
    except Exception:
        return 0

def get_naver_consensus(ticker):
    """네이버 금융에서 컨센서스(목표주가, 영업이익 전망)를 크롤링합니다."""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 1. 목표주가
        target_price = 0
        try:
            # .rgt > .f_down/up em.no_down/up ... 구조가 복잡하므로 blind 텍스트나 특정 클래스 탐색
            # 상단 투자의견/목표주가 영역
            target_area = soup.select_one('.rgt em.no_up') or soup.select_one('.rgt em.no_down') or soup.select_one('.rgt em.no_flat')
            if target_area:
                 # 더 정확한 셀렉터 시도: class="em_pw" 안의 blind가 아님. 
                 # 네이버 금융 구조: div.rgt > table > ... > tr > td > em.no_up > span.blind
                 # 간단히: .rgt .blind 중 '목표주가' 라벨 옆 값 혹은 id='_target_price_none' 등 확인
                 
                 # 분석 영역 데이터 (종목분석 탭)을 보는 게 나을 수도 있음. 
                 # 하지만 메인 페이지(overview)에 '투자의견/목표주가'가 뜸.
                 pass

            # 메인 페이지 description 영역 내 목표주가 span
            # 예: <em class="no_up"> <span class="blind">85,000</span> </em>
            # 구조가 동적이므로, "투자의견 목표주가" 테이블을 찾는 게 빠름
            
            # wrapper > content > ... > .rwidth
            # 대안: 값들이 있는 wrapper ID
            wrap = soup.select_one('#tab_con1') # 종합정보 탭
            
            # 목표주가 찾기 (텍스트 매칭)
            tp_wrappers = soup.find_all('em')
            for em in tp_wrappers:
                # em 부모가 목표주가 td 인지 확인 등을 할 수 있으나 복잡함.
                pass
                
        except:
            pass

        # 더 쉬운 방법: 기업실적분석 테이블에서 'E' (Estimated) 년도 찾기
        next_op = 0       # 내년 영업이익
        next2_op = 0      # 내후년 영업이익 (요청은 향후 2개년이므로 내년만 먼저)
        target_price_val = 0

        # 목표주가 (컨센서스 영역) - 메인 페이지 우측 '목표주가'
        # <div class="rgt"> ... <em class="no_up">...<span class="blind">90,000</span></em>
        # 정확한 위치: div.invest_opinion > ... 
        # class="blind" 값을 가져옴
        try:
            invest_opinion = soup.select('.invest_opinion .blind')
            # 보통 순서: [투자의견, 목표주가, ...]
            if len(invest_opinion) >= 2:
                tp_str = invest_opinion[1].text.replace(',', '').strip()
                if tp_str.isdigit():
                    target_price_val = int(tp_str)
        except:
            pass

        # 영업이익 전망 (기업실적분석 테이블)
        # 테이블 클래스: .cop_analysis
        table = soup.select_one('.cop_analysis')
        if table:
            # thead에서 날짜 확인 (최근 연간 실적 & 추정)
            years = [th.text.strip() for th in table.select('thead tr:nth-of-type(2) th')]
            # '2025.12(E)' 형태 찾기
            
            # 영업이익 행 찾기
            # th class="" 혹은 텍스트 '영업이익'
            rows = table.select('tbody tr')
            op_row = None
            for row in rows:
                th = row.select_one('th')
                if th and '영업이익' in th.text:
                    op_row = row
                    break
            
            if op_row:
                cols = op_row.select('td')
                # years 리스트와 cols 인덱스 매핑 (years 앞쪽은 과거, 뒤쪽은 미래)
                # (E) 가 붙은 첫번째, 두번째 컬럼 찾기
                
                found_estimates = []
                for i, y in enumerate(years):
                    if '(E)' in y or 'E' in y: # 2024.12(E)
                        val_str = cols[i].text.strip().replace(',', '')
                        if val_str and val_str != '-':
                            try:
                                found_estimates.append(int(val_str))
                            except:
                                pass
                
                if len(found_estimates) > 0:
                    next_op = found_estimates[0]
                # 내후년까지 필요하면 found_estimates[1] 등 사용 가능. 요청은 '향후 2개년'이라 하나,
                # 보통 내년도만 확실히 나와있는 경우가 많음. 여기선 next_op(내년) 위주로.

        return target_price_val, next_op

    except Exception as e:
        return 0, 0

def main(stock_count=100, selected_fields=None, market='KOSPI'):
    try:
        print("=" * 80)
        if stock_count == 0:
            print(f"{market} 전체 종목 데이터 수집 시작")
        else:
            print(f"{market} 상위 {stock_count}개 종목 데이터 수집 시작")
        print("수집 우선순위: pykrx > DART API")
        print("=" * 80)

        dart = OpenDartReader(API_KEY)

        # 1. 최근 영업일 조회
        latest_date = get_latest_business_day()
        print(f"\n최근 영업일: {latest_date}")

        # 2. pykrx로 시가총액 상위 100개 종목 조회
        print("\n[1단계] pykrx로 시가총액 및 기본 데이터 수집 중...")
        df_cap = stock.get_market_cap_by_ticker(latest_date, market=market)
        df_fundamental = stock.get_market_fundamental(latest_date, market=market)
        
        # [Fix] market='ALL'일 때 get_market_sector_classifications에서 KeyError: '종가' 발생하는 문제 우회
        # KOSPI와 KOSDAQ을 각각 조회하여 병합
        if market == 'ALL':
            try:
                df_sector_kospi = stock.get_market_sector_classifications(latest_date, market="KOSPI")
                df_sector_kosdaq = stock.get_market_sector_classifications(latest_date, market="KOSDAQ")
                df_sector = pd.concat([df_sector_kospi, df_sector_kosdaq])
            except Exception as e:
                print(f"업종 정보 병합 실패 (개별 조회 시도): {e}")
                # 실패 시 KOSPI만이라도
                df_sector = stock.get_market_sector_classifications(latest_date, market="KOSPI")
        else:
            df_sector = stock.get_market_sector_classifications(latest_date, market=market)

        # 상위 N개 종목 (0이면 전체)
        if stock_count == 0:
            df_top100 = df_cap.sort_values(by='시가총액', ascending=False)
            print(f"대상 종목 수: 전체 {len(df_top100)}개")
        else:
            df_top100 = df_cap.sort_values(by='시가총액', ascending=False).head(stock_count)
            print(f"대상 종목 수: {len(df_top100)}개")

        # 3. 업종별 평균 PBR, PER 계산 (PBR, PER > 0 인 종목만 대상)
        print("\n[2단계] 업종별 평균 PBR, PER 계산 중...")
        # pykrx.stock.get_market_sector_classifications 결과 병합 (인덱스 기준)
        # 업종 정보가 없는 종목이 있을 수 있으므로 how='left' (fundamental 기준)
        df_merged = df_fundamental.join(df_sector[['업종명']], how='left')
        
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

        # [New] 수급 데이터 (20일 외인/기관 순매수)
        print("\n[2.5단계] 최근 20일 수급 데이터(외인/기관) 일괄 수집 중...")
        supply_start_date = (datetime.strptime(latest_date, "%Y%m%d") - timedelta(days=30)).strftime("%Y%m%d")
        
        # 전체 종목 수급을 한번에 가져오는 것이 효율적 (market=market)
        # pykrx의 get_market_net_purchases_of_equities_by_ticker는 기간 합산을 반환함 ("ALL" or specific market)
        df_supply = stock.get_market_net_purchases_of_equities_by_ticker(supply_start_date, latest_date, "ALL")
        # 컬럼: 종목명, 매도거래량, 매수거래량, 순매수거래량, 매도거래대금, 매수거래대금, 순매수거래대금
        # 여기서 필요한 건 '순매수거래대금' -> 그러나 투자주체별(외인/기관)로 나눠진 API가 아님.
        # 이 API는 투자자별로 쪼개주지 않음. 
        # stock.get_market_net_purchases_of_equities_by_ticker는 '개인/기관/외국인' 구분이 없음 (전체 합계라 의미가 다름).
        # 종목별/투자자별 순매수를 '기간'으로 조회 가능한 API -> stock.get_market_net_purchases_of_equities_by_ticker(from, to, "KOSPI", investor="...") 
        # -> 이게 아님. stock.get_market_net_purchases_of_equities_by_ticker는 파라미터가 start, end, market, investor 임.
        
        # 외국인 순매수 (거래대금 기준)
        df_foreign = stock.get_market_net_purchases_of_equities_by_ticker(supply_start_date, latest_date, "ALL", investor="외국인")
        # 기관 순매수
        df_inst = stock.get_market_net_purchases_of_equities_by_ticker(supply_start_date, latest_date, "ALL", investor="기관합계")

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
            
            # [New] DART 수주잔고
            backlog = get_dart_order_backlog(dart, name, current_year) # 종목명(name) 사용 권장 (dart.report)

            # [New] 네이버 금융 컨센서스
            target_price, next_op = get_naver_consensus(ticker)

            # [New] 수급 데이터 매핑
            net_buy_foreign = 0
            net_buy_inst = 0
            if ticker in df_foreign.index:
                net_buy_foreign = df_foreign.loc[ticker, '순매수거래대금']
            if ticker in df_inst.index:
                net_buy_inst = df_inst.loc[ticker, '순매수거래대금']

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
                'EBITDA': int(ebitda),
                '수주잔고': int(backlog),
                '외국인순매수': int(net_buy_foreign),
                '기관순매수': int(net_buy_inst),
                '내년예상영업이익': int(next_op),
                '목표주가': int(target_price)
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

        # 4. 엑셀 저장 먼저 수행 (절대 경로 사용)
        output_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "result.xlsx")
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
        sys.exit(1)  # 오류 발생 시 비정상 종료 알림

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='국내 주식 데이터 수집 (KOSPI/KOSDAQ)')
    parser.add_argument('--count', type=int, default=100,
                       help='수집할 종목 수 (기본값: 100)')
    parser.add_argument('--fields', type=str, default=None,
                       help='수집할 필드 (쉼표로 구분, 예: PBR,PER,ROE)')
    parser.add_argument('--market', type=str, default='KOSPI',
                       help='대상 시장 (KOSPI 또는 KOSDAQ, 기본값: KOSPI)')
    args = parser.parse_args()

    # 필드를 리스트로 변환
    selected_fields = args.fields.split(',') if args.fields else None

    main(stock_count=args.count, selected_fields=selected_fields, market=args.market)
