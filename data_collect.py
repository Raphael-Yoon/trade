# -*- coding: utf-8 -*-
import sys
import os
import time
import requests
import json
import re
import argparse
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import OpenDartReader
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
import warnings
from concurrent.futures import ThreadPoolExecutor
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

warnings.filterwarnings('ignore')

# Windows 콘솔 UTF-8 설정
if os.name == 'nt':
    os.system('chcp 65001 > nul')
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# API 키 설정
API_KEY = '08e04530eea4ba322907021334794e4164002525'

# 캐시 디렉토리 설정
CACHE_DIR = os.path.join(os.path.dirname(__file__), 'docs_cache')
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

def get_cached_data(ticker, year):
    """로컬 캐시에서 재무 데이터를 가져옵니다."""
    cache_path = os.path.join(CACHE_DIR, f"{ticker}_{year}.json")
    if os.path.exists(cache_path):
        # 파일 수정 시간이 오늘인 경우에만 캐시 사용 (데이터 최신성 유지)
        mtime = datetime.fromtimestamp(os.path.getmtime(cache_path))
        if mtime.date() == datetime.now().date():
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except: pass
    return None

def save_cache_data(ticker, year, data):
    """재무 데이터를 로컬 캐시에 저장합니다."""
    cache_path = os.path.join(CACHE_DIR, f"{ticker}_{year}.json")
    try:
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except: pass

def get_top_tickers_from_naver(session, market='KOSPI', count=100):
    """네이버 금융에서 시가총액 상위 종목 리스트를 가져옵니다."""
    markets_to_fetch = ['KOSPI', 'KOSDAQ'] if market.upper() == 'ALL' else [market.upper()]
    all_tickers = []
    
    for m in markets_to_fetch:
        sosok = 0 if m == 'KOSPI' else 1
        base_url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}"
        page = 1
        market_tickers = []
        
        # ALL인 경우 각각 count만큼 시도
        target_count = count
        
        while len(market_tickers) < target_count:
            url = f"{base_url}&page={page}"
            res = session.get(url)
            soup = BeautifulSoup(res.text, 'html.parser')
            table = soup.find('table', {'class': 'type_2'})
            if not table: break
            
            found = False
            for a in table.find_all('a', {'class': 'tltle'}):
                code = a.get('href').split('code=')[1]
                name = a.text.strip()
                market_tickers.append((code, name))
                found = True
                if len(market_tickers) >= target_count: break
            
            if not found: break
            page += 1
            time.sleep(0.05)
        all_tickers.extend(market_tickers)
    
    return all_tickers[:count] if count > 0 else all_tickers

def get_naver_financials(session, ticker):
    """네이버 금융에서 상세 데이터를 크롤링합니다."""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        res = session.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        market_cap = 0
        price = 0
        try:
            market_cap_area = soup.select_one('#_market_sum')
            if market_cap_area:
                market_cap = int(market_cap_area.text.strip().replace(',', '').replace('조', '0000').replace('억', ''))
            
            price_area = soup.select_one('.no_today .no_up .blind, .no_today .no_down .blind, .no_today .no_steady .blind')
            if price_area:
                price = int(price_area.text.strip().replace(',', ''))
        except: pass

        sector = 'N/A'
        try:
            h4_sector = soup.select_one('.section.trade_compare h4 em a')
            if h4_sector:
                sector = h4_sector.text.strip()
        except: pass

        high_52w = 0
        low_52w = 0
        try:
            tab_con1 = soup.select_one('#tab_con1')
            if tab_con1:
                for tr in tab_con1.select('tr'):
                    if '52주최고' in tr.text:
                        tds = tr.select('td')
                        if len(tds) >= 1:
                            parts = tds[0].text.split('l')
                            if len(parts) == 2:
                                high_52w = int(parts[0].strip().replace(',', ''))
                                low_52w = int(parts[1].strip().replace(',', ''))
        except: pass

        per = 0.0
        pbr = 0.0
        eps = 0
        bps = 0
        div_yield = 0.0
        avg_per = 0.0
        avg_pbr = 0.0
        
        aside = soup.find('div', {'class': 'aside_invest_info'})
        if aside:
            for th in aside.find_all('th'):
                th_text = th.text.strip()
                td = th.find_next('td')
                if not td: continue
                val_text = td.text.strip()
                nums = re.findall(r'[-+]?\d*\.\d+|\d+', val_text.replace(',', ''))
                
                if 'PER' in th_text and '동일업종' not in th_text:
                    if len(nums) >= 1: per = float(nums[0])
                if 'EPS' in th_text:
                    if len(nums) >= 2: eps = int(nums[1])
                    elif len(nums) == 1 and 'PER' not in th_text: eps = int(nums[0])
                if 'PBR' in th_text:
                    if len(nums) >= 1: pbr = float(nums[0])
                if 'BPS' in th_text:
                    if len(nums) >= 2: bps = int(nums[1])
                    elif len(nums) == 1 and 'PBR' not in th_text: bps = int(nums[0])
                if '배당수익률' in th_text:
                    if nums: div_yield = float(nums[0])
                if '동일업종 PER' in th_text:
                    if nums: avg_per = float(nums[0])
                if '동일업종 PBR' in th_text:
                    if nums: avg_pbr = float(nums[0])
                elif '업종 PBR' in th_text:
                    if nums: avg_pbr = float(nums[0])

        target_price_val = 0
        next_op = 0
        debt_ratio = 0.0
        op_margin = 0.0
        net_margin = 0.0
        
        try:
            for th in soup.find_all('th'):
                if '목표주가' in th.text:
                    td = th.find_next_sibling('td')
                    if td:
                        ems = td.find_all('em')
                        for em in ems:
                            val_str = em.text.replace(',', '').strip()
                            if val_str.isdigit() and int(val_str) > 100:
                                target_price_val = int(val_str)
                                break
                    if target_price_val > 0: break
        except: pass

        table = soup.select_one('.cop_analysis')
        if table:
            years = [th.text.strip() for th in table.select('thead tr:nth-of-type(2) th')]
            rows = table.select('tbody tr')
            for row in rows:
                th = row.select_one('th')
                if not th: continue
                th_text = th.text.strip()
                if '영업이익' in th_text and '률' not in th_text:
                    cols = row.select('td')
                    for i, y in enumerate(years):
                        if '(E)' in y or 'E' in y:
                            val_str = cols[i].text.strip().replace(',', '')
                            if val_str and val_str != '-':
                                try:
                                    next_op = int(val_str)
                                    break
                                except: pass
                elif '부채비율' in th_text:
                    cols = row.select('td')
                    for i in range(len(cols)-1, -1, -1):
                        val_str = cols[i].text.strip().replace(',', '')
                        if val_str and val_str != '-' and val_str != '':
                            try:
                                debt_ratio = float(val_str)
                                break
                            except: pass
                elif '영업이익률' in th_text:
                    cols = row.select('td')
                    # 최근 실적(마지막에서 두번째 또는 세번째) 가져오기
                    for i in range(len(cols)-1, -1, -1):
                        if '(E)' not in years[i]:
                            val_str = cols[i].text.strip().replace(',', '')
                            if val_str and val_str != '-':
                                try:
                                    op_margin = float(val_str)
                                    break
                                except: pass
                elif '순이익률' in th_text:
                    cols = row.select('td')
                    for i in range(len(cols)-1, -1, -1):
                        if '(E)' not in years[i]:
                            val_str = cols[i].text.strip().replace(',', '')
                            if val_str and val_str != '-':
                                try:
                                    net_margin = float(val_str)
                                    break
                                except: pass

        return {
            'price': price,
            'market_cap': market_cap,
            'sector': sector,
            'high_52w': high_52w,
            'low_52w': low_52w,
            'per': per,
            'pbr': pbr,
            'eps': eps,
            'bps': bps,
            'div_yield': div_yield,
            'avg_per': avg_per,
            'avg_pbr': avg_pbr,
            'target_price': target_price_val,
            'next_op': next_op,
            'debt_ratio': debt_ratio,
            'op_margin': op_margin,
            'net_margin': net_margin
        }
    except Exception as e:
        print(f"[Naver] {ticker} 데이터 크롤링 실패: {e}")
        return None

def get_naver_investor_data(session, ticker):
    """네이버 금융에서 외국인/기관 순매수 데이터를 크롤링합니다."""
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
        res = session.get(url)
        soup = BeautifulSoup(res.content.decode('euc-kr', 'replace'), 'html.parser')
        
        tables = soup.find_all('table', {'class': 'type2'})
        table = None
        for t in tables:
            if len(t.select('tr[onmouseover]')) > 10:
                table = t
                break
        
        if not table: return 0, 0, 0.0
        
        rows = table.select('tr[onmouseover]')[:20]
        net_buy_foreign = 0
        net_buy_inst = 0
        foreign_ratio = 0.0
        
        # 첫 번째 행에서 외국인 보유비율 가져오기
        try:
            first_row_cols = rows[0].select('td')
            if len(first_row_cols) >= 9:
                fr_text = first_row_cols[8].text.strip().replace('%', '')
                if fr_text: foreign_ratio = float(fr_text)
        except: pass

        for row in rows:
            cols = row.select('td')
            if len(cols) < 9: continue
            try:
                i_nums = re.findall(r'[-+]?\d+', cols[5].text.replace(',', ''))
                f_nums = re.findall(r'[-+]?\d+', cols[6].text.replace(',', ''))
                if i_nums: net_buy_inst += int(i_nums[0])
                if f_nums: net_buy_foreign += int(f_nums[0])
            except: pass
            
        return int(net_buy_foreign), int(net_buy_inst), foreign_ratio
    except:
        return 0, 0, 0.0

def get_dart_financials(dart, ticker, year):
    """OpenDARTReader를 사용하여 가장 최신의 재무 데이터를 추출합니다."""
    # 시도할 보고서 코드: 사업보고서(11011), 3분기(11014), 반기(11012), 1분기(11013)
    # 최신성 순서로 시도
    report_codes = [
        ('11011', '사업보고서'),
        ('11014', '3분기보고서'),
        ('11012', '반기보고서'),
        ('11013', '1분기보고서')
    ]
    
    # 올해(year)와 작년(year-1) 데이터를 순차적으로 탐색
    for target_year in [year, year - 1]:
        for code, code_nm in report_codes:
            try:
                df = dart.finstate_all(ticker, target_year, code)
                if df is not None and not df.empty:
                    # 데이터가 유효한지 확인 (매출액 등이 있는지)
                    if any(df['account_nm'].str.contains('매출액|영업수익', na=False)):
                        report_nm = f"{target_year}년 {code_nm}"
                        return parse_finstate_df(df, report_nm, ticker)
            except:
                continue
    
    return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "N/A", 0, 0, 0, 0, 0, 0


def parse_finstate_df(df, report_nm, ticker):
    """추출된 DataFrame에서 실시간 수치와 전년 동기 수치를 함께 파싱합니다."""
    try:
        revenue = op = re_val = cash = liabilities = equity = ocf = capex = da = net_income = current_assets = current_liabilities = 0
        prev_revenue = prev_op = prev_net_income = 0
        prev2_revenue = prev2_op = prev2_net_income = 0
        
        # 계정 ID 매핑
        mapping = {
            'revenue': ['ifrs-full_Revenue', 'ifrs-full_RevenueFromContractWithCustomers', 'ifrs_Revenue'],
            'op': ['dart_OperatingIncomeLoss'],
            're': ['ifrs-full_RetainedEarnings'],
            'cash': ['ifrs-full_CashAndCashEquivalents', 'ifrs_CashAndCashEquivalents'],
            'liabilities': ['ifrs-full_Liabilities', 'ifrs_Liabilities'],
            'equity': ['ifrs-full_Equity', 'ifrs_Equity', 'ifrs-full_EquityAttributableToOwnersOfParent'],
            'ocf': ['ifrs-full_CashFlowsFromUsedInOperatingActivities', 'ifrs_CashFlowsFromUsedInOperatingActivities'],
            'capex': ['ifrs-full_PurchaseOfPropertyPlantAndEquipment', 'ifrs-full_PurchaseOfIntangibleAssets'],
            'depreciation': ['ifrs-full_DepreciationAndAmortisationExpense', 'ifrs-full_DepreciationExpense', 'ifrs-full_AmortisationExpense'],
            'net_income': ['ifrs-full_ProfitLoss', 'ifrs_ProfitLoss', 'ifrs-full_ProfitLossAttributableToOwnersOfParent'],
            'current_assets': ['ifrs-full_CurrentAssets', 'ifrs_CurrentAssets'],
            'current_liabilities': ['ifrs-full_CurrentLiabilities', 'ifrs_CurrentLiabilities']
        }

        for _, row in df.iterrows():
            acc_id = str(row.get('account_id', ''))
            acc_name = str(row['account_nm']).replace(" ", "")
            sj_div = str(row.get('sj_div', ''))
            # 당기 금액 및 전기 금액 추출
            val = pd.to_numeric(row.get('thstrm_amount'), errors='coerce')
            
            # 전년 동기/전년 금액 추출 (분기보고서의 경우 frmtrm_q_amount 또는 frmtrm_amount 확인)
            prev_val = pd.to_numeric(row.get('frmtrm_amount'), errors='coerce')
            if pd.isna(prev_val) or prev_val == 0:
                # 분기/반기 보고서의 경우 전년 동기 수치가 다른 컬럼에 있을 수 있음
                prev_val = pd.to_numeric(row.get('frmtrm_q_amount'), errors='coerce')
            if pd.isna(prev_val) or prev_val == 0:
                prev_val = pd.to_numeric(row.get('frmtrm_add_amount'), errors='coerce')
                
            # 전전년 동기/전전년 금액 추출 (사업보고서 등에 주로 존재, bfefrmtrm_amount)
            prev2_val = pd.to_numeric(row.get('bfefrmtrm_amount'), errors='coerce')
            if pd.isna(prev2_val): prev2_val = 0
                
            if pd.isna(val): val = 0
            if pd.isna(prev_val): prev_val = 0
            # prev2_val은 없을 수도 있으므로 (분기보고서 등) 0으로 유지됨

            # 1. Revenue
            if acc_id in mapping['revenue'] or acc_name in ['매출액', '수익(매출액)', '영업수익']:
                if revenue == 0 or acc_id in mapping['revenue']: 
                    revenue = val
                    prev_revenue = prev_val
                    prev2_revenue = prev2_val
            
            # 2. Operating Income
            elif acc_id in mapping['op'] or acc_name in ['영업이익', '영업이익(손실)']:
                if op == 0 or acc_id in mapping['op']: 
                    op = val
                    prev_op = prev_val
                    prev2_op = prev2_val
            
            # 3. Retained Earnings
            elif sj_div == 'BS' and (acc_id in mapping['re'] or ('이익잉여금' in acc_name and '기타' not in acc_name)):
                if re_val == 0 or acc_id in mapping['re']: re_val = val
            
            # 4. Cash
            elif sj_div == 'BS' and (acc_id in mapping['cash'] or '현금및현금성자산' in acc_name):
                if cash == 0 or acc_id in mapping['cash']: cash = val
            
            # 5. Liabilities
            elif sj_div == 'BS' and (acc_id in mapping['liabilities'] or acc_name == '부채총계'):
                if liabilities == 0 or acc_id in mapping['liabilities']: liabilities = val
            
            # 6. Equity
            elif sj_div == 'BS' and (acc_id in mapping['equity'] or acc_name == '자본총계'):
                if equity == 0 or acc_id in mapping['equity']: equity = val
            
            # 7. OCF
            elif sj_div == 'CF' and (acc_id in mapping['ocf'] or acc_name == '영업활동현금흐름'):
                ocf = val
            
            # 8. CAPEX
            elif sj_div == 'CF' and (any(tag in acc_id for tag in mapping['capex']) or acc_name in ['유형자산의취득', '무형자산의취득']):
                capex += val
            
            # 9. D&A (EBITDA 계산용)
            elif any(tag in acc_id for tag in mapping['depreciation']) or '감가상각' in acc_name:
                da += val
 
            # 10. Net Income
            elif acc_id in mapping['net_income'] or acc_name in ['당기순이익', '당기순이익(손실)']:
                if net_income == 0 or acc_id in mapping['net_income']: 
                    net_income = val
                    prev_net_income = prev_val
                    prev2_net_income = prev2_val

            # 11. Current Assets
            elif sj_div == 'BS' and (acc_id in mapping['current_assets'] or acc_name == '유동자산'):
                if current_assets == 0 or acc_id in mapping['current_assets']: current_assets = val

            # 12. Current Liabilities
            elif sj_div == 'BS' and (acc_id in mapping['current_liabilities'] or acc_name == '유동부채'):
                if current_liabilities == 0 or acc_id in mapping['current_liabilities']: current_liabilities = val

        return revenue, op, re_val, cash, liabilities, equity, ocf, capex, da, net_income, current_assets, current_liabilities, report_nm, prev_revenue, prev_op, prev_net_income, prev2_revenue, prev2_op, prev2_net_income
    except Exception as e:
        print(f"[DART] {ticker} 재무제표 조회 실패: {e}")
        return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "N/A", 0, 0, 0, 0, 0, 0


def get_audit_opinions(session, corp_code, year, api_key):
    """DART API를 사용하여 회계감사 의견 및 내부통제 의견을 가져옵니다."""
    audit_opinion = 'N/A'
    internal_control = 'N/A'
    
    # 최근 2개년도 시도 (2024년 데이터가 없을 경우 2023년 시도)
    years_to_try = [str(year), str(int(year)-1)]
    
    for y in years_to_try:
        try:
            # 1. 회계감사인의 명칭 및 감사의견 API (가장 기본)
            url = f"https://opendart.fss.or.kr/api/accnutAdtorNmNdAdtOpinion.json?crtfc_key={api_key}&corp_code={corp_code}&bsns_year={y}&reprt_code=11011"
            res = session.get(url, timeout=5).json()
            
            if res.get('status') == '000' and 'list' in res and len(res['list']) > 0:
                # DART 응답 리스트 중 의견이 실제 기재된 항목 찾기 (첫 번째 항목이 '-'인 경우 대비)
                best_item = None
                for item in res['list']:
                    op = item.get('adt_opinion')
                    if op and op != '-' and op != 'None' and op.strip() != '':
                        best_item = item
                        break
                
                if not best_item:
                    continue # 의견이 있는 항목이 없으면 이전 연도 시도
                
                audit_opinion = best_item.get('adt_opinion', 'N/A')
                emphs_raw = (best_item.get('emphs_matter', '') or '') + (best_item.get('adt_reprt_spcmnt_matter', '') or '')
                
                # 내부회계관리제도 의견 판별
                if '내부회계' in emphs_raw:
                    if '적정' in emphs_raw: internal_control = '적정'
                    elif any(word in emphs_raw for word in ['비적정', '취약', '부적정', '부적합']): 
                        internal_control = '부적정(취약)'
                    else:
                        internal_control = '적정'
                elif audit_opinion and '적정' in audit_opinion:
                    internal_control = '적정'
                
            if audit_opinion != 'N/A':
                return audit_opinion, internal_control, f"{y}년 사업보고서"
        except Exception as e:
            print(f"[DART] {corp_code} ({y}) 감사의견 조회 중 오류: {e}")
            continue
            
    return audit_opinion, internal_control, "N/A"

def main(stock_count=100, selected_fields=None, market='KOSPI', output_path=None, tickers=None):
    try:
        if tickers:
            print("=" * 80)
            print(f"지정된 {len(tickers)}개 종목 데이터 수집 시작")
            print("=" * 80)
        else:
            print("=" * 80)
            print(f"{market} 데이터 수집 시작 (상위 {stock_count if stock_count > 0 else '전체'}개)")
            print("=" * 80)

        # 세션 초기화 및 재시도 전략 설정
        session = requests.Session()
        retry_strategy = Retry(
            total=3,  # 최대 재시도 횟수
            backoff_factor=1,  # 재시도 간격 (1초, 2초, 4초...)
            status_forcelist=[429, 500, 502, 503, 504],  # 재시도할 HTTP 상태 코드
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})

        # 모든 요청에 기본 타임아웃 적용을 위한 래퍼 (선택 사항이지만 안전함)
        original_get = session.get
        def timeout_get(*args, **kwargs):
            if 'timeout' not in kwargs:
                kwargs['timeout'] = 10
            return original_get(*args, **kwargs)
        session.get = timeout_get

        dart = OpenDartReader(API_KEY)
        
        if tickers:
            # 지정된 티커 리스트가 있는 경우 (종목명은 네이버에서 가져옴)
            tickers_with_names = []
            for t in tickers:
                try:
                    res = session.get(f"https://finance.naver.com/item/main.naver?code={t}")
                    soup = BeautifulSoup(res.text, 'html.parser')
                    name_area = soup.select_one('.wrap_company h2 a')
                    name = name_area.text.strip() if name_area else t
                    tickers_with_names.append((t, name))
                except:
                    tickers_with_names.append((t, t))
        else:
            tickers_with_names = get_top_tickers_from_naver(session, market, stock_count if stock_count > 0 else 3000)
        
        now = datetime.now()
        # 단순히 2년을 빼는 게 아니라, 직전 연도를 기준으로 잡고 내부 로직에서 최신 보고서를 탐색하도록 변경
        current_year = now.year - 1 
            
        results = []
        total = len(tickers_with_names)
        processed_count = 0
        lock = threading.Lock()

        def process_stock(ticker_info):
            nonlocal processed_count
            
            # ticker_info가 'code:price:qty' 형식인 경우 파싱
            ticker_meta = str(ticker_info[0]).split(':')
            ticker = ticker_meta[0]
            name = ticker_info[1]
            
            purchase_price = 0
            quantity = 0
            if len(ticker_meta) >= 2:
                try: purchase_price = float(ticker_meta[1])
                except: pass
            if len(ticker_meta) >= 3:
                try: quantity = int(ticker_meta[2])
                except: pass
            
            try:
                naver_data = get_naver_financials(session, ticker)
                if not naver_data:
                    with lock:
                        processed_count += 1
                    return None

                net_buy_foreign_vol, net_buy_inst_vol, foreign_ratio = get_naver_investor_data(session, ticker)
                price = naver_data.get('price', 0)
                net_buy_foreign = net_buy_foreign_vol * price
                net_buy_inst = net_buy_inst_vol * price

                # DART 데이터 캐시 확인
                cached = get_cached_data(ticker, current_year)
                # 캐시가 있고, 리포트명이 정상이며, 전년 데이터가 포함되어 있는지 확인
                if cached and cached.get('report_nm') != "N/A" and 'prev_rev' in cached:
                    revenue, op, re_val, cash, liabilities, equity, ocf, capex, da, net_income, cur_assets, cur_liab, report_nm, prev_rev, prev_op, prev_ni, prev2_rev, prev2_op, prev2_ni = (
                        cached['revenue'], cached['op'], cached['re_val'], cached['cash'],
                        cached['liabilities'], cached['equity'], cached['ocf'], cached['capex'], cached['da'],
                        cached.get('net_income', 0), cached.get('cur_assets', 0), cached.get('cur_liab', 0),
                        cached.get('report_nm', f"{current_year}년 사업보고서"),
                        cached.get('prev_rev', 0), cached.get('prev_op', 0), cached.get('prev_ni', 0),
                        cached.get('prev2_rev', 0), cached.get('prev2_op', 0), cached.get('prev2_ni', 0)
                    )
                else:
                    # 캐시가 없거나 전년 데이터가 없는 구버전 캐시라면 새로 수집
                    revenue, op, re_val, cash, liabilities, equity, ocf, capex, da, net_income, cur_assets, cur_liab, report_nm, prev_rev, prev_op, prev_ni, prev2_rev, prev2_op, prev2_ni = get_dart_financials(dart, ticker, current_year)
                    # 캐시 저장
                    save_cache_data(ticker, current_year, {
                        'revenue': revenue, 'op': op, 're_val': re_val, 'cash': cash,
                        'liabilities': liabilities, 'equity': equity, 'ocf': ocf, 'capex': capex, 'da': da,
                        'net_income': net_income, 'cur_assets': cur_assets, 'cur_liab': cur_liab,
                        'report_nm': report_nm, 
                        'prev_rev': prev_rev, 'prev_op': prev_op, 'prev_ni': prev_ni,
                        'prev2_rev': prev2_rev, 'prev2_op': prev2_op, 'prev2_ni': prev2_ni
                    })
                
                # 감사 의견 가져오기 (고유번호 필요)
                corp_code = dart.find_corp_code(ticker)
                if not corp_code: corp_code = ticker
                audit_op, internal_op, audit_report_nm = get_audit_opinions(session, corp_code, current_year, API_KEY)

                # 데이터 기준 정보 (재무제표 보고서 우선, 없으면 감사의견 보고서)
                data_basis = report_nm if report_nm != "N/A" else audit_report_nm

                fcf = ocf - capex
                ebitda = op + da
                current_ratio = round((cur_assets / cur_liab) * 100, 2) if cur_liab > 0 else 0.0
                
                # ROE 계산 개선: 자본총계(equity) 기준 우선, 없으면 네이버 데이터 활용
                roe = 0.0
                if equity > 0 and op > 0:
                    # 단순 영업이익/자본총계 (DART 기준)
                    roe = round((op / equity) * 100, 2)
                elif naver_data.get('per', 0) > 0:
                    # 네이버 ROE 활용 (eps/bps)
                    if naver_data.get('bps', 0) > 0:
                        roe = round((naver_data.get('eps', 0) / naver_data.get('bps', 0)) * 100, 2)

                # 성장성 지표 계산
                # 1. 당기 성장률 (YoY): (당기 - 전년) / 전년
                rev_growth = round(((revenue - prev_rev) / abs(prev_rev) * 100), 2) if prev_rev != 0 else 0.0
                op_growth = round(((op - prev_op) / abs(prev_op) * 100), 2) if prev_op != 0 else 0.0
                ni_growth = round(((net_income - prev_ni) / abs(prev_ni) * 100), 2) if prev_ni != 0 else 0.0

                # 2. 전년 성장률 (Prev YoY): (전년 - 전전년) / 전전년
                # 성장 추세(가속/둔화)를 판단하기 위함
                prev2_rev = cached.get('prev2_rev', 0) if cached else 0
                prev2_op = cached.get('prev2_op', 0) if cached else 0
                prev2_ni = cached.get('prev2_ni', 0) if cached else 0

                prev_rev_growth = round(((prev_rev - prev2_rev) / abs(prev2_rev) * 100), 2) if prev2_rev != 0 else 0.0
                prev_op_growth = round(((prev_op - prev2_op) / abs(prev2_op) * 100), 2) if prev2_op != 0 else 0.0
                prev_ni_growth = round(((prev_ni - prev2_ni) / abs(prev2_ni) * 100), 2) if prev2_ni != 0 else 0.0

                res_dict = {
                    '종목코드': ticker,
                    '종목명': name,
                    '데이터기준': data_basis,
                    '회계감사의견': audit_op,
                    '내부통제의견': internal_op,
                    '업종': naver_data.get('sector'),
                    'PBR': naver_data.get('pbr'),
                    '업종평균PBR': naver_data.get('avg_pbr'),
                    'PER': naver_data.get('per'),
                    '업종평균PER': naver_data.get('avg_per'),
                    'ROE': roe,
                    'EPS': naver_data.get('eps'),
                    'BPS': naver_data.get('bps'),
                    '배당수익률': naver_data.get('div_yield'),
                    
                    # 매출액 관련
                    '매출액': revenue,
                    '전년동기매출액': prev_rev,
                    '전전년동기매출액': prev2_rev,
                    '매출액증가율(%)': rev_growth,
                    '작년매출액증가율(%)': prev_rev_growth, # 추세 확인용
                    
                    # 영업이익 관련
                    '영업이익': op,
                    '전년동기영업이익': prev_op,
                    '전전년동기영업이익': prev2_op,
                    '영업이익증가율(%)': op_growth,
                    '작년영업이익증가율(%)': prev_op_growth, # 추세 확인용
 
                    # 순이익 관련
                    '당기순이익': net_income,
                    '전년동기순이익': prev_ni,
                    '전전년동기순이익': prev2_ni,
                    '순이익증가율(%)': ni_growth,
                    '작년순이익증가율(%)': prev_ni_growth, # 추세 확인용
 
                    '영업이익률': naver_data.get('op_margin'),
                    '순이익률': naver_data.get('net_margin'),
                    '이익잉여금': re_val,
                    '현금및현금성자산': cash,
                    '52주최고가': naver_data.get('high_52w'),
                    '52주최저가': naver_data.get('low_52w'),
                    '부채비율': naver_data.get('debt_ratio') if naver_data.get('debt_ratio') > 0 else (round(liabilities/equity*100, 2) if equity > 0 else 0),
                    '유동비율': current_ratio,
                    'FCF': fcf,
                    'EBITDA': ebitda,
                    '외국인보유율': foreign_ratio,
                    '외국인순매수': net_buy_foreign,
                    '기관순매수': net_buy_inst,
                    '내년예상영업이익': naver_data.get('next_op'),
                    '목표주가': naver_data.get('target_price')
                }

                # 내 종목 분석인 경우 수익률 계산 추가
                if purchase_price > 0:
                    res_dict['현재가'] = price
                    res_dict['매입단가'] = purchase_price
                    res_dict['보유수량'] = quantity
                    res_dict['평가손익'] = (price - purchase_price) * quantity
                    res_dict['수익률(%)'] = round(((price - purchase_price) / purchase_price) * 100, 2)
                
                with lock:
                    processed_count += 1
                    print(f"진행률: [{processed_count}/{total}] {processed_count*100//total}% 완료 ({name})", flush=True)
                
                return res_dict
            except Exception as e:
                print(f"\n[{name}] 처리 중 오류: {e}")
                with lock:
                    processed_count += 1
                return None

        # ThreadPoolExecutor를 사용하여 병렬 처리 (최대 8개 스레드)
        with ThreadPoolExecutor(max_workers=8) as executor:
            thread_results = list(executor.map(process_stock, tickers_with_names))

        # None 결과 제외
        results = [r for r in thread_results if r is not None]

        df = pd.DataFrame(results)
        if selected_fields:
            # 내 종목 분석인 경우 필수 필드 추가
            if any('현재가' in r for r in results):
                my_fields = ['현재가', '매입단가', '보유수량', '평가손익', '수익률(%)']
                for f in my_fields:
                    if f not in selected_fields:
                        selected_fields.insert(2, f) # 종목명 뒤에 삽입

            # 전년 동기 데이터가 컬럼에 있다면 자동으로 선택 필드에 추가
            yoy_fields = ['전년동기매출액', '매출액증가율(%)', '전년동기영업이익', '영업이익증가율(%)', '전년동기순이익', '순이익증가율(%)']
            for f in yoy_fields:
                if f in df.columns and f not in selected_fields:
                    selected_fields.append(f)
            
            df = df[[f for f in selected_fields if f in df.columns]]

        output_file = output_path if output_path else os.path.join(os.path.dirname(os.path.abspath(__file__)), "result.xlsx")
        df.to_excel(output_file, index=False)
        
        print(f"\n\nData saved: {output_file}")
        print(f"Total stocks: {len(df)}")

    except Exception as e:
        print(f"\n메인 루프 오류 발생: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', type=int, default=100)
    parser.add_argument('--market', type=str, default='KOSPI')
    parser.add_argument('--fields', type=str, default='')
    parser.add_argument('--output', type=str, default='')
    parser.add_argument('--tickers', type=str, default='')
    args = parser.parse_args()
    
    fields = args.fields.split(',') if args.fields else None
    tickers = args.tickers.split(',') if args.tickers else None
    main(args.count, fields, args.market, args.output, tickers)
