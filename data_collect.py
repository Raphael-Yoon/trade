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

warnings.filterwarnings('ignore')

# Windows 콘솔 UTF-8 설정
if os.name == 'nt':
    os.system('chcp 65001 > nul')
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# API 키 설정
API_KEY = '08e04530eea4ba322907021334794e4164002525'

def get_top_tickers_from_naver(market='KOSPI', count=100):
    """네이버 금융에서 시가총액 상위 종목 리스트를 가져옵니다."""
    markets_to_fetch = ['KOSPI', 'KOSDAQ'] if market.upper() == 'ALL' else [market.upper()]
    all_tickers = []
    
    for m in markets_to_fetch:
        sosok = 0 if m == 'KOSPI' else 1
        base_url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        page = 1
        market_tickers = []
        
        # ALL인 경우 각각 count만큼 시도
        target_count = count
        
        while len(market_tickers) < target_count:
            url = f"{base_url}&page={page}"
            res = requests.get(url, headers=headers)
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
            time.sleep(0.1)
        all_tickers.extend(market_tickers)
    
    return all_tickers[:count] if count > 0 else all_tickers

def get_naver_financials(ticker):
    """네이버 금융에서 상세 데이터를 크롤링합니다."""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers)
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

        target_price_val = 0
        next_op = 0
        debt_ratio = 0.0
        
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
                if '영업이익' in th.text:
                    cols = row.select('td')
                    for i, y in enumerate(years):
                        if '(E)' in y or 'E' in y:
                            val_str = cols[i].text.strip().replace(',', '')
                            if val_str and val_str != '-':
                                try:
                                    next_op = int(val_str)
                                    break
                                except: pass
                elif '부채비율' in th.text:
                    cols = row.select('td')
                    for i in range(len(cols)-1, -1, -1):
                        val_str = cols[i].text.strip().replace(',', '')
                        if val_str and val_str != '-' and val_str != '':
                            try:
                                debt_ratio = float(val_str)
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
            'target_price': target_price_val,
            'next_op': next_op,
            'debt_ratio': debt_ratio
        }
    except Exception as e:
        print(f"[Naver] {ticker} 데이터 크롤링 실패: {e}")
        return None

def get_naver_investor_data(ticker):
    """네이버 금융에서 외국인/기관 순매수 데이터를 크롤링합니다."""
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers)
        soup = BeautifulSoup(res.content.decode('euc-kr', 'replace'), 'html.parser')
        
        tables = soup.find_all('table', {'class': 'type2'})
        table = None
        for t in tables:
            if len(t.select('tr[onmouseover]')) > 10:
                table = t
                break
        
        if not table: return 0, 0
        
        rows = table.select('tr[onmouseover]')[:20]
        net_buy_foreign = 0
        net_buy_inst = 0
        
        for row in rows:
            cols = row.select('td')
            if len(cols) < 9: continue
            try:
                i_nums = re.findall(r'[-+]?\d+', cols[5].text.replace(',', ''))
                f_nums = re.findall(r'[-+]?\d+', cols[6].text.replace(',', ''))
                if i_nums: net_buy_inst += int(i_nums[0])
                if f_nums: net_buy_foreign += int(f_nums[0])
            except: pass
            
        return int(net_buy_foreign), int(net_buy_inst)
    except:
        return 0, 0

def get_dart_financials(dart, ticker, year):
    """OpenDARTReader를 사용하여 재무 데이터를 추출합니다."""
    try:
        df = dart.finstate_all(ticker, year)
        if df is None or df.empty: return 0, 0, 0, 0, 0, 0, 0, 0, 0

        revenue = op = re_val = cash = liabilities = equity = ocf = capex = da = 0
        for _, row in df.iterrows():
            acc_id = str(row.get('account_id', ''))
            acc_name = str(row['account_nm']).replace(" ", "")
            sj_div = str(row.get('sj_div', ''))
            val = pd.to_numeric(row['thstrm_amount'], errors='coerce')
            if pd.isna(val): val = 0

            if acc_id in ['ifrs-full_Revenue', 'ifrs-full_RevenueFromContractWithCustomers'] or acc_name in ['매출액', '수익(매출액)']:
                if revenue == 0 or 'Revenue' in acc_id: revenue = val
            elif acc_id == 'dart_OperatingIncomeLoss' or acc_name in ['영업이익', '영업이익(손실)']:
                if op == 0 or acc_id == 'dart_OperatingIncomeLoss': op = val
            elif sj_div == 'BS' and (acc_id == 'ifrs-full_RetainedEarnings' or ('이익잉여금' in acc_name and '기타' not in acc_name)):
                if re_val == 0 or acc_id == 'ifrs-full_RetainedEarnings': re_val = val
            elif sj_div == 'BS' and (acc_id == 'ifrs-full_CashAndCashEquivalents' or '현금및현금성자산' in acc_name):
                if cash == 0 or acc_id == 'ifrs-full_CashAndCashEquivalents': cash = val
            elif sj_div == 'BS' and (acc_id == 'ifrs-full_Liabilities' or acc_name == '부채총계'):
                if liabilities == 0 or acc_id == 'ifrs-full_Liabilities': liabilities = val
            elif sj_div == 'BS' and (acc_id in ['ifrs-full_Equity', 'ifrs-full_EquityAttributableToOwnersOfParent'] or acc_name == '자본총계'):
                if equity == 0 or 'Equity' in acc_id: equity = val
            elif sj_div == 'CF' and (acc_id == 'ifrs-full_CashFlowsFromUsedInOperatingActivities' or acc_name == '영업활동현금흐름'):
                ocf = val
            elif sj_div == 'CF' and ('PurchaseOfPropertyPlantAndEquipment' in acc_id or 'PurchaseOfIntangibleAssets' in acc_id or acc_name in ['유형자산의취득', '무형자산의취득']):
                capex += val
            if 'Depreciation' in acc_id or 'Amortisation' in acc_id or '감가상각' in acc_name:
                if da == 0 or 'Depreciation' in acc_id: da = val

        return revenue, op, re_val, cash, liabilities, equity, ocf, capex, da
    except Exception as e:
        print(f"[DART] {ticker} 재무제표 조회 실패: {e}")
        return 0, 0, 0, 0, 0, 0, 0, 0, 0

def get_audit_opinions(corp_code, year, api_key):
    """DART API를 사용하여 회계감사 의견 및 내부통제 의견을 가져옵니다."""
    audit_opinion = 'N/A'
    internal_control = 'N/A'
    
    # 최근 2개년도 시도 (2024년 데이터가 없을 경우 2023년 시도)
    years_to_try = [str(year), str(int(year)-1)]
    
    for y in years_to_try:
        try:
            # 1. 회계감사인의 명칭 및 감사의견 API (가장 기본)
            url = f"https://opendart.fss.or.kr/api/accnutAdtorNmNdAdtOpinion.json?crtfc_key={api_key}&corp_code={corp_code}&bsns_year={y}&reprt_code=11011"
            res = requests.get(url, timeout=5).json()
            
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
                    break # 성공적으로 찾았으면 루프 종료
        except Exception as e:
            print(f"[DART] {corp_code} ({y}) 감사의견 조회 중 오류: {e}")
            continue
            
    return audit_opinion, internal_control

def main(stock_count=100, selected_fields=None, market='KOSPI'):
    try:
        print("=" * 80)
        print(f"{market} 데이터 수집 시작 (상위 {stock_count if stock_count > 0 else '전체'}개)")
        print("=" * 80)

        dart = OpenDartReader(API_KEY)
        tickers_with_names = get_top_tickers_from_naver(market, stock_count if stock_count > 0 else 3000)
        
        now = datetime.now()
        current_year = now.year - 2 if now.month < 4 else now.year - 1
            
        results = []
        total = len(tickers_with_names)
        for idx, (ticker, name) in enumerate(tickers_with_names, 1):
            print(f"진행률: [{idx}/{total}] {idx*100//total}% 완료 ({name})", flush=True)

            naver_data = get_naver_financials(ticker)
            if not naver_data: continue

            net_buy_foreign_vol, net_buy_inst_vol = get_naver_investor_data(ticker)
            price = naver_data.get('price', 0)
            net_buy_foreign = net_buy_foreign_vol * price
            net_buy_inst = net_buy_inst_vol * price

            revenue, op, re_val, cash, liabilities, equity, ocf, capex, da = get_dart_financials(dart, ticker, current_year)
            
            # 감사 의견 가져오기 (고유번호 필요)
            corp_code = dart.find_corp_code(ticker)
            if not corp_code: corp_code = ticker
            audit_op, internal_op = get_audit_opinions(corp_code, current_year, API_KEY)

            fcf = ocf - capex
            ebitda = op + da
            roe = naver_data.get('per', 0) # 임시
            if naver_data.get('bps', 0) > 0:
                roe = round((naver_data.get('eps', 0) / naver_data.get('bps', 0)) * 100, 2)

            res_dict = {
                '종목코드': ticker,
                '종목명': name,
                '회계감사의견': audit_op,
                '내부통제의견': internal_op,
                '업종': naver_data.get('sector'),
                'PBR': naver_data.get('pbr'),
                '업종평균PBR': 0.0,
                'PER': naver_data.get('per'),
                '업종평균PER': naver_data.get('avg_per'),
                'ROE': roe,
                'EPS': naver_data.get('eps'),
                'BPS': naver_data.get('bps'),
                '배당수익률': naver_data.get('div_yield'),
                '매출액': revenue,
                '영업이익': op,
                '이익잉여금': re_val,
                '현금및현금성자산': cash,
                '52주최고가': naver_data.get('high_52w'),
                '52주최저가': naver_data.get('low_52w'),
                '부채비율': naver_data.get('debt_ratio') if naver_data.get('debt_ratio') > 0 else (round(liabilities/equity*100, 2) if equity > 0 else 0),
                'FCF': fcf,
                'EBITDA': ebitda,
                '외국인순매수': net_buy_foreign,
                '기관순매수': net_buy_inst,
                '내년예상영업이익': naver_data.get('next_op'),
                '목표주가': naver_data.get('target_price')
            }
            results.append(res_dict)

        df = pd.DataFrame(results)
        if selected_fields:
            df = df[[f for f in selected_fields if f in df.columns]]

        output_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "result.xlsx")
        df.to_excel(output_file, index=False)
        
        print(f"\n\nData saved: {output_file}")
        print(f"Total stocks: {len(df)}")

    except Exception as e:
        print(f"\n오류 발생: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', type=int, default=100)
    parser.add_argument('--market', type=str, default='KOSPI')
    parser.add_argument('--fields', type=str, default='')
    args = parser.parse_args()
    
    fields = args.fields.split(',') if args.fields else None
    main(args.count, fields, args.market)
