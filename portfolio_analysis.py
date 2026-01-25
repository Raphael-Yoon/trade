# -*- coding: utf-8 -*-
import sqlite3
import os
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import time

# 설정
DB_FILE = os.path.join(os.path.dirname(__file__), 'trade.db')
RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')

if not os.path.exists(RESULTS_DIR):
    os.makedirs(RESULTS_DIR)

def get_my_stocks():
    """DB에서 내 보유 종목 정보를 가져옵니다."""
    conn = sqlite3.connect(DB_FILE)
    query = "SELECT code, name, purchase_price, quantity FROM my_stocks"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_portfolio_details(ticker):
    """네이버 금융에서 기술적 지표 및 컨센서스 데이터를 수집합니다."""
    url = f"https://finance.naver.com/item/main.naver?code={ticker}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        data = {
            'code': ticker,
            '현재가': 0,
            '전일대비': 0,
            '등락률(%)': 0,
            '시가총액(억)': 0,
            '외인소진율(%)': 0,
            '투자의견': 'N/A',
            '목표주가': 0,
            '52주최고': 0,
            '52주최저': 0,
            'PER': 0,
            'PBR': 0,
            '배당수익률(%)': 0
        }
        
        # 현재가 정보 파싱
        new_totalinfo = soup.find('div', class_='new_totalinfo')
        if new_totalinfo:
            blind = new_totalinfo.find('dl', class_='blind')
            if blind:
                dd_list = blind.find_all('dd')
                # 종가(현재가), 전일대비, 등락률 파싱 (네이버 구조에 따라 인덱스 확인 필요)
                # 보통: 현재가[3], 전일대비[4], 등락률[5]
                data['현재가'] = int(dd_list[3].text.split()[1].replace(',', ''))
                
        # 시가총액, 외인소진율 등 (aside 섹션)
        aside = soup.find('div', class_='aside')
        if aside:
            # 시가총액
            tab_con1 = aside.find('div', id='_market_sum')
            if tab_con1:
                data['시가총액(억)'] = tab_con1.text.strip().replace(',', '').replace('조', '').replace('억원', '')
            
            # 외인소진율
            # 투자의견/목표주가 (있는 경우만)
            cns_table = aside.find('table', class_='rwidth')
            if cns_table:
                trs = cns_table.find_all('tr')
                for tr in trs:
                    if '투자의견' in tr.text:
                        opinion_td = tr.find('span', class_='f_up') or tr.find('em')
                        if opinion_td:
                            data['투자의견'] = opinion_td.text.strip()
                    if '목표주가' in tr.text:
                        target_td = tr.find('em')
                        if target_td:
                            data['목표주가'] = int(target_td.text.strip().replace(',', ''))

        # 52주 최고/최저 및 기타 지표
        tab_section = soup.find('div', class_='tab_con1')
        if tab_section:
            trs = tab_section.find_all('tr')
            for tr in trs:
                if '52주 최고' in tr.text:
                    v = tr.find_all('em')
                    if len(v) >= 2:
                        data['52주최고'] = int(v[0].text.replace(',', ''))
                        data['52주최저'] = int(v[1].text.replace(',', ''))
                if 'PER' in tr.text and '배당' not in tr.text:
                    per_em = tr.find('em', id='_per')
                    if per_em: data['PER'] = float(per_em.text.replace(',', ''))
                if 'PBR' in tr.text:
                    pbr_em = tr.find('em', id='_pbr')
                    if pbr_em: data['PBR'] = float(pbr_em.text.replace(',', ''))
                if '배당수익률' in tr.text:
                    d_em = tr.find('em', id='_dvr')
                    if d_em: data['배당수익률(%)'] = float(d_em.text.replace(',', '').replace('%', ''))

        return data
    except Exception as e:
        print(f"Error collecting data for {ticker}: {e}")
        return None

def main():
    print("포트폴리오 분석 데이터 수집을 시작합니다...")
    
    # 1. 내 종목 로드
    my_stocks_df = get_my_stocks()
    if my_stocks_df.empty:
        print("등록된 종목이 없습니다.")
        return

    results = []
    
    # 2. 각 종목 상세 수집
    for index, row in my_stocks_df.iterrows():
        print(f"[{index+1}/{len(my_stocks_df)}] {row['name']} ({row['code']}) 수집 중...")
        detail = get_portfolio_details(row['code'])
        if detail:
            # DB 정보와 합치기
            detail['종목명'] = row['name']
            detail['평균단가'] = row['purchase_price']
            detail['보유수량'] = row['quantity']
            
            # 수익률 계산
            if detail['현재가'] > 0 and detail['평균단가'] > 0:
                detail['수익률(%)'] = round(((detail['현재가'] / detail['평균단가']) - 1) * 100, 2)
                detail['평가손익'] = int((detail['현재가'] - detail['평균단가']) * detail['보유수량'])
            else:
                detail['수익률(%)'] = 0
                detail['평가손익'] = 0
                
            results.append(detail)
        time.sleep(0.5) # 서버 부하 방지

    # 3. 데이터프레임 생성 및 저장
    if results:
        df = pd.DataFrame(results)
        # 컬럼 순서 정리
        cols = ['code', '종목명', '현재가', '평균단가', '수익률(%)', '평가손익', '보유수량', 
                '시가총액(억)', '투자의견', '목표주가', 'PER', 'PBR', '배당수익률(%)', '52주최고', '52주최저']
        df = df[cols]
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(RESULTS_DIR, f'portfolio_analysis_{timestamp}.xlsx')
        df.to_excel(output_file, index=False)
        
        print(f"\n수집 완료! 결과 파일: {output_file}")
        return output_file
    else:
        print("수집된 데이터가 없습니다.")
        return None

if __name__ == "__main__":
    main()
