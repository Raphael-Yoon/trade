# -*- coding: utf-8 -*-
"""
네이버 금융에서 모든 데이터를 수집하는 완전한 함수
trade.py의 get_portfolio_details를 대체할 강화된 버전
"""
import re
import requests
from bs4 import BeautifulSoup
import sys
import os


def get_all_naver_data(ticker):
    """
    네이버 금융에서 가져올 수 있는 모든 데이터를 수집합니다.

    Returns:
        dict: 50개 이상의 상세 데이터 포함
    """
    main_url = f"https://finance.naver.com/item/main.naver?code={ticker}"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    # 모든 가능한 데이터 필드 초기화
    data = {
        # 기본 정보
        'code': ticker,
        'name': '',
        'current_price': 0,
        'prev_price': 0,
        'open_price': 0,
        'high_price': 0,
        'low_price': 0,
        'upper_limit': 0,
        'lower_limit': 0,
        'volume': 0,
        'trading_value': 0,

        # 시가총액 & 순위
        'market_cap': 'N/A',
        'market_cap_rank': 'N/A',
        'outstanding_shares': 0,

        # 외국인 정보
        'foreign_limit_shares': 0,
        'foreign_owned_shares': 0,
        'foreign_ownership_ratio': 0,
        'foreign_exhaustion_ratio': 0,

        # 투자의견 & 목표가
        'opinion': 'N/A',
        'opinion_score': 0,
        'target_price': 0,

        # 52주 고/저
        'high_52w': 0,
        'low_52w': 0,

        # PER/PBR/EPS/BPS
        'per': 0,
        'eps': 0,
        'estimated_per': 0,
        'estimated_eps': 0,
        'pbr': 0,
        'bps': 0,

        # 업종 비교
        'sector_per': 0,
        'sector_change_rate': 0,

        # 배당
        'dividend_yield': 0,

        # 성장성 지표
        'revenue_growth': 'N/A',
        'profit_growth': 'N/A',
        'revenue': 'N/A',
        'operating_profit': 'N/A',
        'net_profit': 'N/A',

        # 재무 건전성
        'roe': 0,
        'debt_ratio': 0,
        'current_ratio': 0,

        # 수급 (오늘)
        'foreign_net_buy_today': 0,
        'inst_net_buy_today': 0,

        # 기술적 지표
        'rsi': 0,
        'price_position_52w': 0
    }

    try:
        response = requests.get(main_url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')

        # ===================================================================
        # 1. DL/DD 구조에서 기본 시세 정보 추출
        # ===================================================================
        blind_dl = soup.find('dl', class_='blind')
        if blind_dl:
            dds = blind_dl.find_all('dd')
            if len(dds) >= 12:
                try:
                    # DD 1: 종목명
                    name_text = dds[1].get_text()
                    if '종목명' in name_text:
                        data['name'] = name_text.replace('종목명', '').strip()

                    # DD 3: 현재가
                    price_text = dds[3].get_text()
                    price_nums = re.findall(r'[\d,]+', price_text)
                    if price_nums:
                        data['current_price'] = int(price_nums[0].replace(',', ''))

                    # DD 4: 전일가
                    if '전일가' in dds[4].get_text():
                        prev_nums = re.findall(r'[\d,]+', dds[4].get_text())
                        if prev_nums:
                            data['prev_price'] = int(prev_nums[0].replace(',', ''))

                    # DD 5: 시가
                    if '시가' in dds[5].get_text():
                        open_nums = re.findall(r'[\d,]+', dds[5].get_text())
                        if open_nums:
                            data['open_price'] = int(open_nums[0].replace(',', ''))

                    # DD 6: 고가
                    if '고가' in dds[6].get_text():
                        high_nums = re.findall(r'[\d,]+', dds[6].get_text())
                        if high_nums:
                            data['high_price'] = int(high_nums[0].replace(',', ''))

                    # DD 7: 상한가
                    if '상한가' in dds[7].get_text():
                        upper_nums = re.findall(r'[\d,]+', dds[7].get_text())
                        if upper_nums:
                            data['upper_limit'] = int(upper_nums[0].replace(',', ''))

                    # DD 8: 저가
                    if '저가' in dds[8].get_text():
                        low_nums = re.findall(r'[\d,]+', dds[8].get_text())
                        if low_nums:
                            data['low_price'] = int(low_nums[0].replace(',', ''))

                    # DD 9: 하한가
                    if '하한가' in dds[9].get_text():
                        lower_nums = re.findall(r'[\d,]+', dds[9].get_text())
                        if lower_nums:
                            data['lower_limit'] = int(lower_nums[0].replace(',', ''))

                    # DD 10: 거래량
                    if '거래량' in dds[10].get_text():
                        vol_nums = re.findall(r'[\d,]+', dds[10].get_text())
                        if vol_nums:
                            data['volume'] = int(vol_nums[0].replace(',', ''))

                    # DD 11: 거래대금
                    if '거래대금' in dds[11].get_text():
                        val_nums = re.findall(r'[\d,]+', dds[11].get_text())
                        if val_nums:
                            data['trading_value'] = int(val_nums[0].replace(',', ''))
                except Exception as e:
                    print(f"DD 파싱 오류: {e}")

        # ===================================================================
        # 2. 시가총액 테이블 (테이블 6)
        # ===================================================================
        tables = soup.find_all('table')
        for table in tables:
            summary = table.get('summary', '')
            if '시가총액 정보' in summary:
                rows = table.find_all('tr')
                for row in rows:
                    th = row.find('th')
                    td = row.find('td')
                    if not th or not td:
                        continue

                    th_text = th.get_text(strip=True)
                    td_text = td.get_text(strip=True)

                    if '시가총액' in th_text and '순위' not in th_text:
                        data['market_cap'] = td_text
                    elif '시가총액순위' in th_text:
                        data['market_cap_rank'] = td_text
                    elif '상장주식수' in th_text:
                        nums = re.findall(r'[\d,]+', td_text)
                        if nums:
                            data['outstanding_shares'] = int(nums[0].replace(',', ''))
                break

        # ===================================================================
        # 3. 외국인 정보 테이블 (테이블 7)
        # ===================================================================
        for table in tables:
            summary = table.get('summary', '')
            if '외국인한도주식수 정보' in summary:
                rows = table.find_all('tr')
                for row in rows:
                    th = row.find('th')
                    td = row.find('td')
                    if not th or not td:
                        continue

                    th_text = th.get_text(strip=True)
                    td_text = td.get_text(strip=True)

                    if '외국인한도주식수' in th_text and '(' not in th_text:
                        nums = re.findall(r'[\d,]+', td_text)
                        if nums:
                            data['foreign_limit_shares'] = int(nums[0].replace(',', ''))
                    elif '외국인보유주식수' in th_text:
                        nums = re.findall(r'[\d,]+', td_text)
                        if nums:
                            data['foreign_owned_shares'] = int(nums[0].replace(',', ''))
                    elif '외국인소진율' in th_text or '외국인(한도)소진율' in th_text:
                        percent = re.findall(r'[\d.]+', td_text.replace('%', ''))
                        if percent:
                            data['foreign_exhaustion_ratio'] = float(percent[0])

                # 외국인 보유율 계산
                if data['outstanding_shares'] > 0 and data['foreign_owned_shares'] > 0:
                    data['foreign_ownership_ratio'] = round(
                        (data['foreign_owned_shares'] / data['outstanding_shares']) * 100, 2
                    )
                break

        # ===================================================================
        # 4. 투자의견/목표주가 테이블 (테이블 8 - rwidth 클래스)
        # ===================================================================
        for table in tables:
            summary = table.get('summary', '')
            if '투자의견 정보' in summary:
                rows = table.find_all('tr')
                for row in rows:
                    th = row.find('th')
                    td = row.find('td')
                    if not th or not td:
                        continue

                    th_text = th.get_text(strip=True)

                    if '투자의견' in th_text and '목표주가' in th_text:
                        td_text = td.get_text(strip=True)
                        # 형식: "4.00매수l166,385"
                        parts = td_text.split('l')
                        if len(parts) >= 2:
                            # 투자의견
                            opinion_part = parts[0]
                            opinion_match = re.search(r'[가-힣]+', opinion_part)
                            if opinion_match:
                                data['opinion'] = opinion_match.group()

                            # 의견 점수 (숫자 부분)
                            score_match = re.search(r'[\d.]+', opinion_part)
                            if score_match:
                                data['opinion_score'] = float(score_match.group())

                            # 목표주가
                            target_nums = re.findall(r'[\d,]+', parts[1])
                            if target_nums:
                                data['target_price'] = int(target_nums[0].replace(',', ''))

                    elif '52주최고' in th_text or '52주 최고' in th_text:
                        td_text = td.get_text(strip=True)
                        # 형식: "157,000l50,800"
                        nums = re.findall(r'[\d,]+', td_text)
                        if len(nums) >= 2:
                            data['high_52w'] = int(nums[0].replace(',', ''))
                            data['low_52w'] = int(nums[1].replace(',', ''))
                break

        # ===================================================================
        # 5. PER/EPS/PBR/BPS 테이블 (테이블 9 - per_table 클래스)
        # ===================================================================
        for table in tables:
            if 'per_table' in table.get('class', []):
                rows = table.find_all('tr')
                for row in rows:
                    th = row.find('th')
                    td = row.find('td')
                    if not th or not td:
                        continue

                    th_text = th.get_text(strip=True)
                    td_text = td.get_text(strip=True)

                    # PERlEPS
                    if 'PER' in th_text and 'EPS' in th_text and '추정' not in th_text:
                        nums = re.findall(r'[\d,]+(?:\.\d+)?', td_text)
                        if len(nums) >= 2:
                            per_val = nums[0].replace(',', '')
                            if '.' in per_val or per_val.replace('.', '').isdigit():
                                data['per'] = float(per_val)
                            eps_val = nums[1].replace(',', '')
                            if eps_val.isdigit():
                                data['eps'] = int(eps_val)

                    # 추정PERlEPS
                    elif '추정PER' in th_text or '추정 PER' in th_text:
                        nums = re.findall(r'[\d,]+(?:\.\d+)?', td_text)
                        if len(nums) >= 2:
                            est_per_val = nums[0].replace(',', '')
                            if '.' in est_per_val or est_per_val.replace('.', '').isdigit():
                                data['estimated_per'] = float(est_per_val)
                            est_eps_val = nums[1].replace(',', '')
                            if est_eps_val.isdigit():
                                data['estimated_eps'] = int(est_eps_val)

                    # PBRlBPS
                    elif 'PBR' in th_text and 'BPS' in th_text:
                        nums = re.findall(r'[\d,]+(?:\.\d+)?', td_text)
                        if len(nums) >= 2:
                            pbr_val = nums[0].replace(',', '')
                            if '.' in pbr_val or pbr_val.replace('.', '').isdigit():
                                data['pbr'] = float(pbr_val)
                            bps_val = nums[1].replace(',', '')
                            if bps_val.isdigit():
                                data['bps'] = int(bps_val)
                break

        # ===================================================================
        # 6. 동일업종 PER (테이블 10)
        # ===================================================================
        for table in tables:
            summary = table.get('summary', '')
            if '동일업종 PER 정보' in summary:
                rows = table.find_all('tr')
                for row in rows:
                    th = row.find('th')
                    td = row.find('td')
                    if not th or not td:
                        continue

                    th_text = th.get_text(strip=True)
                    td_text = td.get_text(strip=True)

                    if '동일업종 PER' in th_text:
                        nums = re.findall(r'[\d.]+', td_text)
                        if nums:
                            data['sector_per'] = float(nums[0])
                    elif '동일업종 등락률' in th_text:
                        percent = re.findall(r'[+-]?[\d.]+', td_text)
                        if percent:
                            data['sector_change_rate'] = float(percent[0])
                break

        # ===================================================================
        # 7. 재무 지표 테이블 (테이블 4 - 주요재무정보)
        # ===================================================================
        for table in tables:
            summary = table.get('summary', '')
            if '기업실적분석' in summary or '주요재무정보' in summary:
                rows = table.find_all('tr')
                for row in rows:
                    th = row.find('th')
                    tds = row.find_all('td')
                    if not th or not tds:
                        continue

                    th_text = th.get_text(strip=True)

                    def get_last_valid_val(td_list):
                        """뒤에서부터 유효한 값 찾기"""
                        for i in range(len(td_list) - 2, -1, -1):
                            val = td_list[i].get_text(strip=True).replace(',', '')
                            if val and val != '-' and val != 'N/A':
                                return val
                        return None

                    # 성장성 지표 - 직접 계산 (증가율 행이 없으므로)
                    if th_text == '매출액':
                        # 모든 값 수집
                        vals = [td.get_text(strip=True).replace(',', '') for td in tds]
                        # 최근 값
                        current_val = get_last_valid_val(tds)
                        if current_val:
                            data['revenue'] = current_val

                        # 성장률 계산: 최근 2개 유효값 비교
                        valid_vals = [v for v in vals if v and v != '-' and v.replace('.', '').isdigit()]
                        if len(valid_vals) >= 2:
                            try:
                                # 뒤에서 2개: [-1]이 최신, [-2]가 전년
                                current = float(valid_vals[-1])
                                previous = float(valid_vals[-2])
                                if previous > 0:
                                    growth = round((current - previous) / previous * 100, 1)
                                    data['revenue_growth'] = str(growth)
                            except:
                                pass

                    elif th_text == '영업이익':
                        vals = [td.get_text(strip=True).replace(',', '') for td in tds]
                        current_val = get_last_valid_val(tds)
                        if current_val:
                            data['operating_profit'] = current_val

                        # 성장률 계산
                        valid_vals = [v for v in vals if v and v != '-' and v.replace('.', '').replace('-', '').isdigit()]
                        if len(valid_vals) >= 2:
                            try:
                                current = float(valid_vals[-1])
                                previous = float(valid_vals[-2])
                                if previous != 0:
                                    growth = round((current - previous) / abs(previous) * 100, 1)
                                    data['profit_growth'] = str(growth)
                            except:
                                pass

                    elif th_text == '당기순이익' or th_text == '순이익':
                        val = get_last_valid_val(tds)
                        if val:
                            data['net_profit'] = val

                    # 재무 건전성
                    elif 'ROE' in th_text:
                        val = get_last_valid_val(tds)
                        if val:
                            try:
                                data['roe'] = float(val)
                            except:
                                pass
                    elif '부채비율' in th_text:
                        val = get_last_valid_val(tds)
                        if val:
                            try:
                                data['debt_ratio'] = float(val)
                            except:
                                pass
                    elif '유동비율' in th_text:
                        val = get_last_valid_val(tds)
                        if val:
                            try:
                                data['current_ratio'] = float(val)
                            except:
                                pass
                break

        # ===================================================================
        # 8. 배당수익률 (ID 기반)
        # ===================================================================
        dvr_em = soup.find('em', id='_dvr')
        if dvr_em:
            val = dvr_em.get_text(strip=True).replace(',', '').replace('%', '')
            if val and val != '-' and val != 'N/A':
                try:
                    data['dividend_yield'] = float(val)
                except:
                    pass

        # ===================================================================
        # 9. 수급 데이터 (테이블 3 - 투자자별 매매동향)
        # ===================================================================
        # 오늘 수급뿐만 아니라 5일/20일 추세도 가져오기 위해 frgn.naver 페이지 활용 권장
        # 여기서는 일단 기본 수집을 하고, 아래에서 추가 수집 함수를 호출합니다.
        
        # ===================================================================
        # 10. 기술적 지표 계산
        # ===================================================================
        if data['high_52w'] > data['low_52w'] > 0:
            data['price_position_52w'] = round(
                (data['current_price'] - data['low_52w']) / (data['high_52w'] - data['low_52w']) * 100,
                1
            )
            data['rsi'] = data['price_position_52w']

        # 11. 추가 데이터 수집 (뉴스 검색, 수급 추세)
        # ===================================================================
        extra_data = get_extra_stock_data(ticker, data.get('name', ''), headers)
        data.update(extra_data)

        return data

    except Exception as e:
        print(f"Error collecting data for {ticker}: {e}")
        import traceback
        traceback.print_exc()
        return data

def get_extra_stock_data(ticker, name, headers):
    """뉴스 및 수급 추세 데이터를 추가로 수집합니다."""
    extra = {
        'news': [],
        'foreign_5d_net': 0,
        'foreign_20d_net': 0,
        'inst_5d_net': 0,
        'inst_20d_net': 0,
        'ma5': 0,
        'ma20': 0,
        'ma60': 0,
        'ma120': 0
    }
    
def get_moving_averages(ticker, headers):
    """
    네이버 금융 일별 시세 페이지에서 최근 20일 종가를 가져와 5일, 20일 이동평균선을 계산합니다.
    """
    ma_data = {'ma5': 0, 'ma20': 0, 'ma5_diff': 0, 'ma20_diff': 0}
    try:
        url = f"https://finance.naver.com/item/sise_day.naver?code={ticker}&page=1"
        res = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        prices = []
        rows = soup.select('tr[onmouseover]')
        for row in rows:
            tds = row.find_all('td')
            if len(tds) >= 2:
                try:
                    price = int(tds[1].get_text(strip=True).replace(',', ''))
                    prices.append(price)
                except: continue
        
        if len(prices) >= 5:
            ma5 = sum(prices[:5]) / 5
            ma_data['ma5'] = round(ma5, 2)
            ma_data['ma5_diff'] = round(((prices[0] - ma5) / ma5) * 100, 2)
            
        if len(prices) >= 20:
            ma20 = sum(prices[:20]) / 20
            ma_data['ma20'] = round(ma20, 2)
            ma_data['ma20_diff'] = round(((prices[0] - ma20) / ma20) * 100, 2)
        elif len(prices) > 0:
            # 20일치가 안되면 있는만큼만
            ma20 = sum(prices) / len(prices)
            ma_data['ma20'] = round(ma20, 2)
            ma_data['ma20_diff'] = round(((prices[0] - ma20) / ma20) * 100, 2)
            
    except Exception as e:
        print(f"MA calculation error for {ticker}: {e}")
    return ma_data

def get_extra_stock_data(ticker, name, headers):
    """
    수급, 뉴스, 이동평균선 등 추가 데이터를 수집합니다.
    """
    extra = {
        'foreign_5d_net': 0,
        'foreign_20d_net': 0,
        'inst_5d_net': 0,
        'inst_20d_net': 0,
        'news': [],
        'ma5': 0,
        'ma20': 0,
        'ma5_diff': 0,
        'ma20_diff': 0
    }
    
    try:
        # 1. 수급 추세 (frgn.naver)
        frgn_url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
        res = requests.get(frgn_url, headers=headers, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        tables = soup.find_all('table', class_='type2')
        for table in tables:
            if '날짜' in table.get_text() and '기관' in table.get_text():
                rows = table.select('tr[onmouseover]')
                f_5d, f_20d, i_5d, i_20d = 0, 0, 0, 0
                for i, row in enumerate(rows):
                    tds = row.find_all('td')
                    if len(tds) >= 9:
                        try:
                            i_val = int(re.sub(r'[^0-9\-]', '', tds[5].get_text(strip=True) or '0'))
                            f_val = int(re.sub(r'[^0-9\-]', '', tds[6].get_text(strip=True) or '0'))
                            
                            if i < 5:
                                f_5d += f_val
                                i_5d += i_val
                            if i < 20:
                                f_20d += f_val
                                i_20d += i_val
                        except: continue
                
                extra['foreign_5d_net'] = f_5d
                extra['foreign_20d_net'] = f_20d
                extra['inst_5d_net'] = i_5d
                extra['inst_20d_net'] = i_20d
                break

        # 2. 뉴스 검색 (news_search.naver)
        import urllib.parse
        query = name if name else ticker
        encoded_query = urllib.parse.quote(query.encode('euc-kr'))
        news_url = f"https://finance.naver.com/news/news_search.naver?q={encoded_query}"
        res = requests.get(news_url, headers=headers, timeout=5)
        soup = BeautifulSoup(res.content.decode('euc-kr', 'replace'), 'html.parser')
        
        articles = soup.select('.newsList dl')
        for art in articles:
            subject_a = art.select_one('dt.articleSubject a, dd.articleSubject a, dt a')
            if not subject_a: continue
            
            title = subject_a.get_text(strip=True)
            link = "https://finance.naver.com" + subject_a['href']
            
            source = ""
            date = ""
            press_el = art.select_one('.press')
            date_el = art.select_one('.wdate')
            if press_el: source = press_el.get_text(strip=True)
            if date_el: date = date_el.get_text(strip=True)
            
            extra['news'].append({
                'title': title,
                'link': link,
                'source': source,
                'date': date
            })
            if len(extra['news']) >= 5: break

        # 3. 이동평균선 계산
        ma_data = get_moving_averages(ticker, headers)
        extra.update(ma_data)
        
    except Exception as e:
        print(f"Extra data collection error for {ticker}: {e}")
        
    return extra

# 테스트
if __name__ == '__main__':
    import sys
    if os.name == 'nt':
        sys.stdout.reconfigure(encoding='utf-8')

    result = get_all_naver_data('005930')

    print("\n" + "=" * 80)
    print("네이버 금융 전체 데이터 수집 결과 - 삼성전자")
    print("=" * 80)

    # 카테고리별로 출력
    categories = {
        '기본 정보': ['code', 'name', 'current_price', 'prev_price', 'open_price', 'high_price', 'low_price', 'volume', 'trading_value'],
        '시가총액': ['market_cap', 'market_cap_rank', 'outstanding_shares'],
        '외국인 정보': ['foreign_limit_shares', 'foreign_owned_shares', 'foreign_ownership_ratio', 'foreign_exhaustion_ratio'],
        '투자의견': ['opinion', 'opinion_score', 'target_price'],
        '52주 고/저': ['high_52w', 'low_52w', 'price_position_52w'],
        'PER/PBR': ['per', 'eps', 'estimated_per', 'estimated_eps', 'pbr', 'bps'],
        '업종 비교': ['sector_per', 'sector_change_rate'],
        '배당': ['dividend_yield'],
        '성장성': ['revenue_growth', 'profit_growth', 'revenue', 'operating_profit', 'net_profit'],
        '재무 건전성': ['roe', 'debt_ratio', 'current_ratio'],
        '수급': ['foreign_net_buy_today', 'inst_net_buy_today']
    }

    for category, keys in categories.items():
        print(f"\n### {category}")
        for key in keys:
            value = result.get(key)
            if isinstance(value, (int, float)) and value != 0:
                print(f"  ✓ {key:30s}: {value:,}" if isinstance(value, int) else f"  ✓ {key:30s}: {value}")
            elif isinstance(value, str) and value != 'N/A' and value != '':
                print(f"  ✓ {key:30s}: {value}")
            else:
                print(f"  ❌ {key:30s}: {value}")

    # 통계
    total_fields = len(result)
    filled_fields = sum(1 for v in result.values() if v not in [0, 'N/A', '', None])
    print(f"\n\n총 데이터 필드: {total_fields}개")
    print(f"수집 성공: {filled_fields}개 ({filled_fields/total_fields*100:.1f}%)")
