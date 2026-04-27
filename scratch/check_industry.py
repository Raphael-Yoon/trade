import requests
from bs4 import BeautifulSoup

code = "033100" # 제룡전기
url = f"https://finance.naver.com/item/main.naver?code={code}"
res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
soup = BeautifulSoup(res.text, 'html.parser')
# 업종 찾기
industry_el = soup.select_one("h4.h_sub.sub_tit7 + em")
if not industry_el:
    # 다른 시도
    industry_el = soup.select_one(".description em a")
if industry_el:
    print(f"Industry: {industry_el.text.strip()}")
else:
    print("Industry not found")
