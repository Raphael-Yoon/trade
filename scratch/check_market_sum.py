import requests
from bs4 import BeautifulSoup

url = "https://finance.naver.com/sise/sise_market_sum.naver?sosok=0"
res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
soup = BeautifulSoup(res.text, 'html.parser')
table = soup.find('table', {'class': 'type_2'})
rows = table.select("tr")
for row in rows:
    cols = row.select("td")
    if len(cols) < 6: continue
    print([c.text.strip() for c in cols[:7]])
    break
