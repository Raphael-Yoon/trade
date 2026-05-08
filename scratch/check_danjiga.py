import requests
from bs4 import BeautifulSoup

url = "https://finance.naver.com/sise/sise_danjiga.naver"
res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
soup = BeautifulSoup(res.text, 'html.parser')
rows = soup.select("table.type_2 tr")
for row in rows:
    cols = row.select("td")
    if len(cols) < 5: continue
    print([c.text.strip() for c in cols[:8]])
    break
