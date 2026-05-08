import requests
url = "https://finance.naver.com/sise/sise_danjiga.naver"
res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
with open('scratch/danjiga.html', 'w', encoding='utf-8') as f:
    f.write(res.text)
