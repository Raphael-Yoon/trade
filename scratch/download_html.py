import requests
url = "https://finance.naver.com/item/main.naver?code=033100"
res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
with open('scratch/item_main.html', 'w', encoding='utf-8') as f:
    f.write(res.text)
