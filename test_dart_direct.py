import requests
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("DART_API_KEY")

# 삼성전자 corp_code는 00126380
url = f"https://opendart.fss.or.kr/api/fnlttSinglAcnt.json?crtfc_key={api_key}&corp_code=00126380&bsns_year=2023&reprt_code=11011"
res = requests.get(url).json()
print(res.get('status'))
print(res.get('message'))
