import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from datetime import datetime

def fetch_tpex_closing_prices():
    """
    取得所有上櫃股票的最新收盤價資料。
    回傳格式：list of dict，每筆 dict 包含 code, name, price 欄位。
    """
    url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
    headers = {
        "accept": "application/json",
        "If-Modified-Since": datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT"),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache"
    }
    response = requests.get(url, headers=headers, verify=False)
    response.raise_for_status()
    data = response.json()
    result = []
    for item in data:
        code = item.get("SecuritiesCompanyCode")
        name = item.get("CompanyName")
        price = item.get("Close")
        date = item.get("Date") or item.get("date") or datetime.today().strftime("%Y-%m-%d")
        result.append({
            "code": code,
            "name": name,
            "price": price,
            "date": date
        })
    return result