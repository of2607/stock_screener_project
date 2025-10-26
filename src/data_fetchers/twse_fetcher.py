import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from datetime import datetime

def fetch_twse_closing_prices():
    """
    取得台灣證券交易所上市股票最新收盤價資料。

    Returns:
        List[dict]: 每筆 dict 包含 'code'、'name'、'price' 欄位。
    """
    url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_AVG_ALL"
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
        # 只取上市資料（API已僅回傳上市，不含上櫃）
        code = item.get("Code")
        name = item.get("Name")
        price = item.get("ClosingPrice")
        # 嘗試取得日期欄位，若無則用今日日期
        date = item.get("Date") or item.get("date") or datetime.today().strftime("%Y-%m-%d")
        if code and name and price is not None:
            result.append({
                "code": code,
                "name": name,
                "price": price,
                "date": date
            })
    return result