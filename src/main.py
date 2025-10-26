import json
from data_fetchers.twse_fetcher import fetch_twse_closing_prices
from data_fetchers.tpex_fetcher import fetch_tpex_closing_prices

def valid_price(item):
    try:
        return float(item['price']) >= 10
    except (ValueError, TypeError):
        return False

def main():
    twse_data = fetch_twse_closing_prices()
    twse_data = [item for item in twse_data if valid_price(item)]
    for item in twse_data:
        item["market"] = "上市"

    tpex_data = fetch_tpex_closing_prices()
    tpex_data = [item for item in tpex_data if valid_price(item)]
    for item in tpex_data:
        item["market"] = "上櫃"

    merged_data = twse_data + tpex_data

    print(f"上市股票筆數：{len(twse_data)}")
    print(f"上櫃股票筆數：{len(tpex_data)}")
    print(f"合計股票筆數：{len(merged_data)}")

    # merged_data 每筆 dict 現在都包含 date 欄位
    with open('closing_prices.json', 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()