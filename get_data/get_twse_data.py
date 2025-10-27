import requests
from bs4 import BeautifulSoup
import os
import shutil
import time
import certifi
from tqdm import tqdm
import pandas as pd

# =========================
# 設定年份與市場
# =========================
start_year = 110
end_year = 114
markets = ["sii", "otc"]
seasons = ["01", "02", "03", "04"]

# 可控制下載的報表類別
# [] 或 ['all'] 代表下載全部
download_reports = ['股利']  # ['股利','資產負債表'] 只下載指定報表

report_types = {
    "資產負債表": {
        "ajax": "https://mopsov.twse.com.tw/mops/web/ajax_t163sb05?year={year}&TYPEK={market}&season={season}&firstin=1",
        "download_base": "https://mopsov.twse.com.tw/server-java/t105sb02"
    },
    "股利": {
        "ajax": "https://mopsov.twse.com.tw/server-java/t05st09sub?YEAR={year}&qryType=2&TYPEK={market}&step=1",
        "download_base": "https://mopsov.twse.com.tw/server-java/t105sb02"
    },
    "損益表": {
        "ajax": "https://mopsov.twse.com.tw/mops/web/ajax_t163sb04?year={year}&TYPEK={market}&season={season}&firstin=1",
        "download_base": "https://mopsov.twse.com.tw/server-java/t105sb02"
    },
    "現金流量表": {
        "ajax": "https://mopsov.twse.com.tw/mops/web/ajax_t163sb20?year={year}&TYPEK={market}&season={season}&firstin=1",
        "download_base": "https://mopsov.twse.com.tw/server-java/t105sb02"
    }
}

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

base_dir = "csv_downloads"
merge_dir = os.path.join(base_dir, "merge")
os.makedirs(base_dir, exist_ok=True)
os.makedirs(merge_dir, exist_ok=True)

# =========================
# 主流程
# =========================
for report_name, urls in report_types.items():
    # 控制下載項目
    if download_reports and 'all' not in download_reports and report_name not in download_reports:
        continue

    print(f"\n=== 開始下載 {report_name} ===")
    for year in range(start_year, end_year + 1):
        year_str = str(year)
        year_dir = os.path.join(base_dir, report_name, year_str)

        # 清空年份資料夾
        if os.path.exists(year_dir):
            shutil.rmtree(year_dir)
        os.makedirs(year_dir, exist_ok=True)

        all_filenames = []

        # 抓 Ajax 取得 CSV 名稱
        for market in markets:
            for season in seasons:
                ajax_url = urls["ajax"].format(
                    year=year_str,
                    market=market,
                    season=season
                ) if report_name != "股利" else urls["ajax"].format(year=year_str, market=market)

                try:
                    res = requests.get(ajax_url, headers=headers, verify=False, timeout=10)
                    res.encoding = "utf-8"
                    soup = BeautifulSoup(res.text, "lxml")
                    input_tags = soup.find_all("input", {"name": "filename"})
                    filenames = [tag.get("value") for tag in input_tags if tag.get("value")]
                    all_filenames.extend(filenames)
                except Exception as e:
                    print(f"抓 {year_str} {market} {season} CSV 名稱失敗: {e}")
                time.sleep(0.5)

        # 去重
        seen = set()
        unique_filenames = []
        for f in all_filenames:
            if f not in seen:
                unique_filenames.append(f)
                seen.add(f)

        print(f"{year_str} 年找到 {len(all_filenames)} 個 CSV，去重後 {len(unique_filenames)} 個")

        # 下載 CSV
        for fname in tqdm(unique_filenames, desc=f"{year_str} {report_name} 下載進度"):
            save_path = os.path.join(year_dir, fname)
            download_url = f"{urls['download_base']}?firstin=true&step=10&filename={fname}"

            for attempt in range(3):
                try:
                    r = requests.get(download_url, headers=headers, verify=False, timeout=10)
                    r.encoding = "big5"
                    with open(save_path, "w", encoding="utf-8-sig", newline="") as f:
                        f.write(r.text)
                    break
                except Exception as e:
                    print(f"下載 {fname} 失敗: {e}，第 {attempt+1} 次重試")
                    time.sleep(2)
            else:
                print(f"❌ {fname} 下載失敗，跳過")

        # =========================
        # 合併成總表放 merge
        # =========================
        all_dfs = []
        for fname in os.listdir(year_dir):
            if fname.endswith(".csv"):
                path = os.path.join(year_dir, fname)
                try:
                    # 股利特殊讀取方式
                    if report_name == "股利":
                        df = pd.read_csv(path, encoding="utf-8-sig", dtype=str, skiprows=1, engine="python")
                        df = df.dropna(how="all")
                    else:
                        df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
                    all_dfs.append(df)
                except Exception as e:
                    print(f"讀取 {fname} 失敗: {e}")

        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            combined_path = os.path.join(merge_dir, f"{year_str}-{report_name}.csv")
            combined_df.to_csv(combined_path, index=False, encoding="utf-8-sig")
            print(f"✅ {year_str} 年 {report_name} 合併完成: {combined_path}")
        else:
            print(f"❌ {year_str} 年 {report_name} 沒有可合併的 CSV")
False