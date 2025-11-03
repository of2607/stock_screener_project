import requests
from bs4 import BeautifulSoup
import os
import shutil
import time
import certifi
from tqdm import tqdm
import pandas as pd
import json
from datetime import datetime

import urllib3
# 忽略 SSL 證書警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

"""
TWSE 財報資料下載與合併工具
==============================

新增功能說明：
1. 僅合併模式：設定 only_merge = True，跳過下載直接合併現有的 raw_data 檔案
2. 欄位過濾：設定 keep_columns 字典來指定每種報表要保留的欄位
3. 自動排序：合併後的資料會自動依公司代號排序

處理流程：
1. 下載或讀取 CSV 檔案
2. 合併所有資料
3. 過濾指定欄位（如有設定）
4. 依公司代號排序
5. 儲存為 CSV/JSON 格式

使用範例：
---------
# 一般模式（下載 + 合併）
only_merge = False
keep_columns = {}

# 僅合併模式 + 欄位過濾
only_merge = True
keep_columns = {
    'balance_sheet': ['公司代號', '公司名稱', '流動資產', '資產總額'],
    'income_statement': ['公司代號', '公司名稱', '營業收入', '稅後淨利'],
    'dividend': ['公司代號名稱', '股東會日期', '股利合計']
}
"""

# =========================
# Config
# =========================
start_year = 109
end_year = 114
markets = ["sii", "otc"]
seasons = ["01", "02", "03", "04"]

download_reports = ['dividend']  # or ['dividend', 'balance_sheet']
save_format = ['csv', 'json']  # 可為 ['csv'], ['json'], ['csv', 'json']

# 新增功能設定
only_merge = True  # 設為 True 時只做合併，不下載

# 指定要保留的欄位，格式: {'report_name': ['column1', 'column2', ...]}
# 欄位保留設定範例：
# keep_columns = {
#     'balance_sheet': ['公司代號', '公司名稱', '流動資產', '資產總額'],
#     'income_statement': ['公司代號', '公司名稱', '營業收入', '稅後淨利'],
#     'dividend': ['公司代號名稱', '股東會日期', '股利合計'],
#     'cash_flow': ['公司代號', '公司名稱', '營業活動之現金流量']
# }
keep_columns = {
    'balance_sheet': [
        # 識別與時間序列
        '公司代號',
        '公司名稱',
        '年度',
        '季別',
        # 核心計算 (ROE, 盈再率) - 這些欄位已確認存在於 Source [1] 中
        '歸屬於母公司業主之權益合計',  # ROE 分母
        '不動產及設備－淨額',       # 盈再率組件
        '無形資產－淨額',           # 盈再率組件
        # 風險與輔助資訊 - 這些欄位已確認存在於 Source [1] 中
        '流動資產',
        '資產總額',
        '流動負債',
        '非流動負債',
        '非控制權益',
        '每股參考淨值',
    ],
    'income_statement': [
        # 識別與時間序列
        '公司代號',
        '公司名稱',
        '年度',
        '季別',
        '出表日期',
        # 核心計算 (ROE, 穩定性)
        '淨利（損）歸屬於母公司業主',
        '營業收入',
        '營業成本',
        # 輔助與相容性
        '稅後淨利',
        '基本每股盈餘（元）',
    ],
    'dividend': [
        # 識別與時間序列
        '公司代號名稱',  # 原始欄位，會被拆分成公司代號和公司名稱
        '股東會日期',
        '股利所屬年(季)度',  # 原始欄位，會被拆分成年度和季別
        '股利所屬期間',
        '決議（擬議）進度',
        # 核心計算 (現金配發/IRR)
        '股東配發-盈餘分配之現金股利(元/股)',
        '股東配發-法定盈餘公積發放之現金(元/股)',
        '股東配發-資本公積發放之現金(元/股)',
        '股東配發-股東配發之現金(股利)總金額(元)',
        # 配股相關
        '股東配發-盈餘轉增資配股(元/股)',
        '股東配發-法定盈餘公積轉增資配股(元/股)',
        '股東配發-資本公積轉增資配股(元/股)',
        '股東配發-股東配股總股數(股)'
    ],
    'cash_flow': [
        # 識別與時間序列
        '公司代號',
        '公司名稱',
        '年度',
        '季別',
        # 核心計算 (風險驗證)
        '營業活動之淨現金流入（流出）',
    ]
}

report_types = {
    "balance_sheet": {
        "ajax": "https://mopsov.twse.com.tw/mops/web/ajax_t163sb05?year={year}&TYPEK={market}&season={season}&firstin=1",
        "download_base": "https://mopsov.twse.com.tw/server-java/t105sb02"
    },
    "dividend": {
        "ajax": "https://mopsov.twse.com.tw/server-java/t05st09sub?YEAR={year}&qryType=2&TYPEK={market}&step=1",
        "download_base": "https://mopsov.twse.com.tw/server-java/t105sb02"
    },
    "income_statement": {
        "ajax": "https://mopsov.twse.com.tw/mops/web/ajax_t163sb04?year={year}&TYPEK={market}&season={season}&firstin=1",
        "download_base": "https://mopsov.twse.com.tw/server-java/t105sb02"
    },
    "cash_flow": {
        "ajax": "https://mopsov.twse.com.tw/mops/web/ajax_t163sb20?year={year}&TYPEK={market}&season={season}&firstin=1",
        "download_base": "https://mopsov.twse.com.tw/server-java/t105sb02"
    }
}

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

base_dir = "raw_data"
merge_dir = "merged_data"
log_path = os.path.join(merge_dir, "log.json")
os.makedirs(merge_dir, exist_ok=True)

# =========================
# Helper: sort by company code
# =========================
def sort_by_company_code(df: pd.DataFrame, report_name: str) -> pd.DataFrame:
    """依公司代號排序"""
    if df.empty:
        return df

    # 找出公司代號欄位
    company_code_col = None
    if "公司代號" in df.columns:
        company_code_col = "公司代號"
    elif "公司代號名稱" in df.columns:
        company_code_col = "公司代號名稱"
    else:
        # 嘗試找到包含"公司代號"的欄位
        for col in df.columns:
            if "公司代號" in col:
                company_code_col = col
                break

    if company_code_col is None:
        print(f"⚠️ {report_name} 找不到公司代號欄位，跳過排序")
        return df

    print(f"🔢 {report_name} 依 '{company_code_col}' 排序")

    # 如果是公司代號名稱格式 (例如: "2330 - 台積電")，提取前面的數字進行排序
    if company_code_col == "公司代號名稱" or (len(df) > 0 and " - " in str(df[company_code_col].iloc[0])):
        # 創建一個臨時欄位用於排序
        df_sorted = df.copy()
        df_sorted['_sort_key'] = df_sorted[company_code_col].astype(str).str.extract(r'(\d+)')[0]
        df_sorted['_sort_key'] = pd.to_numeric(df_sorted['_sort_key'], errors='coerce')
        df_sorted = df_sorted.sort_values(by='_sort_key', ascending=True, ignore_index=True)
        df_sorted = df_sorted.drop(columns=['_sort_key'])
        return df_sorted
    else:
        # 直接依公司代號排序（適用於已拆分的公司代號欄位）
        # 保持公司代號為字串格式，但用數字排序
        df_sorted = df.copy()

        # 創建臨時排序鍵，提取公司代號中的數字部分
        df_sorted['_sort_key'] = df_sorted[company_code_col].astype(str).str.extract(r'(\d+)')[0]
        df_sorted['_sort_key'] = pd.to_numeric(df_sorted['_sort_key'], errors='coerce')

        # 按數字排序但保持原始字串格式
        df_sorted = df_sorted.sort_values(by='_sort_key', ascending=True, ignore_index=True)
        df_sorted = df_sorted.drop(columns=['_sort_key'])

        return df_sorted


# =========================
# Helper: process company code name
# =========================
def process_company_code_name(df: pd.DataFrame, report_name: str) -> pd.DataFrame:
    """
    處理股利資料表的欄位拆分：
    1. 公司代號名稱 → 公司代號 + 公司名稱
    2. 股利所屬年(季)度 → 年度(整數) + 季別(Q1, Q2, Q3, Q4, H1, H2, Y1)
    
    Args:
        df: 資料框
        report_name: 報表名稱
        
    Returns:
        處理後的資料框
    """
    if df.empty:
        return df
    
    # 只處理股利資料表
    if report_name == "dividend":
        df_processed = df.copy()
        
        # 1. 拆分公司代號名稱欄位
        if "公司代號名稱" in df_processed.columns:
            print(f"🔧 {report_name} 正在拆分公司代號名稱欄位...")
            
            # 拆分公司代號名稱 (格式: "1234 - 公司名稱")
            company_info = df_processed["公司代號名稱"].str.split(" - ", n=1, expand=True)
            
            # 新增公司代號和公司名稱欄位
            df_processed["公司代號"] = company_info[0].str.strip()
            df_processed["公司名稱"] = company_info[1].str.strip()
            
            # 移除原始的公司代號名稱欄位
            df_processed = df_processed.drop(columns=["公司代號名稱"])
            
            print(f"✅ 成功拆分公司代號名稱欄位")
        
        # 2. 拆分股利所屬年(季)度欄位
        if "股利所屬年(季)度" in df_processed.columns:
            print(f"🔧 {report_name} 正在拆分股利所屬年(季)度欄位...")
            
            # 提取年度 (例如: "113年 年度" → 113)
            df_processed["年度"] = df_processed["股利所屬年(季)度"].str.extract(r'(\d+)年')[0]
            df_processed["年度"] = pd.to_numeric(df_processed["年度"], errors='coerce').astype('Int64')
            
            # 提取季別並標準化
            def standardize_period(period_str):
                if pd.isna(period_str):
                    return None
                
                period_str = str(period_str).strip()
                
                # 年度
                if "年度" in period_str:
                    return "YEAR"
                # 季度
                elif "第1季" in period_str:
                    return "Q1"
                elif "第2季" in period_str:
                    return "Q2"
                elif "第3季" in period_str:
                    return "Q3"
                elif "第4季" in period_str:
                    return "Q4"
                # 半年
                elif "上半年" in period_str:
                    return "H1"
                elif "下半年" in period_str:
                    return "H2"
                # 月份 (如果有的話)
                elif "月" in period_str:
                    month_match = pd.Series([period_str]).str.extract(r'第?(\d+)月')[0].iloc[0]
                    if month_match:
                        return f"M{month_match.zfill(2)}"
                
                return "OTHER"
            
            df_processed["季別"] = df_processed["股利所屬年(季)度"].apply(standardize_period)
            
            # 移除原始的股利所屬年(季)度欄位
            df_processed = df_processed.drop(columns=["股利所屬年(季)度"])
            
            print(f"✅ 成功拆分股利所屬年(季)度欄位")
        
        # 3. 重新排列欄位順序
        cols = df_processed.columns.tolist()
        
        # 確定新欄位的順序：公司代號、公司名稱、年度、季別
        priority_cols = []
        if "公司代號" in cols:
            priority_cols.append("公司代號")
            cols.remove("公司代號")
        if "公司名稱" in cols:
            priority_cols.append("公司名稱")
            cols.remove("公司名稱")
        if "年度" in cols:
            priority_cols.append("年度")
            cols.remove("年度")
        if "季別" in cols:
            priority_cols.append("季別")
            cols.remove("季別")
        
        # 重新組合欄位順序
        new_cols = priority_cols + cols
        df_processed = df_processed[new_cols]
        
        print(f"✅ {report_name} 欄位處理完成")
        print(f"   新增欄位: {', '.join(priority_cols)}")
        
        return df_processed
    else:
        # 其他報表直接返回原資料框
        return df
# =========================
# Helper: filter columns
# =========================
def filter_columns(df: pd.DataFrame, report_name: str) -> pd.DataFrame:
    """根據設定過濾欄位"""
    if not keep_columns or report_name not in keep_columns:
        print(f"📋 {report_name} 未設定欄位過濾，保留所有 {len(df.columns)} 欄")
        return df

    columns_to_keep = keep_columns[report_name]
    existing_columns = [col for col in columns_to_keep if col in df.columns]

    if existing_columns:
        missing_columns = set(columns_to_keep) - set(existing_columns)
        if missing_columns:
            print(f"⚠️ {report_name} 找不到欄位: {list(missing_columns)}")

        print(f"📋 {report_name} 欄位過濾: {len(df.columns)} → {len(existing_columns)} 欄")
        print(f"   保留欄位: {existing_columns}")
        return df[existing_columns].copy()
    else:
        print(f"⚠️ {report_name} 找不到任何指定的欄位，保留所有 {len(df.columns)} 欄")
        return df


# =========================
# Helper: clean + sort dividend CSV
# =========================
def clean_and_sort_dividend(path: str) -> pd.DataFrame:
    """強化版股利報表清理：跳過前置說明行，載入後排序並移除有問題的列"""

    # 先讀取文本找到真正的表頭位置
    with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
        lines = f.readlines()

    header_idx = None
    for i, line in enumerate(lines):
        # 尋找包含 "公司代號名稱" 或同時包含 "公司代號" 和 "公司名稱" 的表頭行
        if ("公司代號名稱" in line) or (("公司代號" in line) and ("公司名稱" in line)):
            if line.count(",") > 2:  # 確保是表格開頭
                header_idx = i
                break

    if header_idx is None:
        print(f"⚠️ 無法在 {os.path.basename(path)} 找到公司代號欄位")
        return pd.DataFrame()

    # 用 pandas 載入，跳過前面的說明行
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", dtype=str, engine="python",
                        on_bad_lines="skip", skiprows=header_idx)
    except:
        print(f"⚠️ 無法讀取 {os.path.basename(path)}")
        return pd.DataFrame()

    # 檢查是否有資料
    if df.empty:
        print(f"⚠️ {os.path.basename(path)} 為空檔案")
        return pd.DataFrame()

    # 確定第一欄的名稱（可能是 "公司代號名稱" 或 "公司代號"）
    first_col = df.columns[0]
    if "公司代號" not in first_col:
        print(f"⚠️ 無法在 {os.path.basename(path)} 找到公司代號欄位")
        return pd.DataFrame()

    # 移除全空列
    df = df.dropna(how="all")

    # 移除 Unnamed 欄位
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    # 按第一欄排序（將有問題的列排到一起）
    df = df.sort_values(by=first_col, ascending=True, ignore_index=True, na_position='last')

    # 移除有問題的列：
    # 1. 第一欄不包含 " - " 的列（除了表頭）
    # 2. 第一欄包含表頭文字的重複列
    # 3. 第一欄為空或只有少量文字的列

    mask_to_keep = pd.Series([True] * len(df))

    for i, val in enumerate(df[first_col]):
        val_str = str(val).strip()

        # 跳過空值
        if val_str in ['nan', '', 'None']:
            mask_to_keep[i] = False
            continue

        # 移除重複的表頭行
        if "公司代號" in val_str and not " - " in val_str:
            mask_to_keep[i] = False
            continue

        # 移除不包含 " - " 的行（正常的公司代號應該是 "1234 - 公司名稱" 格式）
        if " - " not in val_str:
            mask_to_keep[i] = False
            continue

        # 移除太短的行（可能是斷行造成的）
        if len(val_str) < 5:
            mask_to_keep[i] = False
            continue

    # 套用過濾
    df_cleaned = df[mask_to_keep].copy()

    # 重新索引
    df_cleaned.reset_index(drop=True, inplace=True)

    # 最後按正常欄位重新排序
    sort_cols = []
    if "公司代號" in df_cleaned.columns:
        sort_cols = [col for col in ["公司代號", "公司名稱", "股東會日期"] if col in df_cleaned.columns]
    elif "公司代號名稱" in df_cleaned.columns:
        sort_cols = [col for col in ["公司代號名稱", "股東會日期"] if col in df_cleaned.columns]

    if sort_cols:
        df_cleaned = df_cleaned.sort_values(by=sort_cols, ascending=True, ignore_index=True)

    print(f"✅ {os.path.basename(path)} 清理完成，保留 {len(df_cleaned)} 行")

    return df_cleaned



# =========================
# Helper: log writer
# =========================
def write_log(year, report_name, csv_path, json_path, row_count):
    log_data = []
    if os.path.exists(log_path):
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                log_data = json.load(f)
        except Exception:
            log_data = []

    entry = {
        "year": year,
        "report": report_name,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "files": {
            "csv": csv_path if csv_path else None,
            "json": json_path if json_path else None
        },
        "total_rows": int(row_count)
    }

    log_data.append(entry)
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)


# =========================
# Main Process
# =========================
for report_name, urls in report_types.items():
    if download_reports and 'all' not in download_reports and report_name not in download_reports:
        continue

    print(f"\n=== Start processing {report_name} ===")
    for year in range(start_year, end_year + 1):
        year_str = str(year)
        year_dir = os.path.join(base_dir, report_name, year_str)

        if only_merge:
            print(f"🔄 僅合併模式: 處理 {year_str} {report_name}")
            if not os.path.exists(year_dir):
                print(f"❌ 找不到資料夾: {year_dir}")
                continue
        else:
            print(f"⬇️ 下載模式: 處理 {year_str} {report_name}")
            if os.path.exists(year_dir):
                shutil.rmtree(year_dir)
            os.makedirs(year_dir, exist_ok=True)

            all_filenames = []

            # Step 1: 抓 CSV 檔名
            for market in markets:
                for season in seasons:
                    ajax_url = (
                        urls["ajax"].format(year=year_str, market=market, season=season)
                        if report_name != "dividend"
                        else urls["ajax"].format(year=year_str, market=market)
                    )
                    try:
                        res = requests.get(ajax_url, headers=headers, verify=False, timeout=10)
                        res.encoding = "utf-8"
                        soup = BeautifulSoup(res.text, "lxml")
                        input_tags = soup.find_all("input", {"name": "filename"})
                        filenames = [tag.get("value") for tag in input_tags if tag.get("value")]
                        all_filenames.extend(filenames)
                    except Exception as e:
                        print(f"Fetch {year_str} {market} {season} filenames failed: {e}")
                    time.sleep(0.5)

            # 去重
            seen = set()
            unique_filenames = [f for f in all_filenames if not (f in seen or seen.add(f))]
            print(f"{year_str} found {len(all_filenames)} CSVs, {len(unique_filenames)} unique")

            # Step 2: 下載
            for fname in tqdm(unique_filenames, desc=f"{year_str} {report_name} download"):
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
                        print(f"Download {fname} failed: {e} (try {attempt+1})")
                        time.sleep(2)
                else:
                    print(f"❌ {fname} download failed, skipped")

        # Step 3: 清理與合併 (下載模式和僅合併模式都會執行)
        all_dfs = []
        csv_files = [f for f in os.listdir(year_dir) if f.endswith(".csv")]
        print(f"📁 找到 {len(csv_files)} 個 CSV 檔案")

        for fname in csv_files:
            path = os.path.join(year_dir, fname)
            try:
                if report_name == "dividend":
                    df = clean_and_sort_dividend(path)
                else:
                    df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
                    df = df.dropna(how="all")

                # 先不過濾欄位，保留所有資料進行合併
                all_dfs.append(df)
            except Exception as e:
                print(f"Read {fname} failed: {e}")

        # Step 4: 合併後再過濾欄位和排序
        if all_dfs:
            # 先合併所有資料
            combined_df = pd.concat(all_dfs, ignore_index=True)
            print(f"📊 合併完成，總計 {len(combined_df)} 行，{len(combined_df.columns)} 欄")

            # 合併後再過濾欄位
            combined_df = filter_columns(combined_df, report_name)

            # 整理欄位：將股利資料表的"公司代號名稱"分成"公司代號"和"公司名稱"兩欄
            combined_df = process_company_code_name(combined_df, report_name)

            # 依公司代號排序
            combined_df = sort_by_company_code(combined_df, report_name)

            csv_path = json_path = None

            if "csv" in save_format:
                csv_path = os.path.join(merge_dir, f"{year_str}-{report_name}.csv")
                combined_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
                print(f"✅ CSV saved: {csv_path}")

            if "json" in save_format:
                json_path = os.path.join(merge_dir, f"{year_str}-{report_name}.json")
                combined_df.to_json(json_path, orient="records", force_ascii=False, indent=2)
                print(f"✅ JSON saved: {json_path}")

            write_log(year_str, report_name, csv_path, json_path, len(combined_df))
            print(f"📝 Log updated for {year_str} {report_name} - Total rows: {len(combined_df)}")
        else:
            print(f"❌ {year_str} {report_name} no valid CSVs to merge")
