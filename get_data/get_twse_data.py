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
start_year = 107
end_year = 114
markets = ["sii", "otc"]
seasons = ["01", "02", "03", "04"]

download_reports = ['all']  # 處理所有報表類型
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
        # 識別與時間序列 (使用處理後的統一格式)
        '代號',     # 處理後：公司代號 → 代號
        '名稱',     # 處理後：公司名稱 → 名稱
        '年度',     # 處理後：民國年度格式
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
        # 識別與時間序列 (使用處理後的統一格式)
        '代號',     # 處理後：公司代號 → 代號
        '名稱',     # 處理後：公司名稱 → 名稱
        '年度',     # 處理後：民國年度格式
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
        # 識別與時間序列 (使用處理後的統一格式)
        '代號',        # 處理後：公司代號名稱 → 代號
        '名稱',        # 處理後：公司代號名稱 → 名稱
        '年度',        # 處理後：民國年度格式
        '季別',        # 處理後：標準化格式
        '股東會日期',
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
        # 識別與時間序列 (使用處理後的統一格式)
        '代號',     # 處理後：公司代號 → 代號
        '名稱',     # 處理後：公司名稱 → 名稱
        '年度',     # 處理後：民國年度格式
        '季別',
        # 核心計算 (風險驗證)
        '營業活動之淨現金流入（流出）',
    ],
    'etf_dividend': [
        # 識別與時間序列 (使用處理後的統一格式)
        '代號',        # 處理後：證券代號 → 代號
        '名稱',        # 處理後：證券簡稱 → 名稱
        '年度',        # 處理後：民國年度格式
        '季別',        # 處理後：依除息交易日判斷
        # 日期資訊
        '除息交易日',
        '收益分配基準日',
        '收益分配發放日',
        # 收益資訊
        '配息',
        '公告年度'
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
    },
    "etf_dividend": {
        "url": "https://www.twse.com.tw/rwd/zh/ETF/etfDiv?stkNo=&startDate={start_date}&endDate={end_date}&response=json",
        "csv_export": "https://www.twse.com.tw/rwd/zh/ETF/etfDiv?stkNo=&startDate={start_date}&endDate={end_date}&response=csv"
    }
}

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

base_dir = "raw_data"
merge_dir = "merged_data"
csv_output_dir = os.path.join(merge_dir, "csv")
json_output_dir = os.path.join(merge_dir, "json")
log_path = os.path.join(merge_dir, "log.json")

# 建立輸出目錄
os.makedirs(merge_dir, exist_ok=True)
os.makedirs(csv_output_dir, exist_ok=True)
os.makedirs(json_output_dir, exist_ok=True)

# =========================
# Helper: sort by company code
# =========================
def sort_by_company_code(df: pd.DataFrame, report_name: str) -> pd.DataFrame:
    """依公司代號排序"""
    if df.empty:
        return df

    # 找出代號欄位 (統一格式)
    company_code_col = None
    if "代號" in df.columns:
        company_code_col = "代號"
    elif "公司代號" in df.columns:
        company_code_col = "公司代號"
    elif "公司代號名稱" in df.columns:
        company_code_col = "公司代號名稱"
    else:
        # 嘗試找到包含"代號"或"公司代號"的欄位
        for col in df.columns:
            if "代號" in col:
                company_code_col = col
                break

    if company_code_col is None:
        print(f"⚠️ {report_name} 找不到代號欄位，跳過排序")
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
    處理各類報表的欄位標準化：
    1. 股利資料表：
       - 公司代號名稱 → 公司代號 + 公司名稱
       - 股利所屬年(季)度 → 年度(整數) + 季別(Q1, Q2, Q3, Q4, H1, H2, Y1)
    2. 其他報表（balance_sheet、cash_flow、income_statement）：
       - 季別格式標準化：1, 2, 3, 4 → Q1, Q2, Q3, Q4
       - 確保公司代號為文字格式

    Args:
        df: 資料框
        report_name: 報表名稱

    Returns:
        處理後的資料框
    """
    if df.empty:
        return df

    df_processed = df.copy()

    # 通用處理：確保公司代號為文字格式
    if "公司代號" in df_processed.columns:
        df_processed["公司代號"] = df_processed["公司代號"].astype(str)

    print(f"🔧 {report_name} 正在處理欄位標準化...")

    # 1. 統一欄位重新命名 (所有報表)
    rename_mapping = {}
    if "公司代號" in df_processed.columns:
        rename_mapping["公司代號"] = "代號"
    if "公司名稱" in df_processed.columns:
        rename_mapping["公司名稱"] = "名稱"

    if rename_mapping:
        df_processed = df_processed.rename(columns=rename_mapping)
        print(f"   欄位重新命名: {rename_mapping}")

    # 2. 年度處理：保持民國年格式
    if "年度" in df_processed.columns:
        # 確保年度為整數格式，但保持民國年
        year_numeric = pd.to_numeric(df_processed["年度"], errors='coerce')
        df_processed["年度"] = year_numeric.astype('Int64')
        print(f"   年度保持民國年格式")

    # 3. 股利資料表專屬處理
    if report_name == "dividend":

        # 拆分公司代號名稱欄位
        if "公司代號名稱" in df_processed.columns:
            print(f"   正在拆分公司代號名稱欄位...")

            # 拆分公司代號名稱 (格式: "1234 - 公司名稱")
            company_info = df_processed["公司代號名稱"].str.split(" - ", n=1, expand=True)

            # 新增代號和名稱欄位
            df_processed["代號"] = company_info[0].str.strip()
            df_processed["名稱"] = company_info[1].str.strip()

            # 移除原始的公司代號名稱欄位
            df_processed = df_processed.drop(columns=["公司代號名稱"])

            print(f"   成功拆分公司代號名稱欄位")

        # 拆分股利所屬年(季)度欄位
        if "股利所屬年(季)度" in df_processed.columns:
            print(f"   正在拆分股利所屬年(季)度欄位...")

            # 提取年度 (例如: "113年 年度" → 113)
            year_match = df_processed["股利所屬年(季)度"].str.extract(r'(\d+)年')[0]
            year_numeric = pd.to_numeric(year_match, errors='coerce').astype('Int64')
            # 保持民國年格式
            df_processed["年度"] = year_numeric

            # 提取季別並標準化
            def standardize_dividend_period(period_str):
                if pd.isna(period_str):
                    return None

                period_str = str(period_str).strip()

                # 年度
                if "年度" in period_str:
                    return "Y1"
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

            df_processed["季別"] = df_processed["股利所屬年(季)度"].apply(standardize_dividend_period)

            # 移除原始的股利所屬年(季)度欄位
            df_processed = df_processed.drop(columns=["股利所屬年(季)度"])

            print(f"   成功拆分股利所屬年(季)度欄位")

    # 4. 其他報表（balance_sheet、cash_flow、income_statement）處理
    elif report_name in ["balance_sheet", "cash_flow", "income_statement"]:

        # 標準化季別格式：1, 2, 3, 4 → Q1, Q2, Q3, Q4
        if "季別" in df_processed.columns:
            print(f"   正在標準化季別格式...")

            def standardize_quarter(quarter_val):
                if pd.isna(quarter_val):
                    return None

                quarter_str = str(quarter_val).strip()

                if quarter_str == "1":
                    return "Q1"
                elif quarter_str == "2":
                    return "Q2"
                elif quarter_str == "3":
                    return "Q3"
                elif quarter_str == "4":
                    return "Q4"
                else:
                    return quarter_str  # 保持原值如果不是1-4

            df_processed["季別"] = df_processed["季別"].apply(standardize_quarter)

            print(f"   季別標準化完成：1,2,3,4 → Q1,Q2,Q3,Q4")

    # 5. 重新排列欄位順序（所有報表統一）
    cols = df_processed.columns.tolist()
    priority_cols = []

    for col_name in ['代號', '名稱', '年度', '季別']:
        if col_name in cols:
            priority_cols.append(col_name)
            cols.remove(col_name)

    # 重新組合欄位順序
    new_cols = priority_cols + cols
    df_processed = df_processed[new_cols]

    # 6. 數值欄位轉換
    numeric_columns = {
        'balance_sheet': [
            '歸屬於母公司業主之權益合計',
            '不動產及設備－淨額',
            '無形資產－淨額',
            '流動資產',
            '資產總額',
            '流動負債',
            '非流動負債',
            '非控制權益',
            '每股參考淨值'
        ],
        'income_statement': [
            '淨利（損）歸屬於母公司業主',
            '營業收入',
            '營業成本',
            '稅後淨利',
            '基本每股盈餘（元）'
        ],
        'dividend': [
            '股東配發-盈餘分配之現金股利(元/股)',
            '股東配發-法定盈餘公積發放之現金(元/股)',
            '股東配發-資本公積發放之現金(元/股)',
            '股東配發-股東配發之現金(股利)總金額(元)',
            '股東配發-盈餘轉增資配股(元/股)',
            '股東配發-法定盈餘公積轉增資配股(元/股)',
            '股東配發-資本公積轉增資配股(元/股)',
            '股東配發-股東配股總股數(股)'
        ],
        'cash_flow': [
            '營業活動之淨現金流入（流出）'
        ],
        'etf_dividend': [
            '配息',
            '公告年度'
        ]
    }

    if report_name in numeric_columns:
        columns_to_convert = numeric_columns[report_name]
        existing_numeric_cols = [col for col in columns_to_convert if col in df_processed.columns]

        if existing_numeric_cols:
            print(f"   轉換數值欄位: {existing_numeric_cols}")
            for col in existing_numeric_cols:
                # 清理數值：移除逗號、空格、特殊字符
                df_processed[col] = df_processed[col].astype(str).str.replace(',', '')
                df_processed[col] = df_processed[col].str.replace(' ', '')
                df_processed[col] = df_processed[col].str.replace('--', '')
                df_processed[col] = df_processed[col].str.replace('-', '')
                df_processed[col] = df_processed[col].replace(['', 'nan', 'None', 'null'], None)

                # 轉換為數值
                df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')

            print(f"   ✅ 成功轉換 {len(existing_numeric_cols)} 個數值欄位")

    print(f"✅ {report_name} 欄位處理完成，統一格式：代號、名稱、年度(民國)、季別")

    return df_processed


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
# Helper: ETF Dividend Downloader
# =========================
def download_etf_dividend(year_str, year_dir):
    """下載 ETF 股利資料 - 優先CSV格式"""
    print(f"📈 下載 {year_str} ETF 股利資料...")

    # 民國年轉西元年
    roc_year = int(year_str)
    ad_year = roc_year + 1911

    # 設定日期範圍 (整年度)
    start_date = f"{ad_year}0101"
    end_date = f"{ad_year + 1}0101"  # 修正：下一年的1月1日

    # ETF 股利 API URL (使用您提供的格式)
    csv_url = f"https://www.twse.com.tw/rwd/zh/ETF/etfDiv?stkNo=&startDate={start_date}&endDate={end_date}&response=csv"
    json_url = f"https://www.twse.com.tw/rwd/zh/ETF/etfDiv?stkNo=&startDate={start_date}&endDate={end_date}&response=json"

    csv_filename = f"etf_dividend_{ad_year}.csv"
    csv_path = os.path.join(year_dir, csv_filename)

    # 優先嘗試 CSV 下載
    print(f"🔗 優先嘗試 CSV: {csv_url}")

    try:
        response = requests.get(csv_url, headers=headers, verify=False, timeout=30)
        response.encoding = "utf-8"

        if response.status_code == 200 and len(response.text.strip()) > 100:
            # 儲存 CSV 內容到 raw_data
            with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
                f.write(response.text)

            # 檢查是否為有效的CSV檔案
            try:
                test_df = pd.read_csv(csv_path, encoding="utf-8-sig", nrows=5)
                if not test_df.empty and len(test_df.columns) > 3:
                    print(f"✅ ETF 股利 CSV 下載成功: {csv_path}")
                    return True
                else:
                    print(f"⚠️ CSV 檔案格式異常，嘗試 JSON 下載")
                    os.remove(csv_path)
            except Exception as e:
                print(f"⚠️ CSV 檔案讀取失敗: {e}，嘗試 JSON 下載")
                if os.path.exists(csv_path):
                    os.remove(csv_path)
        else:
            print(f"⚠️ CSV 回應異常: status={response.status_code}, length={len(response.text)}")

    except Exception as e:
        print(f"⚠️ CSV 下載失敗: {e}")

    # CSV 失敗，嘗試 JSON 下載並轉換為 CSV
    print(f"🔄 嘗試 JSON 下載: {json_url}")

    try:
        response = requests.get(json_url, headers=headers, verify=False, timeout=30)
        response.encoding = "utf-8"

        if response.status_code == 200:
            data = response.json()

            # 檢查是否有資料
            if 'data' in data and len(data['data']) > 0:
                # 解析 JSON 資料並轉為 DataFrame
                fields = data.get('fields', [])
                rows = data.get('data', [])

                if fields and rows:
                    df = pd.DataFrame(rows, columns=fields)

                    # 儲存為 CSV 格式到 raw_data
                    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

                    print(f"✅ ETF 股利 JSON→CSV 轉換成功: {len(df)} 筆資料")
                    return True
                else:
                    print(f"⚠️ JSON 資料格式異常")
            else:
                print(f"⚠️ {year_str} 無 ETF 股利資料")
                print(f"API 回應: {data}")
        else:
            print(f"❌ JSON API 請求失敗: {response.status_code}")

    except Exception as e:
        print(f"❌ JSON 下載失敗: {e}")

    return False


# =========================
# Helper: clean ETF dividend CSV
# =========================
def clean_etf_dividend_csv(path: str) -> pd.DataFrame:
    """清理 ETF 股利 CSV 檔案"""
    print(f"🧹 清理 ETF 股利檔案: {os.path.basename(path)}")

    # 先讀取文本找到真正的表頭位置
    with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
        lines = f.readlines()

    header_idx = None
    for i, line in enumerate(lines):
        # 尋找包含 ETF 相關欄位的表頭行
        if any(keyword in line for keyword in ['代號', '證券代號', 'ETF', '名稱', '證券簡稱', '除息交易日']):
            if line.count(',') > 2:  # 確保是表格開頭
                header_idx = i
                break

    if header_idx is None:
        print(f"⚠️ 無法在 {os.path.basename(path)} 找到有效的表頭")
        # 嘗試直接讀取
        try:
            df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
            if not df.empty:
                return df
        except:
            pass
        return pd.DataFrame()

    # 用 pandas 載入，跳過前面的說明行
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", dtype=str, engine="python",
                        on_bad_lines="skip", skiprows=header_idx)
    except Exception as e:
        print(f"⚠️ 無法讀取 {os.path.basename(path)}: {e}")
        return pd.DataFrame()

    # 檢查是否有資料
    if df.empty:
        print(f"⚠️ {os.path.basename(path)} 為空檔案")
        return pd.DataFrame()

    # 移除全空列和 Unnamed 欄位
    df = df.dropna(how="all")
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    print(f"✅ {os.path.basename(path)} 清理完成，保留 {len(df)} 行")

    return df


# =========================
# Helper: process ETF dividend data (與dividend同步格式)
# =========================
def process_etf_dividend_data(df, year_str):
    """處理 ETF 股利資料 - 與dividend格式同步"""
    if df.empty:
        return df

    df_processed = df.copy()

    print(f"🔧 ETF 股利資料處理中...")

    # 1. 欄位重新命名 (與dividend格式同步)
    if '證券代號' in df_processed.columns:
        df_processed = df_processed.rename(columns={'證券代號': '代號'})
        print(f"   證券代號 → 代號")

    if '證券簡稱' in df_processed.columns:
        df_processed = df_processed.rename(columns={'證券簡稱': '名稱'})
        print(f"   證券簡稱 → 名稱")

    if '收益分配金額 (每1受益權益單位)' in df_processed.columns:
        df_processed = df_processed.rename(columns={'收益分配金額 (每1受益權益單位)': '配息'})
        print(f"   收益分配金額 (每1受益權益單位) → 配息")

    # 2. 年度處理：保持民國年格式
    roc_year = int(year_str)
    df_processed['年度'] = roc_year  # 直接使用民國年
    print(f"   年度設為: {roc_year} (民國年)")

    # 3. 季別處理：依除息交易日判斷月份 (參考dividend格式)
    if '除息交易日' in df_processed.columns:
        print(f"   正在分析除息交易日以判斷月份...")

        def determine_month_from_date(date_str):
            """從除息交易日判斷月份 (參考dividend格式)"""
            if pd.isna(date_str) or date_str == '':
                return None

            date_str = str(date_str).strip()

            # 嘗試提取月份
            # 格式可能是: 114年01月22日, 2024/01/22, 01/22, 等
            import re

            # 匹配各種日期格式中的月份
            month_patterns = [
                r'(\d+)年(\d+)月',  # 114年01月22日
                r'(\d{4})[/-](\d{1,2})[/-]',  # 2024/01/22 或 2024-01-22
                r'(\d{1,2})[/-](\d{1,2})',  # 01/22
            ]

            month = None
            for pattern in month_patterns:
                match = re.search(pattern, date_str)
                if match:
                    if '年' in pattern:
                        month = int(match.group(2))  # 月份是第二組
                    else:
                        month = int(match.group(2)) if len(match.groups()) > 1 else int(match.group(1))
                    break

            if month is None:
                return "OTHER"

            # 根據月份返回格式 (參考dividend的M{月份}格式)
            if 1 <= month <= 12:
                return f"M{month:02d}"  # M01, M02, ..., M12
            else:
                return "OTHER"

        df_processed['季別'] = df_processed['除息交易日'].apply(determine_month_from_date)

        # 統計月份分布
        month_counts = df_processed['季別'].value_counts()
        print(f"   月份分布: {dict(month_counts)}")
    else:
        df_processed['季別'] = "OTHER"
        print(f"   無除息交易日欄位，季別設為 OTHER")

    # 4. 確保關鍵欄位格式正確
    if '代號' in df_processed.columns:
        df_processed['代號'] = df_processed['代號'].astype(str)

    if '名稱' in df_processed.columns:
        df_processed['名稱'] = df_processed['名稱'].astype(str)

    # 5. 數值欄位轉換
    numeric_columns = ['配息', '公告年度']
    existing_numeric_cols = [col for col in numeric_columns if col in df_processed.columns]

    if existing_numeric_cols:
        print(f"   轉換數值欄位: {existing_numeric_cols}")
        for col in existing_numeric_cols:
            # 清理數值：移除逗號、空格、特殊字符
            df_processed[col] = df_processed[col].astype(str).str.replace(',', '')
            df_processed[col] = df_processed[col].str.replace(' ', '')
            df_processed[col] = df_processed[col].str.replace('--', '')
            df_processed[col] = df_processed[col].str.replace('-', '')
            df_processed[col] = df_processed[col].replace(['', 'nan', 'None', 'null'], None)

            # 轉換為數值
            df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')

        print(f"   ✅ 成功轉換 {len(existing_numeric_cols)} 個數值欄位")

    # 6. 重新排列欄位順序 (與dividend同步)
    cols = df_processed.columns.tolist()
    priority_cols = []

    for col_name in ['代號', '名稱', '年度', '季別']:
        if col_name in cols:
            priority_cols.append(col_name)
            cols.remove(col_name)

    # 重新組合欄位順序
    new_cols = priority_cols + cols
    df_processed = df_processed[new_cols]

    print(f"✅ ETF 股利資料處理完成: {len(df_processed)} 筆")
    print(f"   最終欄位順序: {new_cols[:6]}...")  # 顯示前6個欄位

    return df_processed


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

            # ETF 股利處理
            if report_name == "etf_dividend":
                download_etf_dividend(year_str, year_dir)
            else:
                # 一般報表處理
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
                elif report_name == "etf_dividend":
                    # 使用專門的 ETF 股利清理函數
                    df = clean_etf_dividend_csv(path)
                    if not df.empty:
                        df = process_etf_dividend_data(df, year_str)
                else:
                    df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
                    df = df.dropna(how="all")

                # 先不過濾欄位，保留所有資料進行合併
                if not df.empty:
                    all_dfs.append(df)
            except Exception as e:
                print(f"Read {fname} failed: {e}")

        # Step 4: 合併後再過濾欄位和排序
        if all_dfs:
            # 先合併所有資料
            combined_df = pd.concat(all_dfs, ignore_index=True)
            print(f"📊 合併完成，總計 {len(combined_df)} 行，{len(combined_df.columns)} 欄")

            # 先整理欄位：統一欄位名稱和格式
            combined_df = process_company_code_name(combined_df, report_name)

            # 然後過濾欄位 (使用統一後的欄位名稱)
            combined_df = filter_columns(combined_df, report_name)

            # 依代號排序 (ETF 與 dividend 格式統一)
            if report_name == "etf_dividend":
                if '代號' in combined_df.columns:
                    combined_df = combined_df.sort_values(by='代號', ascending=True, ignore_index=True)
                    print(f"🔢 {report_name} 依 '代號' 排序")
            else:
                combined_df = sort_by_company_code(combined_df, report_name)

            csv_path = json_path = None

            if "csv" in save_format:
                csv_path = os.path.join(csv_output_dir, f"{year_str}-{report_name}.csv")
                combined_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
                print(f"✅ CSV saved: {csv_path}")

            if "json" in save_format:
                json_path = os.path.join(json_output_dir, f"{year_str}-{report_name}.json")
                combined_df.to_json(json_path, orient="records", force_ascii=False, indent=2)
                print(f"✅ JSON saved: {json_path}")

            write_log(year_str, report_name, csv_path, json_path, len(combined_df))
            print(f"📝 Log updated for {year_str} {report_name} - Total rows: {len(combined_df)}")
        else:
            print(f"❌ {year_str} {report_name} no valid CSVs to merge")

print("\n🎉 所有處理完成！")