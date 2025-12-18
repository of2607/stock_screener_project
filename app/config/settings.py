"""
TWSE 資料下載工具 - 基本設定
"""

from typing import List, Dict
import os
from utils.date_utils import get_current_roc_year

# =========================
# 基本處理參數
# =========================
START_YEAR: int = get_current_roc_year()
END_YEAR: int = get_current_roc_year()
MARKETS: List[str] = ["sii", "otc"]
SEASONS: List[str] = ["01", "02", "03", "04"]

# =========================
# 處理選項
# =========================
DOWNLOAD_REPORTS: List[str] = ['all']  # 處理所有報表類型
SAVE_FORMAT: List[str] = ['csv', 'json']  # 可為 ['csv'], ['json'], ['csv', 'json']
ENABLE_DOWNLOAD_REPORTS: bool = False # 是否下載報表資料
ENABLE_MERGE_REPORTS: bool = False # 是否合併報表資料
ENABLE_PRECOMPUTE_METRICS: bool = False # 是否預計算長表
ENABLE_SUMMARY_REPORT: bool = True # 是否自動產生彙總報表
UPLOAD_SUMMARY_REPORT: bool = True # 是否上傳自動產生的彙總報表
UPLOAD_SUMMARY_REPORT_TARGET: List[str] = ["all"]  # r2, gdrive, supabase

# =========================
# 目錄設定
# =========================
LOG_DIR_BASE: str = "datas/logs"
RAW_DATA_DIR: str = "datas/raw_data"
MERGED_DATA_DIR: str = "datas/merged_data"
MERGED_CSV_DIR: str = os.path.join(MERGED_DATA_DIR, "csv")
MERGED_JSON_DIR: str = os.path.join(MERGED_DATA_DIR, "json")
MERGED_LOG_DIR: str = os.path.join(LOG_DIR_BASE, "log.json")

# =========================
# 預計算長表相關路徑
# =========================
PRECOMPUTED_METRICS_DIR: str = "datas/precomputed_metrics"
HISTORICAL_METRICS_FILE: str = os.path.join(PRECOMPUTED_METRICS_DIR, "historical_metrics.csv")
METRICS_UPDATE_LOG_FILE: str = os.path.join(PRECOMPUTED_METRICS_DIR, "update_log.json")

# =========================
# Summary Report 相關預設路徑
# =========================
REPORT_DATA_DIR: str = "datas/reports_data"
REPORT_CSV_DIR: str = os.path.join(REPORT_DATA_DIR, "csv/summary_report.csv")
REPORT_JSON_DIR: str = os.path.join(REPORT_DATA_DIR, "json/summary_report.json")
SUMMARY_FROM_DIR: str = MERGED_CSV_DIR
SUMMARY_PRICE_FILE: str = os.path.join(MERGED_CSV_DIR, "latest_stock_prices.csv")
SUMMARY_LOG_DIR: str = os.path.join(LOG_DIR_BASE, "summary_report_log.json")
SUMMARY_YEARS = 8  # 預設彙總報表的年份數

# =========================
# 上傳相關獨立設定
# =========================
# Google Drive Apps Script 設定 ( as@poohsreg )
GDRIVE_AS_URL: str = "https://script.google.com/macros/s/AKfycbxpdvseNcLthew9C9VPAFiCtzDQdDvTcHWN0hc2x9PCusZhvoWmWyvOKJIzzFyabTk8kA/exec"
GDRIVE_FOLDER_ID: str = "12ar4cYTO8zq8xgLY76cljn5b_bA2q6ux"
# Supabase 設定 ( supabase@of2607 )
SUPABASE_URL: str = "https://btzjjozytwtbgdznralj.supabase.co"
SUPABASE_KEY: str = "sb_secret_fjzIJ8cx3xnsE8xQ18-AMw_ctrb8gkD"
SUPABASE_BUCKET: str = "public-data/reports"
# Cloudflare R2 設定 ( cloudflare@of2607 )
R2_ACCOUNT_ID: str = "67d73f06307398dd3a9f766976a5efec"
R2_ACCESS_KEY: str = "7a33a736d0b434c6d92348f8d3778329"
R2_SECRET_KEY: str = "0e6a600b9e4c54ffdd313f4575600001ccef79cd19c02aa39ebb97f1f1d9efaf"
R2_BUCKET: str = "stock-reports"
R2_REGION: str = "auto"

# =========================
# HTTP 設定
# =========================
HEADERS: Dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
}

REQUEST_TIMEOUT: int = 30
RETRY_ATTEMPTS: int = 3
RETRY_DELAY: float = 2.0

# =========================
# 股價相關設定
# =========================
# 股價過濾條件
STOCK_MIN_PRICE: float = 0  # 最小股價門檻

# 原始資料保留天數
RAW_DATA_RETENTION_DAYS: int = 7  # 保留7天的原始資料

# =========================
# 自動建立必要目錄
# =========================
def ensure_directories() -> None:
    """確保所有必要的目錄存在"""
    directories = [MERGED_DATA_DIR, MERGED_CSV_DIR, MERGED_JSON_DIR, RAW_DATA_DIR]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)