"""
TWSE 資料下載工具 - 基本設定
"""
from typing import List, Dict
import os

# =========================
# 基本處理參數
# =========================
START_YEAR: int = 114
END_YEAR: int = 114
MARKETS: List[str] = ["sii", "otc"]
SEASONS: List[str] = ["01", "02", "03", "04"]

# =========================
# 處理選項
# =========================
DOWNLOAD_REPORTS: List[str] = ['all']  # 處理所有報表類型
SAVE_FORMAT: List[str] = ['csv', 'json']  # 可為 ['csv'], ['json'], ['csv', 'json']
ENABLE_DOWNLOAD_REPORTS: bool = False # 是否下載報表資料
ENABLE_MERGE_REPORTS: bool = False # 是否合併報表資料
ENABLE_SUMMARY_REPORT: bool = True # 是否自動產生彙總報表

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
# Summary Report 相關預設路徑
# =========================
REPORT_DATA_DIR: str = "datas/reports_data"
REPORT_CSV_DIR: str = os.path.join(REPORT_DATA_DIR, "csv/summary_report.csv")
REPORT_JSON_DIR: str = os.path.join(REPORT_DATA_DIR, "json/summary_report.json")
SUMMARY_FROM_DIR: str = MERGED_CSV_DIR
SUMMARY_PRICE_FILE: str = os.path.join(MERGED_CSV_DIR, "latest_stock_prices.csv")
SUMMARY_LOG_DIR: str = os.path.join(LOG_DIR_BASE, "summary_report_log.json")
SUMMARY_YEARS = 5  # 預設彙總報表的年份數

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
STOCK_MIN_PRICE: float = 10.0  # 最小股價門檻

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