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
ONLY_MERGE: bool = False  # 設為 True 時只做合併，不下載
DOWNLOAD_REPORTS: List[str] = ['all']  # 處理所有報表類型
SAVE_FORMAT: List[str] = ['csv', 'json']  # 可為 ['csv'], ['json'], ['csv', 'json']

# =========================
# 目錄設定
# =========================
BASE_DIR: str = "datas/raw_data"
MERGE_DIR: str = "datas/merged_data"
CSV_OUTPUT_DIR: str = os.path.join(MERGE_DIR, "csv")
JSON_OUTPUT_DIR: str = os.path.join(MERGE_DIR, "json")
LOG_PATH: str = os.path.join(MERGE_DIR, "log.json")

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
    directories = [MERGE_DIR, CSV_OUTPUT_DIR, JSON_OUTPUT_DIR, BASE_DIR]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)