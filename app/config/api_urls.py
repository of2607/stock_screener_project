"""
TWSE 資料下載工具 - API 網址設定
"""
from typing import Dict, Any

# =========================
# 報表類型與 API 設定
# =========================
REPORT_TYPES: Dict[str, Dict[str, str]] = {
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

def get_ajax_url(report_type: str, year: str, market: str, season: str = None) -> str:
    """
    取得指定報表的 AJAX URL
    
    Args:
        report_type: 報表類型
        year: 年度
        market: 市場別 (sii/otc)
        season: 季別 (dividend 不需要)
        
    Returns:
        格式化的 AJAX URL
        
    Raises:
        KeyError: 不支援的報表類型
        ValueError: 缺少必要參數
    """
    if report_type not in REPORT_TYPES:
        raise KeyError(f"不支援的報表類型: {report_type}")
    
    config = REPORT_TYPES[report_type]
    
    if "ajax" not in config:
        raise ValueError(f"報表類型 {report_type} 沒有 ajax 設定")
    
    if report_type == "dividend":
        return config["ajax"].format(year=year, market=market)
    else:
        if season is None:
            raise ValueError(f"報表類型 {report_type} 需要提供 season 參數")
        return config["ajax"].format(year=year, market=market, season=season)

def get_download_url(report_type: str, filename: str) -> str:
    """
    取得檔案下載 URL
    
    Args:
        report_type: 報表類型
        filename: 檔案名稱
        
    Returns:
        完整的下載 URL
        
    Raises:
        KeyError: 不支援的報表類型
    """
    if report_type not in REPORT_TYPES:
        raise KeyError(f"不支援的報表類型: {report_type}")
    
    config = REPORT_TYPES[report_type]
    
    if "download_base" not in config:
        raise ValueError(f"報表類型 {report_type} 沒有 download_base 設定")
    
    return f"{config['download_base']}?firstin=true&step=10&filename={filename}"

def get_etf_urls(start_date: str, end_date: str) -> Dict[str, str]:
    """
    取得 ETF 股利資料的 URL
    
    Args:
        start_date: 開始日期 (格式: YYYYMMDD)
        end_date: 結束日期 (格式: YYYYMMDD)
        
    Returns:
        包含 CSV 和 JSON URL 的字典
    """
    config = REPORT_TYPES["etf_dividend"]
    return {
        "csv": config["csv_export"].format(start_date=start_date, end_date=end_date),
        "json": config["url"].format(start_date=start_date, end_date=end_date)
    }