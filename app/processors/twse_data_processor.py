import os
import sys
import shutil

from typing import List

current_dir = os.path.dirname(os.path.abspath(__file__))
app_dir = os.path.abspath(os.path.join(current_dir, ".."))
if app_dir not in sys.path:
    sys.path.insert(0, app_dir)

from utils.logger import Logger
from processors.report_processor import ReportProcessor
from downloaders.twse_downloader import TWSEDownloader
from downloaders.etf_downloader import ETFDownloader
# from config.settings import MERGED_LOG_DIR, DOWNLOAD_REPORTS, ensure_directories
from config.settings import (
        START_YEAR, END_YEAR, ENABLE_DOWNLOAD_REPORTS, ENABLE_MERGE_REPORTS, DOWNLOAD_REPORTS, SAVE_FORMAT,
        RAW_DATA_DIR, MERGED_CSV_DIR, MERGED_JSON_DIR, MERGED_LOG_DIR, ensure_directories
    )

class TWSEDataProcessor:
    """TWSE 資料處理主控制器 - 簡潔版"""
    
    def __init__(self):
        """初始化處理器"""
        self.logger = Logger(MERGED_LOG_DIR)
        self.report_processor = ReportProcessor(self.logger)
        self.twse_downloader = TWSEDownloader(self.logger)
        self.etf_downloader = ETFDownloader(self.logger)
        
        ensure_directories()
        
        # 支援的報表類型
        self.supported_reports = [
            "balance_sheet", "income_statement", "cash_flow", 
            "dividend", "etf_dividend"
        ]
    
    def process_all_reports(self) -> None:
        """處理所有報表"""
        self.logger.info("🚀 開始處理 TWSE 資料...")
        
        reports_to_process = self._get_reports_to_process()
        
        for report_name in reports_to_process:
            self._process_single_report(report_name)
        
        self.logger.success("🎉 所有處理完成！")
    
    def _get_reports_to_process(self) -> List[str]:
        """取得要處理的報表清單"""
        if DOWNLOAD_REPORTS and 'all' not in DOWNLOAD_REPORTS:
            return [r for r in DOWNLOAD_REPORTS if r in self.supported_reports]
        else:
            return self.supported_reports.copy()
    
    def _process_single_report(self, report_name: str) -> None:
        """處理單一報表類型"""
        self.logger.info(f"\n=== 開始處理 {report_name} ===")
        
        for year in range(START_YEAR, END_YEAR + 1):
            year_str = str(year)
            year_dir = os.path.join(RAW_DATA_DIR, report_name, year_str)
            
            # 1. 確保資料可用（下載或檢查現有資料）
            if not self._ensure_data_available(report_name, year_str, year_dir):
                continue
            
            # 2. 處理資料（使用專門的處理器）
            processed_df = self.report_processor.process_year_data(report_name, year_str, year_dir)
            
            if processed_df.empty:
                continue
            
            # 3. 儲存結果
            self._save_processed_data(processed_df, report_name, year_str)
    
    def _ensure_data_available(self, report_name: str, year_str: str, year_dir: str) -> bool:
        """確保資料可用（下載或檢查現有資料）"""
        ONLY_MERGE = ENABLE_MERGE_REPORTS and not ENABLE_DOWNLOAD_REPORTS
        if ONLY_MERGE:
            self.logger.progress(f"僅合併模式: 處理 {year_str} {report_name}")
            if not os.path.exists(year_dir):
                self.logger.error(f"找不到資料夾: {year_dir}")
                return False
            return True
        elif ENABLE_DOWNLOAD_REPORTS:
            self.logger.progress(f"下載模式: 處理 {year_str} {report_name}")
            return self._download_data(report_name, year_str, year_dir)
    
    def _download_data(self, report_name: str, year_str: str, year_dir: str) -> bool:
        """下載資料"""
        # 清理舊資料
        if os.path.exists(year_dir):
            shutil.rmtree(year_dir)
        os.makedirs(year_dir, exist_ok=True)
        
        try:
            if report_name == "etf_dividend":
                return self.etf_downloader.download_data(year_str, year_dir)
            else:
                return self.twse_downloader.download_data(year_str, report_name, year_dir)
        except Exception as e:
            self.logger.error(f"下載 {year_str} {report_name} 失敗: {e}")
            return False
    
    def _save_processed_data(self, df, report_name: str, year_str: str) -> None:
        """儲存處理後的資料"""
        csv_path = json_path = None
        
        # 儲存 CSV
        if "csv" in SAVE_FORMAT:
            csv_path = os.path.join(MERGED_CSV_DIR, f"{year_str}-{report_name}.csv")
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
            self.logger.success(f"CSV 已儲存: {csv_path}")
        
        # 儲存 JSON
        if "json" in SAVE_FORMAT:
            json_path = os.path.join(MERGED_JSON_DIR, f"{year_str}-{report_name}.json")
            df.to_json(json_path, orient="records", force_ascii=False, indent=2)
            self.logger.success(f"JSON 已儲存: {json_path}")
        
        # 寫入日誌
        self.logger.write_processing_log(year_str, report_name, csv_path, json_path, len(df))

def main() -> None:
    """主程式入口"""
    try:
        processor = TWSEDataProcessor()
        processor.process_all_reports()
    except KeyboardInterrupt:
        print("\n⚠️ 使用者中斷程式執行")
    except Exception as e:
        print(f"❌ 程式執行失敗: {e}")
        raise

if __name__ == "__main__":
    main()