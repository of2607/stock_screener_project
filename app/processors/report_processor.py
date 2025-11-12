"""
TWSE 資料下載工具 - 報表處理器
"""
import os
import pandas as pd
from typing import List
from utils.logger import Logger
from processors.csv_cleaner import CSVCleaner
from processors.data_standardizer import DataStandardizer
from processors.column_filter import ColumnFilter
from processors.data_sorter import DataSorter


class ReportProcessor:
    """報表處理器 - 負責單一報表的完整處理流程"""
    
    def __init__(self, logger: Logger):
        """
        初始化報表處理器
        
        Args:
            logger: 日誌記錄器
        """
        self.logger = logger
        self.csv_cleaner = CSVCleaner(logger)
        self.data_standardizer = DataStandardizer(logger)
        self.column_filter = ColumnFilter(logger)
        self.data_sorter = DataSorter(logger)
    
    def process_year_data(self, report_name: str, year_str: str, year_dir: str) -> pd.DataFrame:
        """
        處理特定年度的報表資料
        
        Args:
            report_name: 報表類型
            year_str: 年度
            year_dir: 資料目錄
            
        Returns:
            處理後的資料框
        """
        # 1. 載入並清理 CSV 檔案
        dataframes = self._load_and_clean_csv_files(report_name, year_str, year_dir)
        
        if not dataframes:
            self.logger.warning(f"❌ {year_str} {report_name} 沒有有效的 CSV 檔案")
            return pd.DataFrame()
        
        # 2. 合併與處理資料
        processed_df = self._merge_and_process_data(dataframes, report_name, year_str)
        
        if processed_df.empty:
            self.logger.warning(f"❌ {year_str} {report_name} 處理後無資料")
            return pd.DataFrame()
        
        return processed_df
    
    def _load_and_clean_csv_files(
        self, 
        report_name: str, 
        year_str: str, 
        year_dir: str
    ) -> List[pd.DataFrame]:
        """載入並清理 CSV 檔案"""
        csv_files = [f for f in os.listdir(year_dir) if f.endswith(".csv")]
        self.logger.info(f"📁 找到 {len(csv_files)} 個 CSV 檔案")
        
        dataframes = []
        
        for filename in csv_files:
            file_path = os.path.join(year_dir, filename)
            
            try:
                df = self._clean_single_csv_file(report_name, file_path, year_str)
                if not df.empty:
                    dataframes.append(df)
            except Exception as e:
                self.logger.warning(f"處理檔案 {filename} 失敗: {e}")
        
        return dataframes
    
    def _clean_single_csv_file(
        self, 
        report_name: str, 
        file_path: str, 
        year_str: str
    ) -> pd.DataFrame:
        """清理單一 CSV 檔案"""
        if report_name == "dividend":
            return self.csv_cleaner.clean_dividend_csv(file_path)
        elif report_name == "etf_dividend":
            df = self.csv_cleaner.clean_etf_dividend_csv(file_path)
            if not df.empty:
                # 使用 data_standardizer 處理 ETF 資料
                df = self.data_standardizer.process_etf_dividend_data(df, year_str)
            return df
        else:
            return self.csv_cleaner.clean_standard_csv(file_path)
    
    def _merge_and_process_data(
        self, 
        dataframes: List[pd.DataFrame], 
        report_name: str, 
        year_str: str
    ) -> pd.DataFrame:
        """合併並處理資料"""
        # 1. 合併所有資料框
        combined_df = pd.concat(dataframes, ignore_index=True)
        self.logger.info(f"📊 合併完成，總計 {len(combined_df)} 行，{len(combined_df.columns)} 欄")
        
        # 2. 先整理欄位：統一欄位名稱和格式 (僅對非 ETF 股利資料)
        if report_name != "etf_dividend":
            combined_df = self.data_standardizer.standardize_data(combined_df, report_name)
        
        # 3. 然後過濾欄位 (使用統一後的欄位名稱)
        combined_df = self.column_filter.filter_columns(combined_df, report_name)
        
        # 4. 依代號排序 (ETF 與 dividend 格式統一)
        if report_name == "etf_dividend":
            if '代號' in combined_df.columns:
                combined_df = combined_df.sort_values(by='代號', ascending=True, ignore_index=True)
                self.logger.debug(f"🔢 {report_name} 依 '代號' 排序")
        else:
            combined_df = self.data_sorter.sort_by_company_code(combined_df, report_name)
        
        return combined_df