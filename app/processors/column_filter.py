"""
TWSE 資料下載工具 - 欄位過濾器
"""
import pandas as pd
from typing import List
from utils.logger import Logger
from config.column_configs import get_columns_to_keep


class ColumnFilter:
    """欄位過濾器 - 統一處理欄位過濾需求"""
    
    def __init__(self, logger: Logger):
        """
        初始化欄位過濾器
        
        Args:
            logger: 日誌記錄器
        """
        self.logger = logger
    
    def filter_columns(self, df: pd.DataFrame, report_type: str) -> pd.DataFrame:
        """
        根據設定過濾欄位
        
        Args:
            df: 要過濾的資料框
            report_type: 報表類型
            
        Returns:
            過濾後的資料框
        """
        if df.empty:
            return df
        
        try:
            columns_to_keep = get_columns_to_keep(report_type)
        except KeyError:
            self.logger.info(f"📋 {report_type} 未設定欄位過濾，保留所有 {len(df.columns)} 欄")
            return df
        
        existing_columns = [col for col in columns_to_keep if col in df.columns]
        
        if existing_columns:
            missing_columns = set(columns_to_keep) - set(existing_columns)
            if missing_columns:
                self.logger.warning(f"{report_type} 找不到欄位: {list(missing_columns)}")
            
            self.logger.info(f"📋 {report_type} 欄位過濾: {len(df.columns)} → {len(existing_columns)} 欄")
            self.logger.debug(f"   保留欄位: {existing_columns}")
            return df[existing_columns].copy()
        else:
            self.logger.warning(f"{report_type} 找不到任何指定的欄位，保留所有 {len(df.columns)} 欄")
            return df
    
    def get_available_columns(self, df: pd.DataFrame) -> List[str]:
        """
        取得資料框中可用的欄位清單
        
        Args:
            df: 資料框
            
        Returns:
            欄位名稱清單
        """
        return df.columns.tolist()
    
    def check_required_columns(self, df: pd.DataFrame, required_columns: List[str]) -> bool:
        """
        檢查是否包含必要欄位
        
        Args:
            df: 資料框
            required_columns: 必要欄位清單
            
        Returns:
            是否包含所有必要欄位
        """
        missing_columns = set(required_columns) - set(df.columns)
        
        if missing_columns:
            self.logger.warning(f"缺少必要欄位: {list(missing_columns)}")
            return False
        
        return True