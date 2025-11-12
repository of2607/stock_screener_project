"""
TWSE 資料下載工具 - 資料排序器
"""
import pandas as pd
from typing import Optional
from utils.logger import Logger


class DataSorter:
    """資料排序器 - 統一處理各種排序需求"""
    
    def __init__(self, logger: Logger):
        """
        初始化資料排序器
        
        Args:
            logger: 日誌記錄器
        """
        self.logger = logger
    
    def sort_by_company_code(self, df: pd.DataFrame, report_name: str) -> pd.DataFrame:
        """
        依公司代號排序
        
        Args:
            df: 要排序的資料框
            report_name: 報表名稱 (用於日誌)
            
        Returns:
            排序後的資料框
        """
        if df.empty:
            return df
        
        company_code_col = self._find_company_code_column(df)
        
        if company_code_col is None:
            self.logger.warning(f"{report_name} 找不到代號欄位，跳過排序")
            return df
        
        self.logger.debug(f"{report_name} 依 '{company_code_col}' 排序")
        
        return self._sort_by_numeric_code(df, company_code_col)
    
    def _find_company_code_column(self, df: pd.DataFrame) -> Optional[str]:
        """
        找出代號欄位
        
        Args:
            df: 資料框
            
        Returns:
            代號欄位名稱，找不到則回傳 None
        """
        # 優先順序：代號 > 公司代號 > 公司代號名稱 > 包含"代號"的欄位
        priority_columns = ["代號", "公司代號", "公司代號名稱"]
        
        for col_name in priority_columns:
            if col_name in df.columns:
                return col_name
        
        # 找包含"代號"的欄位
        for col in df.columns:
            if "代號" in col:
                return col
        
        return None
    
    def _sort_by_numeric_code(self, df: pd.DataFrame, code_column: str) -> pd.DataFrame:
        """
        依數字代號排序 (支援 "1234 - 公司名稱" 格式)
        
        Args:
            df: 資料框
            code_column: 代號欄位名稱
            
        Returns:
            排序後的資料框
        """
        df_sorted = df.copy()
        
        # 提取數字部分作為排序鍵
        df_sorted['_sort_key'] = (
            df_sorted[code_column]
            .astype(str)
            .str.extract(r'(\d+)')[0]
        )
        df_sorted['_sort_key'] = pd.to_numeric(df_sorted['_sort_key'], errors='coerce')
        
        # 按數字排序
        df_sorted = df_sorted.sort_values(
            by='_sort_key', 
            ascending=True, 
            ignore_index=True
        )
        
        # 移除臨時排序鍵
        df_sorted = df_sorted.drop(columns=['_sort_key'])
        
        return df_sorted
    
    def sort_by_columns(
        self, 
        df: pd.DataFrame, 
        columns: list, 
        ascending: bool = True
    ) -> pd.DataFrame:
        """
        依指定欄位排序
        
        Args:
            df: 要排序的資料框
            columns: 排序欄位清單
            ascending: 是否遞增排序
            
        Returns:
            排序後的資料框
        """
        if df.empty:
            return df
        
        # 過濾出存在的欄位
        existing_columns = [col for col in columns if col in df.columns]
        
        if not existing_columns:
            self.logger.warning(f"找不到排序欄位: {columns}")
            return df
        
        return df.sort_values(
            by=existing_columns,
            ascending=ascending,
            ignore_index=True
        )