"""
TWSE 資料下載工具 - CSV 清理器
"""
import pandas as pd
import os
from typing import Optional, List
from utils.logger import Logger


class CSVCleaner:
    """CSV 清理器 - 統一處理各種 CSV 清理需求"""
    
    def __init__(self, logger: Logger):
        """
        初始化 CSV 清理器
        
        Args:
            logger: 日誌記錄器
        """
        self.logger = logger
    
    def clean_dividend_csv(self, file_path: str) -> pd.DataFrame:
        """
        清理股利報表 CSV 檔案
        
        Args:
            file_path: CSV 檔案路徑
            
        Returns:
            清理後的資料框
        """
        self.logger.debug(f"清理股利檔案: {os.path.basename(file_path)}")
        
        # 1. 找到真正的表頭位置
        header_idx = self._find_dividend_header(file_path)
        
        if header_idx is None:
            self.logger.warning(f"無法在 {os.path.basename(file_path)} 找到公司代號欄位")
            return pd.DataFrame()
        
        # 2. 載入資料
        df = self._load_csv_with_header(file_path, header_idx)
        
        if df.empty:
            return df
        
        # 3. 基本清理
        df = self._basic_cleanup(df)
        
        # 4. 股利專用清理
        df = self._clean_dividend_specific_data(df)
        
        # 5. 最終排序
        df = self._final_sort_dividend(df)
        
        self.logger.success(f"{os.path.basename(file_path)} 清理完成，保留 {len(df)} 行")
        
        return df
    
    def clean_etf_dividend_csv(self, file_path: str) -> pd.DataFrame:
        """
        清理 ETF 股利 CSV 檔案
        
        Args:
            file_path: CSV 檔案路徑
            
        Returns:
            清理後的資料框
        """
        self.logger.debug(f"清理 ETF 股利檔案: {os.path.basename(file_path)}")
        
        # 1. 找到真正的表頭位置
        header_idx = self._find_etf_header(file_path)
        
        if header_idx is None:
            self.logger.warning(f"無法在 {os.path.basename(file_path)} 找到有效的表頭")
            # 嘗試直接讀取
            try:
                df = pd.read_csv(file_path, encoding="utf-8-sig", dtype=str)
                if not df.empty:
                    return self._basic_cleanup(df)
            except:
                pass
            return pd.DataFrame()
        
        # 2. 載入並清理資料
        df = self._load_csv_with_header(file_path, header_idx)
        
        if df.empty:
            return df
        
        df = self._basic_cleanup(df)
        
        self.logger.success(f"{os.path.basename(file_path)} 清理完成，保留 {len(df)} 行")
        
        return df
    
    def clean_standard_csv(self, file_path: str) -> pd.DataFrame:
        """
        清理標準財務報表 CSV 檔案
        
        Args:
            file_path: CSV 檔案路徑
            
        Returns:
            清理後的資料框
        """
        # 嘗試多種編碼格式
        encodings = ["utf-8-sig", "utf-8", "big5", "cp950", "gbk"]
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, dtype=str,
                               on_bad_lines="skip", engine="python")
                if not df.empty:
                    self.logger.debug(f"{os.path.basename(file_path)} 使用 {encoding} 編碼成功讀取")
                    return self._basic_cleanup(df)
            except Exception:
                continue
        
        self.logger.error(f"讀取 {os.path.basename(file_path)} 失敗: 嘗試所有編碼格式均失敗")
        return pd.DataFrame()
    
    def _find_dividend_header(self, file_path: str) -> Optional[int]:
        """找到股利報表的表頭位置"""
        try:
            with open(file_path, "r", encoding="utf-8-sig", errors="ignore") as f:
                lines = f.readlines()
            
            for i, line in enumerate(lines):
                # 尋找包含 "公司代號名稱" 或同時包含 "公司代號" 和 "公司名稱" 的表頭行
                if ("公司代號名稱" in line) or (("公司代號" in line) and ("公司名稱" in line)):
                    if line.count(",") > 2:  # 確保是表格開頭
                        return i
            
            return None
        except Exception:
            return None
    
    def _find_etf_header(self, file_path: str) -> Optional[int]:
        """找到 ETF 股利報表的表頭位置"""
        try:
            with open(file_path, "r", encoding="utf-8-sig", errors="ignore") as f:
                lines = f.readlines()
            
            for i, line in enumerate(lines):
                # 尋找包含 ETF 相關欄位的表頭行
                if any(keyword in line for keyword in ['代號', '證券代號', 'ETF', '名稱', '證券簡稱', '除息交易日']):
                    if line.count(',') > 2:  # 確保是表格開頭
                        return i
            
            return None
        except Exception:
            return None
    
    def _load_csv_with_header(self, file_path: str, header_idx: int) -> pd.DataFrame:
        """載入指定表頭位置的 CSV"""
        try:
            return pd.read_csv(
                file_path, 
                encoding="utf-8-sig", 
                dtype=str, 
                engine="python",
                on_bad_lines="skip", 
                skiprows=header_idx
            )
        except Exception as e:
            self.logger.error(f"無法讀取 {os.path.basename(file_path)}: {e}")
            return pd.DataFrame()
    
    def _basic_cleanup(self, df: pd.DataFrame) -> pd.DataFrame:
        """基本清理：移除空列、Unnamed 欄位"""
        if df.empty:
            return df
        
        # 移除全空列
        df = df.dropna(how="all")
        
        # 移除 Unnamed 欄位
        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
        
        return df
    
    def _clean_dividend_specific_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """股利資料專用清理"""
        if df.empty:
            return df
        
        # 確定第一欄的名稱
        first_col = df.columns[0]
        if "公司代號" not in first_col:
            self.logger.warning(f"第一欄不是公司代號欄位: {first_col}")
            return df
        
        # 按第一欄排序（將有問題的列排到一起）
        df = df.sort_values(by=first_col, ascending=True, ignore_index=True, na_position='last')
        
        # 移除有問題的列
        mask_to_keep = self._create_dividend_filter_mask(df, first_col)
        df_cleaned = df[mask_to_keep].copy()
        df_cleaned.reset_index(drop=True, inplace=True)
        
        return df_cleaned
    
    def _create_dividend_filter_mask(self, df: pd.DataFrame, first_col: str) -> pd.Series:
        """建立股利資料過濾遮罩"""
        mask = pd.Series([True] * len(df))
        
        for i, val in enumerate(df[first_col]):
            val_str = str(val).strip()
            
            # 跳過空值
            if val_str in ['nan', '', 'None']:
                mask[i] = False
                continue
            
            # 移除重複的表頭行
            if "公司代號" in val_str and " - " not in val_str:
                mask[i] = False
                continue
            
            # 移除不包含 " - " 的行（正常的公司代號應該是 "1234 - 公司名稱" 格式）
            if " - " not in val_str:
                mask[i] = False
                continue
            
            # 移除太短的行（可能是斷行造成的）
            if len(val_str) < 5:
                mask[i] = False
                continue
        
        return mask
    
    def _final_sort_dividend(self, df: pd.DataFrame) -> pd.DataFrame:
        """股利資料最終排序"""
        if df.empty:
            return df
        
        # 決定排序欄位
        sort_cols = []
        if "公司代號" in df.columns:
            sort_cols = [col for col in ["公司代號", "公司名稱", "股東會日期"] if col in df.columns]
        elif "公司代號名稱" in df.columns:
            sort_cols = [col for col in ["公司代號名稱", "股東會日期"] if col in df.columns]
        
        if sort_cols:
            df = df.sort_values(by=sort_cols, ascending=True, ignore_index=True)
        
        return df