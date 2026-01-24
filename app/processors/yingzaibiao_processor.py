"""
盈再表資料處理器
================

處理下載的 twlist.xlsx 檔案，轉換為 CSV 和 JSON 格式
"""
import os
import json
import csv
from pathlib import Path
from typing import Optional
import pandas as pd

from utils.logger import Logger
from config.settings import (
    YINGZAIBIAO_EXCEL_PATH,
    YINGZAIBIAO_CSV_PATH,
    YINGZAIBIAO_JSON_PATH
)
from config.column_configs import get_text_columns, get_numeric_columns


class YingZaiBiaoProcessor:
    """盈再表資料處理器 - 負責 xlsx 轉換、清理和標準化"""
    
    def __init__(self, logger: Logger):
        """
        初始化處理器
        
        Args:
            logger: 日誌記錄器
        """
        self.logger = logger
        # 台股路徑
        self.input_path = Path(YINGZAIBIAO_EXCEL_PATH)
        self.csv_output_path = Path(YINGZAIBIAO_CSV_PATH)
        self.json_output_path = Path(YINGZAIBIAO_JSON_PATH)
    
    def process_and_save(self) -> bool:
        """
        處理台股盈再表資料並儲存為 CSV 和 JSON
        
        Returns:
            是否成功處理
        """
        return self._process_market(
            self.input_path,
            self.csv_output_path,
            self.json_output_path,
            'yingzaibiao',
            '台股盈再表'
        )
    
    def process_us_and_save(self) -> bool:
        """
        處理美股盈再表資料並儲存為 CSV 和 JSON
        
        Returns:
            是否成功處理
        """
        # 取得美股檔案路徑
        from config.settings import YINGZAIBIAO_RAW_DIR
        
        us_input_path = Path(YINGZAIBIAO_RAW_DIR) / "uslist.xlsx"
        us_csv_path = Path(str(YINGZAIBIAO_CSV_PATH).replace('.csv', '_us.csv'))
        us_json_path = Path(str(YINGZAIBIAO_JSON_PATH).replace('.json', '_us.json'))
        
        return self._process_market(
            us_input_path,
            us_csv_path,
            us_json_path,
            'yingzaibiao_us',
            '美股盈再表'
        )
    
    def _process_market(
        self, 
        input_path: Path, 
        csv_path: Path, 
        json_path: Path, 
        column_config_key: str,
        market_name: str
    ) -> bool:
        """
        處理指定市場的盈再表資料
        
        Args:
            input_path: 輸入 Excel 檔案路徑
            csv_path: 輸出 CSV 檔案路徑
            json_path: 輸出 JSON 檔案路徑
            column_config_key: 欄位配置的 key (例如: 'yingzaibiao' 或 'yingzaibiao_us')
            market_name: 市場名稱 (用於日誌顯示)
            
        Returns:
            是否成功處理
        """
        self.logger.info(f"🔄 開始處理{market_name}資料...")
        
        # 1. 檢查輸入檔案是否存在
        if not input_path.exists():
            self.logger.error(f"找不到輸入檔案: {input_path}")
            return False
        
        try:
            # 2. 讀取 Excel 檔案
            self.logger.progress(f"讀取 {input_path.name}...")
            df = self._read_excel_file(input_path)
            
            if df is None or df.empty:
                self.logger.error("讀取檔案失敗或檔案為空")
                return False
            
            self.logger.info(f"讀取資料: {len(df)} 筆，{len(df.columns)} 欄")
            
            # 3. 清理和標準化資料
            self.logger.progress("清理資料...")
            df = self._clean_data(df, column_config_key)
            
            if df.empty:
                self.logger.warning("清理後資料為空")
                return False
            
            # 4. 儲存為 CSV
            self.logger.progress("儲存 CSV 檔案...")
            self._save_csv(df, csv_path, column_config_key)
            
            # 5. 儲存為 JSON
            self.logger.progress("儲存 JSON 檔案...")
            self._save_json(df, json_path)
            
            self.logger.success(f"{market_name}資料處理完成: {len(df)} 筆")
            return True
            
        except Exception as e:
            self.logger.error(f"處理資料時發生錯誤: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
            return False
    
    def _read_excel_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """
        讀取 Excel 檔案
        
        Args:
            file_path: Excel 檔案路徑
            
        Returns:
            DataFrame 或 None
        """
        try:
            # 嘗試讀取第一個工作表
            df = pd.read_excel(file_path, sheet_name=0)
            
            # 如果第一行是標題，pandas 會自動處理
            # 但如果有多餘的空行，需要清理
            df = df.dropna(how='all')  # 移除全空行
            
            return df
            
        except Exception as e:
            self.logger.error(f"讀取 Excel 檔案失敗: {e}")
            
            # 嘗試使用其他編碼或方法
            try:
                self.logger.debug("嘗試使用 openpyxl 引擎...")
                df = pd.read_excel(file_path, sheet_name=0, engine='openpyxl')
                df = df.dropna(how='all')
                return df
            except Exception as e2:
                self.logger.error(f"使用 openpyxl 引擎也失敗: {e2}")
                return None
    
    def _clean_data(self, df: pd.DataFrame, column_config_key: str) -> pd.DataFrame:
        """
        清理資料（針對 Google Sheets 相容性優化）
        
        Args:
            df: 原始 DataFrame
            column_config_key: 欄位配置的 key (例如: 'yingzaibiao' 或 'yingzaibiao_us')
            
        Returns:
            清理後的 DataFrame
        """
        # 移除全空行
        df = df.dropna(how='all')
        
        # 移除全空列
        df = df.dropna(axis=1, how='all')
        
        # 重置索引
        df = df.reset_index(drop=True)
        
        # 欄位名稱標準化（只保留英數字和底線，移除特殊符號）
        df.columns = df.columns.astype(str).str.strip()
        df.columns = df.columns.str.replace('\n', '_').str.replace('\r', '_').str.replace('\t', '_')
        df.columns = df.columns.str.replace(r'\s+', '_', regex=True)
        # 移除不安全的字符
        df.columns = df.columns.str.replace(r'[^\w\u4e00-\u9fff_]', '', regex=True)
        
        # 取得數值欄位清單
        numeric_cols = get_numeric_columns(column_config_key)
        
        # 處理非數值欄位的資料 - 轉為字串並清理
        for col in df.columns:
            if col in numeric_cols:
                continue  # 數值欄位稍後處理
            df[col] = df[col].astype(str).str.strip()  # 加上 strip() 移除多餘空白
            # 清理不可見字符
            df[col] = df[col].str.replace(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', regex=True)
            # 移除多餘換行和 tab
            df[col] = df[col].str.replace('\r\n', ' ', regex=False)
            df[col] = df[col].str.replace('\n', ' ', regex=False)
            df[col] = df[col].str.replace('\r', ' ', regex=False)
            df[col] = df[col].str.replace('\t', ' ', regex=False)
            # 移除引號（避免 CSV 衝突）
            df[col] = df[col].str.replace('"', '', regex=False)
            # 將 pandas 的 NA 標記轉為空字串
            df[col] = df[col].replace(['nan', 'None', '<NA>', 'NaT'], '')
            # 移除多餘的連續空白
            df[col] = df[col].str.replace(r'\s+', ' ', regex=True).str.strip()
        
        # 轉換數值欄位
        df = self._convert_numeric_columns(df, column_config_key)
        
        self.logger.debug(f"清理後資料: {len(df)} 筆，{len(df.columns)} 欄")
        
        return df
    
    def _convert_numeric_columns(self, df: pd.DataFrame, column_config_key: str) -> pd.DataFrame:
        """
        轉換數值欄位為數字格式
        
        Args:
            df: 要轉換的 DataFrame
            column_config_key: 欄位配置的 key (例如: 'yingzaibiao' 或 'yingzaibiao_us')
            
        Returns:
            轉換後的 DataFrame
        """
        numeric_cols = get_numeric_columns(column_config_key)
        
        for col in numeric_cols:
            if col not in df.columns:
                continue
                
            try:
                # 轉換為數值，錯誤的值會變成 NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except Exception as e:
                self.logger.warning(f"轉換 {col} 為數值時發生錯誤: {e}")
        
        return df
    
    def _save_csv(self, df: pd.DataFrame, csv_path: Path, column_config_key: str) -> None:
        """
        儲存為 CSV 檔案（使用架構定義的文字欄位格式）
        
        Args:
            df: 要儲存的 DataFrame
            csv_path: CSV 檔案路徑
            column_config_key: 欄位配置的 key (例如: 'yingzaibiao' 或 'yingzaibiao_us')
        """
        try:
            # 確保目錄存在
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 取得需要以文字格式儲存的欄位，確保為字串型態
            text_columns = get_text_columns(column_config_key)
            for col in text_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)
                    self.logger.debug(f"{col} 欄位已設為文字格式")
            
            # 儲存 CSV - 使用 QUOTE_NONNUMERIC 讓所有文字欄位自動加上雙引號
            df.to_csv(
                csv_path, 
                index=False, 
                encoding='utf-8-sig',
                lineterminator='\n',
                quoting=csv.QUOTE_NONNUMERIC  # 對所有非數字內容加雙引號
            )
            
            self.logger.success(f"CSV 已儲存: {csv_path}")
            self.logger.info(f"檔案大小: {csv_path.stat().st_size / 1024:.2f} KB")
            
        except Exception as e:
            self.logger.error(f"儲存 CSV 失敗: {e}")
            raise
    
    def _save_json(self, df: pd.DataFrame, json_path: Path) -> None:
        """
        儲存為 JSON 檔案（覆蓋模式）
        
        Args:
            df: 要儲存的 DataFrame
            json_path: JSON 檔案路徑
        """
        try:
            # 確保目錄存在
            json_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 轉換為 JSON 格式（records 格式）
            records = df.to_dict(orient='records')
            
            # 儲存（覆蓋舊檔）
            with open(json_path, 'w', encoding='utf-8-sig') as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            
            self.logger.success(f"JSON 已儲存: {json_path}")
            
        except Exception as e:
            self.logger.error(f"儲存 JSON 失敗: {e}")
            raise
