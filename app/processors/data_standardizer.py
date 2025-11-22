"""
TWSE 資料下載工具 - 資料標準化器
"""
import pandas as pd
import re
from typing import Dict, List
from utils.logger import Logger
from config.column_configs import get_numeric_columns, get_rename_mapping


class DataStandardizer:
    """資料標準化器 - 統一處理各種資料格式標準化"""
    
    def __init__(self, logger: Logger):
        """
        初始化資料標準化器
        
        Args:
            logger: 日誌記錄器
        """
        self.logger = logger
        
        # 預編譯正則表達式以提高效能
        self._date_patterns = [
            re.compile(r'(\d+)年(\d+)月'),  # 114年01月22日
            re.compile(r'(\d{4})[/-](\d{1,2})[/-]'),  # 2024/01/22 or 2024-01-22
            re.compile(r'(\d{1,2})[/-](\d{1,2})')  # 01/22
        ]
        self._year_pattern = re.compile(r'(\d+)年')
        self._month_pattern = re.compile(r'第?(\d+)月')
        
        # 期間標準化映射 - 按優先級排序
        self._period_mapping = {
            # 年度映射（最高優先級）
            "年度": "Y1",
            # 半年映射（高優先級）
            "上半年": "H1", 
            "下半年": "H2",
            # 季度映射（低優先級）
            **{f"第{i}季": f"Q{i}" for i in range(1, 5)}
        }
    
    def standardize_data(self, df: pd.DataFrame, report_type: str, skip_rename: bool = False) -> pd.DataFrame:
        """
        標準化資料格式 - 增強錯誤處理
        
        Args:
            df: 要標準化的資料框
            report_type: 報表類型
            
        Returns:
            標準化後的資料框
        """
        if df.empty:
            return df
        
        try:
            self.logger.debug(f"{report_type} 正在處理欄位標準化...")
            df_processed = df.copy()
            # 處理步驟可選擇是否跳過欄位名稱統一
            processing_steps = []
            if not skip_rename:
                processing_steps.append(("重新命名欄位", lambda x: self._rename_columns(x, report_type)))
            processing_steps += [
                ("標準化年度格式", self._standardize_year_format),
                ("特殊資料處理", lambda x: self._process_by_report_type(x, report_type)),
                ("重新排列欄位", self._reorder_columns),
                ("轉換數值欄位", lambda x: self._convert_numeric_columns(x, report_type))
            ]
            for step_name, step_func in processing_steps:
                try:
                    df_processed = step_func(df_processed)
                except Exception as e:
                    self.logger.error(f"{report_type} {step_name}失敗: {str(e)}")
                    raise
            self.logger.success(f"{report_type} 欄位處理完成，統一格式：代號、名稱、年度(民國)、季別")
            return df_processed
        except Exception as e:
            self.logger.error(f"{report_type} 資料標準化失敗: {str(e)}")
            return df  # 返回原始資料而非空資料框
    
    def _process_by_report_type(self, df: pd.DataFrame, report_type: str) -> pd.DataFrame:
        """根據報表類型進行特殊處理"""
        processors = {
            "dividend": self._process_dividend_data,
            "balance_sheet": self._process_financial_statement_data,
            "cash_flow": self._process_financial_statement_data,
            "income_statement": self._process_financial_statement_data
        }
        
        processor = processors.get(report_type)
        return processor(df) if processor else df
    
    def _rename_columns(self, df: pd.DataFrame, report_type: str) -> pd.DataFrame:
        """重新命名欄位，並統一『淨利（損）歸屬於母公司業主』相關欄位名稱"""
        from config.column_configs import get_semantic_unify_columns
        rename_mapping = get_rename_mapping(report_type)
        # 1. 依 config 統一所有語意相同欄位（支援所有報表型態）
        semantic_unify = get_semantic_unify_columns(report_type)
        for std_col, variants in semantic_unify.items():
            for col in variants:
                if col in df.columns and col != std_col:
                    df = df.rename(columns={col: std_col})
                    self.logger.debug(f"   欄位重新命名: {col} → {std_col}")
        # 2. 其餘欄位依照 config 設定進行命名
        if rename_mapping:
            existing_mapping = {k: v for k, v in rename_mapping.items() if k in df.columns}
            if existing_mapping:
                df = df.rename(columns=existing_mapping)
                self.logger.debug(f"   欄位重新命名: {existing_mapping}")
        return df
    
    def _standardize_year_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """標準化年度格式 (保持民國年)"""
        if "年度" in df.columns:
            year_numeric = pd.to_numeric(df["年度"], errors='coerce')
            df["年度"] = year_numeric.astype('Int64')
            self.logger.debug("   年度保持民國年格式")
        
        return df
    
    def _process_dividend_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """處理股利資料特殊格式"""
        # 拆分公司代號名稱欄位
        if "公司代號名稱" in df.columns:
            self.logger.debug("   正在拆分公司代號名稱欄位...")
            
            company_info = df["公司代號名稱"].str.split(" - ", n=1, expand=True)
            df["代號"] = company_info[0].str.strip()
            df["名稱"] = company_info[1].str.strip()
            df = df.drop(columns=["公司代號名稱"])
            
            self.logger.debug("   成功拆分公司代號名稱欄位")
        
        # 拆分股利所屬年(季)度欄位
        if "股利所屬年(季)度" in df.columns:
            df = self._process_dividend_period(df)

        # === [MODIFY] 新增現金股利欄位 ===
        cash_dividend_cols = [
            "股東配發-盈餘分配之現金股利(元/股)",
            "股東配發-法定盈餘公積發放之現金(元/股)",
            "股東配發-資本公積發放之現金(元/股)"
        ]
        def safe_float(val):
            try:
                if pd.isna(val):
                    return 0.0
                return float(val)
            except Exception:
                return 0.0

        df["現金股利"] = df.apply(
            lambda row: sum(safe_float(row.get(col, 0)) for col in cash_dividend_cols),
            axis=1
        )
        # === [MODIFY END] ===

        return df
    
    def _process_dividend_period(self, df: pd.DataFrame) -> pd.DataFrame:
        """處理股利所屬期間"""
        self.logger.debug("   正在拆分股利所屬年(季)度欄位...")
        
        # 提取年度
        year_match = df["股利所屬年(季)度"].str.extract(r'(\d+)年')[0]
        year_numeric = pd.to_numeric(year_match, errors='coerce').astype('Int64')
        df["年度"] = year_numeric
        
        # 標準化季別
        df["季別"] = df["股利所屬年(季)度"].apply(self._standardize_dividend_period)
        df = df.drop(columns=["股利所屬年(季)度"])
        
        self.logger.debug("   成功拆分股利所屬年(季)度欄位")
        return df
    
    def _standardize_dividend_period(self, period_str) -> str:
        """標準化股利期間格式 - 精確匹配版本"""
        if pd.isna(period_str):
            return None
        
        period_str = str(period_str).strip()
        
        # 精確匹配，按優先級順序
        # 1. 年度 (例如: "111年 年度")
        if "年度" in period_str:
            return "Y1"
        
        # 2. 半年 (例如: "111年 上半年", "111年 下半年")
        if "上半年" in period_str:
            return "H1"
        if "下半年" in period_str:
            return "H2"
        
        # 3. 季度 - 使用更精確的匹配 (例如: "111年 第1季")
        for i in range(1, 5):
            if f"第{i}季" in period_str:
                return f"Q{i}"
        
        # 4. 月份處理 (例如: "111年 第1月")
        month_match = self._month_pattern.search(period_str)
        if month_match:
            month = month_match.group(1)
            return f"M{month}"
        
        # 5. 僅數字的季度匹配（最後檢查，避免誤判）
        if period_str in ["1", "2", "3", "4"]:
            return f"Q{period_str}"
        
        return "OTHER"
    

    
    def _determine_month_from_date(self, date_str) -> str:
        """從除息交易日判斷月份 - 優化版本"""
        if pd.isna(date_str) or date_str == '':
            return None
        
        date_str = str(date_str).strip()
        
        # 使用預編譯的正則表達式
        for i, pattern in enumerate(self._date_patterns):
            match = pattern.search(date_str)
            if match:
                if i == 0:  # 年月格式
                    month = int(match.group(2))
                else:  # 其他格式
                    month = int(match.group(2)) if len(match.groups()) > 1 else int(match.group(1))
                
                return f"M{month}" if 1 <= month <= 12 else "OTHER"
        
        return "OTHER"
    
    def _process_financial_statement_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """處理財務報表資料 (資產負債表、損益表、現金流量表)"""
        # 標準化季別格式：1, 2, 3, 4 → Q1, Q2, Q3, Q4
        if "季別" in df.columns:
            self.logger.debug("   正在標準化季別格式...")
            df["季別"] = df["季別"].apply(self._standardize_quarter)
            self.logger.debug("   季別標準化完成：1,2,3,4 → Q1,Q2,Q3,Q4")
        
        return df
    
    def _standardize_quarter(self, quarter_val) -> str:
        """標準化季別格式"""
        if pd.isna(quarter_val):
            return None
        
        quarter_str = str(quarter_val).strip()
        
        quarter_map = {
            "1": "Q1",
            "2": "Q2", 
            "3": "Q3",
            "4": "Q4"
        }
        
        return quarter_map.get(quarter_str, quarter_str)
    
    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """重新排列欄位順序"""
        cols = df.columns.tolist()
        priority_cols = []
        
        for col_name in ['代號', '名稱', '年度', '季別']:
            if col_name in cols:
                priority_cols.append(col_name)
                cols.remove(col_name)
        
        new_cols = priority_cols + cols
        return df[new_cols]
    
    def _convert_numeric_columns(self, df: pd.DataFrame, report_type: str) -> pd.DataFrame:
        """轉換數值欄位"""
        numeric_cols = get_numeric_columns(report_type)
        existing_numeric_cols = [col for col in numeric_cols if col in df.columns]

        if existing_numeric_cols:
            self.logger.debug(f"   轉換數值欄位: {existing_numeric_cols}")
            for col in existing_numeric_cols:
                # 若欄位重複，pandas 會回傳 DataFrame，否則為 Series
                col_data = df[col]
                if isinstance(col_data, pd.DataFrame):
                    # 欄位重複，合併為一欄（取第一欄非空值）
                    self.logger.warning(f"   欄位 {col} 有重複，將自動合併僅保留第一欄非空值")
                    df[col] = col_data.bfill(axis=1).iloc[:, 0]
                df[col] = self._clean_and_convert_numeric(df[col])
            self.logger.success(f"   成功轉換 {len(existing_numeric_cols)} 個數值欄位")
        return df
    
    def _clean_and_convert_numeric(self, series: pd.Series) -> pd.Series:
        """清理並轉換數值 - 強化版本，去除雜訊並正確轉型"""
        cleaned = (
            series.astype(str)
            .str.replace(r'[\s,，]', '', regex=True)  # 去除空白、逗號
            .str.replace(r'[()]', '', regex=True)      # 去除括號
            .str.replace(r'－', '-', regex=False)      # 全形負號轉半形
            .str.replace(r'．', '.', regex=False)      # 全形小數點轉半形
            .replace(['', 'nan', 'None', 'null'], None)
        )
        return pd.to_numeric(cleaned, errors='coerce')
    
    def _process_etf_dividend_data(self, df: pd.DataFrame, year_str: str) -> pd.DataFrame:
        """處理 ETF 股利資料"""
        if df.empty:
            return df

        self.logger.debug("ETF 股利資料處理中...")
        df_processed = df.copy()
        
        # 1. 欄位重新命名 (與dividend格式同步)
        column_mappings = {
            '證券代號': '代號',
            '證券簡稱': '名稱',
            '收益分配金額 (每1受益權益單位)': '配息'
        }
        
        for old_col, new_col in column_mappings.items():
            if old_col in df_processed.columns:
                df_processed = df_processed.rename(columns={old_col: new_col})
                self.logger.debug(f"   {old_col} → {new_col}")
        
        # 2. 年度處理：保持民國年格式
        roc_year = int(year_str)
        df_processed['年度'] = roc_year
        self.logger.debug(f"   年度設為: {roc_year} (民國年)")
        
        # 3. 季別處理：依除息交易日判斷月份
        if '除息交易日' in df_processed.columns:
            self.logger.debug("   正在分析除息交易日以判斷月份...")
            df_processed['季別'] = df_processed['除息交易日'].apply(self._determine_month_from_date)
            
            # 統計月份分布
            month_counts = df_processed['季別'].value_counts()
            self.logger.debug(f"   月份分布: {dict(month_counts)}")
        else:
            df_processed['季別'] = "OTHER"
            self.logger.debug("   無除息交易日欄位，季別設為 OTHER")
        
        # 4. 確保關鍵欄位格式正確
        if '代號' in df_processed.columns:
            df_processed['代號'] = df_processed['代號'].astype(str)
        
        if '名稱' in df_processed.columns:
            df_processed['名稱'] = df_processed['名稱'].astype(str)
        
        # 5. 重新排列欄位順序和數值轉換
        df_processed = self._reorder_columns(df_processed)
        df_processed = self._convert_numeric_columns(df_processed, "etf_dividend")
        
        self.logger.success(f"ETF 股利資料處理完成: {len(df_processed)} 筆")
        return df_processed

