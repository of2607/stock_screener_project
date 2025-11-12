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
    
    def standardize_data(self, df: pd.DataFrame, report_type: str) -> pd.DataFrame:
        """
        標準化資料格式
        
        Args:
            df: 要標準化的資料框
            report_type: 報表類型
            
        Returns:
            標準化後的資料框
        """
        if df.empty:
            return df
        
        self.logger.debug(f"{report_type} 正在處理欄位標準化...")
        
        df_processed = df.copy()
        
        # 1. 重新命名欄位
        df_processed = self._rename_columns(df_processed, report_type)
        
        # 2. 標準化年度格式
        df_processed = self._standardize_year_format(df_processed)
        
        # 3. 根據報表類型進行特殊處理
        if report_type == "dividend":
            df_processed = self._process_dividend_data(df_processed)
        elif report_type == "etf_dividend":
            df_processed = self._process_etf_dividend_data(df_processed)
        elif report_type in ["balance_sheet", "cash_flow", "income_statement"]:
            df_processed = self._process_financial_statement_data(df_processed)
        
        # 4. 重新排列欄位順序
        df_processed = self._reorder_columns(df_processed)
        
        # 5. 轉換數值欄位
        df_processed = self._convert_numeric_columns(df_processed, report_type)
        
        self.logger.success(f"{report_type} 欄位處理完成，統一格式：代號、名稱、年度(民國)、季別")
        
        return df_processed
    
    def _rename_columns(self, df: pd.DataFrame, report_type: str) -> pd.DataFrame:
        """重新命名欄位"""
        rename_mapping = get_rename_mapping(report_type)
        
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
        """標準化股利期間格式"""
        if pd.isna(period_str):
            return None
        
        period_str = str(period_str).strip()
        
        # 年度
        if "年度" in period_str:
            return "Y1"
        # 季度
        elif "第1季" in period_str:
            return "Q1"
        elif "第2季" in period_str:
            return "Q2"
        elif "第3季" in period_str:
            return "Q3"
        elif "第4季" in period_str:
            return "Q4"
        # 半年
        elif "上半年" in period_str:
            return "H1"
        elif "下半年" in period_str:
            return "H2"
        # 月份
        elif "月" in period_str:
            month_match = pd.Series([period_str]).str.extract(r'第?(\d+)月')[0].iloc[0]
            if month_match:
                return f"M{month_match.zfill(2)}"
        
        return "OTHER"
    
    def _process_etf_dividend_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """處理 ETF 股利資料"""
        # 設定年度 (從外部傳入或從日期推算)
        # 這裡簡化處理，實際應該從除息交易日推算
        
        # 季別處理：依除息交易日判斷月份
        if '除息交易日' in df.columns:
            self.logger.debug("   正在分析除息交易日以判斷月份...")
            df['季別'] = df['除息交易日'].apply(self._determine_month_from_date)
        else:
            df['季別'] = "OTHER"
            self.logger.debug("   無除息交易日欄位，季別設為 OTHER")
        
        return df
    
    def _determine_month_from_date(self, date_str) -> str:
        """從除息交易日判斷月份"""
        if pd.isna(date_str) or date_str == '':
            return None
        
        date_str = str(date_str).strip()
        
        # 匹配各種日期格式中的月份
        month_patterns = [
            r'(\d+)年(\d+)月',  # 114年01月22日
            r'(\d{4})[/-](\d{1,2})[/-]',  # 2024/01/22 或 2024-01-22
            r'(\d{1,2})[/-](\d{1,2})',  # 01/22
        ]
        
        month = None
        for pattern in month_patterns:
            match = re.search(pattern, date_str)
            if match:
                if '年' in pattern:
                    month = int(match.group(2))  # 月份是第二組
                else:
                    month = int(match.group(2)) if len(match.groups()) > 1 else int(match.group(1))
                break
        
        if month is None:
            return "OTHER"
        
        # 根據月份返回格式
        if 1 <= month <= 12:
            return f"M{month:02d}"  # M01, M02, ..., M12
        else:
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
                df[col] = self._clean_and_convert_numeric(df[col])
            
            self.logger.success(f"   成功轉換 {len(existing_numeric_cols)} 個數值欄位")
        
        return df
    
    def _clean_and_convert_numeric(self, series: pd.Series) -> pd.Series:
        """清理並轉換數值"""
        # 清理數值：移除逗號、空格、特殊字符
        cleaned = (
            series.astype(str)
            .str.replace(',', '')
            .str.replace(' ', '')
            .str.replace('--', '')
            .str.replace('-', '')
            .replace(['', 'nan', 'None', 'null'], None)
        )
        
        # 轉換為數值
        return pd.to_numeric(cleaned, errors='coerce')
    
    def process_etf_dividend_data(self, df: pd.DataFrame, year_str: str) -> pd.DataFrame:
        """處理 ETF 股利資料 - 與原始版本完全一致的邏輯"""
        if df.empty:
            return df

        df_processed = df.copy()

        self.logger.debug(f"ETF 股利資料處理中...")

        # 1. 欄位重新命名 (與dividend格式同步)
        if '證券代號' in df_processed.columns:
            df_processed = df_processed.rename(columns={'證券代號': '代號'})
            self.logger.debug(f"   證券代號 → 代號")

        if '證券簡稱' in df_processed.columns:
            df_processed = df_processed.rename(columns={'證券簡稱': '名稱'})
            self.logger.debug(f"   證券簡稱 → 名稱")

        if '收益分配金額 (每1受益權益單位)' in df_processed.columns:
            df_processed = df_processed.rename(columns={'收益分配金額 (每1受益權益單位)': '配息'})
            self.logger.debug(f"   收益分配金額 (每1受益權益單位) → 配息")

        # 2. 年度處理：保持民國年格式
        roc_year = int(year_str)
        df_processed['年度'] = roc_year  # 直接使用民國年
        self.logger.debug(f"   年度設為: {roc_year} (民國年)")

        # 3. 季別處理：依除息交易日判斷月份 (參考dividend格式)
        if '除息交易日' in df_processed.columns:
            self.logger.debug(f"   正在分析除息交易日以判斷月份...")
            df_processed['季別'] = df_processed['除息交易日'].apply(self._determine_month_from_date)
            
            # 統計月份分布
            month_counts = df_processed['季別'].value_counts()
            self.logger.debug(f"   月份分布: {dict(month_counts)}")
        else:
            df_processed['季別'] = "OTHER"
            self.logger.debug(f"   無除息交易日欄位，季別設為 OTHER")

        # 4. 確保關鍵欄位格式正確
        if '代號' in df_processed.columns:
            df_processed['代號'] = df_processed['代號'].astype(str)

        if '名稱' in df_processed.columns:
            df_processed['名稱'] = df_processed['名稱'].astype(str)

        # 5. 數值欄位轉換
        numeric_columns = ['配息', '公告年度']
        existing_numeric_cols = [col for col in numeric_columns if col in df_processed.columns]

        if existing_numeric_cols:
            self.logger.debug(f"   轉換數值欄位: {existing_numeric_cols}")
            for col in existing_numeric_cols:
                df_processed[col] = self._clean_and_convert_numeric(df_processed[col])
            self.logger.success(f"   成功轉換 {len(existing_numeric_cols)} 個數值欄位")

        # 6. 重新排列欄位順序 (與dividend同步)
        df_processed = self._reorder_columns(df_processed)

        self.logger.success(f"ETF 股利資料處理完成: {len(df_processed)} 筆")
        self.logger.debug(f"   最終欄位順序: {df_processed.columns.tolist()[:6]}...")  # 顯示前6個欄位

        return df_processed
    
    def _determine_month_from_date(self, date_str) -> str:
        """從除息交易日判斷月份"""
        if pd.isna(date_str) or date_str == '':
            return None

        date_str = str(date_str).strip()

        # 嘗試提取月份
        import re
        
        # 匹配各種日期格式中的月份
        month_patterns = [
            r'(\d+)年(\d+)月',  # 114年01月22日
            r'(\d{4})[/-](\d{1,2})[/-]',  # 2024/01/22 或 2024-01-22
            r'(\d{1,2})[/-](\d{1,2})',  # 01/22
        ]

        month = None
        for pattern in month_patterns:
            match = re.search(pattern, date_str)
            if match:
                if '年' in pattern:
                    month = int(match.group(2))  # 月份是第二組
                else:
                    month = int(match.group(2)) if len(match.groups()) > 1 else int(match.group(1))
                break

        if month is None:
            return "OTHER"

        # 根據月份返回格式 (參考dividend的M{月份}格式)
        if 1 <= month <= 12:
            return f"M{month:02d}"  # M01, M02, ..., M12
        else:
            return "OTHER"