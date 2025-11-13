"""
TWSE 資料下載工具 - 股價資料處理器
"""
import pandas as pd
from typing import List, Dict
from datetime import datetime

from utils.logger import Logger
from config.settings import STOCK_MIN_PRICE


class StockPriceProcessor:
    """股價資料處理器 - 負責股價資料的清理、排序和格式化"""
    
    def __init__(self, logger: Logger):
        """
        初始化股價處理器
        
        Args:
            logger: 日誌記錄器
        """
        self.logger = logger
    
    def process_stock_data(self, raw_data_dict: Dict[str, List[Dict]]) -> pd.DataFrame:
        """
        處理股價資料
        
        Args:
            raw_data_dict: {"twse": [...], "tpex": [...]} 原始資料字典
            
        Returns:
            處理後的資料框
        """
        if not raw_data_dict:
            self.logger.warning("沒有股價資料需要處理")
            return pd.DataFrame()
        
        self.logger.progress("開始處理股價資料...")
        
        # 1. 標準化和過濾資料 (對應 supabase normalize 邏輯)
        normalized_data = []
        
        if 'twse' in raw_data_dict:
            twse_normalized = self._normalize_data(raw_data_dict['twse'], is_twse=True)
            normalized_data.extend(twse_normalized)
            self.logger.info(f"上市股票處理完成: {len(twse_normalized)} 筆")
        
        if 'tpex' in raw_data_dict:
            tpex_normalized = self._normalize_data(raw_data_dict['tpex'], is_twse=False)
            normalized_data.extend(tpex_normalized)
            self.logger.info(f"上櫃股票處理完成: {len(tpex_normalized)} 筆")
        
        if not normalized_data:
            self.logger.warning("標準化後沒有有效資料")
            return pd.DataFrame()
        
        # 2. 轉換為 DataFrame
        df = pd.DataFrame(normalized_data)
        self.logger.info(f"合併資料: {len(df)} 筆")
        
        # 3. 去重處理
        df = self._remove_duplicates(df)
        
        # 4. 最終驗證和統計
        df = self._final_validation(df)
        
        self.logger.success(f"股價資料處理完成: {len(df)} 筆有效資料")
        return df
    
    def _normalize_data(self, raw_data: List[Dict], is_twse: bool) -> List[Dict]:
        """標準化資料格式，過濾和排序股價資料"""
        processed_data = []
        
        for item in raw_data:
            try:
                # 欄位對應
                stock_code = item.get('Code' if is_twse else 'SecuritiesCompanyCode', '').strip()
                stock_name = item.get('Name' if is_twse else 'CompanyName', '').strip()
                closing_price = item.get('ClosingPrice' if is_twse else 'Close', '0')
                date_str = item.get('Date', '')
                
                # 處理價格
                price = self._parse_price(closing_price)
                
                # 處理日期格式（民國年轉西元年）
                formatted_date = self._parse_roc_date(date_str)
                
                processed_data.append({
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'price': price,
                    'market': 'TSE' if is_twse else 'OTC',
                    'date': formatted_date
                })
                
            except Exception as e:
                self.logger.warning(f"處理{'上市' if is_twse else '上櫃'}股票資料項目失敗: {e}")
                continue
        
        # 過濾條件: 股價 >= 設定值 且股票代號有效
        filtered_data = [
            d for d in processed_data
            if d['price'] >= STOCK_MIN_PRICE and d['stock_code'] and d['stock_code'].strip()
        ]
        
        # 依股票代號排序
        try:
            filtered_data.sort(key=lambda x: int(x['stock_code']))
        except (ValueError, TypeError):
            # 如果有非數字的股票代號，用字串排序
            filtered_data.sort(key=lambda x: x['stock_code'])
        
        return filtered_data
    
    def _parse_price(self, price_str: str) -> float:
        """解析價格字串為數字"""
        try:
            if price_str is None or price_str == '':
                return 0.0
            
            # 移除逗號，保留小數點
            cleaned = str(price_str).strip().replace(',', '')
            result = float(cleaned)
            return result if not (result != result) else 0.0  # 檢查 NaN
            
        except (ValueError, TypeError):
            return 0.0
    
    def _parse_roc_date(self, date_str: str) -> str:
        """解析民國年日期格式，例如: "1131113" -> "2024-11-13" """
        try:
            if not date_str:
                return datetime.now().strftime('%Y-%m-%d')
            
            # 匹配民國年格式 YYYMMDD
            import re
            match = re.match(r'^(\d{3})(\d{2})(\d{2})$', str(date_str))
            if match:
                year_roc, month, day = match.groups()
                year_ad = int(year_roc) + 1911  # 民國年轉西元年
                return f"{year_ad}-{month}-{day}"
            
            return datetime.now().strftime('%Y-%m-%d')
            
        except Exception:
            return datetime.now().strftime('%Y-%m-%d')
    
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """去除重複資料"""
        original_count = len(df)
        
        # 以股票代號為準去重，保留第一筆
        df = df.drop_duplicates(subset=['stock_code'], keep='first')
        
        duplicate_count = original_count - len(df)
        if duplicate_count > 0:
            self.logger.info(f"去重處理: 移除 {duplicate_count} 筆重複資料")
        
        return df
    
    def _resort_if_needed(self, df: pd.DataFrame) -> pd.DataFrame:
        """重新排序 (下載器已經排序，這裡確保合併後順序正確)"""
        if df.empty:
            return df
        
        try:
            # 按市場分組，然後按股票代號排序
            df['stock_code_numeric'] = pd.to_numeric(df['stock_code'], errors='coerce')
            df = df.sort_values(['market', 'stock_code_numeric'], ascending=[True, True])
            df = df.drop(['stock_code_numeric'], axis=1)
            df = df.reset_index(drop=True)
            self.logger.debug("重新排序完成: TSE -> OTC，股票代號升序")
        except Exception as e:
            self.logger.warning(f"排序過程出現問題: {e}")
        
        return df
    
    def _final_validation(self, df: pd.DataFrame) -> pd.DataFrame:
        """最終資料驗證"""
        if df.empty:
            self.logger.warning("處理後沒有有效的股價資料")
            return df
        
        # 統計資訊
        tse_count = len(df[df['market'] == 'TSE'])
        otc_count = len(df[df['market'] == 'OTC'])
        
        # 價格統計
        price_stats = df['price'].describe()
        
        self.logger.info(f"市場分布 - 上市: {tse_count} 筆, 上櫃: {otc_count} 筆")
        self.logger.info(f"價格範圍: {price_stats['min']:.2f} ~ {price_stats['max']:.2f}")
        self.logger.info(f"平均價格: {price_stats['mean']:.2f}")
        
        # 檢查必要欄位
        required_columns = ['stock_code', 'stock_name', 'price', 'market', 'date']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            self.logger.error(f"缺少必要欄位: {missing_columns}")
            return pd.DataFrame()
        
        return df
    
    def format_for_output(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        格式化資料以供輸出
        
        Args:
            df: 處理後的資料框
            
        Returns:
            格式化後的資料框
        """
        if df.empty:
            return df
        
        # 確保欄位順序和格式
        output_df = df.copy()
        
        # 統一欄位順序
        column_order = ['stock_code', 'stock_name', 'price', 'market', 'date']
        output_df = output_df[column_order]
        
        # 格式化數值
        output_df['price'] = output_df['price'].round(2)
        
        # 加入處理時間戳記（用於追蹤資料時效性）
        output_df['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return output_df
    
    def get_summary_stats(self, df: pd.DataFrame) -> Dict:
        """
        取得資料摘要統計
        
        Args:
            df: 資料框
            
        Returns:
            統計資訊字典
        """
        if df.empty:
            return {}
        
        stats = {
            'total_count': len(df),
            'tse_count': len(df[df['market'] == 'TSE']),
            'otc_count': len(df[df['market'] == 'OTC']),
            'price_min': float(df['price'].min()),
            'price_max': float(df['price'].max()),
            'price_mean': float(df['price'].mean()),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return stats