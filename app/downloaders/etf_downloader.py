"""
TWSE 資料下載工具 - ETF 股利下載器
"""
import os
import pandas as pd
from typing import Dict

from .base_downloader import BaseDownloader
from config.api_urls import get_etf_urls


class ETFDownloader(BaseDownloader):
    """ETF 股利資料下載器"""
    
    def download_data(self, year: str, output_dir: str) -> bool:
        """
        下載指定年度的 ETF 股利資料
        
        Args:
            year: 民國年度
            output_dir: 輸出目錄
            
        Returns:
            是否成功下載
        """
        self.logger.progress(f"下載 {year} ETF 股利資料...")
        
        # 1. 計算日期範圍
        date_range = self._calculate_date_range(year)
        
        # 2. 取得 API URLs
        urls = get_etf_urls(date_range["start"], date_range["end"])
        
        # 3. 設定檔案路徑
        csv_filename = f"etf_dividend_{date_range['ad_year']}.csv"
        csv_path = os.path.join(output_dir, csv_filename)
        
        # 4. 優先嘗試 CSV 下載
        if self._download_csv(urls["csv"], csv_path):
            return True
        
        # 5. CSV 失敗，嘗試 JSON 轉 CSV
        return self._download_json_as_csv(urls["json"], csv_path)
    
    def _calculate_date_range(self, roc_year: str) -> Dict[str, str]:
        """
        計算日期範圍
        
        Args:
            roc_year: 民國年度
            
        Returns:
            包含開始日期、結束日期和西元年的字典
        """
        roc_year_int = int(roc_year)
        ad_year = roc_year_int + 1911
        
        return {
            "start": f"{ad_year}0101",
            "end": f"{ad_year + 1}0101",  # 下一年的1月1日
            "ad_year": str(ad_year),
            "roc_year": roc_year
        }
    
    def _download_csv(self, csv_url: str, csv_path: str) -> bool:
        """
        下載 CSV 格式資料
        
        Args:
            csv_url: CSV API URL
            csv_path: 儲存路徑
            
        Returns:
            是否成功下載並驗證
        """
        self.logger.debug(f"嘗試 CSV 下載: {csv_url}")
        
        response = self.make_request(csv_url)
        
        if response is None:
            return False
        
        # 檢查回應內容
        if len(response.text.strip()) <= 100:
            self.logger.warning("CSV 回應內容過短")
            return False
        
        # 儲存 CSV 內容
        if not self.save_response_to_file(response, csv_path, "utf-8-sig"):
            return False
        
        # 驗證 CSV 檔案
        return self._validate_csv_file(csv_path)
    
    def _download_json_as_csv(self, json_url: str, csv_path: str) -> bool:
        """
        下載 JSON 格式並轉換為 CSV
        
        Args:
            json_url: JSON API URL  
            csv_path: CSV 儲存路徑
            
        Returns:
            是否成功下載並轉換
        """
        self.logger.progress("嘗試 JSON 下載並轉換為 CSV")
        
        response = self.make_request(json_url)
        
        if response is None:
            return False
        
        try:
            data = response.json()
            
            # 檢查是否有資料
            if 'data' not in data or len(data['data']) == 0:
                year_info = os.path.basename(csv_path).replace('etf_dividend_', '').replace('.csv', '')
                self.logger.warning(f"{year_info} 無 ETF 股利資料")
                return False
            
            # 解析 JSON 資料
            fields = data.get('fields', [])
            rows = data.get('data', [])
            
            if not fields or not rows:
                self.logger.warning("JSON 資料格式異常")
                return False
            
            # 轉換為 DataFrame 並儲存為 CSV
            df = pd.DataFrame(rows, columns=fields)
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
            
            self.logger.success(f"ETF 股利 JSON→CSV 轉換成功: {len(df)} 筆資料")
            return True
            
        except Exception as e:
            self.logger.error(f"JSON 處理失敗: {e}")
            return False
    
    def _validate_csv_file(self, csv_path: str) -> bool:
        """
        驗證 CSV 檔案是否有效
        
        Args:
            csv_path: CSV 檔案路徑
            
        Returns:
            是否為有效的 CSV 檔案
        """
        try:
            test_df = pd.read_csv(csv_path, encoding="utf-8-sig", nrows=5)
            
            if test_df.empty or len(test_df.columns) <= 3:
                self.logger.warning("CSV 檔案格式異常")
                if os.path.exists(csv_path):
                    os.remove(csv_path)
                return False
            
            self.logger.success(f"ETF 股利 CSV 下載成功: {csv_path}")
            return True
            
        except Exception as e:
            self.logger.warning(f"CSV 檔案讀取失敗: {e}")
            if os.path.exists(csv_path):
                os.remove(csv_path)
            return False