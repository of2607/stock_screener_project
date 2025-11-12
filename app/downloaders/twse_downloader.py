"""
TWSE 資料下載工具 - 證交所財務報表下載器
"""
import os
import time
from typing import List, Dict
from bs4 import BeautifulSoup

from .base_downloader import BaseDownloader
from config.api_urls import get_ajax_url, get_download_url
from config.settings import MARKETS, SEASONS


class TWSEDownloader(BaseDownloader):
    """證交所財務報表下載器"""
    
    def download_data(self, year: str, report_type: str, output_dir: str) -> bool:
        """
        下載指定年度和報表類型的資料
        
        Args:
            year: 年度
            report_type: 報表類型
            output_dir: 輸出目錄
            
        Returns:
            是否成功下載
        """
        self.logger.progress(f"下載 {year} {report_type} 資料...")
        
        # 1. 取得所有檔案名稱
        all_filenames = self._fetch_all_filenames(year, report_type)
        
        if not all_filenames:
            self.logger.warning(f"{year} {report_type} 沒有找到任何檔案")
            return False
        
        # 2. 去重
        unique_filenames = self._remove_duplicates(all_filenames)
        self.logger.info(
            f"{year} {report_type} 找到 {len(all_filenames)} 個檔案，"
            f"去重後 {len(unique_filenames)} 個"
        )
        
        # 3. 建立下載任務
        download_tasks = self._create_download_tasks(
            unique_filenames, report_type, output_dir
        )
        
        # 4. 批次下載
        success_count = self.batch_download(
            download_tasks, 
            f"{year} {report_type} 下載"
        )
        
        return success_count > 0
    
    def _fetch_all_filenames(self, year: str, report_type: str) -> List[str]:
        """取得所有檔案名稱"""
        all_filenames = []
        
        for market in MARKETS:
            if report_type == "dividend":
                # 股利資料不需要季別
                filenames = self._fetch_filenames_for_market(
                    year, report_type, market, None
                )
                all_filenames.extend(filenames)
            else:
                # 其他報表需要各季資料
                for season in SEASONS:
                    filenames = self._fetch_filenames_for_market(
                        year, report_type, market, season
                    )
                    all_filenames.extend(filenames)
                    time.sleep(0.5)  # 避免請求過於頻繁
        
        return all_filenames
    
    def _fetch_filenames_for_market(
        self, 
        year: str, 
        report_type: str, 
        market: str, 
        season: str = None
    ) -> List[str]:
        """取得特定市場的檔案名稱"""
        try:
            ajax_url = get_ajax_url(report_type, year, market, season)
            response = self.make_request(ajax_url)
            
            if response is None:
                return []
            
            response.encoding = "utf-8"
            soup = BeautifulSoup(response.text, "lxml")
            input_tags = soup.find_all("input", {"name": "filename"})
            filenames = [tag.get("value") for tag in input_tags if tag.get("value")]
            
            if filenames:
                season_info = f" {season}" if season else ""
                self.logger.debug(f"  {market}{season_info}: 找到 {len(filenames)} 個檔案")
            
            return filenames
            
        except Exception as e:
            season_info = f" {season}" if season else ""
            self.logger.warning(f"取得 {year} {market}{season_info} 檔案清單失敗: {e}")
            return []
    
    def _remove_duplicates(self, filenames: List[str]) -> List[str]:
        """移除重複的檔案名稱"""
        seen = set()
        unique_filenames = []
        
        for filename in filenames:
            if filename not in seen:
                seen.add(filename)
                unique_filenames.append(filename)
        
        return unique_filenames
    
    def _create_download_tasks(
        self, 
        filenames: List[str], 
        report_type: str, 
        output_dir: str
    ) -> List[Dict[str, str]]:
        """建立下載任務清單"""
        tasks = []
        
        for filename in filenames:
            download_url = get_download_url(report_type, filename)
            file_path = os.path.join(output_dir, filename)
            
            tasks.append({
                "url": download_url,
                "path": file_path,
                "encoding": "big5"  # TWSE 檔案通常是 big5 編碼
            })
        
        return tasks