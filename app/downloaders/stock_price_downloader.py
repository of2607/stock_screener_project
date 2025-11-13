"""
TWSE 資料下載工具 - 股價下載器
"""
import json
import os
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import re

from .base_downloader import BaseDownloader
from config.api_urls import get_twse_stock_url, get_tpex_stock_url
from config.settings import BASE_DIR, RAW_DATA_RETENTION_DAYS


class StockPriceDownloader(BaseDownloader):
    """股價下載器 - 負責原始資料下載"""
    
    def download_data(self, output_dir: str = None) -> Tuple[bool, Dict[str, List[Dict]]]:
        """
        下載最新股價資料
        
        Returns:
            Tuple[bool, Dict]: (是否成功, {"twse": [...], "tpex": [...]})
        """
        self.logger.progress("開始下載最新股價資料...")
        
        # 1. 同時抓取上市和上櫃資料
        twse_data = self._fetch_twse_data()
        tpex_data = self._fetch_tpex_data()
        
        if not twse_data and not tpex_data:
            self.logger.error("上市和上櫃資料都無法取得")
            return False, {}
        
        # 整理原始資料
        raw_data = {}
        
        if twse_data:
            raw_data['twse'] = twse_data
            self.logger.info(f"上市原始資料: {len(twse_data)} 筆")
        
        if tpex_data:
            raw_data['tpex'] = tpex_data
            self.logger.info(f"上櫃原始資料: {len(tpex_data)} 筆")
        
        # 儲存原始資料到 raw_data 目錄
        self._save_raw_data(raw_data)
        
        # 清理過期的原始資料
        self._cleanup_old_raw_data()
        
        self.logger.success("原始股價資料下載完成")
        return True, raw_data
    
    def _save_raw_data(self, raw_data: Dict[str, List[Dict]]) -> None:
        """儲存原始資料到 raw_data 目錄"""
        try:
            # 建立股價原始資料目錄
            stock_raw_dir = os.path.join(BASE_DIR, "stock_prices")
            os.makedirs(stock_raw_dir, exist_ok=True)
            
            # 使用當前日期作為檔名
            current_date = datetime.now().strftime('%Y%m%d')
            
            # 儲存上市資料
            if 'twse' in raw_data:
                twse_file = os.path.join(stock_raw_dir, f"{current_date}_twse_raw.json")
                with open(twse_file, 'w', encoding='utf-8') as f:
                    json.dump(raw_data['twse'], f, ensure_ascii=False, indent=2)
                self.logger.debug(f"上市原始資料已儲存: {twse_file}")
            
            # 儲存上櫃資料
            if 'tpex' in raw_data:
                tpex_file = os.path.join(stock_raw_dir, f"{current_date}_tpex_raw.json")
                with open(tpex_file, 'w', encoding='utf-8') as f:
                    json.dump(raw_data['tpex'], f, ensure_ascii=False, indent=2)
                self.logger.debug(f"上櫃原始資料已儲存: {tpex_file}")
            
        except Exception as e:
            self.logger.warning(f"儲存原始資料失敗: {e}")
    
    def _cleanup_old_raw_data(self) -> None:
        """清理過期的原始資料檔案"""
        try:
            stock_raw_dir = os.path.join(BASE_DIR, "stock_prices")
            if not os.path.exists(stock_raw_dir):
                return
            
            # 計算保留截止日期
            cutoff_date = datetime.now() - timedelta(days=RAW_DATA_RETENTION_DAYS)
            cutoff_str = cutoff_date.strftime('%Y%m%d')
            
            # 取得所有檔案
            files = os.listdir(stock_raw_dir)
            deleted_count = 0
            
            for filename in files:
                if filename.endswith('_raw.json'):
                    # 解析檔案日期 (格式: YYYYMMDD_*)
                    try:
                        file_date_str = filename.split('_')[0]
                        if len(file_date_str) == 8 and file_date_str.isdigit():
                            if file_date_str < cutoff_str:
                                file_path = os.path.join(stock_raw_dir, filename)
                                os.remove(file_path)
                                deleted_count += 1
                                self.logger.debug(f"已刪除過期檔案: {filename}")
                    except (ValueError, IndexError):
                        continue
            
            if deleted_count > 0:
                self.logger.info(f"清理完成: 刪除 {deleted_count} 個過期原始資料檔案 (保留 {RAW_DATA_RETENTION_DAYS} 天)")
            
        except Exception as e:
            self.logger.warning(f"清理原始資料失敗: {e}")
    
    def _fetch_twse_data(self) -> Optional[List[Dict]]:
        """抓取上市股票資料"""
        try:
            url = get_twse_stock_url()
            self.logger.debug(f"請求上市股票 API: {url}")
            
            response = self.make_request(url)
            if response is None:
                return None
            
            data = response.json()
            self.logger.debug(f"上市股票原始資料筆數: {len(data) if isinstance(data, list) else 'N/A'}")
            return data if isinstance(data, list) else []
            
        except Exception as e:
            self.logger.error(f"抓取上市股票資料失敗: {e}")
            return None
    
    def _fetch_tpex_data(self) -> Optional[List[Dict]]:
        """抓取上櫃股票資料"""
        try:
            url = get_tpex_stock_url()
            self.logger.debug(f"請求上櫃股票 API: {url}")
            
            response = self.make_request(url)
            if response is None:
                return None
            
            data = response.json()
            self.logger.debug(f"上櫃股票原始資料筆數: {len(data) if isinstance(data, list) else 'N/A'}")
            return data if isinstance(data, list) else []
            
        except Exception as e:
            self.logger.error(f"抓取上櫃股票資料失敗: {e}")
            return None
    