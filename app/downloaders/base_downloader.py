"""
TWSE 資料下載工具 - 基礎下載器
"""
import requests
import time
import urllib3
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from utils.logger import Logger
from config.settings import HEADERS, REQUEST_TIMEOUT, RETRY_ATTEMPTS, RETRY_DELAY

# 忽略 SSL 證書警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class BaseDownloader(ABC):
    """基礎下載器抽象類"""
    
    def __init__(self, logger: Logger):
        """
        初始化基礎下載器
        
        Args:
            logger: 日誌記錄器
        """
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
    
    def __del__(self):
        """清理資源"""
        if hasattr(self, 'session'):
            self.session.close()
    
    @abstractmethod
    def download_data(self, year: str, output_dir: str) -> bool:
        """
        下載指定年度的資料
        
        Args:
            year: 年度
            output_dir: 輸出目錄
            
        Returns:
            是否成功下載
        """
        pass
    
    def make_request(
        self, 
        url: str, 
        method: str = "GET", 
        **kwargs
    ) -> Optional[requests.Response]:
        """
        發送 HTTP 請求 (含重試機制)
        
        Args:
            url: 請求網址
            method: HTTP 方法
            **kwargs: 額外參數
            
        Returns:
            回應物件，失敗時回傳 None
        """
        for attempt in range(RETRY_ATTEMPTS):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    timeout=REQUEST_TIMEOUT,
                    verify=False,
                    **kwargs
                )
                
                if response.status_code == 200:
                    return response
                else:
                    self.logger.warning(
                        f"請求失敗 (嘗試 {attempt + 1}/{RETRY_ATTEMPTS}): "
                        f"HTTP {response.status_code}"
                    )
                    
            except Exception as e:
                self.logger.warning(
                    f"請求失敗 (嘗試 {attempt + 1}/{RETRY_ATTEMPTS}): {e}"
                )
            
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(RETRY_DELAY)
        
        self.logger.error(f"請求最終失敗: {url}")
        return None
    
    def save_response_to_file(
        self,
        response: requests.Response,
        file_path: str,
        encoding: str = "utf-8-sig"
    ) -> bool:
        """
        將回應內容儲存到檔案
        
        Args:
            response: HTTP 回應
            file_path: 檔案路徑
            encoding: 編碼格式
            
        Returns:
            是否成功儲存
        """
        try:
            # 智能編碼檢測和轉換
            if encoding == "big5":
                # 嘗試 big5 解碼，失敗則自動偵測編碼
                try:
                    response.encoding = "big5"
                    content = response.text
                except (UnicodeDecodeError, UnicodeEncodeError):
                    # big5 失敗，嘗試 utf-8
                    try:
                        response.encoding = "utf-8"
                        content = response.text
                    except (UnicodeDecodeError, UnicodeEncodeError):
                        # 使用原始 bytes 並嘗試 errors='ignore'
                        content = response.content.decode('big5', errors='ignore')
            else:
                response.encoding = "utf-8"
                content = response.text
            
            # 儲存時使用 utf-8-sig 以確保相容性
            with open(file_path, "w", encoding="utf-8-sig", newline="") as f:
                f.write(content)
            
            return True
            
        except Exception as e:
            self.logger.error(f"儲存檔案失敗 {file_path}: {e}")
            return False
    
    def download_file_with_retry(
        self, 
        url: str, 
        file_path: str, 
        encoding: str = "utf-8-sig"
    ) -> bool:
        """
        下載檔案 (含重試機制)
        
        Args:
            url: 下載網址
            file_path: 儲存路徑
            encoding: 編碼格式
            
        Returns:
            是否成功下載
        """
        response = self.make_request(url)
        
        if response is None:
            return False
        
        return self.save_response_to_file(response, file_path, encoding)
    
    def batch_download(
        self, 
        download_tasks: List[Dict[str, str]], 
        description: str = "下載中"
    ) -> int:
        """
        批次下載檔案
        
        Args:
            download_tasks: 下載任務清單 [{"url": "...", "path": "...", "encoding": "..."}]
            description: 進度描述
            
        Returns:
            成功下載的檔案數量
        """
        success_count = 0
        
        try:
            from tqdm import tqdm
            iterator = tqdm(download_tasks, desc=description)
        except ImportError:
            iterator = download_tasks
            self.logger.info(f"開始 {description}，共 {len(download_tasks)} 個檔案")
        
        for task in iterator:
            url = task["url"]
            file_path = task["path"]
            encoding = task.get("encoding", "utf-8-sig")
            
            if self.download_file_with_retry(url, file_path, encoding):
                success_count += 1
            else:
                self.logger.warning(f"下載失敗: {file_path}")
        
        self.logger.info(f"{description} 完成: {success_count}/{len(download_tasks)} 成功")
        return success_count