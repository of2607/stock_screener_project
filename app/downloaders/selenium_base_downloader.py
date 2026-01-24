"""
Selenium 基礎下載器
===================

提供基於 Selenium 的自動化瀏覽器下載功能，支援登入、檔案下載監控等
"""
import os
import time
import json
import pickle
import base64
from abc import abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, List, Dict

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import WebDriverException

try:
    import undetected_chromedriver as uc
    HAS_UC = True
except ImportError:
    HAS_UC = False

try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_WEBDRIVER_MANAGER = True
except ImportError:
    HAS_WEBDRIVER_MANAGER = False

from .base_downloader import BaseDownloader
from config.settings import LOG_DIR_BASE


class SeleniumBaseDownloader(BaseDownloader):
    """Selenium 下載器基礎類別 - 提供瀏覽器自動化功能"""
    
    def __init__(self, logger, download_dir: str):
        """
        初始化 Selenium 下載器
        
        Args:
            logger: 日誌記錄器
            download_dir: 下載目錄路徑
        """
        super().__init__(logger)
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        self.driver: Optional[webdriver.Chrome] = None
        self.wait: Optional[WebDriverWait] = None
    
    def _init_driver(self) -> None:
        """初始化 Chrome WebDriver（支援 headless 模式，使用 undetected-chromedriver 繞過檢測）"""
        # 檢測是否在 CI 環境（GitHub Actions 等）
        is_ci = os.getenv('CI') == 'true' or os.getenv('GITHUB_ACTIONS') == 'true'
        
        # 設定下載目錄和禁用密碼儲存提示
        prefs = {
            "download.default_directory": str(self.download_dir.absolute()),
            "download.prompt_for_download": False,
            "safebrowsing.enabled": False,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_settings.popups": 0,
            "profile.default_content_settings.automatic_downloads": 1,  # 允許多檔案下載
            "autofill.profile_enabled": False,
            "autofill.credit_card_enabled": False,
            "password_manager_enabled": False,
            "profile.password_manager_leak_detection": False
        }
        
        try:
            if HAS_UC and not is_ci:
                # 使用 undetected-chromedriver 繞過 Google 驗證
                options = uc.ChromeOptions()
                options.add_experimental_option("prefs", prefs)
                
                options.add_argument('--ignore-certificate-errors')
                options.add_argument('--ignore-ssl-errors')
                options.add_argument('--disable-blink-features=AutomationControlled')
                options.add_argument('--disable-web-security')
                options.add_argument('--disable-features=IsolateOrigins,site-per-process,PasswordLeakDetection')
                options.add_argument('--disable-save-password-bubble')
                options.add_argument('--disable-password-generation')
                options.add_argument('--disable-password-manager-reauthentication')
                options.add_argument('--password-store=basic')
                
                # 禁用通知和彈窗
                options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
                options.add_experimental_option('useAutomationExtension', False)
                options.add_argument('--disable-popup-blocking')
                
                # 設定更真實的 user agent
                options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
                
                self.driver = uc.Chrome(options=options, version_main=None, use_subprocess=False)
                
                # 移除 webdriver 特徵
                self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                    "userAgent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                })
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                
                self.logger.info("使用 undetected-chromedriver 初始化，已增強反檢測")
            else:
                # CI 環境或未安裝 undetected-chromedriver，使用標準 webdriver
                chrome_options = Options()
                
                # 使用臨時用戶配置檔，避免密碼警告
                import tempfile
                temp_profile = tempfile.mkdtemp(prefix="chrome_profile_")
                chrome_options.add_argument(f'--user-data-dir={temp_profile}')
                
                if is_ci:
                    chrome_options.add_argument('--headless')
                    chrome_options.add_argument('--no-sandbox')
                    chrome_options.add_argument('--disable-dev-shm-usage')
                    chrome_options.add_argument('--disable-gpu')
                    self.logger.info("偵測到 CI 環境，使用 headless 模式")
                
                chrome_options.add_argument('--disable-features=PasswordLeakDetection')
                chrome_options.add_argument('--disable-save-password-bubble')
                chrome_options.add_argument('--disable-password-generation')
                chrome_options.add_argument('--disable-password-manager-reauthentication')
                
                # 禁用通知和彈窗
                chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
                chrome_options.add_experimental_option('useAutomationExtension', False)
                chrome_options.add_argument('--disable-popup-blocking')
                
                chrome_options.add_experimental_option("prefs", prefs)
                chrome_options.add_argument('--ignore-certificate-errors')
                chrome_options.add_argument('--ignore-ssl-errors')
                
                # 使用 webdriver-manager 自動管理 ChromeDriver
                if HAS_WEBDRIVER_MANAGER:
                    service = Service(ChromeDriverManager().install())
                    self.driver = webdriver.Chrome(service=service, options=chrome_options)
                else:
                    # 假設系統已安裝 chromedriver
                    self.driver = webdriver.Chrome(options=chrome_options)
                
                self.logger.info("使用標準 Chrome WebDriver")
            
            self.wait = WebDriverWait(self.driver, 10)
            self.logger.success("Chrome WebDriver 初始化成功")
        except Exception as e:
            self.logger.error(f"Chrome WebDriver 初始化失敗: {e}")
            raise
    
    def _close_driver(self) -> None:
        """關閉並清理 WebDriver 資源"""
        if self.driver:
            try:
                self.driver.quit()
                self.logger.debug("Chrome WebDriver 已關閉")
            except Exception as e:
                self.logger.warning(f"關閉 WebDriver 時發生錯誤: {e}")
            finally:
                self.driver = None
                self.wait = None
    
    def save_cookies(self, cookies_path: Path) -> bool:
        """儲存當前 cookies 到檔案
        
        Args:
            cookies_path: cookies 檔案路徑
            
        Returns:
            是否成功儲存
        """
        if not self.driver:
            self.logger.error("WebDriver 未初始化")
            return False
        
        try:
            cookies = self.driver.get_cookies()
            cookies_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(cookies_path, 'wb') as f:
                pickle.dump(cookies, f)
            
            self.logger.success(f"Cookies 已儲存: {cookies_path}")
            return True
        except Exception as e:
            self.logger.error(f"儲存 cookies 失敗: {e}")
            return False
    
    def load_cookies(self, cookies_data: str = None, cookies_path: Path = None) -> bool:
        """載入 cookies（從環境變數或檔案）
        
        Args:
            cookies_data: Base64 編碼的 cookies 資料（來自環境變數）
            cookies_path: cookies 檔案路徑（本地測試用）
            
        Returns:
            是否成功載入
        """
        if not self.driver:
            self.logger.error("WebDriver 未初始化")
            return False
        
        try:
            cookies = None
            
            # 優先使用環境變數（GitHub Actions）
            if cookies_data:
                self.logger.debug("從環境變數載入 cookies...")
                try:
                    # 移除可能的空白字符
                    cookies_data = cookies_data.strip()
                    cookies_bytes = base64.b64decode(cookies_data)
                    cookies = pickle.loads(cookies_bytes)
                except (UnicodeDecodeError, ValueError, pickle.UnpicklingError) as e:
                    self.logger.warning(f"Cookie 解碼失敗，可能格式錯誤: {e}")
                    return False
            # 其次使用本地檔案
            elif cookies_path and cookies_path.exists():
                self.logger.debug(f"從檔案載入 cookies: {cookies_path}")
                with open(cookies_path, 'rb') as f:
                    cookies = pickle.load(f)
            else:
                self.logger.debug("沒有找到 cookies")
                return False
            
            # 載入 cookies 到瀏覽器
            for cookie in cookies:
                # 移除可能導致錯誤的欄位
                cookie.pop('sameSite', None)
                cookie.pop('expiry', None)
                try:
                    self.driver.add_cookie(cookie)
                except Exception as e:
                    self.logger.debug(f"跳過無效 cookie: {e}")
            
            self.logger.success(f"成功載入 {len(cookies)} 個 cookies")
            return True
            
        except Exception as e:
            self.logger.error(f"載入 cookies 失敗: {e}")
            return False
    
    def _take_screenshot(self, name: str = "error") -> None:
        """
        擷取當前頁面截圖（用於除錯）
        
        Args:
            name: 截圖檔案名稱前綴
        """
        if self.driver:
            try:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                screenshot_path = Path(LOG_DIR_BASE) / f"{name}_{timestamp}.png"
                screenshot_path.parent.mkdir(parents=True, exist_ok=True)
                
                self.driver.save_screenshot(str(screenshot_path))
                self.logger.warning(f"截圖已儲存: {screenshot_path}")
            except Exception as e:
                self.logger.warning(f"截圖失敗: {e}")
    
    def _wait_for_download_complete(
        self, 
        expected_filename: Optional[str] = None,
        timeout: int = 60
    ) -> Optional[Path]:
        """
        等待下載完成
        
        Args:
            expected_filename: 預期的檔案名稱（可選）
            timeout: 超時秒數
            
        Returns:
            下載完成的檔案路徑，失敗則返回 None
        """
        self.logger.progress("等待檔案下載完成...")
        
        elapsed = 0
        while elapsed < timeout:
            try:
                files = list(self.download_dir.glob('*'))
                # 過濾掉未完成的下載檔案
                valid_files = [f for f in files if not f.name.endswith('.crdownload') and not f.name.endswith('.tmp')]
                
                if valid_files:
                    # 如果指定了檔名，尋找匹配的檔案
                    if expected_filename:
                        for f in valid_files:
                            if f.name == expected_filename:
                                # 確認檔案是最近下載的
                                if (time.time() - f.stat().st_ctime) < 120:
                                    self.logger.success(f"檔案下載完成: {f.name}")
                                    return f
                    else:
                        # 否則返回最新的檔案
                        latest_file = max(valid_files, key=lambda x: x.stat().st_ctime)
                        if (time.time() - latest_file.stat().st_ctime) < 120:
                            self.logger.success(f"檔案下載完成: {latest_file.name}")
                            return latest_file
                
                time.sleep(2)
                elapsed += 2
            except Exception as e:
                self.logger.warning(f"檢查下載狀態時發生錯誤: {e}")
                time.sleep(2)
                elapsed += 2
        
        self.logger.error(f"下載超時（{timeout} 秒）")
        return None
    
    @abstractmethod
    def _perform_login(self) -> bool:
        """
        執行登入流程（由子類實現）
        
        Returns:
            登入是否成功
        """
        pass
    
    @abstractmethod
    def _trigger_download(self) -> bool:
        """
        觸發下載動作（由子類實現）
        
        Returns:
            下載是否成功觸發
        """
        pass
    
    def download_data(self, year: str = None, output_dir: str = None) -> Tuple[bool, Optional[Path]]:
        """
        執行完整的下載流程
        
        Args:
            year: 年度（可選，保持與基類接口一致）
            output_dir: 輸出目錄（可選）
            
        Returns:
            (是否成功, 下載的檔案路徑)
        """
        try:
            # 初始化瀏覽器
            self._init_driver()
            
            # 執行登入
            if not self._perform_login():
                self.logger.error("登入失敗")
                self._take_screenshot("login_failed")
                return False, None
            
            # 觸發下載
            if not self._trigger_download():
                self.logger.error("觸發下載失敗")
                self._take_screenshot("download_trigger_failed")
                return False, None
            
            # 等待下載完成
            downloaded_file = self._wait_for_download_complete()
            if not downloaded_file:
                self._take_screenshot("download_timeout")
                return False, None
            
            return True, downloaded_file
            
        except WebDriverException as e:
            self.logger.error(f"Selenium 錯誤: {e}")
            self._take_screenshot("selenium_error")
            return False, None
        except Exception as e:
            self.logger.error(f"下載過程發生錯誤: {e}")
            self._take_screenshot("unknown_error")
            return False, None
        finally:
            # 確保資源被清理
            self._close_driver()
    
    def __del__(self):
        """確保資源被釋放"""
        self._close_driver()
        super().__del__()
