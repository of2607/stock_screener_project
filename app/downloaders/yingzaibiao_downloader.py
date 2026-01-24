"""
盈再表下載器
============

使用 Selenium 自動登入並下載 twlist.xlsx
"""
import os
import time
from pathlib import Path
from typing import Tuple, Optional

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from .selenium_base_downloader import SeleniumBaseDownloader
from config.settings import (
    YINGZAIBIAO_URL, 
    YINGZAIBIAO_LOGIN_URL,
    YINGZAIBIAO_DOWNLOAD_DIR,
    YINGZAIBIAO_COOKIES_PATH,
    YINGZAIBIAO_RAW_DIR
)


class YingZaiBiaoDownloader(SeleniumBaseDownloader):
    """盈再表下載器 - 自動登入並下載 twlist.xlsx"""
    
    def __init__(self, logger):
        """
        初始化盈再表下載器
        
        Args:
            logger: 日誌記錄器
        """
        # 設定下載目錄為 settings 中配置的路徑
        super().__init__(logger, YINGZAIBIAO_DOWNLOAD_DIR)
        
        # 從設定檔讀取 URL，從環境變數讀取憑證
        self.login_url = YINGZAIBIAO_LOGIN_URL
        self.target_url = YINGZAIBIAO_URL
        self.username = os.getenv("YINGZAIBIAO_USERNAME", "")
        self.password = os.getenv("YINGZAIBIAO_PASSWORD", "")
        self.cookies_data = os.getenv("YINGZAIBIAO_COOKIES", "")  # 從環境變數讀取 cookies
        self.cookies_path = Path(YINGZAIBIAO_COOKIES_PATH)  # 從 settings 讀取 cookies 路徑
        
        if not self.username or not self.password:
            if not self.cookies_data and not self.cookies_path.exists():
                self.logger.warning("未設定盈再表登入憑證或 cookies")
    
    def _perform_login(self) -> bool:
        """
        執行登入流程（優先使用 cookies，失敗則使用帳密登入）
        
        Returns:
            登入是否成功
        """
        try:
            # 策略 1: 嘗試使用 cookies 登入
            if self.cookies_data or self.cookies_path.exists():
                self.logger.info("嘗試使用 cookies 登入...")
                
                # 先訪問網站以設定 domain
                self.driver.get(self.target_url)
                time.sleep(2)
                
                # 載入 cookies
                if self.load_cookies(cookies_data=self.cookies_data, cookies_path=self.cookies_path):
                    # 重新載入頁面以套用 cookies
                    self.driver.refresh()
                    time.sleep(3)
                    
                    # 驗證是否已登入（檢查是否在下載頁面）
                    try:
                        self.wait.until(
                            EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_Linkbutton1"))
                        )
                        self.logger.success("使用 cookies 登入成功")
                        return True
                    except:
                        self.logger.warning("Cookies 可能已過期，改用帳密登入...")
            
            # 策略 2: 使用帳號密碼登入
            self.logger.progress("前往盈再表登入頁面...")
            self.driver.get(self.login_url)
            self.logger.info("等待頁面載入（包含 Google reCAPTCHA）...")
            time.sleep(5)  # 增加等待時間讓 Google 驗證完成
            
            # 檢查登入憑證
            if not self.username or not self.password:
                self.logger.error("未提供登入憑證")
                return False
            
            self.logger.info("開始自動登入...")
            
            # 使用具體的 ID 定位元素
            try:
                # 等待並找到帳號輸入框
                username_input = self.wait.until(
                    EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_txtUsername"))
                )
                
                # 找到密碼輸入框
                password_input = self.driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtPassword")
                
                # 找到登入按鈕
                login_button = self.driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_btnLogin")
                
                # 清空並輸入帳號密碼（使用 send_keys 讓 reCAPTCHA 看到真實用戶行為）
                self.logger.debug("輸入登入憑證...")
                username_input.clear()
                username_input.send_keys(self.username)
                time.sleep(0.5)
                
                password_input.clear()
                password_input.send_keys(self.password)
                time.sleep(0.5)
                
                # 等待 reCAPTCHA 驗證完成（給予足夠時間）
                self.logger.info("等待 Google reCAPTCHA 驗證...")
                time.sleep(3)
                
                # 使用 JavaScript 點擊登入按鈕（避免被廣告遮擋）
                self.logger.debug("點擊登入按鈕...")
                self.driver.execute_script("arguments[0].click();", login_button)
                
                # 等待登入處理和頁面跳轉
                self.logger.info("等待登入處理...")
                time.sleep(10)  # 增加等待時間確保跳轉完成
                
                # 驗證登入是否成功（檢查URL是否已改變或登入按鈕是否還存在）
                current_url = self.driver.current_url
                self.logger.debug(f"當前 URL: {current_url}")
                
                # 如果仍在登入頁面，表示登入失敗
                if "Login.aspx" in current_url:
                    self.logger.error("登入失敗（仍在登入頁面）")
                    # 檢查是否有錯誤訊息
                    try:
                        error_element = self.driver.find_element(By.CLASS_NAME, "error")
                        self.logger.error(f"錯誤訊息: {error_element.text}")
                    except:
                        pass
                    return False
                
                # 登入成功
                self.logger.success("登入成功（URL 已改變）")
                
                # 儲存 cookies 供下次使用
                self.save_cookies(self.cookies_path)
                self.logger.info("提示：可將 cookies 加入 GitHub Secrets 以跳過驗證")
                self.logger.info(f"執行：cat {self.cookies_path} | base64 > datas/raw_data/yingzaibiao/cookies.txt")
                self.logger.info("然後將 cookies.txt 內容設為 GitHub Secret: YINGZAIBIAO_COOKIES")
                
                # 登入後會跳轉，需要再次前往下載頁面
                self.logger.debug("前往下載頁面...")
                self.driver.get(self.target_url)
                time.sleep(5)  # 增加等待時間
                
                return True
                    
            except NoSuchElementException as e:
                self.logger.error(f"找不到登入元素: {e}")
                return False
                
        except TimeoutException:
            self.logger.error("頁面載入超時")
            return False
        except Exception as e:
            self.logger.error(f"登入過程發生錯誤: {e}")
            return False
    
    def _trigger_download(self) -> bool:
        """
        觸發下載動作（點擊下載按鈕）
        
        Returns:
            下載是否成功觸發
        """
        try:
            # 等待一段時間讓用戶手動關閉Chrome密碼彈窗（如果出現）
            self.logger.warning("⚠️ 如果出現Chrome密碼警告彈窗，請手動關閉...")
            self.logger.info("等待10秒讓你關閉彈窗...")
            time.sleep(10)
            
            self.logger.progress("尋找下載按鈕...")
            
            # 等待下載按鈕可點擊
            download_button = self.wait.until(
                EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_Linkbutton1"))
            )
            
            # 先移除可能遮擋的 iframe（廣告）
            try:
                self.logger.debug("移除可能遮擋的 iframe 廣告...")
                self.driver.execute_script("""
                    var iframes = document.querySelectorAll('iframe[style*="z-index: 2147483647"]');
                    iframes.forEach(function(iframe) {
                        iframe.remove();
                    });
                """)
            except:
                pass
            
            # 滾動到按鈕位置
            self.driver.execute_script("arguments[0].scrollIntoView(true);", download_button)
            time.sleep(0.5)
            
            # 使用 JavaScript 點擊（避免被遮擋）
            self.logger.debug("點擊下載按鈕...")
            self.driver.execute_script("arguments[0].click();", download_button)
            self.logger.success("已觸發下載")
            
            return True
            
        except TimeoutException:
            self.logger.error("找不到下載按鈕或按鈕無法點擊")
            return False
        except Exception as e:
            self.logger.error(f"觸發下載時發生錯誤: {e}")
            return False
    
    def download_and_save(self) -> Tuple[bool, Optional[Path]]:
        """
        下載 twlist.xlsx 並儲存到指定位置
        
        Returns:
            (是否成功, 最終檔案路徑)
        """
        self.logger.info("🚀 開始下載盈再表資料...")
        
        # 執行下載
        success, downloaded_file = self.download_data()
        
        if not success or not downloaded_file:
            self.logger.error("下載失敗")
            return False, None
        
        # 移動檔案到最終位置（覆蓋舊檔）
        try:
            final_dir = Path(YINGZAIBIAO_RAW_DIR)
            final_dir.mkdir(parents=True, exist_ok=True)
            
            final_path = final_dir / "twlist.xlsx"
            
            # 檢查下載文件是否存在
            if not downloaded_file.exists():
                self.logger.error(f"下載的檔案不存在: {downloaded_file}")
                self.logger.info(f"嘗試在下載目錄中搜索: {self.download_dir}")
                # 搜索所有可能的文件
                all_files = list(self.download_dir.rglob('*'))
                self.logger.info(f"找到的檔案: {[f.name for f in all_files if f.is_file()]}")
                return False, None
            
            # 如果檔名不是 twlist.xlsx，重新命名
            if downloaded_file.name != "twlist.xlsx":
                self.logger.warning(f"下載的檔名是 {downloaded_file.name}，將重新命名為 twlist.xlsx")
            
            # 移動並覆蓋
            if final_path.exists():
                self.logger.debug(f"移除舊檔案: {final_path}")
                final_path.unlink()
            
            downloaded_file.rename(final_path)
            self.logger.success(f"檔案已儲存: {final_path}")
            
            # 清理 temp 目錄的其他檔案
            for temp_file in self.download_dir.glob('*'):
                if temp_file.is_file():
                    temp_file.unlink()
            
            return True, final_path
            
        except Exception as e:
            self.logger.error(f"移動檔案時發生錯誤: {e}")
            return False, None
