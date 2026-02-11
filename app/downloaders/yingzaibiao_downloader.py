"""
盈再表下載器 - 重構版本
========================

採用策略模式，分離不同環境的下載方式：
1. CookieBasedStrategy - 仅使用 Cookie，无需登入（GitHub Actions）
2. LocalDevelopmentStrategy - 本地開發，支持手動 reCAPTCHA
3. CacheLoaderStrategy - 离线降级，使用本地缓存

架構簡潔，易於維護和擴展
"""
import os
import time
import json
import base64
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Tuple, Optional, Dict, List
from datetime import datetime, timedelta

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from .selenium_base_downloader import SeleniumBaseDownloader
from config.settings import (
    YINGZAIBIAO_URL,
    YINGZAIBIAO_LOGIN_URL,
    YINGZAIBIAO_DOWNLOAD_DIR,
    YINGZAIBIAO_COOKIES_PATH,
    YINGZAIBIAO_RAW_DIR,
)


# ============================================================================
# 輔助類：提供最小實作的 SeleniumBaseDownloader
# ============================================================================

class _PlainSelenium(SeleniumBaseDownloader):
    """給策略使用的輕量封裝，實作抽象方法為 no-op"""

    def __init__(self, logger, download_dir: str):
        super().__init__(logger, download_dir)

    def _perform_login(self) -> bool:
        return True

    def _trigger_download(self) -> bool:
        return True

# ============================================================================
# 策略基類
# ============================================================================

class YingZaiBiaoStrategy(ABC):
    """盈再表下載策略抽象基類"""

    def __init__(self, logger):
        self.logger = logger
        self.driver = None
        self.wait = None
        self.download_dir = Path(YINGZAIBIAO_DOWNLOAD_DIR)
        self.download_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def download(self) -> Tuple[bool, str]:
        """
        執行下載
        
        Returns:
            (是否成功, 說明訊息)
        """
        pass

    def _cleanup(self):
        """清理資源"""
        pass

    def _allow_multiple_downloads(self):
        """透過 CDP 設定允許多檔下載，避免提示阻擋"""
        if not self.driver:
            return
        try:
            self.driver.execute_cdp_cmd(
                "Page.setDownloadBehavior",
                {
                    "behavior": "allow",
                    "downloadPath": str(self.download_dir),
                },
            )
            self.logger.debug("已設定允許多檔案下載")
        except Exception as e:
            self.logger.debug(f"設定多檔下載失敗（可忽略）: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cleanup()


# ============================================================================
# 策略 1：仅使用 Cookie（推薦用於 GitHub Actions）
# ============================================================================

class CookieBasedStrategy(YingZaiBiaoStrategy):
    """
    仅使用 Cookie 的下載策略
    
    特點：
    - 無需登入驗證
    - 無 reCAPTCHA 干擾
    - 適合 GitHub Actions 環境
    - 需要预先提供有效的 Cookie
    """

    def __init__(self, logger, cookies_env_var: str = None, cookies_file: Path = None):
        super().__init__(logger)
        self.cookies_env_var = cookies_env_var or "YINGZAIBIAO_COOKIES"
        self.cookies_file = cookies_file or Path(YINGZAIBIAO_COOKIES_PATH)
        self.base_downloader = None

    def download(self) -> Tuple[bool, str]:
        """使用 Cookie 下載"""
        try:
            self.base_downloader = _PlainSelenium(self.logger, str(self.download_dir))
            self.base_downloader._init_driver()
            self.driver = self.base_downloader.driver
            self.wait = self.base_downloader.wait

            # 預先允許多檔案下載，避免 Chrome 再跳出提示
            self._allow_multiple_downloads()

            # 預先允許多檔案下載，避免 Chrome 再跳出提示
            self._allow_multiple_downloads()

            self.logger.info("使用 Cookie 策略下載")
            
            # 檢查 Cookie 可用性
            cookies_data = os.getenv(self.cookies_env_var, "")
            if not cookies_data and not self.cookies_file.exists():
                return False, "無可用 Cookie（請提供環境變數或本地檔案）"

            # 【策略優化】訪問網站根目錄建立 domain context，避免觸發任何業務邏輯
            base_url = "https://stocks.ddns.net/"
            self.logger.debug(f"訪問網站根目錄以建立 Domain Context: {base_url}")
            try:
                self.driver.get(base_url)
                # CI 環境需要更長的載入時間
                wait_time = 5 if (os.getenv('CI') == 'true' or os.getenv('GITHUB_ACTIONS') == 'true') else 3
                time.sleep(wait_time)
            except Exception as e:
                return False, f"無法訪問網站: {e}"

            # 加載 Cookie
            if not self.base_downloader.load_cookies(
                cookies_data=cookies_data,
                cookies_path=self.cookies_file
            ):
                return False, "Cookie 加載失敗"

            # 驗證關鍵 Cookie 是否成功載入
            loaded_cookies = {c['name']: c['value'] for c in self.driver.get_cookies()}
            has_auth = '.ASPXAUTH' in loaded_cookies
            has_session = 'ASP.NET_SessionId' in loaded_cookies
            self.logger.debug(f"Cookie 驗證: .ASPXAUTH={'✓' if has_auth else '✗'}, ASP.NET_SessionId={'✓' if has_session else '✗'}")
            
            if not (has_auth and has_session):
                self.logger.warning("⚠️ 關鍵認證 Cookie 缺失,可能導致登入失敗")
            
            if has_auth:
                auth_value = loaded_cookies['.ASPXAUTH']
                self.logger.debug(f".ASPXAUTH 值前20字元: {auth_value[:20]}...")

            # 直接訪問目標頁面
            self.logger.debug(f"Cookie 注入完成,前往目標頁面: {YINGZAIBIAO_URL}")
            self.driver.get(YINGZAIBIAO_URL)
            time.sleep(5)

            # 驗證是否成功進入下載頁面
            current_url = self.driver.current_url
            self.logger.debug(f"當前驗證 URL: {current_url}")
            
            if current_url and "login.aspx" in current_url.lower():
                # 增強錯誤診斷
                self.logger.error(f"Cookie 已過期或無效，被重導向到: {current_url}")
                loaded_cookies = {c['name']: c.get('value', '')[:20] + '...' for c in self.driver.get_cookies()}
                self.logger.debug(f"當前瀏覽器 Cookies 狀態: {loaded_cookies}")
                return False, "Cookie 已過期，無法進入下載頁面"

            # 檢查下載按鈕
            try:
                self.wait.until(
                    EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_Linkbutton1"))
                )
                self.logger.success("Cookie 驗證成功，準備下載")
            except TimeoutException:
                return False, "找不到下載按鈕，Cookie 可能無效"

            # 執行下載
            success = self._execute_download()
            if success:
                return True, "使用 Cookie 成功下載"
            else:
                return False, "下載過程失敗"

        except Exception as e:
            self.logger.error(f"Cookie 策略執行失敗: {e}")
            return False, f"異常: {str(e)}"
        finally:
            self._cleanup()

    def _execute_download(self) -> bool:
        """執行實際下載操作"""
        tw_success = False
        us_success = False
        jp_success = False

        # 下載台股資料
        try:
            self.logger.info("下載台股資料 (twlist.xlsx)")
            tw_success = self._download_file(
                "ctl00_ContentPlaceHolder1_Linkbutton1",
                "twlist.xlsx"
            )
        except Exception as e:
            self.logger.error(f"下載台股失敗: {e}")

        # 等待並清理
        time.sleep(3)
        self._cleanup_temp_dir()

        # 下載美股資料
        try:
            self.logger.info("下載美股資料 (uslist.xlsx)")
            us_success = self._download_file(
                "ctl00_ContentPlaceHolder1_Export",
                "uslist.xlsx"
            )
        except Exception as e:
            self.logger.error(f"下載美股失敗: {e}")

        # 等待並清理
        time.sleep(3)
        self._cleanup_temp_dir()

        # 下載日股資料
        try:
            self.logger.info("下載日股資料 (jplist.xlsx)")
            jp_success = self._download_file(
                "ctl00_ContentPlaceHolder1_Linkbutton2",
                "jplist.xlsx"
            )
        except Exception as e:
            self.logger.error(f"下載日股失敗: {e}")

        # 最終清理
        self._cleanup_temp_dir()
        return tw_success or us_success or jp_success

    def _download_file(self, button_id: str, filename: str) -> bool:
        """下載單個文件"""
        try:
            button = self.wait.until(EC.element_to_be_clickable((By.ID, button_id)))
            self.logger.progress(f"點擊下載按鈕: {button_id}")
            
            self.driver.execute_script("arguments[0].click();", button)
            time.sleep(2)

            # 等待下載完成
            downloaded = self.base_downloader._wait_for_download_complete(timeout=60)
            if not downloaded:
                self.logger.error(f"下載 {filename} 超時")
                return False

            # 移動到最終位置
            final_dir = Path(YINGZAIBIAO_RAW_DIR)
            final_dir.mkdir(parents=True, exist_ok=True)
            final_path = final_dir / filename

            if final_path.exists():
                final_path.unlink()
            
            downloaded.rename(final_path)
            self.logger.success(f"檔案已保存: {final_path}")
            return True

        except Exception as e:
            self.logger.error(f"下載 {filename} 失敗: {e}")
            return False

    def _cleanup_temp_dir(self):
        """清理臨時目錄"""
        try:
            for f in self.download_dir.glob('*'):
                if f.is_file():
                    f.unlink()
        except Exception as e:
            self.logger.debug(f"清理臨時檔案失敗: {e}")

    def _cleanup(self):
        """清理資源"""
        if self.base_downloader:
            self.base_downloader._close_driver()


# ============================================================================
# 策略 2：本地開發（支持手動 reCAPTCHA）
# ============================================================================

class LocalDevelopmentStrategy(YingZaiBiaoStrategy):
    """
    本地開發專用策略
    
    特點：
    - 支持手動完成 reCAPTCHA
    - 需要用戶帳密
    - 自動保存 Cookie 供後續使用
    - 仅用於本地開發環境
    """

    def __init__(self, logger):
        super().__init__(logger)
        self.username = os.getenv("YINGZAIBIAO_USERNAME", "")
        self.password = os.getenv("YINGZAIBIAO_PASSWORD", "")
        self.base_downloader = None

    def download(self) -> Tuple[bool, str]:
        """執行本地開發下載"""
        if not self.username or not self.password:
            return False, "未設定帳號密碼環境變數"

        try:
            self.base_downloader = _PlainSelenium(self.logger, str(self.download_dir))
            self.base_downloader._init_driver()
            self.driver = self.base_downloader.driver
            self.wait = self.base_downloader.wait

            # 預先允許多檔案下載
            self._allow_multiple_downloads()

            self.logger.info("使用本地開發策略（支持手動 reCAPTCHA）")

            # 登入
            if not self._perform_login():
                return False, "登入失敗"

            # 提示用戶手動處理
            self.logger.warning("\n" + "=" * 70)
            self.logger.warning("⚠️ 如果出現 Chrome 密碼警告彈窗，請手動關閉")
            self.logger.warning("等待 10 秒...")
            self.logger.warning("=" * 70 + "\n")
            time.sleep(10)

            # 執行下載
            success = self._execute_download()
            if success:
                # 保存 Cookie 供 GitHub Actions 使用
                self._save_cookies_for_ci()
                return True, "本地開發下載成功"
            else:
                return False, "下載失敗"

        except Exception as e:
            self.logger.error(f"本地開發策略失敗: {e}")
            return False, f"異常: {str(e)}"
        finally:
            self._cleanup()

    def _perform_login(self) -> bool:
        """執行登入"""
        try:
            self.logger.progress("前往登入頁面")
            self.driver.get(YINGZAIBIAO_LOGIN_URL)
            time.sleep(2)

            # 輸入憑證
            username_input = self.wait.until(
                EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_txtUsername"))
            )
            password_input = self.driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtPassword")
            login_button = self.driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_btnLogin")

            self.logger.debug("輸入憑證")
            username_input.clear()
            username_input.send_keys(self.username)
            time.sleep(0.3)

            password_input.clear()
            password_input.send_keys(self.password)
            time.sleep(0.3)

            # 提示 reCAPTCHA
            self.logger.warning("\n" + "=" * 70)
            self.logger.warning("⚠️ 如果看到 reCAPTCHA 驗證框，請手動完成驗證")
            self.logger.warning("完成後，程式會自動點擊登入按鈕")
            self.logger.warning("=" * 70 + "\n")
            time.sleep(15)  # 給用戶足夠時間完成 reCAPTCHA

            # 點擊登入
            self.logger.debug("點擊登入按鈕")
            self.driver.execute_script("arguments[0].click();", login_button)
            time.sleep(10)

            # 驗證登入結果
            current_url = self.driver.current_url
            if current_url and "login.aspx" in current_url.lower():
                self.logger.error("登入失敗，仍在登入頁面")
                self.base_downloader._take_screenshot("login_failed")
                return False

            self.logger.success("登入成功")
            self.driver.get(YINGZAIBIAO_URL)
            time.sleep(2)
            return True

        except Exception as e:
            self.logger.error(f"登入失敗: {e}")
            return False

    def _execute_download(self) -> bool:
        """執行下載"""
        tw_success = False
        us_success = False
        jp_success = False

        try:
            self.logger.info("下載台股資料")
            tw_success = self._download_file("ctl00_ContentPlaceHolder1_Linkbutton1", "twlist.xlsx")
        except Exception as e:
            self.logger.error(f"下載台股失敗: {e}")

        time.sleep(3)
        self._cleanup_temp_dir()

        try:
            self.logger.info("下載美股資料")
            us_success = self._download_file("ctl00_ContentPlaceHolder1_Export", "uslist.xlsx")
        except Exception as e:
            self.logger.error(f"下載美股失敗: {e}")

        time.sleep(3)
        self._cleanup_temp_dir()

        try:
            self.logger.info("下載日股資料")
            jp_success = self._download_file("ctl00_ContentPlaceHolder1_Linkbutton2", "jplist.xlsx")
        except Exception as e:
            self.logger.error(f"下載日股失敗: {e}")

        self._cleanup_temp_dir()
        return tw_success or us_success or jp_success

    def _download_file(self, button_id: str, filename: str) -> bool:
        """下載文件"""
        try:
            button = self.wait.until(EC.element_to_be_clickable((By.ID, button_id)))
            self.logger.progress(f"下載 {filename}")
            self.driver.execute_script("arguments[0].click();", button)
            time.sleep(2)

            downloaded = self.base_downloader._wait_for_download_complete(timeout=60)
            if not downloaded:
                return False

            final_dir = Path(YINGZAIBIAO_RAW_DIR)
            final_dir.mkdir(parents=True, exist_ok=True)
            final_path = final_dir / filename

            if final_path.exists():
                final_path.unlink()

            downloaded.rename(final_path)
            self.logger.success(f"保存: {final_path}")
            return True

        except Exception as e:
            self.logger.error(f"下載 {filename} 失敗: {e}")
            return False

    def _save_cookies_for_ci(self):
        """保存 Cookie 供 GitHub Actions 使用"""
        try:
            cookies = self.driver.get_cookies()
            
            # 將 cookies 轉換為 JSON 格式（而非 pickle）
            cookies_json = json.dumps(cookies)
            cookies_b64 = base64.b64encode(cookies_json.encode()).decode()

            self.logger.success("Cookie 已保存，用於 GitHub Actions")
            self.logger.info("\n" + "=" * 70)
            self.logger.info("📌 為了在 GitHub Actions 中使用 Cookie，請：")
            self.logger.info("1. 複製以下內容：")
            self.logger.info(cookies_b64[:50] + "...")
            self.logger.info("2. 在 GitHub Repository → Settings → Secrets 中新增：")
            self.logger.info("   名稱: YINGZAIBIAO_COOKIES")
            self.logger.info("   值: <複製的內容>")
            self.logger.info("=" * 70 + "\n")

            # 同時儲存到本地
            cookies_path = Path(YINGZAIBIAO_COOKIES_PATH)
            cookies_path.parent.mkdir(parents=True, exist_ok=True)
            with open(cookies_path, 'w') as f:
                json.dump(cookies, f, indent=2)
            self.logger.debug(f"本地 Cookie 已保存: {cookies_path}")

        except Exception as e:
            self.logger.warning(f"保存 Cookie 失敗: {e}")

    def _cleanup_temp_dir(self):
        """清理臨時目錄"""
        try:
            for f in self.download_dir.glob('*'):
                if f.is_file():
                    f.unlink()
        except Exception as e:
            self.logger.debug(f"清理臨時檔案失敗: {e}")

    def _cleanup(self):
        """清理資源"""
        if self.base_downloader:
            self.base_downloader._close_driver()


# ============================================================================
# 策略 3：緩存降級（离线模式）
# ============================================================================

class CacheLoaderStrategy(YingZaiBiaoStrategy):
    """
    緩存降級策略
    
    特點：
    - 不進行任何下載
    - 直接使用本地緩存文件
    - 檢查緩存是否過期
    - 用於備用方案
    """

    def __init__(self, logger, cache_retention_days: int = 7):
        super().__init__(logger)
        self.cache_retention_days = cache_retention_days

    def download(self) -> Tuple[bool, str]:
        """檢查並返回緩存狀態"""
        try:
            self.logger.info("使用緩存降級策略")

            excel_files = [
                Path(YINGZAIBIAO_RAW_DIR) / "twlist.xlsx",
                Path(YINGZAIBIAO_RAW_DIR) / "uslist.xlsx",
                Path(YINGZAIBIAO_RAW_DIR) / "jplist.xlsx"
            ]

            missing_files = []
            expired_files = []

            for f in excel_files:
                if not f.exists():
                    missing_files.append(f.name)
                else:
                    # 檢查文件是否過期
                    file_age = datetime.now() - datetime.fromtimestamp(f.stat().st_mtime)
                    if file_age > timedelta(days=self.cache_retention_days):
                        expired_files.append(f"{f.name} ({file_age.days} 天前)")

            if missing_files:
                msg = f"缺少文件: {', '.join(missing_files)}"
                self.logger.warning(msg)
                return False, msg

            if expired_files:
                msg = f"緩存已過期: {', '.join(expired_files)}"
                self.logger.warning(msg)
                # 但仍返回 True，因為至少有數據可用
                self.logger.info("使用過期緩存繼續處理")

            self.logger.success("使用本地緩存文件")
            return True, "使用本地緩存"

        except Exception as e:
            self.logger.error(f"緩存檢查失敗: {e}")
            return False, f"異常: {str(e)}"


# ============================================================================
# 策略工廠
# ============================================================================

class DownloadStrategyFactory:
    """選擇合適的下載策略"""

    @staticmethod
    def create_strategy(logger) -> YingZaiBiaoStrategy:
        """
        根據環境自動選擇策略
        
        優先級：
        1. 如果在 CI 環境，使用 Cookie 策略
        2. 如果有本地 Cookie，使用 Cookie 策略
        3. 如果有帳密，使用本地開發策略（如果不在 CI）
        4. 否則使用緩存降級策略
        """
        is_ci = os.getenv('CI') == 'true' or os.getenv('GITHUB_ACTIONS') == 'true'
        has_cookies = bool(os.getenv("YINGZAIBIAO_COOKIES")) or Path(YINGZAIBIAO_COOKIES_PATH).exists()
        has_credentials = bool(os.getenv("YINGZAIBIAO_USERNAME")) and bool(os.getenv("YINGZAIBIAO_PASSWORD"))

        if has_cookies:
            logger.info("選擇: Cookie 策略")
            return CookieBasedStrategy(logger)

        if has_credentials and not is_ci:
            logger.info("選擇: 本地開發策略")
            return LocalDevelopmentStrategy(logger)

        logger.info("選擇: 緩存降級策略")
        return CacheLoaderStrategy(logger)


# ============================================================================
# 主下載器類（簡化協調器）
# ============================================================================

class YingZaiBiaoDownloader:
    """
    盈再表下載器 - 簡化版協調器
    
    使用策略模式，自動選擇合適的下載方式
    """

    def __init__(self, logger):
        self.logger = logger

    def download_and_save(self) -> Tuple[bool, Optional[Path]]:
        """
        執行下載
        
        Returns:
            (是否成功, 說明訊息)
        """
        self.logger.info("🚀 開始盈再表下載流程")

        # 判斷環境與憑證
        is_ci = os.getenv('CI') == 'true' or os.getenv('GITHUB_ACTIONS') == 'true'
        has_credentials = bool(os.getenv("YINGZAIBIAO_USERNAME")) and bool(os.getenv("YINGZAIBIAO_PASSWORD"))

        # 1st: Cookie 策略（若有 Cookie）
        strategy = DownloadStrategyFactory.create_strategy(self.logger)
        with strategy as s:
            success, message = s.download()

        if success:
            self.logger.success(f"✅ 盈再表下載成功: {message}")
            return True, message

        self.logger.warning(f"⚠️ 盈再表下載失敗: {message}")

        # 2nd: 若 Cookie 失效且有帳密且非 CI，嘗試本地登入
        if (not is_ci) and has_credentials:
            self.logger.info("嘗試改用帳密登入策略（本地開發）...")
            try:
                with LocalDevelopmentStrategy(self.logger) as s2:
                    success2, message2 = s2.download()
                if success2:
                    self.logger.success(f"✅ 盈再表下載成功（帳密登入）: {message2}")
                    return True, message2
                self.logger.warning(f"⚠️ 帳密登入策略失敗: {message2}")
            except Exception as e:
                self.logger.error(f"帳密登入策略異常: {e}")

        # 3rd: 最後使用快取降級
        self.logger.info("使用快取降級策略...")
        with CacheLoaderStrategy(self.logger) as s3:
            success3, message3 = s3.download()
        if success3:
            self.logger.success(f"✅ 使用快取成功: {message3}")
        else:
            self.logger.warning(f"⚠️ 快取策略失敗: {message3}")

        return success3, message3
