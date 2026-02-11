"""
盈再表 Cookies 登入功能測試腳本
===================================

測試目標：
1. 檢查 .cookies 檔案的存在性和格式
2. 驗證關鍵認證 cookie 的有效性
3. 測試 cookies 載入流程
4. 驗證登入狀態
5. 診斷問題並提供詳細報告
"""
import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple

# 添加專案路徑
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "app"))

from utils.logger import Logger
from downloaders.yingzaibiao_downloader import CookieBasedStrategy
from config.settings import YINGZAIBIAO_COOKIES_PATH, YINGZAIBIAO_URL


class CookiesLoginTester:
    """Cookies 登入功能測試器"""
    
    def __init__(self):
        # 創建測試專用的 logger
        log_path = project_root / "app" / "datas" / "logs" / "cookie_test_log.json"
        self.logger = Logger(str(log_path))
        # 使用絕對路徑來定位 cookies 檔案
        self.cookies_path = project_root / "app" / "datas" / "raw_data" / "yingzaibiao" / ".cookies"
        self.test_results = {
            "file_exists": False,
            "file_readable": False,
            "cookies_count": 0,
            "has_aspxauth": False,
            "has_session": False,
            "aspxauth_expired": None,
            "cookies_data": None,
            "load_test_passed": False,
            "login_test_passed": False,
            "errors": []
        }
    
    def run_all_tests(self) -> Dict:
        """執行所有測試"""
        self.logger.info("=" * 70)
        self.logger.info("🧪 開始盈再表 Cookies 登入功能測試")
        self.logger.info("=" * 70)
        
        # 測試 1: 檢查 cookies 檔案
        self.test_cookies_file_exists()
        
        # 測試 2: 讀取並驗證 cookies 內容
        if self.test_results["file_exists"]:
            self.test_cookies_content()
        
        # 測試 3: 驗證關鍵 cookies
        if self.test_results["file_readable"]:
            self.test_critical_cookies()
        
        # 測試 4: 測試 cookies 載入
        if self.test_results["file_readable"]:
            self.test_cookies_loading()
        
        # 測試 5: 測試實際登入
        if self.test_results["load_test_passed"]:
            self.test_actual_login()
        
        # 生成測試報告
        self.generate_report()
        
        return self.test_results
    
    def test_cookies_file_exists(self):
        """測試 1: 檢查 cookies 檔案是否存在"""
        self.logger.info("\n📋 測試 1: 檢查 Cookies 檔案")
        self.logger.info("-" * 70)
        
        if self.cookies_path.exists():
            self.test_results["file_exists"] = True
            file_size = self.cookies_path.stat().st_size
            file_mtime = datetime.fromtimestamp(self.cookies_path.stat().st_mtime)
            
            self.logger.success(f"✓ Cookies 檔案存在: {self.cookies_path}")
            self.logger.info(f"  檔案大小: {file_size} 位元組")
            self.logger.info(f"  最後修改: {file_mtime.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            self.test_results["file_exists"] = False
            self.test_results["errors"].append("Cookies 檔案不存在")
            self.logger.error(f"✗ Cookies 檔案不存在: {self.cookies_path}")
    
    def test_cookies_content(self):
        """測試 2: 讀取並驗證 cookies 內容"""
        self.logger.info("\n📋 測試 2: 讀取 Cookies 內容")
        self.logger.info("-" * 70)
        
        try:
            with open(self.cookies_path, 'r', encoding='utf-8') as f:
                cookies = json.load(f)
            
            self.test_results["file_readable"] = True
            self.test_results["cookies_count"] = len(cookies)
            self.test_results["cookies_data"] = cookies
            
            self.logger.success(f"✓ Cookies 檔案可讀取")
            self.logger.info(f"  Cookies 總數: {len(cookies)}")
            
            # 顯示所有 cookies 名稱
            cookie_names = [c.get('name', 'unknown') for c in cookies]
            self.logger.info(f"  Cookies 列表: {', '.join(cookie_names)}")
            
        except json.JSONDecodeError as e:
            self.test_results["file_readable"] = False
            self.test_results["errors"].append(f"JSON 格式錯誤: {e}")
            self.logger.error(f"✗ JSON 格式錯誤: {e}")
        except Exception as e:
            self.test_results["file_readable"] = False
            self.test_results["errors"].append(f"讀取失敗: {e}")
            self.logger.error(f"✗ 讀取失敗: {e}")
    
    def test_critical_cookies(self):
        """測試 3: 驗證關鍵 cookies"""
        self.logger.info("\n📋 測試 3: 驗證關鍵認證 Cookies")
        self.logger.info("-" * 70)
        
        cookies = self.test_results["cookies_data"]
        cookies_dict = {c['name']: c for c in cookies}
        
        # 檢查 .ASPXAUTH
        if '.ASPXAUTH' in cookies_dict:
            self.test_results["has_aspxauth"] = True
            aspxauth = cookies_dict['.ASPXAUTH']
            
            self.logger.success("✓ .ASPXAUTH cookie 存在")
            self.logger.info(f"  Value 前 30 字元: {aspxauth.get('value', '')[:30]}...")
            
            # 檢查過期時間
            if 'expiry' in aspxauth:
                expiry_timestamp = aspxauth['expiry']
                expiry_date = datetime.fromtimestamp(expiry_timestamp, tz=timezone.utc)
                now = datetime.now(timezone.utc)
                
                self.logger.info(f"  過期時間: {expiry_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                self.logger.info(f"  當前時間: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                
                if now < expiry_date:
                    time_left = expiry_date - now
                    self.test_results["aspxauth_expired"] = False
                    self.logger.success(f"  ✓ Cookie 仍有效（剩餘 {time_left.days} 天 {time_left.seconds // 3600} 小時）")
                else:
                    self.test_results["aspxauth_expired"] = True
                    self.test_results["errors"].append(".ASPXAUTH cookie 已過期")
                    self.logger.error(f"  ✗ Cookie 已過期")
            else:
                self.logger.warning("  ⚠ 無過期時間（Session Cookie）")
        else:
            self.test_results["has_aspxauth"] = False
            self.test_results["errors"].append("缺少 .ASPXAUTH cookie")
            self.logger.error("✗ .ASPXAUTH cookie 不存在")
        
        # 檢查 ASP.NET_SessionId
        if 'ASP.NET_SessionId' in cookies_dict:
            self.test_results["has_session"] = True
            session = cookies_dict['ASP.NET_SessionId']
            
            self.logger.success("✓ ASP.NET_SessionId cookie 存在")
            self.logger.info(f"  Value: {session.get('value', 'N/A')}")
            
            if 'expiry' not in session:
                self.logger.info("  類型: Session Cookie（無過期時間）")
        else:
            self.test_results["has_session"] = False
            self.test_results["errors"].append("缺少 ASP.NET_SessionId cookie")
            self.logger.error("✗ ASP.NET_SessionId cookie 不存在")
        
        # 檢查其他屬性
        self.logger.info("\n  關鍵 Cookies 屬性分析:")
        for name in ['.ASPXAUTH', 'ASP.NET_SessionId']:
            if name in cookies_dict:
                cookie = cookies_dict[name]
                self.logger.info(f"  {name}:")
                self.logger.info(f"    - domain: {cookie.get('domain', 'N/A')}")
                self.logger.info(f"    - path: {cookie.get('path', 'N/A')}")
                self.logger.info(f"    - secure: {cookie.get('secure', 'N/A')}")
                self.logger.info(f"    - httpOnly: {cookie.get('httpOnly', 'N/A')}")
                self.logger.info(f"    - sameSite: {cookie.get('sameSite', 'N/A')}")
    
    def test_cookies_loading(self):
        """測試 4: 測試 cookies 載入流程"""
        self.logger.info("\n📋 測試 4: 測試 Cookies 載入流程")
        self.logger.info("-" * 70)
        
        try:
            # 創建 Cookie 策略實例
            strategy = CookieBasedStrategy(self.logger, cookies_file=self.cookies_path)
            
            # 初始化 driver
            from app.downloaders.yingzaibiao_downloader import _PlainSelenium
            strategy.base_downloader = _PlainSelenium(self.logger, str(strategy.download_dir))
            strategy.base_downloader._init_driver()
            strategy.driver = strategy.base_downloader.driver
            strategy.wait = strategy.base_downloader.wait
            
            self.logger.info("✓ WebDriver 初始化成功")
            
            # 訪問網站根目錄建立 domain context
            base_url = "https://stocks.ddns.net/"
            self.logger.info(f"訪問網站: {base_url}")
            strategy.driver.get(base_url)
            time.sleep(2)
            
            # 載入 cookies
            self.logger.info("載入 Cookies...")
            load_result = strategy.base_downloader.load_cookies(
                cookies_data=None,
                cookies_path=self.cookies_path
            )
            
            if load_result:
                self.test_results["load_test_passed"] = True
                self.logger.success("✓ Cookies 載入成功")
                
                # 驗證載入的 cookies
                loaded_cookies = strategy.driver.get_cookies()
                loaded_dict = {c['name']: c for c in loaded_cookies}
                
                self.logger.info(f"  載入的 Cookies 數量: {len(loaded_cookies)}")
                self.logger.info(f"  .ASPXAUTH: {'✓' if '.ASPXAUTH' in loaded_dict else '✗'}")
                self.logger.info(f"  ASP.NET_SessionId: {'✓' if 'ASP.NET_SessionId' in loaded_dict else '✗'}")
                
                if '.ASPXAUTH' in loaded_dict:
                    value = loaded_dict['.ASPXAUTH'].get('value', '')
                    self.logger.info(f"  .ASPXAUTH Value 前 30 字元: {value[:30]}...")
            else:
                self.test_results["load_test_passed"] = False
                self.test_results["errors"].append("Cookies 載入失敗")
                self.logger.error("✗ Cookies 載入失敗")
            
            # 清理
            strategy.base_downloader._close_driver()
            
        except Exception as e:
            self.test_results["load_test_passed"] = False
            self.test_results["errors"].append(f"載入測試異常: {e}")
            self.logger.error(f"✗ 載入測試異常: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
    
    def test_actual_login(self):
        """測試 5: 測試實際登入狀態"""
        self.logger.info("\n📋 測試 5: 測試實際登入狀態")
        self.logger.info("-" * 70)
        
        try:
            # 創建 Cookie 策略實例
            strategy = CookieBasedStrategy(self.logger, cookies_file=self.cookies_path)
            
            # 初始化 driver
            from app.downloaders.yingzaibiao_downloader import _PlainSelenium
            strategy.base_downloader = _PlainSelenium(self.logger, str(strategy.download_dir))
            strategy.base_downloader._init_driver()
            strategy.driver = strategy.base_downloader.driver
            strategy.wait = strategy.base_downloader.wait
            
            # 訪問根目錄
            base_url = "https://stocks.ddns.net/"
            strategy.driver.get(base_url)
            time.sleep(2)
            
            # 載入 cookies
            strategy.base_downloader.load_cookies(
                cookies_data=None,
                cookies_path=self.cookies_path
            )
            
            # 訪問目標頁面
            self.logger.info(f"訪問目標頁面: {YINGZAIBIAO_URL}")
            strategy.driver.get(YINGZAIBIAO_URL)
            time.sleep(5)
            
            # 檢查當前 URL
            current_url = strategy.driver.current_url
            self.logger.info(f"當前 URL: {current_url}")
            
            # 判斷是否成功登入
            if "login.aspx" in current_url.lower():
                self.test_results["login_test_passed"] = False
                self.test_results["errors"].append("被重導向到登入頁面，Cookie 認證失敗")
                self.logger.error("✗ 被重導向到登入頁面，Cookie 認證失敗")
                
                # 截圖
                strategy.base_downloader._take_screenshot("cookie_login_failed")
            else:
                # 檢查下載按鈕
                try:
                    from selenium.webdriver.common.by import By
                    from selenium.webdriver.support import expected_conditions as EC
                    
                    download_button = strategy.wait.until(
                        EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_Linkbutton1"))
                    )
                    
                    self.test_results["login_test_passed"] = True
                    self.logger.success("✓ 成功進入下載頁面，Cookie 認證有效")
                    self.logger.info("  找到下載按鈕，登入狀態正常")
                    
                except Exception as e:
                    self.test_results["login_test_passed"] = False
                    self.test_results["errors"].append(f"找不到下載按鈕: {e}")
                    self.logger.error(f"✗ 找不到下載按鈕: {e}")
                    
                    # 截圖
                    strategy.base_downloader._take_screenshot("download_button_not_found")
            
            # 清理
            strategy.base_downloader._close_driver()
            
        except Exception as e:
            self.test_results["login_test_passed"] = False
            self.test_results["errors"].append(f"登入測試異常: {e}")
            self.logger.error(f"✗ 登入測試異常: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
    
    def generate_report(self):
        """生成測試報告"""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("📊 測試報告總結")
        self.logger.info("=" * 70)
        
        # 測試結果摘要
        total_tests = 5
        passed_tests = sum([
            self.test_results["file_exists"],
            self.test_results["file_readable"],
            self.test_results["has_aspxauth"] and self.test_results["has_session"],
            self.test_results["load_test_passed"],
            self.test_results["login_test_passed"]
        ])
        
        self.logger.info(f"\n通過測試: {passed_tests}/{total_tests}")
        
        # Cookies 檔案狀態
        self.logger.info("\n【Cookies 檔案狀態】")
        self.logger.info(f"  檔案存在: {'✓ 是' if self.test_results['file_exists'] else '✗ 否'}")
        self.logger.info(f"  檔案可讀: {'✓ 是' if self.test_results['file_readable'] else '✗ 否'}")
        self.logger.info(f"  Cookies 數量: {self.test_results['cookies_count']}")
        
        # 關鍵 Cookies 狀態
        self.logger.info("\n【關鍵 Cookies 狀態】")
        self.logger.info(f"  .ASPXAUTH: {'✓ 存在' if self.test_results['has_aspxauth'] else '✗ 不存在'}")
        if self.test_results['has_aspxauth'] and self.test_results['aspxauth_expired'] is not None:
            self.logger.info(f"  .ASPXAUTH 有效性: {'✗ 已過期' if self.test_results['aspxauth_expired'] else '✓ 仍有效'}")
        self.logger.info(f"  ASP.NET_SessionId: {'✓ 存在' if self.test_results['has_session'] else '✗ 不存在'}")
        
        # 功能測試結果
        self.logger.info("\n【功能測試結果】")
        self.logger.info(f"  Cookies 載入: {'✓ 成功' if self.test_results['load_test_passed'] else '✗ 失敗'}")
        self.logger.info(f"  登入驗證: {'✓ 成功' if self.test_results['login_test_passed'] else '✗ 失敗'}")
        
        # 錯誤列表
        if self.test_results["errors"]:
            self.logger.info("\n【發現的問題】")
            for i, error in enumerate(self.test_results["errors"], 1):
                self.logger.error(f"  {i}. {error}")
        
        # 診斷建議
        self.logger.info("\n【診斷建議】")
        if self.test_results["aspxauth_expired"]:
            self.logger.warning("  ⚠ .ASPXAUTH Cookie 已過期，需要重新登入更新 Cookie")
        elif not self.test_results["has_aspxauth"]:
            self.logger.warning("  ⚠ 缺少認證 Cookie，需要執行登入流程")
        elif not self.test_results["load_test_passed"]:
            self.logger.warning("  ⚠ Cookie 載入失敗，可能是格式或屬性問題")
        elif not self.test_results["login_test_passed"]:
            self.logger.warning("  ⚠ Cookie 雖然載入但認證失敗，可能是伺服器端會話已失效")
        else:
            self.logger.success("  ✓ Cookies 登入功能正常運作")
        
        self.logger.info("\n" + "=" * 70)
        self.logger.info("測試完成")
        self.logger.info("=" * 70)


def main():
    """主程式"""
    tester = CookiesLoginTester()
    results = tester.run_all_tests()
    
    # 返回狀態碼
    if results["login_test_passed"]:
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
