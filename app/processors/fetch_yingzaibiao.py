"""
盈再表資料抓取工具
==================

獨立的盈再表抓取功能，整合下載器與處理器
輸出格式：latest_yingzaibiao.json 和 latest_yingzaibiao.csv
"""
import os
import sys

# 修正 sys.path，確保可從 processors 目錄直接執行時正確匯入 app 下模組
current_dir = os.path.dirname(os.path.abspath(__file__))
app_dir = os.path.abspath(os.path.join(current_dir, ".."))
if app_dir not in sys.path:
    sys.path.insert(0, app_dir)

try:
    # 匯入設定
    from config.settings import LOG_DIR_BASE, ensure_directories

    # 匯入功能模組
    from utils.logger import Logger
    from downloaders.yingzaibiao_downloader import YingZaiBiaoDownloader
    from processors.yingzaibiao_processor import YingZaiBiaoProcessor

except ImportError as e:
    print(f"❌ 匯入模組失敗: {e}")
    print("請確認所有必要的模組檔案都存在且路徑正確")
    sys.exit(1)


class YingZaiBiaoFetcher:
    """盈再表抓取主控制器"""
    
    def __init__(self):
        """初始化盈再表抓取器"""
        # 確保目錄存在
        ensure_directories()
        
        # 設定日誌
        log_path = os.path.join(LOG_DIR_BASE, "yingzaibiao_log.json")
        self.logger = Logger(log_path)
        
        # 初始化下載器和處理器
        self.downloader = YingZaiBiaoDownloader(self.logger)
        self.processor = YingZaiBiaoProcessor(self.logger)
    
    async def fetch_and_save(self, skip_download: bool = False) -> bool:
        """
        抓取盈再表資料並儲存 (async/await)
        
        Args:
            skip_download: 是否跳過下載步驟，直接使用現有檔案處理
        
        Returns:
            是否成功完成
        """
        self.logger.info("🚀 開始抓取盈再表資料...")
        
        tw_success = False
        us_success = False
        download_success = False
        
        try:
            # ========================================
            # 步驟 1: 下載台股和美股資料
            # ========================================
            if not skip_download:
                self.logger.info("=" * 50)
                self.logger.info("步驟 1: 下載 twlist.xlsx 和 uslist.xlsx")
                self.logger.info("=" * 50)
                
                download_success, download_msg = self.downloader.download_and_save()
                
                if not download_success:
                    self.logger.warning(f"⚠️ 盈再表資料下載失敗: {download_msg}")
                    self.logger.info("將嘗試處理已有的資料或使用快取...")
                else:
                    self.logger.success(f"✅ 下載成功: {download_msg}")
            else:
                self.logger.info("=" * 50)
                self.logger.info("步驟 1: 跳過下載 (使用現有檔案)")
                self.logger.info("=" * 50)
                download_success = True  # 假設是處理現有檔案，視為成功繼續流程
            
            # ========================================
            # 步驟 2: 處理台股資料
            # ========================================
            self.logger.info("=" * 50)
            self.logger.info("步驟 2: 處理台股資料")
            self.logger.info("=" * 50)
            
            try:
                tw_success = self.processor.process_and_save()
                if tw_success:
                    self.logger.success("✅ 台股盈再表資料處理完成")
                else:
                    self.logger.warning("⚠️ 台股盈再表資料處理失敗")
            except Exception as e:
                self.logger.error(f"處理台股資料時發生錯誤: {e}")
            
            # ========================================
            # 步驟 3: 處理美股資料
            # ========================================
            self.logger.info("=" * 50)
            self.logger.info("步驟 3: 處理美股資料")
            self.logger.info("=" * 50)
            
            try:
                us_success = self.processor.process_us_and_save()
                if us_success:
                    self.logger.success("✅ 美股盈再表資料處理完成")
                else:
                    self.logger.warning("⚠️ 美股盈再表資料處理失敗")
            except Exception as e:
                self.logger.error(f"處理美股資料時發生錯誤: {e}")
            
            # ========================================
            # 總結
            # ========================================
            self.logger.info("=" * 50)
            if tw_success and us_success:
                self.logger.success("✅ 盈再表資料完全處理成功")
            elif tw_success or us_success:
                self.logger.warning(f"⚠️ 部分完成 (台股: {'✓' if tw_success else '✗'}, 美股: {'✓' if us_success else '✗'})")
            else:
                self.logger.warning("❌ 盈再表資料處理無結果，但不中斷流程")
            self.logger.info("=" * 50)
            
            # 只要有一個市場成功就視為成功
            return tw_success or us_success
            
        except Exception as e:
            self.logger.error(f"抓取盈再表資料時發生異常: {e}")
            return False


async def main(skip_download: bool = False):
    """主函數（async 版本）- 供 main.py 呼叫"""
    fetcher = YingZaiBiaoFetcher()
    success = await fetcher.fetch_and_save(skip_download=skip_download)
    
    if not success:
        print("⚠️ 盈再表資料處理未完全成功，但不影響後續流程")
    
    # 不再 sys.exit(1)，讓後續流程繼續執行
    return success


if __name__ == "__main__":
    """直接執行此腳本時的入口點"""
    import asyncio
    import argparse
    
    print("=" * 60)
    print("盈再表資料抓取工具")
    print("=" * 60)
    
    # 解析命令列參數
    parser = argparse.ArgumentParser(description='盈再表資料抓取與處理工具')
    parser.add_argument('--process-only', action='store_true', help='僅執行資料處理，跳過下載步驟')
    args = parser.parse_args()
    
    try:
        # 根據參數決定是否跳過下載
        asyncio.run(main(skip_download=args.process_only))
    except KeyboardInterrupt:
        print("\n\n⚠️ 使用者中斷執行")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ 執行失敗: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
