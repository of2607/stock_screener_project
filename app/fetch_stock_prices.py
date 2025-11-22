"""
TWSE 股價資料抓取工具
============================

獨立的股價抓取功能，支援上市上櫃股價下載
輸出格式：latest_stock_prices.json 和 latest_stock_prices.csv
"""
import os
import sys
import json
from typing import Dict, Any

# 加入當前路徑以確保模組可以正確匯入
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

try:
    # 匯入設定
    from config.settings import MERGED_CSV_DIR, MERGED_JSON_DIR, LOG_DIR_BASE, ensure_directories
    
    # 匯入功能模組
    from utils.logger import Logger
    from downloaders.stock_price_downloader import StockPriceDownloader
    from processors.stock_price_processor import StockPriceProcessor
    
except ImportError as e:
    print(f"❌ 匯入模組失敗: {e}")
    print("請確認所有必要的模組檔案都存在且路徑正確")
    sys.exit(1)


class StockPriceFetcher:
    """股價抓取主控制器"""
    
    def __init__(self):
        """初始化股價抓取器"""
        # 確保目錄存在
        ensure_directories()
        
        # 設定日誌 (放在 merged_data 根目錄)
        log_path = os.path.join(LOG_DIR_BASE, "stock_price_log.json")
        self.logger = Logger(log_path)
        
        # 初始化下載器和處理器
        self.downloader = StockPriceDownloader(self.logger)
        self.processor = StockPriceProcessor(self.logger)
        
        # 輸出檔案路徑
        self.json_output_path = os.path.join(MERGED_JSON_DIR, "latest_stock_prices.json")
        self.csv_output_path = os.path.join(MERGED_CSV_DIR, "latest_stock_prices.csv")
    
    def fetch_and_save(self) -> bool:
        """
        抓取股價資料並儲存
        
        Returns:
            是否成功完成
        """
        self.logger.info("🚀 開始抓取最新股價資料...")
        
        try:
            # 1. 下載股價資料
            success, raw_data_dict = self.downloader.download_data()
            
            if not success or not raw_data_dict:
                self.logger.error("股價資料下載失敗")
                return False
            
            # 2. 處理資料
            processed_df = self.processor.process_stock_data(raw_data_dict)
            
            if processed_df.empty:
                self.logger.error("股價資料處理後為空")
                return False
            
            # 3. 格式化輸出資料
            output_df = self.processor.format_for_output(processed_df)
            
            # 4. 儲存檔案
            json_success = self._save_json(output_df)
            csv_success = self._save_csv(output_df)
            
            if json_success and csv_success:
                # 5. 記錄處理結果
                self._log_result(output_df)
                self.logger.success("🎉 股價資料抓取完成！")
                return True
            else:
                self.logger.error("檔案儲存失敗")
                return False
                
        except Exception as e:
            self.logger.error(f"股價抓取過程發生錯誤: {e}")
            return False
    
    def _save_json(self, df) -> bool:
        """儲存 JSON 檔案"""
        try:
            # 轉換為字典格式
            data = df.to_dict('records')
            
            # 加入元資料
            output_data = {
                'metadata': self.processor.get_summary_stats(df),
                'data': data
            }
            
            with open(self.json_output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            
            self.logger.success(f"JSON 已儲存: {self.json_output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"儲存 JSON 失敗: {e}")
            return False
    
    def _save_csv(self, df) -> bool:
        """儲存 CSV 檔案"""
        try:
            df.to_csv(self.csv_output_path, index=False, encoding='utf-8-sig')
            self.logger.success(f"CSV 已儲存: {self.csv_output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"儲存 CSV 失敗: {e}")
            return False
    
    def _log_result(self, df) -> None:
        """記錄處理結果"""
        stats = self.processor.get_summary_stats(df)
        
        log_entry = {
            'type': 'stock_prices',
            'timestamp': stats.get('updated_at'),
            'total_records': stats.get('total_count', 0),
            'tse_records': stats.get('tse_count', 0),
            'otc_records': stats.get('otc_count', 0),
            'price_range': {
                'min': stats.get('price_min', 0),
                'max': stats.get('price_max', 0),
                'mean': stats.get('price_mean', 0)
            },
            'files': {
                'json': self.json_output_path,
                'csv': self.csv_output_path
            }
        }
        
        # 寫入處理日誌
        self.logger.write_processing_log(
            year='current',
            report_name='stock_prices',
            csv_path=self.csv_output_path,
            json_path=self.json_output_path,
            row_count=stats.get('total_count', 0)
        )


def main() -> None:
    """主程式入口"""
    try:
        print("🏢 TWSE 股價資料抓取工具")
        print("=" * 40)
        
        fetcher = StockPriceFetcher()
        success = fetcher.fetch_and_save()
        
        if success:
            print("\n✅ 股價資料抓取成功！")
            print(f"📄 JSON 檔案: {fetcher.json_output_path}")
            print(f"📊 CSV 檔案: {fetcher.csv_output_path}")
        else:
            print("\n❌ 股價資料抓取失敗！")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️ 使用者中斷程式執行")
    except Exception as e:
        print(f"\n❌ 程式執行失敗: {e}")
        raise


if __name__ == "__main__":
    main()