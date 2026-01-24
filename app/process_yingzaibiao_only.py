"""
僅處理和上傳盈再表資料（跳過下載步驟）
適用於手動下載 twlist.xlsx 後，只需要轉檔和上傳的情況
"""
import sys
import os

# 加入當前路徑
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from utils.logger import Logger
from processors.yingzaibiao_processor import YingZaiBiaoProcessor
from processors.yingzaibiao_upload import main as upload_main


def main():
    """僅處理和上傳盈再表資料"""
    logger = Logger("YingZaiBiao-ProcessOnly")
    
    print("\n" + "="*50)
    print("🔄 盈再表資料處理與上傳（跳過下載）")
    print("="*50 + "\n")
    
    # 步驟 1: 處理 Excel 轉 CSV/JSON
    print("步驟 1: 處理 twlist.xlsx → CSV/JSON")
    print("="*50)
    processor = YingZaiBiaoProcessor(logger)
    success = processor.process_and_save()
    
    if not success:
        print("\n❌ 資料處理失敗")
        return False
    
    print("\n✅ 資料處理完成\n")
    
    # 步驟 2: 上傳 CSV 和 JSON
    print("步驟 2: 上傳 CSV 和 JSON")
    print("="*50)
    try:
        upload_main()
        print("\n✅ 上傳完成\n")
    except Exception as e:
        print(f"\n❌ 上傳失敗: {e}\n")
        return False
    
    print("="*50)
    print("✅ 所有步驟完成！")
    print("="*50)
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
