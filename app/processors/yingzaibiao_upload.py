"""
上傳盈再表處理後的檔案
可依設定選擇上傳方式，預設支援 Cloudflare R2、Google Drive、Supabase
"""
import os
from pathlib import Path
from config import settings
from config.settings import YINGZAIBIAO_CSV_PATH, YINGZAIBIAO_JSON_PATH, YINGZAIBIAO_RAW_DIR
from utils.uploader.upload import upload

def main():
    # 取得上傳目標
    upload_type = getattr(settings, "UPLOAD_YINGZAIBIAO_TARGET", getattr(settings, "UPLOAD_SUMMARY_REPORT_TARGET", ["all"]))
    targets = upload_type if isinstance(upload_type, list) else [upload_type]

    # 從 settings 取得檔案路徑
    tw_csv_path = YINGZAIBIAO_CSV_PATH
    tw_json_path = YINGZAIBIAO_JSON_PATH
    
    # 美股檔案路徑
    us_csv_path = str(tw_csv_path).replace('.csv', '_us.csv')
    us_json_path = str(tw_json_path).replace('.json', '_us.json')

    config = {
        "gdrive_as_url": getattr(settings, "GDRIVE_AS_URL", None),
        "gdrive_folder_id": getattr(settings, "GDRIVE_FOLDER_ID", None),
        "supabase_url": getattr(settings, "SUPABASE_URL", None),
        "supabase_key": getattr(settings, "SUPABASE_KEY", None),
        "supabase_bucket": getattr(settings, "SUPABASE_BUCKET", None),
        "r2_account_id": getattr(settings, "R2_ACCOUNT_ID", None),
        "r2_access_key": getattr(settings, "R2_ACCESS_KEY", None),
        "r2_secret_key": getattr(settings, "R2_SECRET_KEY", None),
        "r2_bucket": getattr(settings, "R2_BUCKET", None),
        "r2_region": getattr(settings, "R2_REGION", "auto"),
    }

    print(f"開始上傳盈再表資料 ({upload_type}) ...")
    
    # 上傳台股資料
    print("\n=== 上傳台股資料 ===")
    for fpath in [tw_csv_path, tw_json_path]:
        if os.path.isfile(fpath):
            print(f"  上傳 {fpath} ...")
            upload(fpath, targets, config)
        else:
            print(f"  檔案不存在：{fpath}")
    
    # 上傳美股資料
    print("\n=== 上傳美股資料 ===")
    for fpath in [us_csv_path, us_json_path]:
        if os.path.isfile(fpath):
            print(f"  上傳 {fpath} ...")
            upload(fpath, targets, config)
        else:
            print(f"  檔案不存在：{fpath}")
    
    print("\n盈再表資料上傳完成！")
