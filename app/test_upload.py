"""
簡單測試 upload.py 多目標上傳介面
"""

from utils.uploader.upload import upload
from config import settings

if __name__ == "__main__":
    config = {
        "gdrive_as_url": settings.GDRIVE_AS_URL,
        "gdrive_folder_id": settings.GDRIVE_FOLDER_ID,
        "supabase_url": settings.SUPABASE_URL,
        "supabase_key": settings.SUPABASE_KEY,
        "supabase_bucket": settings.SUPABASE_BUCKET,
        "r2_account_id": settings.R2_ACCOUNT_ID,
        "r2_access_key": settings.R2_ACCESS_KEY,
        "r2_secret_key": settings.R2_SECRET_KEY,
        "r2_bucket": settings.R2_BUCKET,
        "r2_region": settings.R2_REGION
    }

    file_list = [
        "./datas/reports_data/csv/summary_report.csv",
        "./datas/reports_data/json/summary_report.json"
    ]

    file_path = "./datas/reports_data/csv/summary_report.csv"

    # 單一目標
    # upload(file_path, ["gdrive"], config)
    # upload(file_path, ["supabase"], config)
    # upload(file_path, ["r2"], config)

    # 多檔案上傳測試
    for file_path in file_list:
        # upload(file_path, ["gdrive"], config)
        # upload(file_path, ["supabase"], config)
        # upload(file_path, ["r2"], config)
        # upload(file_path, ["gdrive", "supabase"], config) # 多目標
        upload(file_path, ["all"], config) # 全部

