"""
簡單測試 upload.py 多目標上傳介面
"""
from utils.uploader.upload import upload

if __name__ == "__main__":
    # 假設 config 內容如下，實際應從設定檔或環境變數取得
    config = {
        "gdrive_as_url": "https://script.google.com/macros/s/AKfycbwFR-OoFF4aO42MVVA2VYURTDLIWEXN8J4A2LQIe3duqm_yG2mpOYhDIhmfwBADBwuxyA/exec",
        "gdrive_folder_id": "12ar4cYTO8zq8xgLY76cljn5b_bA2q6ux",
        "supabase_url": "https://btzjjozytwtbgdznralj.supabase.co",
        "supabase_key": "sb_secret_fjzIJ8cx3xnsE8xQ18-AMw_ctrb8gkD",
        "supabase_bucket": "public-data/reports",
        "r2_account_id": "67d73f06307398dd3a9f766976a5efec",
        "r2_access_key": "7a33a736d0b434c6d92348f8d3778329",
        "r2_secret_key": "0e6a600b9e4c54ffdd313f4575600001ccef79cd19c02aa39ebb97f1f1d9efaf",
        "r2_bucket": "stock-reports",
        "r2_region": "auto"
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
        upload(file_path, ["supabase"], config)
        # upload(file_path, ["r2"], config)
        # upload(file_path, ["gdrive", "supabase"], config) # 多目標
        # upload(file_path, ["all"], config) # 全部

