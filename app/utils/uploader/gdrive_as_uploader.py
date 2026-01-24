import os
import requests
import json
from typing import Optional
from .uploader_base import Uploader

class GDriveASUploader(Uploader):
    def __init__(self, as_url: str, folder_id: str, auto_convert_to_sheets: bool = True, keep_csv_backup: bool = True):
        self.as_url = as_url
        self.folder_id = folder_id
        self.auto_convert_to_sheets = auto_convert_to_sheets
        self.keep_csv_backup = keep_csv_backup
        if not self.as_url or not self.folder_id:
            raise ValueError("gdrive_as_url 與 gdrive_folder_id 必須設定")

    def upload(self, file_path: str, dest_path: Optional[str] = None) -> None:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"[GDriveASUploader] 檔案不存在: {file_path}")
        file_name = self._resolve_file_name(file_path, dest_path)
        import base64
        with open(file_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("utf-8")
            data = {
                "folder_id": self.folder_id,
                "filename": file_name,
                "filedata": encoded,
                "convert_to_sheet": str(self.auto_convert_to_sheets).lower(),
                "keep_csv_backup": str(self.keep_csv_backup).lower()
            }
            resp = requests.post(self.as_url, data=data)
        
        if resp.status_code == 200:
            try:
                # 嘗試解析 JSON 回應
                result = json.loads(resp.text)
                if result.get('success'):
                    print(f"[GDriveASUploader] 上傳成功: {file_path}")
                    if result.get('sheetUrl'):
                        print(f"  ✓ Google Sheets: {result['sheetUrl']}")
                        print(f"    - 資料列數: {result.get('rowCount', 'N/A')}")
                        print(f"    - Sheet ID: {result.get('sheetId', 'N/A')}")
                    if result.get('csvUrl'):
                        print(f"  ✓ CSV 備份: {result['csvUrl']}")
                    if result.get('warning'):
                        print(f"  ⚠ 警告: {result['warning']}")
                    print(f"  ⏱ 執行時間: {result.get('executionTime', 'N/A')} 秒")
                else:
                    print(f"[GDriveASUploader] 上傳失敗: {result.get('error', '未知錯誤')}")
            except json.JSONDecodeError:
                # 回退到舊格式（相容性）
                print(f"[GDriveASUploader] 上傳成功: {file_path} -> {self.as_url}\n回應: {resp.text}")
        else:
            print(f"[GDriveASUploader] 上傳失敗: {file_path} -> {self.as_url}\n狀態: {resp.status_code}\n回應: {resp.text}")