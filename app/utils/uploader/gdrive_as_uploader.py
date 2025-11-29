import os
import requests
from typing import Optional
from .uploader_base import Uploader

class GDriveASUploader(Uploader):
    def __init__(self, as_url: str, folder_id: str):
        self.as_url = as_url
        self.folder_id = folder_id
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
                "filedata": encoded
            }
            resp = requests.post(self.as_url, data=data)
        if resp.status_code == 200:
            print(f"[GDriveASUploader] 上傳成功: {file_path} -> {self.as_url}\n回應: {resp.text}")
        else:
            print(f"[GDriveASUploader] 上傳失敗: {file_path} -> {self.as_url}\n狀態: {resp.status_code}\n回應: {resp.text}")