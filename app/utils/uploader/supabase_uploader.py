
import os
from typing import Optional
from .uploader_base import Uploader
from supabase import create_client, Client

class SupabaseUploader(Uploader):
    def __init__(self, url: str, key: str, bucket: str):
        self.url = url
        self.key = key
        self.bucket = bucket
        self.client: Client = create_client(url, key)

    def upload(self, file_path: str, dest_path: Optional[str] = None) -> None:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"[SupabaseUploader] 檔案不存在: {file_path}")
        file_name = self._resolve_file_name(file_path, dest_path)
        with open(file_path, "rb") as f:
            res = self.client.storage.from_(self.bucket).upload(file_name, f, {"upsert": "true"})
        if hasattr(res, "error") and res.error:
            raise Exception(f"[SupabaseUploader] 上傳失敗: {res.error}")
        print(f"[SupabaseUploader] 上傳成功: {file_path} 至 {self.bucket}/{file_name}")
