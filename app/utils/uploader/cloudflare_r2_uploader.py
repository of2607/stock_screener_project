
import os
from typing import Optional
from .uploader_base import Uploader
import boto3
from botocore.client import Config

class CloudflareR2Uploader(Uploader):
    def __init__(self, account_id: str, access_key: str, secret_key: str, bucket: str, region: str = "auto"):
        self.account_id = account_id
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.region = region
        endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url,
            region_name=region,
            config=Config(signature_version="s3v4")
        )

    def upload(self, file_path: str, dest_path: Optional[str] = None) -> None:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"[CloudflareR2Uploader] 檔案不存在: {file_path}")
        file_name = self._resolve_file_name(file_path, dest_path)
        # 根據副檔名自動判斷 Content-Type
        ext = os.path.splitext(file_name)[1].lower()
        if ext == ".csv":
            content_type = "text/csv; charset=utf-8"
        elif ext == ".json":
            content_type = "application/json; charset=utf-8"
        elif ext == ".txt":
            content_type = "text/plain; charset=utf-8"
        else:
            content_type = "application/octet-stream"
        with open(file_path, "rb") as f:
            self.s3.upload_fileobj(
                f, self.bucket, file_name,
                ExtraArgs={"ContentType": content_type}
            )
        print(f"[CloudflareR2Uploader] 上傳成功: {file_path} 至 {self.bucket}/{file_name} (Content-Type: {content_type})")
