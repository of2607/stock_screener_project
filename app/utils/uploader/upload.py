
from typing import List, Optional, Dict
from .uploader_base import Uploader
from .gdrive_as_uploader import GDriveASUploader
from .supabase_uploader import SupabaseUploader
from .cloudflare_r2_uploader import CloudflareR2Uploader
from functools import partial

def get_uploaders(targets: List[str], config: Dict) -> List[Uploader]:
    """根據 targets 取得對應 uploader 實例清單"""
    uploader_map = {
        # Apps Script Web App 方式
        "gdrive": partial(GDriveASUploader,
            as_url=config.get("gdrive_as_url"),
            folder_id=config.get("gdrive_folder_id")
        ),
        "supabase": partial(SupabaseUploader,
            url=config.get("supabase_url"),
            key=config.get("supabase_key"),
            bucket=config.get("supabase_bucket")
        ),
        "r2": partial(CloudflareR2Uploader,
            account_id=config.get("r2_account_id"),
            access_key=config.get("r2_access_key"),
            secret_key=config.get("r2_secret_key"),
            bucket=config.get("r2_bucket"),
            region=config.get("r2_region", "auto")
        ),
    }
    if "all" in targets:
        return [ctor() for ctor in uploader_map.values()]
    return [uploader_map[t]() for t in targets if t in uploader_map]

def upload(file_path: str, targets: List[str], config: Dict, dest_path: Optional[str] = None) -> None:
    """統一上傳介面，可同時上傳到多個目標"""
    for uploader in get_uploaders(targets, config):
        uploader.upload(file_path, dest_path)
