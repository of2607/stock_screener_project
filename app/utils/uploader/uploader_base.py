from abc import ABC, abstractmethod
from typing import Optional

class Uploader(ABC):
    def _resolve_file_name(self, file_path: str, dest_path: Optional[str] = None) -> str:
        """取得實際上傳檔名"""
        import os
        return os.path.basename(file_path) if not dest_path else dest_path

    @abstractmethod
    def upload(self, file_path: str, dest_path: Optional[str] = None) -> None:
        pass
