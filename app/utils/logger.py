"""
TWSE 資料下載工具 - 日誌工具
"""
import json
import os
from datetime import datetime
from typing import Optional, Dict, Any, List


class Logger:
    """統一的日誌記錄器"""
    
    def __init__(self, log_path: str):
        """
        初始化日誌記錄器
        
        Args:
            log_path: 日誌檔案路徑
        """
        self.log_path = log_path
        self.ensure_log_directory()
    
    def ensure_log_directory(self) -> None:
        """確保日誌目錄存在"""
        dir_path = os.path.dirname(self.log_path)
        if dir_path and dir_path.strip():
            os.makedirs(dir_path, exist_ok=True)
    
    def info(self, message: str) -> None:
        """記錄資訊訊息"""
        print(f"ℹ️ {message}")
    
    def success(self, message: str) -> None:
        """記錄成功訊息"""
        print(f"✅ {message}")
    
    def warning(self, message: str) -> None:
        """記錄警告訊息"""
        print(f"⚠️ {message}")
    
    def error(self, message: str) -> None:
        """記錄錯誤訊息"""
        print(f"❌ {message}")
    
    def progress(self, message: str) -> None:
        """記錄進度訊息"""
        print(f"🔄 {message}")
    
    def debug(self, message: str) -> None:
        """記錄除錯訊息"""
        print(f"🔧 {message}")
    
    def write_processing_log(
        self,
        year: str,
        report_name: str,
        csv_path: Optional[str] = None,
        json_path: Optional[str] = None,
        row_count: int = 0
    ) -> None:
        """
        寫入處理日誌
        
        Args:
            year: 年度
            report_name: 報表名稱
            csv_path: CSV 檔案路徑
            json_path: JSON 檔案路徑
            row_count: 資料筆數
        """
        log_data = self._load_existing_log()
        
        entry = {
            "year": year,
            "report": report_name,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "files": {
                "csv": csv_path if csv_path else None,
                "json": json_path if json_path else None
            },
            "total_rows": int(row_count)
        }
        
        log_data.append(entry)
        
        with open(self.log_path, "w", encoding="utf-8") as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2)
        
        self.info(f"📝 Log updated for {year} {report_name} - Total rows: {row_count}")
    
    def _load_existing_log(self) -> List[Dict[str, Any]]:
        """載入現有的日誌資料"""
        if os.path.exists(self.log_path):
            try:
                with open(self.log_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                self.warning(f"無法載入現有日誌檔案: {e}")
                return []
        return []