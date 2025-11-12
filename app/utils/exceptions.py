"""
TWSE 資料下載工具 - 自定義異常
"""


class TWSEDataError(Exception):
    """TWSE 資料處理基礎異常"""
    pass


class ConfigurationError(TWSEDataError):
    """設定錯誤"""
    pass


class DownloadError(TWSEDataError):
    """下載錯誤"""
    pass


class DataProcessingError(TWSEDataError):
    """資料處理錯誤"""
    pass


class FileProcessingError(TWSEDataError):
    """檔案處理錯誤"""
    pass


class ValidationError(TWSEDataError):
    """資料驗證錯誤"""
    pass


class NetworkError(DownloadError):
    """網路錯誤"""
    pass


class APIError(DownloadError):
    """API 錯誤"""
    pass


class CSVFormatError(FileProcessingError):
    """CSV 格式錯誤"""
    pass


class ColumnMappingError(DataProcessingError):
    """欄位對應錯誤"""
    pass