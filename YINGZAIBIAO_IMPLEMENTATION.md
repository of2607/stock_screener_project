# 盈再表自動下載功能說明

## 📋 功能概述

已成功將 `盈再表.py` 的 Selenium 自動登入下載功能整合至現有框架，實現每日自動下載 `twlist.xlsx` → 處理 → 儲存為 CSV/JSON 的完整流程。

## 🏗️ 架構設計

### 新增檔案

1. **下載器層**
   - `app/downloaders/selenium_base_downloader.py` - Selenium 下載器基礎類別
   - `app/downloaders/yingzaibiao_downloader.py` - 盈再表專用下載器

2. **處理器層**
   - `app/processors/yingzaibiao_processor.py` - 盈再表資料處理器
   - `app/processors/fetch_yingzaibiao.py` - 主控制器

3. **配置檔案**
   - 已更新 `app/config/settings.py` - 新增 `ENABLE_YINGZAIBIAO_DOWNLOAD`
   - 已更新 `app/config/column_configs.py` - 新增 `yingzaibiao` 欄位配置
   - 已更新 `app/requirements.txt` - 新增 Selenium 相關依賴
   - 已更新 `app/main.py` - 整合至工作流程
   - 已更新 `.github/workflows/auto-summary-report.yml` - 設定環境變數

## 🔧 技術特點

### 1. Selenium 下載器設計
- ✅ 自動偵測 CI 環境（GitHub Actions），使用 headless 模式
- ✅ 智能登入偵測（自動判斷是否需要登入）
- ✅ 下載完成監控機制
- ✅ 錯誤自動截圖至 `datas/logs/`
- ✅ 資源自動清理

### 2. 資料處理流程
```
下載 twlist.xlsx (覆蓋模式)
    ↓
讀取 Excel 檔案
    ↓
清理資料（移除空行、標準化欄位名稱）
    ↓
儲存為 CSV：datas/merged_data/csv/latest_yingzaibiao.csv
    ↓
儲存為 JSON：datas/merged_data/json/latest_yingzaibiao.json
```

### 3. 與現有框架整合
- ✅ 複用 `BaseDownloader` 架構
- ✅ 使用統一的 `Logger` 系統
- ✅ 整合至 `main.py` 的 `POST_REPORT_TASKS`
- ✅ 支援命令列參數控制（`--ENABLE_YINGZAIBIAO_DOWNLOAD`）
- ✅ 與每日股價下載採用相同的覆蓋儲存模式

## 🚀 使用方式

### 本地執行

1. **安裝新的依賴套件**
```bash
cd app
pip install -r requirements.txt
```

2. **設定環境變數**
```bash
export YINGZAIBIAO_USERNAME="你的帳號"
export YINGZAIBIAO_PASSWORD="你的密碼"
```

或使用 `.env` 檔案（建議）：
```bash
# 在 app/ 目錄建立 .env 檔案
echo "YINGZAIBIAO_USERNAME=你的帳號" >> .env
echo "YINGZAIBIAO_PASSWORD=你的密碼" >> .env
```

3. **執行完整流程**
```bash
python main.py --ENABLE_YINGZAIBIAO_DOWNLOAD=True
```

或單獨執行盈再表下載：
```bash
cd app/processors
python fetch_yingzaibiao.py
```

### GitHub Actions 自動執行

1. **設定 Secrets**（必須）
   - 前往 GitHub Repository → Settings → Secrets and variables → Actions
   - 新增兩個 secrets：
     - `YINGZAIBIAO_USERNAME`：你的帳號
     - `YINGZAIBIAO_PASSWORD`：你的密碼

2. **自動排程**
   - 每日 UTC 18:00（台灣時間 02:00）自動執行
   - 或手動觸發：Actions → Daily Summary Report → Run workflow

## 📂 檔案結構

```
app/
├── downloaders/
│   ├── selenium_base_downloader.py    # 新增：Selenium 基礎類別
│   └── yingzaibiao_downloader.py      # 新增：盈再表下載器
├── processors/
│   ├── yingzaibiao_processor.py       # 新增：盈再表處理器
│   └── fetch_yingzaibiao.py          # 新增：主控制器
├── config/
│   ├── settings.py                    # 已更新：新增配置
│   └── column_configs.py              # 已更新：新增欄位配置
├── datas/
│   ├── raw_data/
│   │   └── yingzaibiao/
│   │       ├── temp/                  # 下載暫存
│   │       └── twlist.xlsx            # 最終檔案
│   ├── merged_data/
│   │   ├── csv/
│   │   │   └── latest_yingzaibiao.csv # 輸出 CSV
│   │   └── json/
│   │       └── latest_yingzaibiao.json # 輸出 JSON
│   └── logs/
│       ├── yingzaibiao_log.json       # 執行日誌
│       └── *_error_*.png              # 錯誤截圖
├── main.py                             # 已更新：整合工作流程
└── requirements.txt                    # 已更新：新增依賴
```

## ⚙️ 配置說明

### settings.py 新增項目
```python
ENABLE_YINGZAIBIAO_DOWNLOAD: bool = True  # 是否啟用盈再表下載
```

### 環境變數
```bash
YINGZAIBIAO_USERNAME  # 登入帳號（必須）
YINGZAIBIAO_PASSWORD  # 登入密碼（必須）
YINGZAIBIAO_URL       # 目標網址（可選，預設值已設定）
```

## 🔍 除錯與監控

### 日誌位置
- 執行日誌：`datas/logs/yingzaibiao_log.json`
- 錯誤截圖：`datas/logs/*_error_*.png`

### 常見問題

1. **登入失敗**
   - 檢查環境變數是否正確設定
   - 查看錯誤截圖 `login_failed_*.png`

2. **下載超時**
   - 預設超時 60 秒
   - 查看錯誤截圖 `download_timeout_*.png`

3. **處理失敗**
   - 確認 `twlist.xlsx` 格式是否正確
   - 查看日誌中的錯誤訊息

## 📝 後續優化建議

1. **欄位配置優化**
   - 目前保留所有欄位
   - 待第一次成功下載後，手動檢視 `twlist.xlsx` 內容
   - 在 `column_configs.py` 中配置需要保留的欄位

2. **上傳雲端**（可選）
   - 參考 `summary_report_upload.py` 模式
   - 新增獨立的上傳任務至 `POST_REPORT_TASKS`

3. **資料驗證**
   - 新增下載資料的完整性檢查
   - 與歷史資料比對，偵測異常

## ✅ 完成檢查清單

- [x] 建立 SeleniumBaseDownloader 基礎類別
- [x] 建立 YingZaiBiaoDownloader 下載器
- [x] 建立 YingZaiBiaoProcessor 處理器
- [x] 建立 fetch_yingzaibiao 主控制器
- [x] 更新 requirements.txt 依賴
- [x] 更新 settings.py 配置
- [x] 更新 column_configs.py 欄位配置
- [x] 整合至 main.py 工作流程
- [x] 更新 GitHub Actions workflow

## 🎯 測試建議

1. **本地測試**（可選）
   ```bash
   # 先設定環境變數
   export YINGZAIBIAO_USERNAME="你的帳號"
   export YINGZAIBIAO_PASSWORD="你的密碼"
   
   # 單獨測試盈再表下載
   cd app/processors
   python fetch_yingzaibiao.py
   ```

2. **GitHub Actions 測試**（建議）
   - 設定好 Secrets 後
   - 手動觸發一次 workflow
   - 檢查執行結果和輸出檔案

---

**重構完成時間**：2026年1月24日  
**符合設計原則**：✅ 優雅、可維護、不重工、整合現有框架
