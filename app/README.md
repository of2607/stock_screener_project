# 🚀 TWSE 財報資料下載工具

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Code Style](https://img.shields.io/badge/Code%20Style-Black-black)](https://github.com/psf/black)

這是台灣證券交易所（TWSE）財務報表資料下載與處理工具的優化版本，採用模組化設計，大幅提升程式碼的可維護性和可讀性。

# TODO
[x]  抓最新股價 (改寫supabase_function抓股價) ✅ 已完成
[ ]  Github Action



## 📋 目錄

- [功能特色](#-功能特色)
- [系統需求](#-系統需求)
- [安裝說明](#-安裝說明)
- [快速開始](#-快速開始)
  - [📈 股價抓取功能](#-股價抓取功能新增)
- [專案結構](#-專案結構)
- [設定說明](#-設定說明)
- [使用範例](#-使用範例)
- [支援的報表類型](#-支援的報表類型)
  - [📈 股價資料詳細說明](#-股價資料詳細說明)
- [輸出格式](#-輸出格式)
- [優化成果](#-優化成果)
- [常見問題](#-常見問題)
- [開發說明](#-開發說明)
- [授權條款](#-授權條款)

## ✨ 功能特色

### 🔄 核心功能
- **自動下載**: 從 TWSE 官網自動下載最新財務報表資料
- **多報表支援**: 支援資產負債表、損益表、現金流量表、股利分派、ETF配息
- **📈 股價抓取**: 抓取最新上市上櫃股價資料 (新功能)
- **智能編碼處理**: 自動檢測並處理 Big5/UTF-8 編碼問題
- **資料標準化**: 統一欄位格式和資料型別
- **欄位過濾**: 僅保留重要的財務指標，去除冗餘資訊

### 📊 輸出功能
- **多格式輸出**: 同時支援 CSV 和 JSON 格式
- **進度顯示**: 即時顯示下載和處理進度
- **日誌記錄**: 完整的處理記錄和錯誤追蹤
- **批次處理**: 支援多年度資料批次下載

### 🏗️ 架構優勢
- **模組化設計**: 清晰的模組分工，易於維護和擴展
- **型別安全**: 完整的型別提示系統
- **錯誤處理**: 統一的錯誤處理機制
- **設定管理**: 外部化設定，靈活調整參數

## 💻 系統需求

- **Python**: 3.8 或以上版本
- **作業系統**: Windows, macOS, Linux
- **記憶體**: 建議 2GB 以上
- **硬碟空間**: 建議 1GB 以上（用於存放資料）

## 🔧 安裝說明

### 1. 克隆專案
```bash
git clone <repository-url>
cd twse-financial-data-tool/app
```

### 2. 建立虛擬環境（建議）
```bash
# 建立虛擬環境
python -m venv venv

# 啟動虛擬環境
# Windows
venv\Scripts\activate
# macOS/Linux
source venv/bin/activate
```

### 3. 安裝依賴套件
```bash
pip install -r requirements.txt
```

### 4. 驗證安裝
```bash
python main.py --help
```

## 🚀 快速開始

### 基本使用
```bash
# 執行預設設定（下載 113-114 年度所有報表）
python main.py

# 🆕 抓取最新股價資料
python fetch_stock_prices.py
```

### 📈 股價抓取功能（新增）
```bash
# 抓取最新股價資料（上市 + 上櫃）
python fetch_stock_prices.py

# 輸出檔案
# - datas/merged_data/csv/latest_stock_prices.csv
# - datas/merged_data/json/latest_stock_prices.json
```

### 自訂設定
編輯 [`config/settings.py`](config/settings.py) 來調整：
```python
# 設定下載年度範圍
START_YEAR = 113
END_YEAR = 114

# 選擇下載的報表類型
DOWNLOAD_REPORTS = ["balance_sheet", "income_statement"]

# 設定輸出格式
SAVE_FORMAT = ["csv", "json"]

# 🆕 股價相關設定
STOCK_MIN_PRICE = 10.0        # 股價過濾門檻（元）
RAW_DATA_RETENTION_DAYS = 7   # 原始資料保留天數
```

### 僅合併模式
如果已有原始資料，只想進行合併處理：
```python
ONLY_MERGE = True  # 在 settings.py 中設定
```

## 📁 專案結構

```
app/
├── 📖 README.md                    # 專案說明文件
├── 📋 requirements.txt             # 依賴套件清單
├── 🚫 .gitignore                   # Git 忽略規則
├── 🚀 main.py                      # 主程式入口（優化版）
├── 📈 fetch_stock_prices.py        # 🆕 股價抓取程式
├── 📜 get_twse_data.py            # 原始版本（參考對照）
│
├── ⚙️  config/                     # 設定模組
│   ├── __init__.py
│   ├── settings.py                # 主要設定檔案
│   ├── api_urls.py                # API 端點管理（含股價 API）
│   └── column_configs.py          # 欄位設定
│
├── 📥 downloaders/                 # 下載器模組
│   ├── __init__.py
│   ├── base_downloader.py         # 下載器基底類別
│   ├── twse_downloader.py         # TWSE 專用下載器
│   ├── etf_downloader.py          # ETF 專用下載器
│   └── stock_price_downloader.py  # 🆕 股價下載器
│
├── 🔄 processors/                  # 資料處理模組
│   ├── __init__.py
│   ├── report_processor.py        # 報表處理協調器
│   ├── data_standardizer.py       # 資料標準化核心
│   ├── csv_cleaner.py             # CSV 清理器
│   ├── column_filter.py           # 欄位過濾器
│   ├── data_sorter.py             # 資料排序器
│   └── stock_price_processor.py   # 🆕 股價資料處理器
│
├── 🛠️  utils/                      # 工具模組
│   ├── __init__.py
│   ├── logger.py                  # 日誌系統
│   └── exceptions.py              # 例外處理
│
└── 📊 datas/                       # 資料目錄
    ├── merged_data/               # 📋 處理後的結果
    │   ├── csv/                   # CSV 格式檔案
    │   │   ├── latest_stock_prices.csv     # 🆕 最新股價 CSV
    │   │   └── 年度-報表類型.csv
    │   ├── json/                  # JSON 格式檔案
    │   │   ├── latest_stock_prices.json    # 🆕 最新股價 JSON
    │   │   └── 年度-報表類型.json
    │   ├── log.json               # 📝 財報處理記錄
    │   └── stock_price_log.json   # 🆕 股價處理記錄
    └── raw_data/                  # 📁 原始下載資料
        ├── balance_sheet/         # 資產負債表
        ├── income_statement/      # 損益表
        ├── cash_flow/            # 現金流量表
        ├── dividend/             # 股利分派
        ├── etf_dividend/         # ETF 配息
        └── stock_prices/         # 🆕 股價原始資料
            └── YYYYMMDD_HHMMSS_市場.json
```

## ⚙️ 設定說明

### 主要設定檔案：[`config/settings.py`](config/settings.py)

```python
# 年度設定
START_YEAR = 107        # 開始年度（民國年）
END_YEAR = 114          # 結束年度（民國年）

# 模式設定
ONLY_MERGE = False      # True: 僅合併模式, False: 下載+合併

# 報表類型設定
DOWNLOAD_REPORTS = [
    "balance_sheet",    # 資產負債表
    "income_statement", # 損益表
    "cash_flow",       # 現金流量表
    "dividend",        # 股利分派
    "etf_dividend"     # ETF 配息
]

# 輸出格式設定
SAVE_FORMAT = ["csv", "json"]  # 可選: "csv", "json" 或兩者

# 路徑設定
BASE_DIR = "datas/raw_data"
CSV_OUTPUT_DIR = "datas/merged_data/csv"
JSON_OUTPUT_DIR = "datas/merged_data/json"
```

### 🆕 股價相關設定

```python
# 股價過濾設定
STOCK_MIN_PRICE: float = 10.0    # 最低股價門檻（元）
                                # 低於此價格的股票將被過濾掉

# 原始資料管理
RAW_DATA_RETENTION_DAYS: int = 7 # 原始資料保留天數
                                # 超過此天數的原始檔案將自動刪除
```

### API 端點設定：[`config/api_urls.py`](config/api_urls.py)

```python
# 🆕 股價 API 端點
STOCK_PRICE_URLS = {
    "twse": "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL",
    "tpex": "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php"
}
```

### 欄位設定：[`config/column_configs.py`](config/column_configs.py)

定義每種報表要保留的欄位：
```python
KEEP_COLUMNS = {
    'balance_sheet': [
        '代號', '名稱', '年度', '季別',
        '流動資產', '資產總額', '流動負債',
        '歸屬於母公司業主之權益合計'
    ],
    'income_statement': [
        '代號', '名稱', '年度', '季別',
        '營業收入', '營業成本',
        '淨利（損）歸屬於母公司業主'
    ]
    # ... 其他報表設定
}
```

## 📈 使用範例

### 範例 1：下載特定年度資料
```python
# 修改 config/settings.py
START_YEAR = 113
END_YEAR = 113
DOWNLOAD_REPORTS = ["balance_sheet", "income_statement"]

# 執行
python main.py
```

### 範例 2：僅處理現有資料
```python
# 修改 config/settings.py
ONLY_MERGE = True

# 執行
python main.py
```

### 範例 3：自訂輸出格式
```python
# 僅輸出 CSV 格式
SAVE_FORMAT = ["csv"]

# 執行
python main.py
```

### 範例 4：📈 股價資料抓取
```bash
# 基本股價抓取
python fetch_stock_prices.py

# 檢視輸出結果
cat datas/merged_data/csv/latest_stock_prices.csv
```

### 範例 5：🔧 股價設定調整
```python
# 修改 config/settings.py
STOCK_MIN_PRICE = 50.0        # 只保留 50 元以上股票
RAW_DATA_RETENTION_DAYS = 3   # 原始資料保留 3 天

# 執行股價抓取
python fetch_stock_prices.py
```

## 📊 支援的資料類型

| 資料類型 | 說明 | 重要欄位 |
|---------|------|---------|
| **balance_sheet** | 資產負債表 | 流動資產、資產總額、流動負債、股東權益 |
| **income_statement** | 損益表 | 營業收入、營業成本、稅後淨利、每股盈餘 |
| **cash_flow** | 現金流量表 | 營業活動現金流量 |
| **dividend** | 股利分派 | 現金股利、股票股利、配息率 |
| **etf_dividend** | ETF配息 | 配息金額、除息日、配息率 |
| **🆕 stock_prices** | 股價資料 | 股票代號、名稱、收盤價、市場別、日期 |

### 📈 股價資料詳細說明

**資料來源**：
- **上市股票**：台灣證券交易所 (TWSE) API
- **上櫃股票**：櫃買中心 (TPEX) API

**欄位說明**：
```json
{
  "stock_code": "2330",           // 股票代號
  "stock_name": "台積電",          // 股票名稱
  "price": 625.00,               // 收盤價（元）
  "market": "上市",               // 市場別（上市/上櫃）
  "date": "2024-11-13",          // 資料日期
  "last_updated": "2024-11-13T16:30:00+08:00"  // 最後更新時間
}
```

**特色功能**：
- ✅ **即時資料**：抓取當日最新收盤價
- ✅ **全市場覆蓋**：包含上市 + 上櫃所有股票
- ✅ **自動過濾**：可設定最低價格門檻（預設 10 元）
- ✅ **雙格式輸出**：同時產生 CSV 和 JSON 檔案
- ✅ **原始資料備份**：自動保存原始 API 回應（可設定保留天數）
- ✅ **錯誤處理**：網路異常時的重試機制

**檔案輸出**：
- **CSV**: `datas/merged_data/csv/latest_stock_prices.csv`
- **JSON**: `datas/merged_data/json/latest_stock_prices.json`
- **原始資料**: `datas/raw_data/stock_prices/YYYYMMDD_HHMMSS_twse.json`

## 💾 輸出格式

### CSV 格式
- 編碼：UTF-8 with BOM
- 分隔符：逗號
- 檔案路徑：`datas/merged_data/csv/年度-報表類型.csv`
- 🆕 股價檔案：`datas/merged_data/csv/latest_stock_prices.csv`

### JSON 格式
- 編碼：UTF-8
- 格式：Records（陣列包含物件）
- 檔案路徑：`datas/merged_data/json/年度-報表類型.json`
- 🆕 股價檔案：`datas/merged_data/json/latest_stock_prices.json`

### 處理日誌
- **財報日誌**：[`datas/merged_data/log.json`](datas/merged_data/log.json)
  - 內容：財報處理時間、檔案路徑、資料筆數
- **🆕 股價日誌**：[`datas/merged_data/stock_price_log.json`](datas/merged_data/stock_price_log.json)
  - 內容：股價處理時間、API 呼叫狀態、抓取筆數、過濾結果

## 🎯 優化成果

### 程式碼簡化
- **原始版本**: `get_twse_data.py` - 1,014 行
- **優化版本**: `main.py` - 153 行
- **🆕 股價功能**: `fetch_stock_prices.py` - 16 行（主程式）
- **簡化程度**: 85% 程式碼簡化 + 全新股價功能

### 架構改進
- ✅ **模組化設計** - 20 個專責模組（含股價模組）
- ✅ **單一責任原則** - 每個模組職責明確
- ✅ **型別安全** - 完整的型別提示
- ✅ **錯誤處理** - 統一的例外處理機制
- ✅ **設定管理** - 外部化設定檔案
- 🆕 **股價整合** - 無縫整合股價抓取功能

### 效能提升
- 🚀 智能編碼處理
- 🚀 批次下載優化
- 🚀 記憶體使用優化
- 🚀 進度顯示改進
- 🆕 **即時股價** - 快速獲取全市場股價資料

### 🆕 新增功能亮點
- 📈 **全市場覆蓋**: 一次抓取上市 + 上櫃所有股票
- ⚡ **高效能**: 平行處理 TWSE 和 TPEX API
- 🔧 **可設定**: 彈性的過濾條件和保留策略
- 💾 **雙重備份**: JSON/CSV 雙格式 + 原始資料保存
- 🛡️ **穩定可靠**: 完整的錯誤處理和重試機制

## ❓ 常見問題

### Q1: 遇到 "lxml parser not found" 錯誤？
**A**: 安裝 lxml 套件：
```bash
pip install lxml
```
或者修改程式碼使用內建解析器（將 "lxml" 改為 "html.parser"）

### Q2: 下載速度很慢？
**A**: 可能的原因和解決方案：
- 網路連線問題：檢查網路狀況
- TWSE 伺服器負載高：嘗試在離峰時間下載
- 防火牆限制：檢查防火牆設定

### Q3: 資料不完整？
**A**: 檢查以下項目：
- 確認年度範圍設定正確
- 檢查 TWSE 網站是否有資料
- 查看日誌檔案了解詳細錯誤

### Q4: 如何新增自訂欄位？
**A**: 修改 [`config/column_configs.py`](config/column_configs.py)：
```python
KEEP_COLUMNS['balance_sheet'].append('新欄位名稱')
```

### Q5: 如何修改輸出路徑？
**A**: 修改 [`config/settings.py`](config/settings.py) 中的路徑設定

### Q6: 📈 股價資料相關問題

**Q6.1: 股價資料不完整？**
**A**: 檢查以下項目：
- 確認網路連線正常
- 檢查 TWSE/TPEX API 服務狀態
- 查看是否為交易日（週末和國定假日無資料）
- 檢查股價過濾門檻設定

**Q6.2: 如何調整股價過濾條件？**
**A**: 修改 [`config/settings.py`](config/settings.py)：
```python
STOCK_MIN_PRICE = 50.0  # 調整最低價格門檻
```

**Q6.3: 原始資料佔用空間太大？**
**A**: 調整保留天數設定：
```python
RAW_DATA_RETENTION_DAYS = 3  # 減少保留天數
```

**Q6.4: 股價抓取失敗？**
**A**: 常見原因和解決方案：
- **網路逾時**：稍後重試或檢查網路連線
- **API 限制**：避免頻繁呼叫，建議間隔 30 秒以上
- **資料格式變更**：檢查 TWSE/TPEX 是否更新 API 格式

## 🔨 開發說明

### 開發環境設定
```bash
# 安裝開發依賴
pip install -r requirements.txt

# 程式碼格式化
black .

# 型別檢查
mypy main.py

# 單元測試
pytest tests/
```

### 貢獻指南
1. Fork 專案
2. 建立功能分支
3. 提交變更
4. 建立 Pull Request

### 程式碼風格
- 遵循 PEP 8 規範
- 使用 Black 進行格式化
- 添加完整的型別提示
- 撰寫清楚的文檔字符串

## 📄 授權條款

本專案採用 MIT 授權條款 - 詳見 [LICENSE](LICENSE) 檔案

## 🙏 致謝

感謝台灣證券交易所提供開放的資料 API，讓我們能夠建立這個實用的工具。

---

📧 如有問題或建議，歡迎建立 Issue 或 Pull Request