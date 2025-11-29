## 安裝與環境設定

### 1. Python 環境安裝
建議使用 Python 3.10 以上版本。

```bash
python -m venv venv
source venv/bin/activate
pip install -r app/requirements.txt
```

### 2. 主要依賴套件
- requests
- pandas
- numpy
- supabase-py
 
- FinMind API Token：用於抓取台股資料
SUPABASE_URL=your_url
CLOUDFLARE_R2_ID=your_id
CLOUDFLARE_R2_SECRET=your_secret
GOOGLE_DRIVE_CREDENTIALS=your_cred.json
```

---


1. 完成上述安裝與設定
2. 進入 app 目錄，執行主程式：
  ```bash
- Google Apps Script：於 google_apps_script/ 目錄撰寫與部署，詳見 GAS 官方文件
- 安裝依賴：`pip install -r app/requirements.txt`
- 清理暫存/日誌：手動刪除 app/datas/logs/

### 維護建議
- 定期檢查 API 金鑰有效性與流量限制
1. **API 流量超限？**
  - 請降低抓取頻率，或升級 API 會員方案
  - 檢查金鑰、網路連線與雲端服務狀態

---

## 進階：流程圖與子文件

### 資料流/邏輯流程圖
（見前文 mermaid 流程圖）

- 詳細參數、API 文件、進階部署請見 docs/ 目錄（可自行擴充）

---
## 主要功能與模組

### 主要功能
- 自動化抓取台股上市櫃、ETF 最新收盤價與歷史財報
- 計算多年度平均配息率、殖利率、股息成長率
- 避免重複抓取，確保資料完整性
- 自動上傳彙整結果至 Supabase、Cloudflare R2、Google Drive
- 提供雲端 API 與 Google Apps Script 自動化
- `app/datas/`：本地資料儲存（原始、清洗、彙整、日誌）
- `supabase_functions/`：雲端自動化 function，API 入口
  A[公開 API/資料來源] --> B[下載模組 (downloaders)]
  C --> D[彙整/產出報表]
  D --> E[雲端上傳 (utils/uploader)]
  E --> F[雲端儲存 (Supabase, Cloudflare R2, Google Drive)]
  D --> G[本地儲存 (app/datas)]
  subgraph 其他功能
    H[Supabase Functions]
    I[Google Apps Script]
  end
  F --> H
  F --> I
```

#### 主要資料夾說明
- `app/`：主程式、設定、資料、下載、處理、上傳等所有核心模組
- `supabase_functions/`：雲端 Supabase Functions，負責自動化觸發與 API 提供
- `google_apps_script/`：Google Apps Script 腳本，支援 Google Drive 自動化

#### 主要資料流邏輯
1. 下載模組（app/downloaders/）負責從公開 API 下載財報、股價、股利等原始資料。
2. 處理模組（app/processors/）將原始資料進行清洗、標準化，並計算各項財務指標。
3. 彙整後的資料會同時儲存於本地（app/datas/）以便備查與後續分析。
4. 上傳模組（app/utils/uploader/）將彙整資料自動上傳至雲端（Supabase、Cloudflare R2、Google Drive）。
5. 雲端自動化（supabase_functions/、google_apps_script/）可根據需求自動觸發後續流程，或提供 API 查詢服務。
6. 所有流程皆會產生日誌，記錄於 app/datas/logs/，方便追蹤與錯誤排查。

## 台股股利成長股自動化篩選系統

本專案為一套自動化台股股利成長股篩選與資料彙整系統，整合多種公開 API 與雲端服務，協助用戶快速取得、整理、分析台股上市櫃公司與 ETF 的關鍵財務與股利資料，並自動化上傳至雲端儲存，方便後續查詢與應用。

### 主要用途
- 自動化抓取台股上市櫃與 ETF 最新收盤價、財報、股利、現金流等資料
- 彙整近 10 年 EPS、股息、殖利率、ROE 等多項指標
- 計算多年度平均配息率、殖利率、股息成長率等
- 支援資料自動上傳至 Supabase、Cloudflare R2 等雲端服務
- 提供完整資料流、模組化設計，方便維護與擴充

    'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_AVG_ALL' \
    -H 'If-Modified-Since: Mon, 26 Jul 1997 05:00:00 GMT' \
    -H 'Pragma: no-cache'

  上櫃
  curl -X 'GET' \
    'https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes' \
    -H 'accept: application/json' \
    -H 'If-Modified-Since: Mon, 26 Jul 1997 05:00:00 GMT' \
    -H 'Cache-Control: no-cache' \
    -H 'Pragma: no-cache'


- 依最新收盤列出的股票及ETF(不包含股價<10)抓取各股的歷史資料，最終我需要的欄位必須包含以下：
  產業類別
  名稱
  代號

  近10年每年的EPS
  近10年每年股票股息

  近3年平均配息率
  近5年平均配息率
  近8年平均配息率

  近3年平均股息
  近5年平均股息
  近8年平均股息

  ROE%
  近3年平均ROE%
  近5年平均ROE%
  近8年平均ROE%

  大前年股息殖利率
  前年股息殖利率
  去年股息殖利率

  近3年平均殖利率
  近5年平均殖利率
  近8年平均殖利率

  去年1QEPS
  今年1QEPS
  去年與今年1QEPS差率

  去年2QEPS
  今年2QEPS
  去年與今年2QEPS差率

  去年3QEPS
  今年3QEPS
  去年與今年3QEPS差率

  去年3QEPS
  今年3QEPS
  去年與今年3QEPS差率

  最新收盤價


- 有抓過的股票記錄下來盡量必免重複抓資料，但要保證資料的完整性
- supabase(免費方案)
- cloudflare works(免費方案)


<!-- ### 備忘 First init

TYPEK=sii 上市 otc 上櫃

資產負債表
https://mopsov.twse.com.tw/mops/web/ajax_t163sb05?year=114&TYPEK=sii&season=01&firstin=1
https://mopsov.twse.com.tw/server-java/t105sb02?firstin=true&step=10&filename=t163sb04_20251028_023348240.csv


股利
https://mopsov.twse.com.tw/server-java/t05st09sub?YEAR=114&qryType=2&TYPEK=sii&step=1
https://mopsov.twse.com.tw/server-java/t105sb02?firstin=true&step=10&filename=t05st09_new_20251028_03360991.csv


損益表
https://mopsov.twse.com.tw/mops/web/ajax_t163sb04?year=114&TYPEK=sii&season=01&firstin=1
https://mopsov.twse.com.tw/server-java/t105sb02?firstin=true&step=10&filename=t163sb04_20251028_034246715.csv

現金流量
https://mopsov.twse.com.tw/mops/web/ajax_t163sb20?year=114&TYPEK=sii&season=01&firstin=1
https://mopsov.twse.com.tw/server-java/t105sb02?firstin=true&step=10&filename=t163sb20_20251028_034534258.csv

ETF
https://www.twse.com.tw/rwd/zh/ETF/etfDiv?stkNo=&startDate={start_date}&endDate={end_date}&response=json
https://www.twse.com.tw/rwd/zh/ETF/etfDiv?stkNo=&startDate={start_date}&endDate={end_date}&response=csv


個股資料
https://fubon-ebrokerdj.fbs.com.tw/Z/ZC/ZCS/ZCS_1101.djhtm