
# summary_report

此 Edge Function 直接用 supabase-js 讀取 Supabase Storage 內的 summary_report.json，並以 `application/json; charset=utf-8` 回應，確保前端/瀏覽器/各語言皆可正確讀取中文。

---

## 說明
- 直接從 Storage bucket `public-data/reports/summary_report.json` 讀取內容，不再代理 public url。
- 回應自動帶 CORS header，支援跨域。
- 內容為 UTF-8 編碼 JSON，適合前端 fetch、Python、Node.js 等直接讀取。

## Function 設定
- Function Name: `summary_report`
- Endpoint URL: `https://btzjjozytwtbgdznralj.supabase.co/functions/v1/summary_report`

## Storage JSON 來源
- 路徑：`public-data/reports/summary_report.json`
- 產生方式：由 Python/Pandas 產生，已確保 UTF-8 編碼

## 用法

### 前端 fetch
```js
fetch('https://btzjjozytwtbgdznralj.supabase.co/functions/v1/summary_report')
  .then(res => res.json())
  .then(data => console.log(data));
```

### Python 讀取
```python
import requests
url = 'https://btzjjozytwtbgdznralj.supabase.co/functions/v1/summary_report'
data = requests.get(url).json()
```

---

## 其他
- 若需自訂來源路徑，可修改 function 內的 STORAGE_BUCKET 與 STORAGE_FILE_PATH。
- 若需自訂 header，可於 function 內調整 `headers` 物件。
