- 用supabase function 抓最新收盤價存到supabase
  上市
  curl -X 'GET' \
    'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_AVG_ALL' \
    -H 'accept: application/json' \
    -H 'If-Modified-Since: Mon, 26 Jul 1997 05:00:00 GMT' \
    -H 'Cache-Control: no-cache' \
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
  近10年每年現金股息
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

- 使用FinMind免費會員的資格抓取得資料，必須考量到可使用的額度 (控制速率避免超過 600 次/小時（可以 batch + 延遲）)
- 有抓過的股票記錄下來盡量必免重複抓資料，但要保證資料的完整性
- supabase(免費方案)
- cloudflare works(免費方案)


<!-- ### 備忘 First init
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt -->

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



































