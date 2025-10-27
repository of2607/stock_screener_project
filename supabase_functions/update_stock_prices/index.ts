// supabase/functions/update_stock_prices/index.ts
import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";
import { corsHeaders } from "./cors.ts";
const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
const TABLE = "stock_prices";
const STORAGE_BUCKET = "public-data";
const STORAGE_FILE_PATH = "latest/stock_prices.json";
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing required environment variables");
}
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
// 通用 JSON 回應
const jsonResponse = (data, status = 200)=>new Response(JSON.stringify(data), {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json"
    }
  });
// GET：從 Storage 拿最新 JSON
async function handleGet() {
  const { data, error } = await supabase.storage.from(STORAGE_BUCKET).download(STORAGE_FILE_PATH);
  if (error) return jsonResponse({
    error: error.message
  }, 500);
  const text = await data.text();
  const json = JSON.parse(text);
  return jsonResponse({
    message: "從 Storage 取得最新股價 JSON",
    date: json?.[0]?.date,
    count: json?.length || 0,
    data: json
  });
}
// POST：抓股價 → 批次插入 → 上傳 JSON
async function handlePost() {
  try {
    // Step1: 抓取上市與上櫃資料
    const [twseRes, tpexRes] = await Promise.all([
      fetch("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_AVG_ALL"),
      fetch("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes")
    ]);
    const twseData = await twseRes.json();
    const tpexData = await tpexRes.json();
    // Step2: 清洗資料
    const parsePrice = (p)=>Number(p) || 0;
    const parseROCDate = (rocDate)=>rocDate?.replace(/^(\d{3})(\d{2})(\d{2})$/, (_, y, m, d)=>`${Number(y) + 1911}-${m}-${d}`);
    const normalize = (data, isTwse = true)=>data.map((d)=>({
          stock_code: isTwse ? d.Code : d.SecuritiesCompanyCode,
          stock_name: isTwse ? d.Name : d.CompanyName,
          price: parsePrice(isTwse ? d.ClosingPrice : d.Close),
          market: isTwse ? "TSE" : "OTC",
          date: parseROCDate(d.Date) || new Date().toISOString().split("T")[0]
        })).filter((d)=>d.price >= 10 && d.stock_code).sort((a, b)=>a.stock_code - b.stock_code);
    const allData = [
      ...normalize(twseData, true),
      ...normalize(tpexData, false)
    ];
    if (allData.length === 0) return jsonResponse({
      message: "No data fetched"
    });
    // Step3: 取得現有資料日期
    const { data: dbData } = await supabase.from(TABLE).select("date").order("stock_code", {
      ascending: true
    }).limit(1);
    const latestDbDate = dbData?.[0]?.date;
    const newDate = allData[0]?.date;
    if (latestDbDate === newDate) {
      return jsonResponse({
        message: "Already up-to-date",
        date: newDate
      });
    }
    // Step4: 清空舊資料
    const { error: delError } = await supabase.rpc(`truncate_${TABLE}`);
    if (delError) throw delError;
    // Step5: 批次插入
    const BATCH_SIZE = 500; // 可以依情況調整
    for(let i = 0; i < allData.length; i += BATCH_SIZE){
      const chunk = allData.slice(i, i + BATCH_SIZE);
      const { error: insertError } = await supabase.from(TABLE).insert(chunk);
      if (insertError) throw insertError;
      console.log(`✅ 已寫入 ${i + chunk.length}/${allData.length}`);
    }
    // Step6: 同步上傳 JSON 到 Storage
    const { error: uploadError } = await supabase.storage.from(STORAGE_BUCKET).upload(STORAGE_FILE_PATH, JSON.stringify(allData), {
      contentType: "application/json",
      upsert: true
    });
    if (uploadError) throw uploadError;
    return jsonResponse({
      message: "Database and Storage updated",
      date: newDate,
      count: allData.length
    });
  } catch (err) {
    console.error(err);
    return jsonResponse({
      error: err.message
    }, 500);
  }
}
// --- Edge Function 主入口 ---
Deno.serve(async (req)=>{
  if (req.method === "OPTIONS") return new Response("ok", {
    headers: corsHeaders
  });
  try {
    if (req.method === "GET") return await handleGet();
    if (req.method === "POST") return await handlePost();
    return jsonResponse({
      error: "Method not allowed"
    }, 405);
  } catch (err) {
    console.error(err);
    return jsonResponse({
      error: err.message
    }, 500);
  }
});
