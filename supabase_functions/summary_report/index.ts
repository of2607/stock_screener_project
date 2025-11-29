import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";


const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
const STORAGE_BUCKET = "public-data";
const STORAGE_FILE_PATH = "reports/summary_report.json";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing required environment variables");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// 統一回應 headers，優雅寫法
const headers = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Content-Type": "application/json; charset=utf-8"
};

Deno.serve(async (_req) => {
  const { data, error } = await supabase.storage.from(STORAGE_BUCKET).download(STORAGE_FILE_PATH);
  if (error) {
    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers
    });
  }
  const text = await data.text();
  try {
    JSON.parse(text);
  } catch (e) {
    return new Response(JSON.stringify({ error: "JSON parse error", detail: e?.message }), {
      status: 500,
      headers
    });
  }
  return new Response(text, {
    status: 200,
    headers
  });
});
