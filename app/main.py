"""
TWSE 資料下載與合併工具 - 優化版本 V2
==============================

簡潔的主程式，專注於流程協調
"""
import os
import sys
import asyncio

# 加入當前路徑以確保模組可以正確匯入
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)


# 可擴充的主流程與後置報表產生任務（統一管理）
POST_REPORT_TASKS = [
    {
        "enable_flag": None,  # 主流程永遠執行
        "desc": "主資料處理流程",
        "module": "processors.twse_data_processor",  # 直接呼叫 main()
        "entry": "main"
    },
    {
        "enable_flag": "ENABLE_SUMMARY_REPORT",
        "desc": "自動抓取最新股價",
        "module": "processors.fetch_stock_prices",
        "entry": "main"
    },
    {
        "enable_flag": "ENABLE_SUMMARY_REPORT",
        "desc": "自動產生彙總報表",
        "module": "processors.summary_report_generator",
        "entry": "main"
    },
    # 未來可在此擴充更多報表產生任務
]

async def main():
    """
    依任務清單以 async/await 方式依序執行所有主流程與後置報表產生任務。
    「抓取最新股價」必須 await 完成後，才能 await「彙總表」產生。
    """
    from importlib import import_module
    from config import settings

    for i, task in enumerate(POST_REPORT_TASKS):
        if task["enable_flag"] is None:
            enabled = True
        else:
            enabled = getattr(settings, task["enable_flag"], False)
        if enabled:
            print(f"\n🚦 {task['desc']}...")
            try:
                mod = import_module(task["module"])
                entry = getattr(mod, task["entry"])
                # 若為 async function，await；否則同步呼叫
                if callable(entry):
                    if asyncio.iscoroutinefunction(entry):
                        await entry()
                    else:
                        entry()
                else:
                    instance = entry()
                    # 若有 async fetch_and_save，await；否則同步呼叫
                    if hasattr(instance, "fetch_and_save"):
                        method = getattr(instance, "fetch_and_save")
                        if asyncio.iscoroutinefunction(method):
                            await method()
                        else:
                            method()
                    elif hasattr(instance, "__call__"):
                        call_method = getattr(instance, "__call__")
                        if asyncio.iscoroutinefunction(call_method):
                            await call_method()
                        else:
                            call_method()
                    else:
                        raise RuntimeError("無法正確執行任務入口")
            except Exception as e:
                print(f"⚠️ {task['desc']}失敗: {e}")


if __name__ == "__main__":
    asyncio.run(main())