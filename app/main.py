"""
TWSE 資料下載與合併工具 - 優化版本 V2
==============================

簡潔的主程式，專注於流程協調
"""
import os
import sys
import argparse
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

def str2bool(v):
    return str(v).lower() in ("yes", "true", "t", "1")

def override_settings_from_args(settings):
    parser = argparse.ArgumentParser()
    # 只抓全大寫的設定
    setting_keys = [k for k in dir(settings) if k.isupper() and not k.startswith("_")]
    for k in setting_keys:
        v = getattr(settings, k)
        if isinstance(v, bool):
            parser.add_argument(f"--{k}", type=str)
        elif isinstance(v, int):
            parser.add_argument(f"--{k}", type=int)
        elif isinstance(v, float):
            parser.add_argument(f"--{k}", type=float)
        elif isinstance(v, list):
            parser.add_argument(f"--{k}", type=str)
        else:
            parser.add_argument(f"--{k}", type=str)
    args, _ = parser.parse_known_args()
    for k in setting_keys:
        arg_val = getattr(args, k, None)
        if arg_val is not None:
            v = getattr(settings, k)
            if isinstance(v, bool):
                setattr(settings, k, str2bool(arg_val))
            elif isinstance(v, list):
                # 支援逗號分隔或單一字串
                if isinstance(arg_val, str):
                    val = [x.strip() for x in arg_val.split(',') if x.strip()]
                else:
                    val = list(arg_val)
                setattr(settings, k, val)
            else:
                setattr(settings, k, arg_val)

async def main():
    """
    依任務清單以 async/await 方式依序執行所有主流程與後置報表產生任務。
    「抓取最新股價」必須 await 完成後，才能 await「彙總表」產生。
    """
    from importlib import import_module

    from config import settings
    override_settings_from_args(settings)

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