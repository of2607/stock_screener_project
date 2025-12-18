"""
長表預計算：整合 merged_data 中的報表成結構化長表
將 income_statement、balance_sheet、dividend 按 (code, year, quarter) 合併
"""
import os
import json
import pandas as pd
import numpy as np
from typing import Dict, List, Any
from datetime import datetime

from config.settings import (
    MERGED_CSV_DIR,
    PRECOMPUTED_METRICS_DIR,
    HISTORICAL_METRICS_FILE,
    METRICS_UPDATE_LOG_FILE,
    SUMMARY_LOG_DIR,
    SUMMARY_PRICE_FILE,
)
from utils.logger import Logger


class MetricsPrecomputer:
    """預計算長表指標"""

    def __init__(self, logger: Logger = None):
        self.logger = logger or Logger(SUMMARY_LOG_DIR)
        self.merged_csv_dir = MERGED_CSV_DIR
        self.output_dir = PRECOMPUTED_METRICS_DIR
        self.output_file = HISTORICAL_METRICS_FILE
        self.update_log_file = METRICS_UPDATE_LOG_FILE

    def _read_csv_safe(self, path: str) -> pd.DataFrame:
        """安全讀取 CSV，處理 BOM 和編碼問題"""
        try:
            df = pd.read_csv(path, dtype=str, encoding="utf-8-sig").replace({"": np.nan})
            df.rename(columns=lambda x: x.strip(), inplace=True)
            # 修正欄位名稱 BOM 問題
            if df.columns[0].startswith("\ufeff"):
                df.columns.values[0] = df.columns[0].replace("\ufeff", "")
            return df
        except Exception as e:
            self.logger.error(f"讀取 {path} 失敗: {e}")
            return pd.DataFrame()

    def _safe_float(self, val: Any) -> float:
        """安全轉換為 float"""
        try:
            if pd.isna(val):
                return np.nan
            return float(val)
        except Exception:
            return np.nan

    def _get_all_years(self) -> List[str]:
        """從 merged_data 中提取所有年度"""
        years = set()
        for filename in os.listdir(self.merged_csv_dir):
            if filename.endswith("-income_statement.csv"):
                year = filename.split("-")[0]
                years.add(year)
        return sorted(list(years), reverse=True)

    def _load_income_statement(self, years: List[str]) -> pd.DataFrame:
        """載入所有年度的 income_statement，提取 code, year, quarter, eps, profit"""
        dfs = []
        for year in years:
            path = os.path.join(self.merged_csv_dir, f"{year}-income_statement.csv")
            if os.path.exists(path):
                df = self._read_csv_safe(path)
                if df.empty:
                    continue
                # 選擇需要的欄位
                df = df[["代號", "年度", "季別", "基本每股盈餘（元）", "淨利"]].copy()
                df.rename(
                    columns={
                        "代號": "code",
                        "年度": "year",
                        "季別": "quarter",
                        "基本每股盈餘（元）": "eps_raw",
                        "淨利": "profit_raw",
                    },
                    inplace=True,
                )
                # 轉換為 float
                df["eps"] = df["eps_raw"].apply(self._safe_float)
                df["profit"] = df["profit_raw"].apply(self._safe_float)
                dfs.append(df[["code", "year", "quarter", "eps", "profit"]])
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()

    def _load_balance_sheet(self, years: List[str]) -> pd.DataFrame:
        """載入所有年度的 balance_sheet，提取 code, year, quarter, equity"""
        dfs = []
        for year in years:
            path = os.path.join(self.merged_csv_dir, f"{year}-balance_sheet.csv")
            if os.path.exists(path):
                df = self._read_csv_safe(path)
                if df.empty:
                    continue
                # 選擇需要的欄位
                df = df[["代號", "年度", "季別", "權益總計"]].copy()
                df.rename(
                    columns={
                        "代號": "code",
                        "年度": "year",
                        "季別": "quarter",
                        "權益總計": "equity_raw",
                    },
                    inplace=True,
                )
                df["equity"] = df["equity_raw"].apply(self._safe_float)
                dfs.append(df[["code", "year", "quarter", "equity"]])
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()

    def _load_dividend(self, years: List[str]) -> pd.DataFrame:
        """載入所有年度的 dividend，提取 code, year, quarter, cash_dividend"""
        dfs = []
        for year in years:
            path = os.path.join(self.merged_csv_dir, f"{year}-dividend.csv")
            if os.path.exists(path):
                df = self._read_csv_safe(path)
                if df.empty:
                    continue
                # 選擇需要的欄位
                df = df[["代號", "年度", "季別", "現金股利"]].copy()
                df.rename(
                    columns={
                        "代號": "code",
                        "年度": "year",
                        "季別": "quarter",
                        "現金股利": "cash_dividend_raw",
                    },
                    inplace=True,
                )
                df["cash_dividend"] = df["cash_dividend_raw"].apply(self._safe_float)
                # 去除 NaN 和 0 的股利
                df.loc[df["cash_dividend"] == 0, "cash_dividend"] = np.nan
                dfs.append(df[["code", "year", "quarter", "cash_dividend"]])
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()

    def _get_valid_stock_codes(self) -> set:
        """從 latest_stock_prices.csv 中獲取有效的股票代號"""
        try:
            price_file = SUMMARY_PRICE_FILE
            if not os.path.exists(price_file):
                self.logger.warning(f"⚠️ 找不到股價檔案: {price_file}，將使用所有股票")
                return set()
            
            df = self._read_csv_safe(price_file)
            if df.empty:
                self.logger.warning("⚠️ 股價檔案為空，將使用所有股票")
                return set()
            
            # 取得代號欄位
            code_col = "stock_code" if "stock_code" in df.columns else "代號"
            if code_col not in df.columns:
                self.logger.warning(f"⚠️ 找不到代號欄位，將使用所有股票")
                return set()
            
            valid_codes = set(df[code_col].dropna().unique())
            self.logger.info(f"📋 發現有效股票: {len(valid_codes)} 支")
            return valid_codes
        except Exception as e:
            self.logger.error(f"❌ 讀取股價檔案失敗: {e}，將使用所有股票")
            return set()

    def precompute(self) -> None:
        """執行預計算：整合三張表成長表"""
        start_time = datetime.now()
        self.logger.info("🚀 開始預計算長表...")

        try:
            # 確保輸出目錄存在
            os.makedirs(self.output_dir, exist_ok=True)

            # 獲取所有年度
            years = self._get_all_years()
            if not years:
                raise ValueError("未找到任何 income_statement CSV 檔案")
            self.logger.info(f"📊 發現年度: {', '.join(years)}")

            # 載入三張表
            self.logger.info("📥 載入 income_statement...")
            eps_df = self._load_income_statement(years)
            self.logger.info(f"   ✓ 共 {len(eps_df)} 筆 EPS 資料")

            self.logger.info("📥 載入 balance_sheet...")
            equity_df = self._load_balance_sheet(years)
            self.logger.info(f"   ✓ 共 {len(equity_df)} 筆權益資料")

            self.logger.info("📥 載入 dividend...")
            dividend_df = self._load_dividend(years)
            self.logger.info(f"   ✓ 共 {len(dividend_df)} 筆股利資料")

            # 合併三張表：outer join on (code, year, quarter)
            self.logger.info("🔗 合併三張表...")
            result = eps_df.copy()
            result = result.merge(equity_df, on=["code", "year", "quarter"], how="outer")
            result = result.merge(dividend_df, on=["code", "year", "quarter"], how="outer")
            
            # 只保留 latest_stock_prices 中有的股票
            valid_codes = self._get_valid_stock_codes()
            if valid_codes:
                result = result[result["code"].isin(valid_codes)]
                self.logger.info(f"   ✓ 過濾後: {len(result)} 筆資料（{result['code'].nunique()} 支股票）")

            # 排序：按 code, year (DESC), quarter (DESC)
            result["year_int"] = result["year"].astype(int)
            result["quarter_order"] = result["quarter"].map({"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4})
            result = result.sort_values(by=["code", "year_int", "quarter_order"], ascending=[True, False, False])
            
            # 記錄年度範圍（在刪除 year_int 之前）
            year_min = result["year_int"].min()
            year_max = result["year_int"].max()
            
            # 只保留最終欄位
            result = result[["code", "year", "quarter", "eps", "profit", "equity", "cash_dividend"]]

            # 保存長表
            self.logger.info(f"💾 保存長表到 {self.output_file}...")
            result.to_csv(self.output_file, index=False, encoding="utf-8-sig")
            self.logger.info(f"   ✓ 共 {len(result)} 筆資料")

            # 記錄更新時間
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            log_data = {
                "last_update": end_time.isoformat(),
                "duration_seconds": duration,
                "total_records": len(result),
                "unique_stocks": result["code"].nunique(),
                "year_range": f"{year_min} - {year_max}",
            }
            os.makedirs(os.path.dirname(self.update_log_file), exist_ok=True)
            with open(self.update_log_file, "w", encoding="utf-8") as f:
                json.dump(log_data, f, ensure_ascii=False, indent=2)

            self.logger.info(f"✅ 預計算完成！耗時 {duration:.2f} 秒")
            return log_data

        except Exception as e:
            self.logger.error(f"❌ 預計算失敗: {e}")
            raise


def main():
    """主入口"""
    logger = Logger(SUMMARY_LOG_DIR)
    precomputer = MetricsPrecomputer(logger)
    precomputer.precompute()


if __name__ == "__main__":
    main()
