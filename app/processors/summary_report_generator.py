"""優化重構：保留所有原有功能與邏輯，分層設計，易於維護與擴充"""
import os
import numpy as np
import pandas as pd
from typing import List, Optional, Dict, Any
from datetime import datetime

# === 匯入依原有 try/except 保留 ===
from processors.csv_cleaner import CSVCleaner
from processors.data_standardizer import DataStandardizer
from processors.data_sorter import DataSorter
from processors.report_processor import ReportProcessor
from utils.logger import Logger
from config.settings import (
    SUMMARY_FROM_DIR,
    SUMMARY_PRICE_FILE,
    SUMMARY_LOG_DIR,
    SUMMARY_YEARS,
    REPORT_CSV_DIR,
    REPORT_JSON_DIR,
)

DEFAULT_REPORT_CSV = REPORT_CSV_DIR
DEFAULT_REPORT_JSON = REPORT_JSON_DIR
DEFAULT_DATA_DIR = SUMMARY_FROM_DIR
DEFAULT_PRICE_FILE = SUMMARY_PRICE_FILE
DEFAULT_QUARTERS = ["Q1", "Q2", "Q3", "Q4"]
DEFAULT_SUMMARY_YEARS = SUMMARY_YEARS

# === 分層設計 ===

class DataLoader:
    def __init__(self, data_dir: str, price_file: str):
        self.data_dir = data_dir
        self.price_file = price_file

    def load(self, years: List[str]) -> Dict[str, pd.DataFrame]:
        # 為了計算 ROE（需要前一年 Q4 作為期初），自動加入最早年份的前一年
        extended_years = years.copy()
        if years:
            earliest_year = min([int(y) for y in years])
            prev_year = str(earliest_year - 1)
            if prev_year not in extended_years:
                extended_years.append(prev_year)
        
        return {
            "eps_df": self._collect_yearly_data("income_statement", extended_years),
            "div_df": self._collect_yearly_data("dividend", years),  # 股利不需要前一年
            "bs_df": self._collect_yearly_data("balance_sheet", extended_years),  # 資產負債需要前一年 Q4
            "price_map": self._get_latest_price_map(),
        }

    def _read_csv_with_nan(self, path: str) -> pd.DataFrame:
        try:
            df = pd.read_csv(path, dtype=str, encoding="utf-8-sig").replace({"": np.nan})
            df.rename(columns=lambda x: x.strip(), inplace=True)
            # 修正欄位名稱 BOM 問題
            if df.columns[0].startswith("\ufeff"):
                df.columns.values[0] = df.columns[0].replace("\ufeff", "")
            return df
        except Exception:
            return pd.DataFrame()

    def _get_latest_price_map(self) -> Dict[str, Any]:
        df = self._read_csv_with_nan(self.price_file)
        if hasattr(df, 'columns'):
            df.columns = [str(col).strip().replace('\ufeff', '') for col in df.columns]
        # 兼容不同欄位名稱
        code_col = "stock_code" if "stock_code" in df.columns else "代號"
        price_col = "price" if "price" in df.columns else "收盤價"
        date_col = "date" if "date" in df.columns else "日期" if "日期" in df.columns else None
        # 回傳 dict: {代號: (收盤價, 收盤日)}
        if date_col:
            return dict(zip(df[code_col], zip(df[price_col], df[date_col])))
        else:
            return dict(zip(df[code_col], zip(df[price_col], [None]*len(df))))

    def _collect_yearly_data(self, report: str, years: List[str]) -> pd.DataFrame:
        dfs = []
        for y in years:
            f = os.path.join(self.data_dir, f"{y}-{report}.csv")
            if os.path.exists(f):
                df = self._read_csv_with_nan(f)
                df["年度"] = y
                dfs.append(df)
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()

class LookupBuilder:
    @staticmethod
    def build_eps_lookup(df: pd.DataFrame) -> Dict:
        lookup = {}
        for _, row in df.iterrows():
            key = (row.get("代號"), row.get("年度"), row.get("季別"))
            lookup[key] = safe_float(row.get("基本每股盈餘（元）"))
        return lookup

    @staticmethod
    def build_profit_lookup(df: pd.DataFrame) -> Dict:
        """
        取得年度淨利。直接從合併資料中的「年度淨利」欄位取得（該欄位已在 merge 時計算）。
        """
        lookup = {}
        for _, row in df.iterrows():
            code = row.get("代號")
            year = row.get("年度")
            quarter = row.get("季別")
            # 只有 Q4 的年度淨利有值
            if quarter == "Q4":
                annual_profit = safe_float(row.get("淨利"))
                if not pd.isna(annual_profit):
                    lookup[(code, year)] = annual_profit
        return lookup

    @staticmethod
    def build_equity_lookup(df: pd.DataFrame) -> Dict:
        lookup = {}
        for _, row in df.iterrows():
            key = (row.get("代號"), row.get("年度"), row.get("季別"))
            lookup[key] = safe_float(row.get("權益總計"))
        return lookup

class MetricCalculator:
    def calc_single_quarter_eps(self, eps_lookup, code, seasons_sorted):
        """回傳近四季單季EPS清單（由舊到新）"""
        single_eps = []
        for i in range(1, len(seasons_sorted)):
            curr_y, curr_q = seasons_sorted[i][:3], seasons_sorted[i][3:]
            prev_y, prev_q = seasons_sorted[i-1][:3], seasons_sorted[i-1][3:]
            curr_eps = eps_lookup.get((code, curr_y, curr_q), np.nan)
            prev_eps = eps_lookup.get((code, prev_y, prev_q), np.nan)
            if curr_q == "Q1":
                single = curr_eps
            elif curr_y == prev_y and not pd.isna(curr_eps) and not pd.isna(prev_eps):
                single = curr_eps - prev_eps
            else:
                single = np.nan
            single_eps.append(single)
        return single_eps

    def calc_eps_diff_rate(self, eps_lookup, code, y, q):
        """計算累計EPS差率"""
        try:
            y_prev = str(int(y) - 1)
        except Exception:
            y_prev = ''
        eps_now = eps_lookup.get((code, y, q), np.nan)
        eps_prev = eps_lookup.get((code, y_prev, q), np.nan)
        if not pd.isna(eps_now) and not pd.isna(eps_prev) and eps_prev != 0:
            return round((eps_now - eps_prev) / abs(eps_prev) * 100, 2)
        else:
            return np.nan
    def __init__(self, quarters: List[str]):
        self.quarters = quarters

    def calc_avg_equity(self, equity_lookup, code, year) -> float:
        """
        計算平均權益：使用期初(前一年Q4)和期末(當年Q4)的平均值。
        這是符合 GoodInfo 等財經網站的 ROE 計算標準方法。
        """
        # 期初：前一年 Q4
        prev_year = str(int(year) - 1)
        begin_equity = equity_lookup.get((code, prev_year, "Q4"), np.nan)
        
        # 期末：當年 Q4
        end_equity = equity_lookup.get((code, year, "Q4"), np.nan)
        
        if not pd.isna(begin_equity) and not pd.isna(end_equity):
            return (begin_equity + end_equity) / 2
        elif not pd.isna(end_equity):
            return end_equity
        elif not pd.isna(begin_equity):
            return begin_equity
        else:
            return np.nan

    def calc_div_yield(self, cash_div, price) -> float:
        try:
            return round(float(cash_div) / float(price) * 100, 2)
        except Exception:
            return np.nan

    def calculate(self, lookups: Dict[str, Dict], data: Dict[str, pd.DataFrame], years: List[str]) -> List[Dict]:
        eps_df, div_df, bs_df, price_map = data["eps_df"], data["div_df"], data["bs_df"], data["price_map"]
        eps_lookup, profit_lookup, equity_lookup = lookups["eps_lookup"], lookups["profit_lookup"], lookups["equity_lookup"]

        # 欄位名稱自動對應與安全取得

        def resolve_col(df, candidates):
            """自動偵測欄位名稱，支援 BOM 與多語系"""
            for c in candidates:
                if c in df.columns:
                    return c
            for c in candidates:
                for col in df.columns:
                    if col.replace('\ufeff', '') == c:
                        return col
            return candidates[0]

        def get_col_vals(df, col):
            return df[col] if col in df.columns else pd.Series(dtype=str)

        def get_name_map(eps_df, div_df, bs_df, code_col, name_col):
            """合併所有名稱對照，去重"""
            frames = [df[[code_col, name_col]] for df in [eps_df, div_df, bs_df] if code_col in df.columns and name_col in df.columns]
            if frames:
                return pd.concat(frames).drop_duplicates().set_index(code_col)[name_col].to_dict()
            return {}

        code_col = resolve_col(eps_df, ["代號", "stock_code"])
        name_col = resolve_col(eps_df, ["名稱", "stock_name"])
        all_codes = pd.concat([
            get_col_vals(eps_df, code_col),
            get_col_vals(div_df, code_col),
            get_col_vals(bs_df, code_col)
        ]).dropna().unique()
        all_names = get_name_map(eps_df, div_df, bs_df, code_col, name_col)

        report_rows = []
        # 預先快取 lookup，減少重複查詢
        eps_lookup_cache = eps_lookup
        profit_lookup_cache = profit_lookup
        equity_lookup_cache = equity_lookup
        price_map_cache = price_map
        def avg_last_n(lst, n):
            vals = [v for v in lst[:n] if not pd.isna(v)]
            return round(np.mean(vals), 2) if vals else np.nan

        def get_cash_div_sum(div_df, code, year, code_col, year_col, cash_div_col):
            """取得年度現金股利加總，去重且型別安全"""
            if not all(col in div_df.columns for col in [code_col, year_col, cash_div_col]):
                return np.nan
            div_rows = div_df[(div_df[code_col] == code) & (div_df[year_col] == year)].copy()
            div_rows[cash_div_col] = div_rows[cash_div_col].apply(safe_float)
            valid_divs = div_rows[~div_rows[cash_div_col].isna()]
            dedup_cols = [code_col, year_col] + [col for col in ["配息日", "季別"] if col in valid_divs.columns] + [cash_div_col]
            valid_divs = valid_divs.drop_duplicates(subset=dedup_cols)
            return round(valid_divs[cash_div_col].sum(), 2) if not valid_divs.empty else np.nan

        for code in all_codes:
            name = all_names.get(code, "")
            row = {"股票代號": code, "股票名稱": name}
            price, close_date = price_map_cache.get(code, (np.nan, None))
            price = safe_float(price)
            row["收盤價"] = price
            row["收盤日"] = close_date
            eps_years, div_years, yield_years, roe_years, payout_years = [], [], [], [], []
            year_col = "年度" if "年度" in div_df.columns else "year"
            cash_div_col = "現金股利" if "現金股利" in div_df.columns else "cash_dividend"
            for y in years:
                curr = eps_lookup_cache.get((code, y, "Q4"), np.nan)
                row[f"{y}EPS_年度"] = curr
                eps_years.append(curr)
                cash_div = get_cash_div_sum(div_df, code, y, code_col, year_col, cash_div_col)
                row[f"{y}現金股利"] = cash_div
                div_years.append(cash_div)
                div_yield = round(float(cash_div) / float(price) * 100, 2) if not pd.isna(cash_div) and not pd.isna(price) and price != 0 else np.nan
                row[f"{y}殖利率"] = div_yield
                yield_years.append(div_yield)
                profit = profit_lookup_cache.get((code, y), np.nan)
                avg_equity = self.calc_avg_equity(equity_lookup_cache, code, y)
                roe = round(profit / avg_equity * 100, 2) if not pd.isna(profit) and not pd.isna(avg_equity) and avg_equity != 0 else np.nan
                row[f"{y}ROE"] = roe
                roe_years.append(roe)
                # 計算配息率 = 現金股利 / EPS × 100（不顯示年度配息率，僅用於計算平均值）
                payout_ratio = round(float(cash_div) / float(curr) * 100, 2) if not pd.isna(cash_div) and not pd.isna(curr) and curr != 0 else np.nan
                payout_years.append(payout_ratio)
            # 近N年平均：排除當前年度（years[0]），只計算完整的過去N年
            row["近3年平均股息"] = avg_last_n(div_years[1:], 3)
            row["近5年平均股息"] = avg_last_n(div_years[1:], 5)
            row["近8年平均股息"] = avg_last_n(div_years[1:], 8)
            row["近3年平均殖利率"] = avg_last_n(yield_years[1:], 3)
            row["近5年平均殖利率"] = avg_last_n(yield_years[1:], 5)
            row["近8年平均殖利率"] = avg_last_n(yield_years[1:], 8)
            row["近3年平均ROE"] = avg_last_n(roe_years[1:], 3)
            row["近5年平均ROE"] = avg_last_n(roe_years[1:], 5)
            row["近8年平均ROE"] = avg_last_n(roe_years[1:], 8)
            row["近3年平均配息率"] = avg_last_n(payout_years[1:], 3)
            row["近5年平均配息率"] = avg_last_n(payout_years[1:], 5)
            row["近8年平均配息率"] = avg_last_n(payout_years[1:], 8)
            # 取得所有可用的年度與季別，組成完整的季序列（新到舊）
            all_seasons = [f"{y}{q}" for y in years for q in reversed(self.quarters)]
            # 近八季逐季EPS（顯示累計值）
            for season in reversed(all_seasons[1:9]):
                y, q = season[:3], season[3:]
                eps = eps_lookup_cache.get((code, y, q), np.nan)
                row[f"{season}_EPS"] = eps
            # 近四季逐季EPS與前同期EPS差率（不含當季）
            for season in reversed(all_seasons[1:5]):
                y, q = season[:3], season[3:]
                eps_now = eps_lookup_cache.get((code, y, q), np.nan)
                y_prev = str(int(y) - 1) if y.isdigit() else ''
                eps_prev = eps_lookup_cache.get((code, y_prev, q), np.nan)
                row[f"{y}{q}_EPS"] = eps_now
                row[f"{y_prev}{q}_EPS"] = eps_prev
                row[f"{y}{q}_vs_{y_prev}{q}_EPS差率"] = self.calc_eps_diff_rate(eps_lookup_cache, code, y, q)
            # 近四季EPS總合（放最後）
            last_5_seasons_sorted = sorted(all_seasons[1:6], key=lambda s: (int(s[:3]), {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}.get(s[3:], 0)))
            eps_4q = self.calc_single_quarter_eps(eps_lookup_cache, code, last_5_seasons_sorted)
            row["近四季EPS總合"] = round(np.nansum(eps_4q), 2) if any([not pd.isna(e) for e in eps_4q]) else np.nan
            
            # 近四季EPS總合vs前一年度EPS差率
            recent_4q_eps = row.get("近四季EPS總合", np.nan)
            if len(years) > 1:
                prev_year = years[1]  # 前一年度（years[0]是最新年度）
                prev_year_eps = row.get(f"{prev_year}EPS_年度", np.nan)
                if not pd.isna(recent_4q_eps) and not pd.isna(prev_year_eps) and prev_year_eps != 0:
                    diff_rate = round((recent_4q_eps - prev_year_eps) / abs(prev_year_eps) * 100, 2)
                else:
                    diff_rate = np.nan
            else:
                diff_rate = np.nan
            row["近四季_vs_前年度_EPS差率"] = diff_rate
            
            report_rows.append(row)
        return report_rows

class ReportAssembler:
    @staticmethod
    def assemble(metrics: List[Dict]) -> pd.DataFrame:
        df_report = pd.DataFrame(metrics)
        # 重新排序欄位：股票代號、股票名稱、收盤價、收盤日、逐年 EPS/現金股利/殖利率/ROE
        cols = list(df_report.columns)
        priority = ["股票代號", "股票名稱", "收盤價", "收盤日"]
        # 取得所有年度（如 '113', '112', ...）
        year_set = set()
        for row in metrics:
            for k in row.keys():
                if k.endswith('EPS_年度') and len(k) >= 6:
                    year_set.add(k[:3])
        years = sorted(year_set, reverse=True)
        # 欄位類型順序
        col_types = ["現金股利", "殖利率", "ROE", "EPS_年度"]
        year_cols = []
        missing_cols = []
        for col_type in col_types:
            for y in years:
                col = f"{y}{col_type}"
                year_cols.append(col)
                if col not in df_report.columns:
                    missing_cols.append(col)
        # 一次性補齊所有缺少欄位
        if missing_cols:
            nan_df = pd.DataFrame(np.nan, index=df_report.index, columns=missing_cols)
            df_report = pd.concat([df_report, nan_df], axis=1)
        # 其他欄位自動排後，並將「近四季EPS總合」及其差率移到最後
        special_cols = ["近四季EPS總合", "近四季EPS總合vs前一年度EPS差率"]
        others = [c for c in df_report.columns if c not in priority + year_cols + special_cols]
        final_special = [c for c in special_cols if c in df_report.columns]
        df_report = df_report[priority + year_cols + others + final_special]
        df_report = df_report.sort_values(by=["股票代號"]).reset_index(drop=True)
        return df_report

class ReportExporter:
    @staticmethod
    def export(df: pd.DataFrame, output_csv: str, output_json: str):
        if output_csv and os.path.dirname(output_csv):
            os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        if output_json and os.path.dirname(output_json):
            os.makedirs(os.path.dirname(output_json), exist_ok=True)
        if output_csv:
            df.to_csv(output_csv, index=False, encoding="utf-8-sig")
        if output_json:
            df.to_json(output_json, orient="records", force_ascii=False, indent=2)

def safe_float(val: Any) -> float:
    try:
        return float(val)
    except Exception:
        return np.nan

def get_recent_roc_years(n: int = 8) -> List[str]:
    current_year = datetime.now().year
    roc_year = current_year - 1911
    return [str(y) for y in range(roc_year, roc_year - n, -1)]

class SummaryReportGenerator:
    def __init__(
        self,
        logger: Logger,
        csv_cleaner: CSVCleaner,
        data_standardizer: DataStandardizer,
        data_sorter: DataSorter,
        report_processor: ReportProcessor,
        data_dir: str = DEFAULT_DATA_DIR,
        price_file: str = DEFAULT_PRICE_FILE,
        quarters: Optional[List[str]] = None,
    ):
        self.logger = logger
        self.csv_cleaner = csv_cleaner
        self.data_standardizer = data_standardizer
        self.data_sorter = data_sorter
        self.report_processor = report_processor
        self.data_dir = data_dir
        self.price_file = price_file
        self.quarters = quarters or DEFAULT_QUARTERS

        self.data_loader = DataLoader(self.data_dir, self.price_file)
        self.metric_calculator = MetricCalculator(self.quarters)
        self.lookup_builder = LookupBuilder()
        self.report_assembler = ReportAssembler()
        self.report_exporter = ReportExporter()

    def generate(
        self,
        years: List[str],
        output_csv: str,
        output_json: str,
    ) -> None:
        data = self.data_loader.load(years)
        lookups = {
            "eps_lookup": self.lookup_builder.build_eps_lookup(data["eps_df"]),
            "profit_lookup": self.lookup_builder.build_profit_lookup(data["eps_df"]),
            "equity_lookup": self.lookup_builder.build_equity_lookup(data["bs_df"]),
        }
        metrics = self.metric_calculator.calculate(lookups, data, years)
        df_report = self.report_assembler.assemble(metrics)
        self.report_exporter.export(
            df_report,
            output_csv or DEFAULT_REPORT_CSV,
            output_json or DEFAULT_REPORT_JSON,
        )


def main():
    logger = Logger(SUMMARY_LOG_DIR)
    csv_cleaner = CSVCleaner(logger)
    data_standardizer = DataStandardizer(logger)
    data_sorter = DataSorter(logger)
    report_processor = ReportProcessor(logger)
    generator = SummaryReportGenerator(
        logger,
        csv_cleaner,
        data_standardizer,
        data_sorter,
        report_processor,
    )
    YEARS = get_recent_roc_years(DEFAULT_SUMMARY_YEARS+1)
    generator.generate(
        years=YEARS,
        output_csv=DEFAULT_REPORT_CSV,
        output_json=DEFAULT_REPORT_JSON,
    )
    print(f"✅ 已產生近{DEFAULT_SUMMARY_YEARS}年({YEARS})彙總報表")

if __name__ == "__main__":
    main()