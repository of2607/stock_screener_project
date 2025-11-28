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
        return {
            "eps_df": self._collect_yearly_data("income_statement", years),
            "div_df": self._collect_yearly_data("dividend", years),
            "bs_df": self._collect_yearly_data("balance_sheet", years),
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
        lookup = {}
        for _, row in df.iterrows():
            key = (row.get("代號"), row.get("年度"))
            lookup[key] = safe_float(row.get("淨利（損）歸屬於母公司業主"))
        return lookup

    @staticmethod
    def build_equity_lookup(df: pd.DataFrame) -> Dict:
        lookup = {}
        for _, row in df.iterrows():
            key = (row.get("代號"), row.get("年度"), row.get("季別"))
            lookup[key] = safe_float(row.get("歸屬於母公司業主之權益合計"))
        return lookup

class MetricCalculator:
    def __init__(self, quarters: List[str]):
        self.quarters = quarters

    def calc_avg_equity(self, equity_lookup, code, year) -> float:
        vals = [equity_lookup.get((code, year, q), np.nan) for q in self.quarters]
        vals = [v for v in vals if not pd.isna(v)]
        return np.mean(vals) if vals else np.nan

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
            for c in candidates:
                if c in df.columns:
                    return c
            for c in candidates:
                for col in df.columns:
                    if col.replace('\ufeff', '') == c:
                        return col
            return candidates[0]

        code_col = resolve_col(eps_df, ["代號", "stock_code"])
        name_col = resolve_col(eps_df, ["名稱", "stock_name"])

        def get_col_vals(df, col):
            return df[col] if col in df.columns else pd.Series(dtype=str)

        all_codes = pd.concat([
            get_col_vals(eps_df, code_col),
            get_col_vals(div_df, code_col),
            get_col_vals(bs_df, code_col)
        ]).dropna().unique()

        def get_name_df(df):
            if code_col in df.columns and name_col in df.columns:
                return df[[code_col, name_col]]
            return pd.DataFrame(columns=[code_col, name_col])

        all_names = pd.concat([
            get_name_df(eps_df),
            get_name_df(div_df),
            get_name_df(bs_df)
        ]).drop_duplicates().set_index(code_col)[name_col].to_dict()

        report_rows = []
        for code in all_codes:
            name = all_names.get(code, "")
            row = {"股票代號": code, "股票名稱": name}
            price_date = price_map.get(code, (np.nan, None))
            price = safe_float(price_date[0])
            close_date = price_date[1]
            row["收盤價"] = price
            row["收盤日"] = close_date
            eps_years, div_years, yield_years, roe_years = [], [], [], []
            for y in years:
                # 所有年度顯示 Q4（年度）EPS
                curr = eps_lookup.get((code, y, "Q4"), np.nan)
                row[f"{y}EPS_年度"] = curr
                eps_years.append(curr)
                # 完全移除 EPS 合計與所有單季 EPS 欄位（不產生）
                # 現金股利、殖利率、ROE（簡化流程）
                year_col = "年度" if "年度" in div_df.columns else "year"
                cash_div_col = "現金股利" if "現金股利" in div_df.columns else "cash_dividend"
                cash_div = np.nan
                if all(col in div_df.columns for col in [code_col, year_col, cash_div_col]):
                    div_row = div_df[(div_df[code_col] == code) & (div_df[year_col] == y)]
                    if not div_row.empty:
                        cash_div = safe_float(div_row.iloc[0][cash_div_col])
                row[f"{y}現金股利"] = cash_div
                div_years.append(cash_div)

                # 修正殖利率計算與欄位遺失問題
                price_val = price_map.get(code, (np.nan, None))
                price = safe_float(price_val[0])
                div_yield = round(float(cash_div) / float(price) * 100, 2) if not pd.isna(cash_div) and not pd.isna(price) and price != 0 else np.nan
                row[f"{y}殖利率"] = div_yield
                yield_years.append(div_yield)

                profit = profit_lookup.get((code, y), np.nan)
                avg_equity = self.calc_avg_equity(equity_lookup, code, y)
                roe = round(profit / avg_equity * 100, 2) if not pd.isna(profit) and not pd.isna(avg_equity) and avg_equity != 0 else np.nan
                row[f"{y}ROE"] = roe
                roe_years.append(roe)
            # 平均計算
            def avg_last_n(lst, n):
                vals = [v for v in lst[:n] if not pd.isna(v)]
                return round(np.mean(vals), 2) if vals else np.nan
            row["近5年平均股息"] = avg_last_n(div_years, 5)
            row["近3年平均股息"] = avg_last_n(div_years, 3)
            row["近5年平均殖利率"] = avg_last_n(yield_years, 5)
            row["近3年平均殖利率"] = avg_last_n(yield_years, 3)
            row["近5年平均ROE"] = avg_last_n(roe_years, 5)
            row["近3年平均ROE"] = avg_last_n(roe_years, 3)
            # 當年與前一年各季 EPS 差率（如有各季資料則顯示）
            if len(years) >= 2:
                y1, y2 = years[0], years[1]
                for i, q in enumerate(self.quarters):
                    eps1 = eps_lookup.get((code, y1, q), np.nan)
                    eps2 = eps_lookup.get((code, y2, q), np.nan)
                    row[f"{y1}{q}_EPS"] = eps1
                    row[f"{y2}{q}_EPS"] = eps2
                    if not pd.isna(eps1) and not pd.isna(eps2) and eps2 != 0:
                        row[f"{y1}_vs_{y2}_{q}_EPS差率"] = round((eps1 - eps2) / abs(eps2) * 100, 2)
                    else:
                        row[f"{y1}_vs_{y2}_{q}_EPS差率"] = np.nan
            report_rows.append(row)
        return report_rows

class ReportAssembler:
    @staticmethod
    def assemble(metrics: List[Dict]) -> pd.DataFrame:
        df_report = pd.DataFrame(metrics)
        # 重新排序欄位：股票代號、股票名稱、收盤價、收盤日、EPS累計（依年度、Q順序）
        cols = list(df_report.columns)
        priority = ["股票代號", "股票名稱", "收盤價", "收盤日"]
        # 近兩年顯示 Q1~Q4 EPS，其他年僅顯示年度 EPS
        eps_cols = []
        # 取得近兩年年份
        year_keys = []
        for c in cols:
            if c.endswith('EPS_Q1'):
                year_keys.append(c[:3])
        year_keys = year_keys[:2]
        # 近兩年 Q1~Q4
        for y in year_keys:
            for q in range(1, 5):
                col = f"{y}EPS_Q{q}"
                if col in cols:
                    eps_cols.append(col)
        # 其他年年度 EPS
        for c in cols:
            if c.endswith('EPS_年度'):
                eps_cols.append(c)
        others = [c for c in cols if c not in priority + eps_cols]
        df_report = df_report[priority + eps_cols + others]
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
    YEARS = get_recent_roc_years(DEFAULT_SUMMARY_YEARS)
    generator.generate(
        years=YEARS,
        output_csv=DEFAULT_REPORT_CSV,
        output_json=DEFAULT_REPORT_JSON,
    )
    print(f"✅ 已產生近{DEFAULT_SUMMARY_YEARS}年({YEARS})彙總報表")

if __name__ == "__main__":
    main()