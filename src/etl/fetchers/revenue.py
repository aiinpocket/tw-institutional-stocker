"""月營收資料 fetcher (MOPS 公開資訊觀測站)."""
from datetime import date
from io import StringIO
import requests
import pandas as pd

from src.common.config import settings


def fetch_monthly_revenue(year: int, month: int, market: str = "sii") -> pd.DataFrame:
    """Fetch 月營收 from MOPS (公開資訊觀測站).

    Args:
        year: 西元年份
        month: 月份 (1-12)
        market: 'sii' for 上市, 'otc' for 上櫃

    API: https://mops.twse.com.tw/nas/t21/{sii|otc}/t21sc03_{year}_{month}_0.html

    Returns:
        DataFrame with columns: code, name, year, month, revenue, mom_change, yoy_change,
                                cumulative_revenue, cumulative_yoy_change, market
    """
    # 轉換為民國年
    tw_year = year - 1911

    url = f"https://mops.twse.com.tw/nas/t21/{market}/t21sc03_{tw_year}_{month}_0.html"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    empty_result = pd.DataFrame(columns=[
        "code", "name", "year", "month", "revenue", "mom_change", "yoy_change",
        "cumulative_revenue", "cumulative_yoy_change", "market"
    ])

    try:
        resp = requests.get(url, headers=headers, timeout=settings.request_timeout)
        resp.encoding = "big5"

        # Parse HTML tables
        dfs = pd.read_html(StringIO(resp.text), encoding="big5")
        if not dfs:
            return empty_result

        # 合併所有產業的表格
        all_records = []
        market_label = "TWSE" if market == "sii" else "TPEX"

        for df in dfs:
            # 跳過標題或空表格
            if df.empty or len(df.columns) < 7:
                continue

            # 嘗試找到有效的資料行
            for _, row in df.iterrows():
                try:
                    # 第一欄應該是公司代號
                    code = str(row.iloc[0]).strip()

                    # 檢查是否為有效的股票代碼（4-6位數字開頭）
                    if not code or not code[0].isdigit() or len(code) < 4:
                        continue

                    # 只取數字部分作為代碼
                    code = code.split()[0] if " " in code else code

                    name = str(row.iloc[1]).strip() if len(row) > 1 else ""

                    def parse_num(val):
                        if pd.isna(val) or val == "" or val == "--" or val == "不適用":
                            return None
                        try:
                            # 移除逗號和空格
                            val_str = str(val).replace(",", "").replace(" ", "").strip()
                            return float(val_str) if val_str else None
                        except (ValueError, TypeError):
                            return None

                    revenue = parse_num(row.iloc[2]) if len(row) > 2 else None
                    mom_change = parse_num(row.iloc[4]) if len(row) > 4 else None  # 月增率
                    yoy_change = parse_num(row.iloc[6]) if len(row) > 6 else None  # 年增率
                    cumulative_revenue = parse_num(row.iloc[7]) if len(row) > 7 else None
                    cumulative_yoy_change = parse_num(row.iloc[9]) if len(row) > 9 else None

                    # 營收為 None 則跳過
                    if revenue is None:
                        continue

                    all_records.append({
                        "code": code,
                        "name": name,
                        "year": year,
                        "month": month,
                        "revenue": int(revenue) if revenue else None,
                        "mom_change": mom_change,
                        "yoy_change": yoy_change,
                        "cumulative_revenue": int(cumulative_revenue) if cumulative_revenue else None,
                        "cumulative_yoy_change": cumulative_yoy_change,
                        "market": market_label,
                    })
                except Exception:
                    continue

        if not all_records:
            return empty_result

        result_df = pd.DataFrame(all_records)

        # Filter valid stock codes
        mask = result_df["code"].str.match(r"^\d{4,6}$")
        return result_df[mask].reset_index(drop=True)

    except Exception as e:
        print(f"[WARN] Failed to fetch revenue for {year}/{month} ({market}): {e}")
        return empty_result


def fetch_all_revenue(year: int, month: int) -> pd.DataFrame:
    """Fetch 月營收 for both TWSE and TPEX.

    Returns:
        Combined DataFrame for both markets
    """
    all_data = []

    # Fetch TWSE (上市)
    print(f"  Fetching TWSE revenue for {year}/{month}...")
    twse_df = fetch_monthly_revenue(year, month, "sii")
    if not twse_df.empty:
        all_data.append(twse_df)
        print(f"    Got {len(twse_df)} TWSE records")

    # Fetch TPEX (上櫃)
    print(f"  Fetching TPEX revenue for {year}/{month}...")
    tpex_df = fetch_monthly_revenue(year, month, "otc")
    if not tpex_df.empty:
        all_data.append(tpex_df)
        print(f"    Got {len(tpex_df)} TPEX records")

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()
