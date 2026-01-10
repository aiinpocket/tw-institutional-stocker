"""月營收資料 fetcher (TWSE/TPEX OpenAPI)."""
from datetime import date
import requests
import pandas as pd
import urllib3

from src.common.config import settings

# Suppress SSL warnings for Taiwan government websites with certificate issues
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def fetch_monthly_revenue(year: int, month: int, market: str = "sii") -> pd.DataFrame:
    """Fetch 月營收 from TWSE/TPEX OpenAPI.

    Args:
        year: 西元年份
        month: 月份 (1-12)
        market: 'sii' for 上市 (TWSE), 'otc' for 上櫃 (TPEX)

    TWSE API: https://openapi.twse.com.tw/v1/openapi/t187ap05_L
    TPEX API: https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O

    Returns:
        DataFrame with columns: code, name, year, month, revenue, mom_change, yoy_change,
                                cumulative_revenue, cumulative_yoy_change, market
    """
    # 轉換為民國年月格式 (例如：11411 = 2025年11月)
    tw_year = year - 1911
    target_ym = f"{tw_year}{month:02d}"

    if market == "sii":
        url = "https://openapi.twse.com.tw/v1/openapi/t187ap05_L"
        market_label = "TWSE"
    else:
        url = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"
        market_label = "TPEX"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    empty_result = pd.DataFrame(columns=[
        "code", "name", "year", "month", "revenue", "mom_change", "yoy_change",
        "cumulative_revenue", "cumulative_yoy_change", "market"
    ])

    try:
        resp = requests.get(url, headers=headers, timeout=settings.request_timeout, verify=False)
        resp.raise_for_status()
        data = resp.json()

        if not data:
            print(f"  [WARN] Empty response from {market_label} OpenAPI")
            return empty_result

        # Filter by target year/month
        records = []
        for item in data:
            # 資料年月格式為 "11411" (民國年+月)
            data_ym = str(item.get("資料年月", "")).strip()
            if data_ym != target_ym:
                continue

            code = str(item.get("公司代號", "")).strip()

            # 驗證股票代碼格式 (4-6位數字)
            if not code or not code.isdigit() or len(code) < 4:
                continue

            name = str(item.get("公司名稱", "")).strip()

            def parse_num(val):
                if val is None or val == "" or val == "-" or val == "不適用":
                    return None
                try:
                    val_str = str(val).replace(",", "").strip()
                    return float(val_str) if val_str else None
                except (ValueError, TypeError):
                    return None

            revenue = parse_num(item.get("營業收入-當月營收"))
            mom_change = parse_num(item.get("營業收入-上月比較增減(%)"))
            yoy_change = parse_num(item.get("營業收入-去年同月增減(%)"))
            cumulative_revenue = parse_num(item.get("累計營業收入-當月累計營收"))
            cumulative_yoy_change = parse_num(item.get("累計營業收入-前期比較增減(%)"))

            # Skip if revenue is missing
            if revenue is None:
                continue

            records.append({
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

        if not records:
            print(f"  [WARN] No revenue data found for {year}/{month} in {market_label} OpenAPI")
            return empty_result

        result_df = pd.DataFrame(records)
        return result_df

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
