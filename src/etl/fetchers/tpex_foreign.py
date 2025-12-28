"""TPEX QFII - 僑外資及陸資持股統計 fetcher."""
from datetime import date
from io import StringIO
import requests
import pandas as pd

from src.common.utils import numeric_series, normalize_columns, find_col_any
from src.common.config import settings


def fetch_tpex_qfii(trade_date: date) -> pd.DataFrame:
    """Fetch 僑外資及陸資持股統計 from TPEX.

    Returns:
        DataFrame with columns: date, code, name, market, total_shares, foreign_shares, foreign_ratio
    """
    url = "https://www.tpex.org.tw/web/stock/3insti/qfii/qfii_result.php"
    params = {
        "l": "zh-tw",
        "o": "data",
    }

    empty_result = pd.DataFrame(columns=[
        "date", "code", "name", "market", "total_shares", "foreign_shares", "foreign_ratio"
    ])

    try:
        resp = requests.get(url, params=params, timeout=settings.request_timeout)
        resp.encoding = "utf-8"
        df = pd.read_csv(StringIO(resp.text))
    except Exception:
        return empty_result

    df = df.dropna(how="all", axis=0)
    df = df.dropna(how="all", axis=1)
    df = normalize_columns(df)

    if df.empty or len(df.columns) == 0:
        return empty_result

    code_col = find_col_any(df, "證券代號", "代號")
    name_col = find_col_any(df, "證券名稱", "名稱")
    shares_col = find_col_any(df, "發行股數")
    foreign_shares_col = find_col_any(df, "僑外資及陸資持有股數")
    foreign_ratio_col = find_col_any(df, "僑外資及陸資持股比率")

    if not all([code_col, name_col]):
        return empty_result

    out = pd.DataFrame()
    out["code"] = df[code_col].astype(str).str.strip().str.zfill(4)
    out["name"] = df[name_col].astype(str).str.strip()

    mask = out["code"].str.match(r"^\d{4,5}[A-Z]*$")
    out = out[mask].copy()

    if out.empty:
        return empty_result

    if shares_col:
        out["total_shares"] = numeric_series(df.loc[mask, shares_col])
    else:
        out["total_shares"] = 0

    if foreign_shares_col:
        out["foreign_shares"] = numeric_series(df.loc[mask, foreign_shares_col])
    else:
        out["foreign_shares"] = 0

    if foreign_ratio_col:
        out["foreign_ratio"] = pd.to_numeric(
            df.loc[mask, foreign_ratio_col].astype(str).str.replace(",", ""),
            errors="coerce"
        ).fillna(0.0)
    else:
        out["foreign_ratio"] = 0.0

    out["date"] = trade_date
    out["market"] = "TPEX"

    return out[["date", "code", "name", "market", "total_shares", "foreign_shares", "foreign_ratio"]].reset_index(drop=True)
