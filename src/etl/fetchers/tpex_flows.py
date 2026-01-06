"""TPEX 三大法人買賣明細 fetcher."""
from datetime import date
from io import StringIO
import requests
import pandas as pd

from src.common.utils import numeric_series, normalize_columns, find_col_any, to_roc_date
from src.common.config import settings


def fetch_tpex_flows(trade_date: date) -> pd.DataFrame:
    """Fetch 上櫃股票三大法人買賣明細 from TPEX.

    Returns:
        DataFrame with columns: date, code, name, foreign_net, trust_net, dealer_net, market
    """
    roc = to_roc_date(trade_date)
    url = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php"
    params = {
        "d": roc,
        "l": "zh-tw",
        "o": "htm",
        "s": "0",
        "se": "EW",
        "t": "D",
    }

    empty_result = pd.DataFrame(
        columns=["date", "code", "name", "foreign_net", "trust_net", "dealer_net", "market"]
    )

    try:
        resp = requests.get(url, params=params, timeout=settings.request_timeout)
        resp.encoding = "utf-8"
        tables = pd.read_html(StringIO(resp.text))
    except Exception:
        return empty_result

    if not tables:
        return empty_result

    df = tables[0]
    df = normalize_columns(df)

    if df.empty or len(df.columns) == 0:
        return empty_result

    code_col = find_col_any(df, "代號")
    name_col = find_col_any(df, "名稱")

    # TPEX API 格式隨時間變化:
    # - 2020+: MultiIndex 格式，欄位名如 "外資及陸資(不含外資自營商)_買賣超股數"
    # - 2011-2019: 舊格式，欄位名如 "外資及陸資淨買股數"
    # - 2010 以前: 更舊格式，欄位名如 "外資及陸資淨買股數"

    # 外資欄位 - 優先找新格式，再找舊格式
    # 注意: 舊格式欄位名可能有空格 (如 "外資 及陸資 淨買股數")
    col_foreign_ex_net = find_col_any(
        df,
        # 新格式 (2020+) - MultiIndex 產生的欄位名
        "外資及陸資(不含外資自營商)_買賣超股數",
        "外資及陸資(不含外資自營商)買賣超股數",
        "外資及陸資買賣超股數(不含外資自營商)",
        "外資及陸資_買賣超股數",
        "外資及陸資買賣超股數",
        # 舊格式 (2011-2019) - 可能有空格
        "外資及陸資淨買股數",
        "外資及陸資_淨買股數",
        "外資 及陸資 淨買股數",  # 有空格版本
    )
    col_foreign_self_net = find_col_any(
        df,
        "外資自營商_買賣超股數",
        "外資自營商買賣超股數",
    )
    # 投信欄位
    col_trust_net = find_col_any(
        df,
        # 新格式
        "投信_買賣超股數",
        "投信買賣超股數",
        # 舊格式 - 可能有空格
        "投信淨買股數",
        "投信_淨買股數",
        "投信 淨買股數",  # 有空格版本
    )
    # 自營商欄位要精確匹配，避免匹配到 (自行買賣) 或 (避險) 或 外資自營商
    col_dealer_net = None
    for col in df.columns:
        col_clean = col.strip()
        # 新格式: "自營商_買賣超股數" 或 "自營商買賣超股數"
        # 舊格式: "自營商淨買股數" 或 "自營商_淨買股數" 或 "自營商 淨買股數"
        is_dealer_col = (
            "_自營商_買賣超股數" in col_clean or
            col_clean.endswith("自營商買賣超股數") or
            "_自營商_淨買股數" in col_clean or
            col_clean.endswith("自營商淨買股數") or
            "_自營商 淨買股數" in col_clean or  # 有空格版本
            col_clean.endswith("自營商 淨買股數")
        )
        is_excluded = (
            "(自行買賣)" in col_clean or
            "(避險)" in col_clean or
            "外資自營商" in col_clean
        )
        if is_dealer_col and not is_excluded:
            col_dealer_net = col
            break

    if not all([code_col, name_col, col_trust_net, col_dealer_net]):
        return empty_result

    df["code"] = df[code_col].astype(str).str.strip().str.zfill(4)
    df["name"] = df[name_col].astype(str).str.strip()

    foreign_ex = numeric_series(df[col_foreign_ex_net]) if col_foreign_ex_net else 0
    foreign_self = numeric_series(df[col_foreign_self_net]) if col_foreign_self_net else 0
    trust_net = numeric_series(df[col_trust_net])
    dealer_net = numeric_series(df[col_dealer_net])

    out = pd.DataFrame({
        "date": trade_date,
        "code": df["code"],
        "name": df["name"],
        "foreign_net": (foreign_ex + foreign_self),
        "trust_net": trust_net,
        "dealer_net": dealer_net,
        "market": "TPEX",
    })

    mask = out["code"].str.match(r"^\d{4,5}[A-Z]*$")
    return out[mask].reset_index(drop=True)
