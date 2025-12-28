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
    col_foreign_ex_net = find_col_any(
        df,
        "外資及陸資(不含外資自營商)買賣超股數",
        "外資及陸資買賣超股數(不含外資自營商)",
        "外資及陸資買賣超股數",
    )
    col_foreign_self_net = find_col_any(df, "外資自營商買賣超股數")
    col_trust_net = find_col_any(df, "投信買賣超股數")
    col_dealer_net = find_col_any(df, "自營商買賣超股數合計", "自營商買賣超股數")

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
