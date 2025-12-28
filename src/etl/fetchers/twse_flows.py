"""TWSE T86 - 三大法人買賣超統計資訊 fetcher."""
from datetime import date
from io import StringIO
import requests
import pandas as pd

from src.common.utils import numeric_series, normalize_columns, find_col_any
from src.common.config import settings


def fetch_twse_t86(trade_date: date) -> pd.DataFrame:
    """Fetch 三大法人買賣超統計資訊 (T86) from TWSE.

    Note: T86 endpoint uses Big5 (cp950) encoding.

    Returns:
        DataFrame with columns: date, code, name, foreign_net, trust_net, dealer_net, market
    """
    datestr = trade_date.strftime("%Y%m%d")
    url = "https://www.twse.com.tw/fund/T86"
    params = {
        "response": "csv",
        "date": datestr,
        "selectType": "ALLBUT0999",
    }

    resp = requests.get(url, params=params, timeout=settings.request_timeout)
    csv_text = resp.content.decode("cp950", errors="ignore")

    df = pd.read_csv(StringIO(csv_text), header=1)
    df = df.dropna(how="all", axis=0)
    df = df.dropna(how="all", axis=1)
    df = normalize_columns(df)

    empty_result = pd.DataFrame(
        columns=["date", "code", "name", "foreign_net", "trust_net", "dealer_net", "market"]
    )

    if df.empty or len(df.columns) == 0:
        return empty_result

    code_col = find_col_any(df, "證券代號")
    name_col = find_col_any(df, "證券名稱")
    col_foreign_ex_net = find_col_any(
        df,
        "外陸資買賣超股數(不含外資自營商)",
        "外資及陸資(不含外資自營商)買賣超股數",
        "外資及陸資買賣超股數(不含外資自營商)",
    )
    col_foreign_self_net = find_col_any(df, "外資自營商買賣超股數")
    col_trust_net = find_col_any(df, "投信買賣超股數")
    col_dealer_net = find_col_any(df, "自營商買賣超股數合計", "自營商買賣超股數")

    if not all([code_col, name_col, col_foreign_ex_net, col_trust_net, col_dealer_net]):
        return empty_result

    df["code"] = df[code_col].astype(str).str.replace("=", "").str.replace('"', "")
    df["code"] = df["code"].str.strip().str.zfill(4)
    df["name"] = df[name_col].astype(str).str.strip()

    foreign_ex = numeric_series(df[col_foreign_ex_net])
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
        "market": "TWSE",
    })

    # Filter valid stock codes
    mask = out["code"].str.match(r"^\d{4,5}[A-Z]*$")
    return out[mask].reset_index(drop=True)
