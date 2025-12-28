"""TPEX Stock Price fetcher - 上櫃股票每日收盤行情."""
from datetime import date
import requests
import pandas as pd

from src.common.utils import to_roc_date
from src.common.config import settings


def fetch_tpex_quotes() -> pd.DataFrame:
    """Fetch all TPEX stock daily prices using OpenAPI.

    Returns:
        DataFrame with columns: date, code, name, market, open_price, high_price,
                                low_price, close_price, volume, turnover, change_amount, transactions
    """
    url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"

    empty_result = pd.DataFrame(columns=[
        "date", "code", "name", "market", "open_price", "high_price",
        "low_price", "close_price", "volume", "turnover", "change_amount", "transactions"
    ])

    try:
        resp = requests.get(url, timeout=settings.request_timeout)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return empty_result

    if not data:
        return empty_result

    df = pd.DataFrame(data)

    # TPEX OpenAPI columns (Chinese names)
    column_map = {
        "SecuritiesCompanyCode": "code",
        "CompanyName": "name",
        "TradingShares": "volume",
        "TransactionAmount": "turnover",
        "Open": "open_price",
        "High": "high_price",
        "Low": "low_price",
        "Close": "close_price",
        "Change": "change_amount",
        "Transaction": "transactions",
    }

    # Try alternative column names if present
    if "SecuritiesCompanyCode" not in df.columns:
        # Alternative format
        alt_map = {
            "公司代號": "code",
            "公司名稱": "name",
            "成交股數": "volume",
            "成交金額": "turnover",
            "開盤": "open_price",
            "最高": "high_price",
            "最低": "low_price",
            "收盤": "close_price",
            "漲跌": "change_amount",
            "成交筆數": "transactions",
        }
        df = df.rename(columns=alt_map)
    else:
        df = df.rename(columns=column_map)

    # Convert numeric columns
    numeric_cols = ["volume", "turnover", "open_price", "high_price",
                    "low_price", "close_price", "change_amount", "transactions"]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "").str.replace("--", "").str.replace("－", "-"),
                errors="coerce"
            )

    # Filter valid stock codes
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.strip()
        mask = df["code"].str.match(r"^\d{4,5}$")
        df = df[mask].copy()

    df["market"] = "TPEX"
    df["date"] = date.today()

    result_cols = ["date", "code", "name", "market", "open_price", "high_price",
                   "low_price", "close_price", "volume", "turnover", "change_amount", "transactions"]

    for col in result_cols:
        if col not in df.columns:
            df[col] = None

    return df[result_cols].reset_index(drop=True)


def fetch_tpex_daily_quotes(trade_date: date) -> pd.DataFrame:
    """Fetch TPEX stock quotes for a specific date.

    Args:
        trade_date: Target trading date

    Returns:
        DataFrame with daily stock quotes
    """
    roc = to_roc_date(trade_date)
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php"
    params = {
        "l": "zh-tw",
        "o": "json",
        "d": roc,
    }

    empty_result = pd.DataFrame(columns=[
        "date", "code", "name", "market", "open_price", "high_price",
        "low_price", "close_price", "volume", "turnover"
    ])

    try:
        resp = requests.get(url, params=params, timeout=settings.request_timeout)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return empty_result

    # Handle both old (aaData) and new (tables[0]['data']) API formats
    raw_data = None
    if "aaData" in data:
        raw_data = data["aaData"]
    elif "tables" in data and len(data["tables"]) > 0 and "data" in data["tables"][0]:
        raw_data = data["tables"][0]["data"]

    if not raw_data:
        return empty_result

    # Data format depends on API version:
    # Old: [代號, 名稱, 收盤, 漲跌, 開盤, 最高, 最低, 成交股數, 成交金額(千元), ...]
    # New: [代號, 名稱, 收盤, 漲跌, 開盤, 最高, 最低, 均價, 成交股數, 成交金額(元), ...]
    # Detect format by checking if we have avg_price field (new format has more columns)
    is_new_format = len(raw_data[0]) >= 10 if raw_data else False

    rows = []
    for row in raw_data:
        try:
            code = str(row[0]).strip()
            if not code.isdigit() or len(code) < 4:
                continue

            if is_new_format:
                # New format with avg_price field
                volume_idx, turnover_idx = 8, 9
                turnover_multiplier = 1  # Already in yuan
            else:
                # Old format
                volume_idx, turnover_idx = 7, 8
                turnover_multiplier = 1000  # Was in thousands

            rows.append({
                "date": trade_date,
                "code": code,
                "name": str(row[1]).strip(),
                "market": "TPEX",
                "close_price": float(str(row[2]).replace(",", "")) if row[2] not in ("--", "") else None,
                "change_amount": float(str(row[3]).replace(",", "").replace("－", "-")) if row[3] not in ("--", "") else None,
                "open_price": float(str(row[4]).replace(",", "")) if row[4] not in ("--", "") else None,
                "high_price": float(str(row[5]).replace(",", "")) if row[5] not in ("--", "") else None,
                "low_price": float(str(row[6]).replace(",", "")) if row[6] not in ("--", "") else None,
                "volume": int(str(row[volume_idx]).replace(",", "")) if row[volume_idx] not in ("--", "") else None,
                "turnover": int(float(str(row[turnover_idx]).replace(",", "")) * turnover_multiplier) if row[turnover_idx] not in ("--", "") else None,
            })
        except (ValueError, IndexError):
            continue

    if not rows:
        return empty_result

    return pd.DataFrame(rows)
