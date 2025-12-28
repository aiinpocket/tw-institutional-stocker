"""TWSE Stock Price fetcher - 每日收盤行情."""
from datetime import date
import requests
import pandas as pd

from src.common.config import settings


def fetch_twse_stock_day_all() -> pd.DataFrame:
    """Fetch all TWSE stock daily prices using OpenAPI.

    Returns:
        DataFrame with columns: date, code, name, market, open_price, high_price,
                                low_price, close_price, volume, turnover, change_amount, transactions
    """
    url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"

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

    # Rename columns
    column_map = {
        "Code": "code",
        "Name": "name",
        "TradeVolume": "volume",
        "TradeValue": "turnover",
        "OpeningPrice": "open_price",
        "HighestPrice": "high_price",
        "LowestPrice": "low_price",
        "ClosingPrice": "close_price",
        "Change": "change_amount",
        "Transaction": "transactions",
    }
    df = df.rename(columns=column_map)

    # Convert numeric columns
    numeric_cols = ["volume", "turnover", "open_price", "high_price",
                    "low_price", "close_price", "change_amount", "transactions"]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "").str.replace("--", ""),
                errors="coerce"
            )

    # Filter valid stock codes (4-5 digits)
    df["code"] = df["code"].astype(str).str.strip()
    mask = df["code"].str.match(r"^\d{4,5}$")
    df = df[mask].copy()

    df["market"] = "TWSE"
    df["date"] = date.today()

    result_cols = ["date", "code", "name", "market", "open_price", "high_price",
                   "low_price", "close_price", "volume", "turnover", "change_amount", "transactions"]

    # Ensure all columns exist
    for col in result_cols:
        if col not in df.columns:
            df[col] = None

    return df[result_cols].reset_index(drop=True)


def fetch_twse_stock_day(stock_code: str, trade_date: date) -> pd.DataFrame:
    """Fetch historical TWSE stock prices for a specific stock.

    Args:
        stock_code: Stock code (e.g., "2330")
        trade_date: Target month's date

    Returns:
        DataFrame with daily prices for the month
    """
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
    params = {
        "response": "json",
        "date": trade_date.strftime("%Y%m%d"),
        "stockNo": stock_code,
    }

    empty_result = pd.DataFrame(columns=[
        "date", "code", "open_price", "high_price", "low_price",
        "close_price", "volume", "turnover", "change_amount", "transactions"
    ])

    try:
        resp = requests.get(url, params=params, timeout=settings.request_timeout)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return empty_result

    if data.get("stat") != "OK" or not data.get("data"):
        return empty_result

    # Fields: ["日期", "成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌價差", "成交筆數"]
    rows = []
    for row in data["data"]:
        try:
            # Parse ROC date (e.g., "114/01/02")
            date_parts = row[0].split("/")
            year = int(date_parts[0]) + 1911
            month = int(date_parts[1])
            day = int(date_parts[2])
            trade_dt = date(year, month, day)

            rows.append({
                "date": trade_dt,
                "code": stock_code,
                "volume": int(row[1].replace(",", "")),
                "turnover": int(row[2].replace(",", "")),
                "open_price": float(row[3].replace(",", "")) if row[3] != "--" else None,
                "high_price": float(row[4].replace(",", "")) if row[4] != "--" else None,
                "low_price": float(row[5].replace(",", "")) if row[5] != "--" else None,
                "close_price": float(row[6].replace(",", "")) if row[6] != "--" else None,
                "change_amount": float(row[7].replace(",", "").replace("+", "")) if row[7] not in ("--", "X") else None,
                "transactions": int(row[8].replace(",", "")),
            })
        except (ValueError, IndexError):
            continue

    if not rows:
        return empty_result

    return pd.DataFrame(rows)
