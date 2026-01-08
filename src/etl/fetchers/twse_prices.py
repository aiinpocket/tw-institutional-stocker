"""TWSE Stock Price fetcher - 每日收盤行情."""
from datetime import date
import requests
import pandas as pd

from src.common.config import settings


def fetch_twse_stock_day_all() -> pd.DataFrame:
    """Fetch all TWSE stock daily prices using official www.twse.com.tw API.

    Uses the regular API instead of OpenAPI because OpenAPI updates slower
    (may lag by 1 day).

    Returns:
        DataFrame with columns: date, code, name, market, open_price, high_price,
                                low_price, close_price, volume, turnover, change_amount, transactions
    """
    # Use the regular www.twse.com.tw API which updates faster than OpenAPI
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    params = {"response": "json"}

    empty_result = pd.DataFrame(columns=[
        "date", "code", "name", "market", "open_price", "high_price",
        "low_price", "close_price", "volume", "turnover", "change_amount", "transactions"
    ])

    try:
        resp = requests.get(url, params=params, timeout=settings.request_timeout)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return empty_result

    if data.get("stat") != "OK" or not data.get("data"):
        return empty_result

    # Parse trade date from API's "date" field (format: "20260108")
    trade_date = None
    date_str = data.get("date", "")
    if len(date_str) == 8:
        try:
            trade_date = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        except (ValueError, IndexError):
            trade_date = date.today()
    else:
        trade_date = date.today()

    # Data format: [代號, 名稱, 成交股數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 漲跌價差, 成交筆數]
    rows = []
    for row in data["data"]:
        try:
            code = str(row[0]).strip()
            # Filter valid stock codes (4-5 digits)
            if not code.isdigit() or len(code) < 4 or len(code) > 5:
                continue

            rows.append({
                "date": trade_date,
                "code": code,
                "name": str(row[1]).strip(),
                "market": "TWSE",
                "volume": int(str(row[2]).replace(",", "")) if row[2] not in ("--", "") else None,
                "turnover": int(str(row[3]).replace(",", "")) if row[3] not in ("--", "") else None,
                "open_price": float(str(row[4]).replace(",", "")) if row[4] not in ("--", "") else None,
                "high_price": float(str(row[5]).replace(",", "")) if row[5] not in ("--", "") else None,
                "low_price": float(str(row[6]).replace(",", "")) if row[6] not in ("--", "") else None,
                "close_price": float(str(row[7]).replace(",", "")) if row[7] not in ("--", "") else None,
                "change_amount": float(str(row[8]).replace(",", "").replace("+", "")) if row[8] not in ("--", "X", "") else None,
                "transactions": int(str(row[9]).replace(",", "")) if row[9] not in ("--", "") else None,
            })
        except (ValueError, IndexError):
            continue

    if not rows:
        return empty_result

    return pd.DataFrame(rows)


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
