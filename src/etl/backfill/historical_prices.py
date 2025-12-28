"""Historical stock price fetcher."""
import time
import logging
from datetime import date, datetime
from typing import Optional
import requests
import pandas as pd

logger = logging.getLogger(__name__)

# TWSE 個股月成交資訊
TWSE_STOCK_DAY_URL = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
# TPEX 個股月成交資訊
TPEX_STOCK_DAY_URL = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"


def fetch_twse_stock_month(stock_code: str, year: int, month: int) -> Optional[pd.DataFrame]:
    """Fetch TWSE stock monthly price data.

    Args:
        stock_code: Stock code (e.g., '2330')
        year: Year (e.g., 2023)
        month: Month (1-12)

    Returns:
        DataFrame with columns: date, open, high, low, close, volume, turnover, change
    """
    date_str = f"{year}{month:02d}01"

    try:
        resp = requests.get(
            TWSE_STOCK_DAY_URL,
            params={
                "response": "json",
                "date": date_str,
                "stockNo": stock_code,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("stat") != "OK" or "data" not in data:
            return None

        rows = data["data"]
        if not rows:
            return None

        records = []
        for row in rows:
            # Format: [日期, 成交股數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 漲跌價差, 成交筆數]
            try:
                # Convert ROC date to AD date
                date_parts = row[0].split("/")
                roc_year = int(date_parts[0])
                ad_year = roc_year + 1911
                trade_date = date(ad_year, int(date_parts[1]), int(date_parts[2]))

                # Parse numeric values (remove commas)
                volume = int(row[1].replace(",", "")) if row[1] != "--" else 0
                turnover = int(row[2].replace(",", "")) if row[2] != "--" else 0
                open_price = float(row[3].replace(",", "")) if row[3] != "--" else None
                high_price = float(row[4].replace(",", "")) if row[4] != "--" else None
                low_price = float(row[5].replace(",", "")) if row[5] != "--" else None
                close_price = float(row[6].replace(",", "")) if row[6] != "--" else None

                # Handle change (can have + or - or X for ex-dividend)
                change_str = row[7].replace(",", "").replace(" ", "")
                if change_str in ("--", "X0.00", ""):
                    change = 0.0
                elif change_str.startswith("X"):
                    change = float(change_str[1:]) if len(change_str) > 1 else 0.0
                else:
                    change = float(change_str)

                records.append({
                    "date": trade_date,
                    "code": stock_code,
                    "market": "TWSE",
                    "open_price": open_price,
                    "high_price": high_price,
                    "low_price": low_price,
                    "close_price": close_price,
                    "volume": volume,
                    "turnover": turnover,
                    "change_amount": change,
                })
            except (ValueError, IndexError) as e:
                logger.warning(f"Error parsing row for {stock_code}: {row}, error: {e}")
                continue

        if not records:
            return None

        df = pd.DataFrame(records)
        return df

    except Exception as e:
        logger.error(f"Error fetching TWSE {stock_code} for {year}/{month}: {e}")
        return None


def fetch_tpex_stock_month(stock_code: str, year: int, month: int) -> Optional[pd.DataFrame]:
    """Fetch TPEX stock monthly price data.

    Args:
        stock_code: Stock code
        year: Year (e.g., 2023)
        month: Month (1-12)

    Returns:
        DataFrame with price data
    """
    # TPEX uses ROC year
    roc_year = year - 1911
    date_str = f"{roc_year}/{month:02d}"

    try:
        resp = requests.get(
            TPEX_STOCK_DAY_URL,
            params={
                "l": "zh-tw",
                "d": date_str,
                "stkno": stock_code,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if not data.get("aaData"):
            return None

        rows = data["aaData"]
        records = []

        for row in rows:
            try:
                # Format: [日期, 成交股數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 漲跌價差, 成交筆數]
                date_parts = row[0].split("/")
                roc_y = int(date_parts[0])
                ad_year = roc_y + 1911
                trade_date = date(ad_year, int(date_parts[1]), int(date_parts[2]))

                volume = int(str(row[1]).replace(",", "")) if row[1] else 0
                turnover = int(str(row[2]).replace(",", "")) if row[2] else 0
                open_price = float(str(row[3]).replace(",", "")) if row[3] and row[3] != "--" else None
                high_price = float(str(row[4]).replace(",", "")) if row[4] and row[4] != "--" else None
                low_price = float(str(row[5]).replace(",", "")) if row[5] and row[5] != "--" else None
                close_price = float(str(row[6]).replace(",", "")) if row[6] and row[6] != "--" else None

                change_str = str(row[7]).replace(",", "").replace(" ", "") if row[7] else "0"
                if change_str in ("--", ""):
                    change = 0.0
                else:
                    change = float(change_str)

                records.append({
                    "date": trade_date,
                    "code": stock_code,
                    "market": "TPEX",
                    "open_price": open_price,
                    "high_price": high_price,
                    "low_price": low_price,
                    "close_price": close_price,
                    "volume": volume,
                    "turnover": turnover,
                    "change_amount": change,
                })
            except (ValueError, IndexError) as e:
                logger.warning(f"Error parsing TPEX row for {stock_code}: {row}, error: {e}")
                continue

        if not records:
            return None

        return pd.DataFrame(records)

    except Exception as e:
        logger.error(f"Error fetching TPEX {stock_code} for {year}/{month}: {e}")
        return None


def fetch_stock_history(
    stock_code: str,
    market: str,
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
    delay: float = 3.0,
) -> pd.DataFrame:
    """Fetch historical price data for a stock.

    Args:
        stock_code: Stock code
        market: 'TWSE' or 'TPEX'
        start_year: Start year
        start_month: Start month
        end_year: End year
        end_month: End month
        delay: Delay between requests in seconds

    Returns:
        Combined DataFrame with all price data
    """
    fetch_func = fetch_twse_stock_month if market == "TWSE" else fetch_tpex_stock_month

    all_data = []
    current_year = start_year
    current_month = start_month

    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        logger.info(f"Fetching {stock_code} ({market}) {current_year}/{current_month:02d}")

        df = fetch_func(stock_code, current_year, current_month)
        if df is not None and len(df) > 0:
            all_data.append(df)

        # Move to next month
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1

        time.sleep(delay)

    if not all_data:
        return pd.DataFrame()

    return pd.concat(all_data, ignore_index=True)
