"""TWSE 融資融券資料 fetcher."""
from datetime import date
from io import StringIO
import requests
import pandas as pd

from src.common.utils import numeric_series, normalize_columns, find_col_any
from src.common.config import settings


def fetch_twse_margin(trade_date: date) -> pd.DataFrame:
    """Fetch 融資融券餘額 from TWSE.

    API: https://www.twse.com.tw/exchangeReport/MI_MARGN

    Returns:
        DataFrame with columns: date, code, name, margin_buy, margin_sell, margin_cash_repay,
                                margin_balance, margin_limit, short_sell, short_buy,
                                short_stock_repay, short_balance, short_limit, offset, market
    """
    datestr = trade_date.strftime("%Y%m%d")
    url = "https://www.twse.com.tw/exchangeReport/MI_MARGN"
    params = {
        "response": "json",
        "date": datestr,
        "selectType": "ALL",
    }

    resp = requests.get(url, params=params, timeout=settings.request_timeout)
    data = resp.json()

    empty_result = pd.DataFrame(columns=[
        "date", "code", "name", "margin_buy", "margin_sell", "margin_cash_repay",
        "margin_balance", "margin_limit", "short_sell", "short_buy",
        "short_stock_repay", "short_balance", "short_limit", "offset", "market"
    ])

    if data.get("stat") != "OK":
        return empty_result

    # 資料在 data["data"] 中
    raw_data = data.get("data", [])
    if not raw_data:
        return empty_result

    # 欄位順序（根據 TWSE 官方文件）：
    # 股票代號, 股票名稱, 融資買進, 融資賣出, 融資現金償還, 融資前日餘額, 融資今日餘額, 融資限額,
    # 融券賣出, 融券買進, 融券現券償還, 融券前日餘額, 融券今日餘額, 融券限額, 資券互抵, 備註
    records = []
    for row in raw_data:
        if len(row) < 15:
            continue

        code = str(row[0]).strip().replace("=", "").replace('"', "")
        name = str(row[1]).strip()

        # 過濾非股票代碼
        if not code or not code[0].isdigit():
            continue

        def parse_num(val):
            if val is None or val == "" or val == "--":
                return 0
            try:
                return int(str(val).replace(",", "").replace(" ", ""))
            except (ValueError, TypeError):
                return 0

        records.append({
            "date": trade_date,
            "code": code,
            "name": name,
            "margin_buy": parse_num(row[2]),
            "margin_sell": parse_num(row[3]),
            "margin_cash_repay": parse_num(row[4]),
            "margin_balance": parse_num(row[6]),  # 今日餘額
            "margin_limit": parse_num(row[7]),
            "short_sell": parse_num(row[8]),
            "short_buy": parse_num(row[9]),
            "short_stock_repay": parse_num(row[10]),
            "short_balance": parse_num(row[12]),  # 今日餘額
            "short_limit": parse_num(row[13]),
            "offset": parse_num(row[14]),
            "market": "TWSE",
        })

    if not records:
        return empty_result

    df = pd.DataFrame(records)

    # Filter valid stock codes (4-5 digits, may have suffix letter)
    mask = df["code"].str.match(r"^\d{4,5}[A-Z]*$")
    return df[mask].reset_index(drop=True)
