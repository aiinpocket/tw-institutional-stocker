"""Historical institutional data fetcher."""
import time
import logging
from datetime import date, timedelta
from typing import Optional
from io import StringIO
import requests
import pandas as pd

logger = logging.getLogger(__name__)

# TWSE T86 三大法人買賣超
TWSE_T86_URL = "https://www.twse.com.tw/fund/T86"
# TWSE MI_QFIIS 外資持股
TWSE_QFIIS_URL = "https://www.twse.com.tw/fund/MI_QFIIS"
# TPEX 三大法人
TPEX_INST_URL = "https://www.tpex.org.tw/web/stock/3itrade/3itrade_hedge.php"
# TPEX 外資持股
TPEX_QFII_URL = "https://www.tpex.org.tw/web/stock/exright/QFII.php"


def fetch_twse_t86_date(trade_date: date) -> Optional[pd.DataFrame]:
    """Fetch TWSE T86 institutional data for a specific date.

    Returns DataFrame with: code, foreign_net, trust_net, dealer_net
    """
    date_str = trade_date.strftime("%Y%m%d")

    try:
        resp = requests.get(
            TWSE_T86_URL,
            params={
                "response": "json",
                "date": date_str,
                "selectType": "ALLBUT0999",
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
            try:
                # [證券代號, 證券名稱, 外資買, 外資賣, 外資淨買, 投信買, 投信賣, 投信淨買, ...]
                code = row[0].strip()
                # Skip non-stock codes
                if not code.isdigit() or len(code) != 4:
                    continue

                def parse_num(s):
                    s = str(s).replace(",", "").replace(" ", "")
                    if s in ("", "--"):
                        return 0
                    return int(float(s))

                foreign_net = parse_num(row[4])
                trust_net = parse_num(row[7])
                dealer_net = parse_num(row[10]) if len(row) > 10 else 0

                records.append({
                    "date": trade_date,
                    "code": code,
                    "market": "TWSE",
                    "foreign_net": foreign_net,
                    "trust_net": trust_net,
                    "dealer_net": dealer_net,
                })
            except (ValueError, IndexError) as e:
                continue

        if not records:
            return None

        return pd.DataFrame(records)

    except Exception as e:
        logger.error(f"Error fetching TWSE T86 for {trade_date}: {e}")
        return None


def fetch_twse_qfiis_date(trade_date: date) -> Optional[pd.DataFrame]:
    """Fetch TWSE foreign holdings for a specific date.

    Returns DataFrame with: code, total_shares, foreign_shares, foreign_ratio
    """
    date_str = trade_date.strftime("%Y%m%d")

    try:
        resp = requests.get(
            TWSE_QFIIS_URL,
            params={
                "response": "json",
                "date": date_str,
                "selectType": "ALLBUT0999",
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
            try:
                code = row[0].strip()
                if not code.isdigit() or len(code) != 4:
                    continue

                def parse_num(s):
                    s = str(s).replace(",", "").replace(" ", "")
                    if s in ("", "--"):
                        return 0
                    return int(float(s))

                def parse_float(s):
                    s = str(s).replace(",", "").replace(" ", "").replace("%", "")
                    if s in ("", "--"):
                        return None
                    return float(s)

                total_shares = parse_num(row[2])
                foreign_shares = parse_num(row[4])
                foreign_ratio = parse_float(row[6])

                records.append({
                    "date": trade_date,
                    "code": code,
                    "market": "TWSE",
                    "total_shares": total_shares,
                    "foreign_shares": foreign_shares,
                    "foreign_ratio": foreign_ratio,
                })
            except (ValueError, IndexError):
                continue

        if not records:
            return None

        return pd.DataFrame(records)

    except Exception as e:
        logger.error(f"Error fetching TWSE QFIIS for {trade_date}: {e}")
        return None


def fetch_tpex_inst_date(trade_date: date) -> Optional[pd.DataFrame]:
    """Fetch TPEX institutional data for a specific date."""
    # TPEX uses ROC date
    roc_year = trade_date.year - 1911
    date_str = f"{roc_year}/{trade_date.month:02d}/{trade_date.day:02d}"

    try:
        resp = requests.get(
            TPEX_INST_URL,
            params={
                "l": "zh-tw",
                "d": date_str,
                "se": "EW",
                "t": "D",
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
                code = str(row[0]).strip()
                if not code.isdigit() or len(code) != 4:
                    continue

                def parse_num(s):
                    s = str(s).replace(",", "").replace(" ", "")
                    if s in ("", "--", "None"):
                        return 0
                    # Handle negative in parentheses
                    if s.startswith("(") and s.endswith(")"):
                        return -int(float(s[1:-1]))
                    return int(float(s))

                foreign_net = parse_num(row[4]) if len(row) > 4 else 0
                trust_net = parse_num(row[8]) if len(row) > 8 else 0
                dealer_net = parse_num(row[11]) if len(row) > 11 else 0

                records.append({
                    "date": trade_date,
                    "code": code,
                    "market": "TPEX",
                    "foreign_net": foreign_net,
                    "trust_net": trust_net,
                    "dealer_net": dealer_net,
                })
            except (ValueError, IndexError):
                continue

        if not records:
            return None

        return pd.DataFrame(records)

    except Exception as e:
        logger.error(f"Error fetching TPEX inst for {trade_date}: {e}")
        return None


def fetch_tpex_qfii_date(trade_date: date) -> Optional[pd.DataFrame]:
    """Fetch TPEX foreign holdings for a specific date."""
    roc_year = trade_date.year - 1911
    date_str = f"{roc_year}/{trade_date.month:02d}/{trade_date.day:02d}"

    try:
        resp = requests.get(
            TPEX_QFII_URL,
            params={
                "l": "zh-tw",
                "d": date_str,
                "se": "EW",
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
                code = str(row[0]).strip()
                if not code.isdigit() or len(code) != 4:
                    continue

                def parse_num(s):
                    s = str(s).replace(",", "").replace(" ", "")
                    if s in ("", "--", "None"):
                        return 0
                    return int(float(s))

                def parse_float(s):
                    s = str(s).replace(",", "").replace(" ", "").replace("%", "")
                    if s in ("", "--", "None"):
                        return None
                    return float(s)

                total_shares = parse_num(row[1]) if len(row) > 1 else 0
                foreign_shares = parse_num(row[2]) if len(row) > 2 else 0
                foreign_ratio = parse_float(row[3]) if len(row) > 3 else None

                records.append({
                    "date": trade_date,
                    "code": code,
                    "market": "TPEX",
                    "total_shares": total_shares,
                    "foreign_shares": foreign_shares,
                    "foreign_ratio": foreign_ratio,
                })
            except (ValueError, IndexError):
                continue

        if not records:
            return None

        return pd.DataFrame(records)

    except Exception as e:
        logger.error(f"Error fetching TPEX QFII for {trade_date}: {e}")
        return None


def fetch_institutional_date(trade_date: date, delay: float = 3.0) -> dict:
    """Fetch all institutional data for a specific date.

    Returns dict with 'flows' and 'holdings' DataFrames.
    """
    results = {"flows": [], "holdings": []}

    # TWSE
    twse_flows = fetch_twse_t86_date(trade_date)
    if twse_flows is not None:
        results["flows"].append(twse_flows)
    time.sleep(delay)

    twse_holdings = fetch_twse_qfiis_date(trade_date)
    if twse_holdings is not None:
        results["holdings"].append(twse_holdings)
    time.sleep(delay)

    # TPEX
    tpex_flows = fetch_tpex_inst_date(trade_date)
    if tpex_flows is not None:
        results["flows"].append(tpex_flows)
    time.sleep(delay)

    tpex_holdings = fetch_tpex_qfii_date(trade_date)
    if tpex_holdings is not None:
        results["holdings"].append(tpex_holdings)

    # Combine
    if results["flows"]:
        results["flows"] = pd.concat(results["flows"], ignore_index=True)
    else:
        results["flows"] = pd.DataFrame()

    if results["holdings"]:
        results["holdings"] = pd.concat(results["holdings"], ignore_index=True)
    else:
        results["holdings"] = pd.DataFrame()

    return results


def get_trading_dates(start_date: date, end_date: date) -> list:
    """Generate list of potential trading dates (weekdays only)."""
    dates = []
    current = start_date
    while current <= end_date:
        # Skip weekends
        if current.weekday() < 5:
            dates.append(current)
        current += timedelta(days=1)
    return dates
