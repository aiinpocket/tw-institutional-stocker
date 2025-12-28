"""Broker branch trading data fetcher using Playwright.

抓取富邦網站的券商分點交易數據。
"""
import re
import time
import atexit
from datetime import date, datetime
from typing import Optional, List

import pandas as pd

# Playwright import with fallback
try:
    from playwright.sync_api import sync_playwright, Browser
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

# Constants
BASE_URL = "https://fubon-ebrokerdj.fbs.com.tw"
BROKER_TRADING_URL = BASE_URL + "/z/zc/zco/zco_{code}.djhtm"
BROKER_HISTORY_URL = BASE_URL + "/z/zc/zco/zco0/zco0.djhtm?a={code}&b={broker_id}"

# Browser singleton
_browser: Optional[Browser] = None
_playwright = None


def _get_browser() -> Browser:
    """Get or create browser instance."""
    global _browser, _playwright
    if not HAS_PLAYWRIGHT:
        raise RuntimeError("Playwright is not installed. Run: pip install playwright && playwright install chromium")

    if _browser is None:
        _playwright = sync_playwright().start()
        _browser = _playwright.chromium.launch(headless=True)

    return _browser


def close_browser():
    """Close browser and cleanup resources."""
    global _browser, _playwright
    if _browser:
        _browser.close()
        _browser = None
    if _playwright:
        _playwright.stop()
        _playwright = None


def _parse_number(text: str) -> int:
    """Parse number from text, handling commas and parentheses."""
    if not text or text.strip() in ("", "-"):
        return 0

    text = text.strip().replace(",", "").replace(" ", "")

    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]

    try:
        return int(float(text))
    except ValueError:
        return 0


def _parse_percent(text: str) -> float:
    """Parse percentage from text."""
    if not text or text.strip() in ("", "-"):
        return 0.0

    text = text.strip().replace("%", "").replace(",", "")
    try:
        return float(text)
    except ValueError:
        return 0.0


def fetch_broker_trading(stock_code: str, target_date: Optional[str] = None) -> pd.DataFrame:
    """Fetch broker branch trading data for a specific stock.

    Args:
        stock_code: Stock code (e.g., "2330" for TSMC)
        target_date: Target date in "MM/DD" format, None for latest

    Returns:
        DataFrame with columns: date, stock_code, broker_name, broker_id,
                                buy_vol, sell_vol, net_vol, pct, rank, side
    """
    if not HAS_PLAYWRIGHT:
        raise RuntimeError("Playwright is not installed")

    browser = _get_browser()
    page = browser.new_page()

    try:
        url = BROKER_TRADING_URL.format(code=stock_code)
        page.goto(url, wait_until="networkidle", timeout=30000)
        page.wait_for_selector("table.t01", timeout=10000)

        if target_date:
            try:
                select = page.query_selector("select")
                if select:
                    options = select.query_selector_all("option")
                    for opt in options:
                        if target_date in (opt.get_attribute("value") or ""):
                            select.select_option(value=opt.get_attribute("value"))
                            page.wait_for_load_state("networkidle")
                            break
            except Exception:
                pass

        records = []
        table = page.query_selector("table.t01")
        if not table:
            return pd.DataFrame()

        rows = table.query_selector_all("tr")

        # Get displayed date
        date_text = ""
        for row in rows[:5]:
            row_text = row.inner_text()
            match = re.search(r"(\d{1,2}/\d{1,2})", row_text)
            if match:
                date_text = match.group(1)
                break

        # Find header row
        data_start_idx = 0
        for i, row in enumerate(rows):
            cells = row.query_selector_all("td")
            if len(cells) >= 10:
                cell_texts = [c.inner_text().strip() for c in cells]
                if "買超券商" in cell_texts[0] and "賣超券商" in cell_texts[5]:
                    data_start_idx = i + 1
                    break

        # Parse data rows
        rank = 0
        for row in rows[data_start_idx:]:
            cells = row.query_selector_all("td")
            if len(cells) < 10:
                continue

            rank += 1

            # Parse buy side
            buy_broker_cell = cells[0]
            buy_broker_link = buy_broker_cell.query_selector("a")
            if buy_broker_link:
                buy_broker_name = buy_broker_link.inner_text().strip()
                href = buy_broker_link.get_attribute("href") or ""
                match = re.search(r"b=([^&]+)", href)
                buy_broker_id = match.group(1) if match else ""
            else:
                buy_broker_name = buy_broker_cell.inner_text().strip()
                buy_broker_id = ""

            if buy_broker_name and buy_broker_name != "買超券商":
                records.append({
                    "date": date_text,
                    "stock_code": stock_code,
                    "broker_name": buy_broker_name,
                    "broker_id": buy_broker_id,
                    "buy_vol": _parse_number(cells[1].inner_text()),
                    "sell_vol": _parse_number(cells[2].inner_text()),
                    "net_vol": _parse_number(cells[3].inner_text()),
                    "pct": _parse_percent(cells[4].inner_text()),
                    "rank": rank,
                    "side": "buy"
                })

            # Parse sell side
            sell_broker_cell = cells[5]
            sell_broker_link = sell_broker_cell.query_selector("a")
            if sell_broker_link:
                sell_broker_name = sell_broker_link.inner_text().strip()
                href = sell_broker_link.get_attribute("href") or ""
                match = re.search(r"b=([^&]+)", href)
                sell_broker_id = match.group(1) if match else ""
            else:
                sell_broker_name = sell_broker_cell.inner_text().strip()
                sell_broker_id = ""

            if sell_broker_name and sell_broker_name != "賣超券商":
                records.append({
                    "date": date_text,
                    "stock_code": stock_code,
                    "broker_name": sell_broker_name,
                    "broker_id": sell_broker_id,
                    "buy_vol": _parse_number(cells[6].inner_text()),
                    "sell_vol": _parse_number(cells[7].inner_text()),
                    "net_vol": -abs(_parse_number(cells[8].inner_text())),
                    "pct": _parse_percent(cells[9].inner_text()),
                    "rank": rank,
                    "side": "sell"
                })

        return pd.DataFrame(records)

    finally:
        page.close()


def fetch_multiple_stocks(stock_codes: List[str], delay: float = 1.0) -> pd.DataFrame:
    """Fetch broker trading data for multiple stocks.

    Args:
        stock_codes: List of stock codes
        delay: Delay between requests in seconds

    Returns:
        Combined DataFrame with all broker trading data
    """
    all_data = []

    for code in stock_codes:
        try:
            df = fetch_broker_trading(code)
            all_data.append(df)
            time.sleep(delay)
        except Exception as e:
            print(f"Error fetching broker data for {code}: {e}")
            continue

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


# Cleanup on module unload
atexit.register(close_browser)
