"""Fix corrupted stock names by re-fetching from TWSE/TPEX APIs."""
import logging
import requests
from sqlalchemy import text
from src.common.database import get_db_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def fetch_twse_stock_names():
    """Fetch TWSE stock names from official API."""
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?response=json"
    try:
        resp = requests.get(url, timeout=30)
        data = resp.json()
        if data.get("stat") == "OK" and data.get("data"):
            stocks = {}
            for row in data["data"]:
                code = str(row[0]).strip()
                name = str(row[1]).strip()
                if code and name:
                    stocks[code] = name
            logger.info(f"Fetched {len(stocks)} TWSE stock names")
            return stocks
    except Exception as e:
        logger.error(f"Failed to fetch TWSE stocks: {e}")
    return {}


def fetch_tpex_stock_names():
    """Fetch TPEX stock names from official API."""
    url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
    try:
        resp = requests.get(url, timeout=30)
        data = resp.json()
        stocks = {}
        for row in data:
            code = str(row.get("SecuritiesCompanyCode", "")).strip()
            name = str(row.get("CompanyName", "")).strip()
            if code and name:
                stocks[code] = name
        logger.info(f"Fetched {len(stocks)} TPEX stock names")
        return stocks
    except Exception as e:
        logger.error(f"Failed to fetch TPEX stocks: {e}")
    return {}


def fix_stock_names():
    """Update stock names in database."""
    # Fetch fresh names
    twse_stocks = fetch_twse_stock_names()
    tpex_stocks = fetch_tpex_stock_names()

    all_stocks = {**twse_stocks, **tpex_stocks}

    if not all_stocks:
        logger.error("No stock names fetched, aborting")
        return 0

    updated = 0
    with get_db_session() as session:
        for code, name in all_stocks.items():
            try:
                result = session.execute(
                    text("UPDATE stocks SET name = :name WHERE code = :code"),
                    {"code": code, "name": name}
                )
                if result.rowcount > 0:
                    updated += 1
            except Exception as e:
                logger.error(f"Failed to update {code}: {e}")

        session.commit()

    logger.info(f"Updated {updated} stock names")
    return updated


if __name__ == "__main__":
    fix_stock_names()
