"""Clean up inactive/delisted stocks in database.

This module:
1. Fetches current trading stocks from TWSE and TPEX
2. Marks stocks not currently trading as inactive
3. Removes junk records (invalid codes/names)
"""
import logging
import urllib.request
import json
from typing import Set
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def fetch_current_twse_stocks() -> Set[str]:
    """Fetch current trading stocks from TWSE."""
    try:
        url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json"
        with urllib.request.urlopen(url, timeout=30) as response:
            data = json.loads(response.read().decode('utf-8'))
            stocks = data.get('data', [])
            codes = {row[0] for row in stocks if row[0]}
            logger.info(f"Fetched {len(codes)} TWSE stocks")
            return codes
    except Exception as e:
        logger.error(f"Failed to fetch TWSE stocks: {e}")
        return set()


def fetch_current_tpex_stocks() -> Set[str]:
    """Fetch current trading stocks from TPEX."""
    try:
        url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
        with urllib.request.urlopen(url, timeout=30) as response:
            data = json.loads(response.read().decode('utf-8'))
            codes = {row.get('SecuritiesCompanyCode', '') for row in data if row.get('SecuritiesCompanyCode')}
            logger.info(f"Fetched {len(codes)} TPEX stocks")
            return codes
    except Exception as e:
        logger.error(f"Failed to fetch TPEX stocks: {e}")
        return set()


def cleanup_inactive_stocks(db):
    """Mark delisted stocks as inactive and clean up junk records.

    Args:
        db: SQLAlchemy database session
    """
    logger.info("Starting stock cleanup...")

    # Fetch current trading stocks
    current_stocks = set()
    current_stocks.update(fetch_current_twse_stocks())
    current_stocks.update(fetch_current_tpex_stocks())

    if not current_stocks:
        logger.warning("No current stocks fetched, skipping cleanup")
        return

    logger.info(f"Total current trading stocks: {len(current_stocks)}")

    # Get all stocks from database
    db_stocks = db.execute(text("""
        SELECT id, code, name, is_active
        FROM stocks
    """)).fetchall()
    logger.info(f"Total stocks in database: {len(db_stocks)}")

    # Find stocks to mark as inactive
    stocks_to_deactivate = []
    junk_records = []

    for stock in db_stocks:
        stock_id, code, name, is_active = stock.id, stock.code, stock.name, stock.is_active

        # Check for junk records (invalid codes/names)
        if name in ('nan', 'NaN', '', None) or code in ('0000', '0002', '0003', '0004', '0005'):
            junk_records.append((stock_id, code, name))
            continue

        # Check if stock is currently trading
        if is_active and code not in current_stocks:
            stocks_to_deactivate.append((stock_id, code, name))

    # Also find stocks to reactivate (if they were marked inactive but are now trading)
    stocks_to_reactivate = []
    for stock in db_stocks:
        stock_id, code, name, is_active = stock.id, stock.code, stock.name, stock.is_active
        if not is_active and code in current_stocks:
            stocks_to_reactivate.append((stock_id, code, name))

    # Mark stocks as inactive
    if stocks_to_deactivate:
        deactivate_ids = [s[0] for s in stocks_to_deactivate]
        db.execute(text("""
            UPDATE stocks SET is_active = false, updated_at = CURRENT_TIMESTAMP
            WHERE id = ANY(:ids)
        """), {"ids": deactivate_ids})
        logger.info(f"Marked {len(stocks_to_deactivate)} stocks as inactive:")
        for sid, code, name in stocks_to_deactivate[:20]:
            logger.info(f"  - {code}: {name}")
        if len(stocks_to_deactivate) > 20:
            logger.info(f"  ... and {len(stocks_to_deactivate) - 20} more")

    # Reactivate stocks if needed
    if stocks_to_reactivate:
        reactivate_ids = [s[0] for s in stocks_to_reactivate]
        db.execute(text("""
            UPDATE stocks SET is_active = true, updated_at = CURRENT_TIMESTAMP
            WHERE id = ANY(:ids)
        """), {"ids": reactivate_ids})
        logger.info(f"Reactivated {len(stocks_to_reactivate)} stocks:")
        for sid, code, name in stocks_to_reactivate:
            logger.info(f"  - {code}: {name}")

    # Delete junk records (optional - just mark as inactive for now)
    if junk_records:
        junk_ids = [s[0] for s in junk_records]
        db.execute(text("""
            UPDATE stocks SET is_active = false, updated_at = CURRENT_TIMESTAMP
            WHERE id = ANY(:ids)
        """), {"ids": junk_ids})
        logger.info(f"Marked {len(junk_records)} junk records as inactive:")
        for sid, code, name in junk_records:
            logger.info(f"  - {code}: {name}")

    db.commit()

    logger.info("Stock cleanup completed:")
    logger.info(f"  - Deactivated: {len(stocks_to_deactivate)}")
    logger.info(f"  - Reactivated: {len(stocks_to_reactivate)}")
    logger.info(f"  - Junk cleaned: {len(junk_records)}")


if __name__ == "__main__":
    from src.common.database import SessionLocal
    db = SessionLocal()
    try:
        cleanup_inactive_stocks(db)
    finally:
        db.close()
