"""Multi-threaded historical stock price backfill.

This script:
1. Fetches historical prices for all stocks with insufficient data
2. Uses concurrent execution for faster processing
3. Can be run on startup to fill gaps since last update
"""
import time
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import threading

from sqlalchemy import text
from src.common.database import SessionLocal, engine
from src.etl.fetchers.twse_prices import fetch_twse_stock_day
from src.etl.fetchers.tpex_prices import fetch_tpex_daily_quotes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Thread-local storage for DB sessions
thread_local = threading.local()


def get_thread_db():
    """Get thread-local database session."""
    if not hasattr(thread_local, "db"):
        thread_local.db = SessionLocal()
    return thread_local.db


def close_thread_db():
    """Close thread-local database session."""
    if hasattr(thread_local, "db"):
        thread_local.db.close()
        del thread_local.db


def get_all_stocks():
    """Get all active stocks."""
    db = SessionLocal()
    try:
        query = text("""
            SELECT id, code, name, market
            FROM stocks
            WHERE is_active = TRUE
            ORDER BY market, code
        """)
        return db.execute(query).fetchall()
    finally:
        db.close()


def get_stock_price_status():
    """Get price data status for all stocks."""
    db = SessionLocal()
    try:
        query = text("""
            SELECT s.id, s.code, s.market,
                   COUNT(p.id) as price_count,
                   MIN(p.trade_date) as min_date,
                   MAX(p.trade_date) as max_date
            FROM stocks s
            LEFT JOIN stock_prices p ON s.id = p.stock_id
            WHERE s.is_active = TRUE
            GROUP BY s.id, s.code, s.market
            ORDER BY price_count ASC
        """)
        return db.execute(query).fetchall()
    finally:
        db.close()


def get_stock_id_map():
    """Get mapping of stock code to stock id."""
    db = SessionLocal()
    try:
        query = text("SELECT id, code FROM stocks")
        rows = db.execute(query).fetchall()
        return {row.code: row.id for row in rows}
    finally:
        db.close()


def upsert_price_record(db, stock_id: int, row: dict):
    """Upsert a single price record."""
    try:
        query = text("""
            INSERT INTO stock_prices (stock_id, trade_date, open_price, high_price,
                                     low_price, close_price, volume, turnover)
            VALUES (:stock_id, :trade_date, :open_price, :high_price,
                    :low_price, :close_price, :volume, :turnover)
            ON CONFLICT (stock_id, trade_date) DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume,
                turnover = EXCLUDED.turnover
        """)
        db.execute(query, {
            "stock_id": stock_id,
            "trade_date": row.get("date"),
            "open_price": row.get("open_price"),
            "high_price": row.get("high_price"),
            "low_price": row.get("low_price"),
            "close_price": row.get("close_price"),
            "volume": row.get("volume"),
            "turnover": row.get("turnover"),
        })
        return True
    except Exception as e:
        logger.warning(f"Error upserting price: {e}")
        return False


def backfill_twse_stock(stock_id: int, stock_code: str, months_back: int = 12):
    """Backfill historical prices for a single TWSE stock."""
    db = get_thread_db()
    total_inserted = 0
    today = date.today()

    for m in range(months_back):
        target_date = today - relativedelta(months=m)
        try:
            df = fetch_twse_stock_day(stock_code, target_date)
            if not df.empty:
                for _, row in df.iterrows():
                    if upsert_price_record(db, stock_id, row.to_dict()):
                        total_inserted += 1
                db.commit()
        except Exception as e:
            logger.debug(f"Error fetching TWSE {stock_code} {target_date}: {e}")

        # Rate limiting - TWSE has stricter limits
        time.sleep(0.3)

    return total_inserted


def backfill_tpex_date(trade_date: date, stock_id_map: dict):
    """Backfill all TPEX stocks for a specific date."""
    db = get_thread_db()
    try:
        df = fetch_tpex_daily_quotes(trade_date)
        if df.empty:
            return 0

        count = 0
        for _, row in df.iterrows():
            code = row.get("code")
            if code in stock_id_map:
                if upsert_price_record(db, stock_id_map[code], row.to_dict()):
                    count += 1
        db.commit()
        return count
    except Exception as e:
        logger.debug(f"Error fetching TPEX for {trade_date}: {e}")
        return 0


def backfill_twse_parallel(stocks: list, months_back: int = 12, max_workers: int = 5):
    """Backfill TWSE stocks in parallel."""
    logger.info(f"Starting TWSE backfill for {len(stocks)} stocks with {max_workers} workers")

    total_records = 0
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="TWSE") as executor:
        futures = {
            executor.submit(backfill_twse_stock, s.id, s.code, months_back): s
            for s in stocks
        }

        for future in as_completed(futures):
            stock = futures[future]
            completed += 1
            try:
                count = future.result()
                total_records += count
                if count > 0:
                    logger.info(f"[{completed}/{len(stocks)}] {stock.code}: {count} records")
                else:
                    logger.debug(f"[{completed}/{len(stocks)}] {stock.code}: no new data")
            except Exception as e:
                logger.error(f"Error processing {stock.code}: {e}")
            finally:
                # Close thread-local DB session
                close_thread_db()

    return total_records


def backfill_tpex_parallel(days_back: int = 365, max_workers: int = 3):
    """Backfill TPEX by date in parallel."""
    stock_id_map = get_stock_id_map()
    today = date.today()

    # Generate trading dates (skip weekends)
    dates = []
    for d in range(days_back):
        target = today - timedelta(days=d)
        if target.weekday() < 5:  # Mon-Fri
            dates.append(target)

    logger.info(f"Starting TPEX backfill for {len(dates)} trading days with {max_workers} workers")

    total_records = 0
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="TPEX") as executor:
        futures = {
            executor.submit(backfill_tpex_date, d, stock_id_map): d
            for d in dates
        }

        for future in as_completed(futures):
            trade_date = futures[future]
            completed += 1
            try:
                count = future.result()
                total_records += count
                if count > 0 and completed % 20 == 0:
                    logger.info(f"[{completed}/{len(dates)}] TPEX progress: {total_records} total records")
            except Exception as e:
                logger.error(f"Error processing {trade_date}: {e}")
            finally:
                close_thread_db()

            # Rate limiting between batches
            time.sleep(0.2)

    return total_records


def get_last_price_date():
    """Get the most recent price date in the database."""
    db = SessionLocal()
    try:
        result = db.execute(text("SELECT MAX(trade_date) FROM stock_prices")).scalar()
        return result
    finally:
        db.close()


def fill_gap_since_last_update():
    """Fill price data gap since last update (for startup recovery)."""
    last_date = get_last_price_date()
    today = date.today()

    if last_date is None:
        logger.info("No price data found, running full backfill...")
        run_full_backfill(months_back=12)
        return

    days_gap = (today - last_date).days
    if days_gap <= 1:
        logger.info("Price data is up to date")
        return

    logger.info(f"Filling gap from {last_date} to {today} ({days_gap} days)")

    # For small gaps, just update recent data
    if days_gap <= 30:
        run_incremental_update(days_back=days_gap + 5)
    else:
        # For larger gaps, do a more thorough update
        months = (days_gap // 30) + 1
        run_full_backfill(months_back=min(months, 12))


def run_incremental_update(days_back: int = 7):
    """Run incremental update for recent days."""
    logger.info(f"Running incremental update for last {days_back} days")

    stocks = get_all_stocks()
    twse_stocks = [s for s in stocks if s.market == "TWSE"]

    # TWSE - fetch recent months for all stocks
    months = max(1, days_back // 30 + 1)
    twse_count = backfill_twse_parallel(twse_stocks, months_back=months, max_workers=8)

    # TPEX - fetch recent dates
    tpex_count = backfill_tpex_parallel(days_back=days_back, max_workers=5)

    logger.info(f"Incremental update complete: {twse_count} TWSE + {tpex_count} TPEX records")


def run_full_backfill(months_back: int = 12, twse_workers: int = 5, tpex_workers: int = 3):
    """Run full historical backfill."""
    logger.info(f"=" * 60)
    logger.info(f"Starting full historical backfill ({months_back} months)")
    logger.info(f"=" * 60)

    stocks = get_all_stocks()
    twse_stocks = [s for s in stocks if s.market == "TWSE"]
    tpex_stocks = [s for s in stocks if s.market == "TPEX"]

    logger.info(f"Found {len(twse_stocks)} TWSE and {len(tpex_stocks)} TPEX stocks")

    # Backfill TWSE (by stock)
    logger.info("\n[PHASE 1] Backfilling TWSE stocks...")
    twse_count = backfill_twse_parallel(twse_stocks, months_back=months_back, max_workers=twse_workers)
    logger.info(f"TWSE complete: {twse_count} records")

    # Backfill TPEX (by date)
    logger.info("\n[PHASE 2] Backfilling TPEX stocks...")
    tpex_days = months_back * 22  # ~22 trading days per month
    tpex_count = backfill_tpex_parallel(days_back=tpex_days, max_workers=tpex_workers)
    logger.info(f"TPEX complete: {tpex_count} records")

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info(f"Backfill complete!")
    logger.info(f"  TWSE: {twse_count} records")
    logger.info(f"  TPEX: {tpex_count} records")
    logger.info(f"  Total: {twse_count + tpex_count} records")
    logger.info("=" * 60)

    return twse_count + tpex_count


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Stock price backfill utility")
    parser.add_argument("--mode", choices=["full", "gap", "incremental"], default="gap",
                       help="Backfill mode: full (all history), gap (since last update), incremental (recent days)")
    parser.add_argument("--months", type=int, default=12, help="Months to backfill (full mode)")
    parser.add_argument("--days", type=int, default=7, help="Days to update (incremental mode)")
    parser.add_argument("--twse-workers", type=int, default=5, help="TWSE parallel workers")
    parser.add_argument("--tpex-workers", type=int, default=3, help="TPEX parallel workers")
    args = parser.parse_args()

    if args.mode == "full":
        run_full_backfill(months_back=args.months, twse_workers=args.twse_workers, tpex_workers=args.tpex_workers)
    elif args.mode == "gap":
        fill_gap_since_last_update()
    elif args.mode == "incremental":
        run_incremental_update(days_back=args.days)
