"""TPEX Historical Data Backfill Script.

This script backfills TPEX institutional flows data for historical dates
that have TWSE data but no TPEX data.

Usage:
    python -m src.etl.backfill_tpex [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--batch-size N]
"""
import os
import sys
import time
import argparse
from datetime import date, timedelta
from typing import List, Optional

from sqlalchemy import text

from src.common.database import get_db_session
from src.common.utils import iter_trading_days
from src.etl.fetchers.tpex_flows import fetch_tpex_flows
from src.etl.fetchers.tpex_foreign import fetch_tpex_qfii
from src.etl.loaders.db_loader import upsert_flows, upsert_foreign_holdings


def get_dates_missing_tpex_flows(start_date: date, end_date: date) -> List[date]:
    """Find dates that have TWSE flows but no TPEX flows."""
    query = text("""
        SELECT DISTINCT f.trade_date
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE s.market = 'TWSE'
          AND f.trade_date >= :start_date
          AND f.trade_date <= :end_date
          AND f.trade_date NOT IN (
              SELECT DISTINCT f2.trade_date
              FROM institutional_flows f2
              JOIN stocks s2 ON f2.stock_id = s2.id
              WHERE s2.market = 'TPEX'
          )
        ORDER BY f.trade_date
    """)

    with get_db_session() as session:
        result = session.execute(query, {
            "start_date": start_date,
            "end_date": end_date
        })
        return [row[0] for row in result.fetchall()]


def backfill_tpex_for_date(trade_date: date) -> dict:
    """Backfill TPEX data for a single date."""
    result = {"date": trade_date, "flows": 0, "foreign": 0, "errors": []}

    # Fetch and store TPEX flows
    try:
        flows_df = fetch_tpex_flows(trade_date)
        if not flows_df.empty:
            result["flows"] = upsert_flows(flows_df)
    except Exception as e:
        result["errors"].append(f"flows: {str(e)[:100]}")

    # Fetch and store TPEX foreign holdings
    try:
        foreign_df = fetch_tpex_qfii(trade_date)
        if not foreign_df.empty:
            result["foreign"] = upsert_foreign_holdings(foreign_df)
    except Exception as e:
        result["errors"].append(f"foreign: {str(e)[:100]}")

    return result


def run_backfill(start_date: date, end_date: date, batch_size: int = 30,
                 delay_seconds: float = 1.0, find_missing: bool = True):
    """Run TPEX backfill for the specified date range."""
    print("=" * 60)
    print("TPEX Historical Data Backfill")
    print("=" * 60)
    print(f"Date range: {start_date} to {end_date}")
    print(f"Batch size: {batch_size}")
    print(f"Delay between requests: {delay_seconds}s")

    if find_missing:
        print("\n[STEP 1] Finding dates missing TPEX data...")
        dates_to_backfill = get_dates_missing_tpex_flows(start_date, end_date)
        print(f"  Found {len(dates_to_backfill)} dates missing TPEX data")
    else:
        print("\n[STEP 1] Generating trading days...")
        dates_to_backfill = iter_trading_days(start_date, end_date)
        print(f"  Generated {len(dates_to_backfill)} potential trading days")

    if not dates_to_backfill:
        print("\n[INFO] No dates to backfill!")
        return

    print(f"\n[STEP 2] Backfilling TPEX data...")
    total_flows = 0
    total_foreign = 0
    total_errors = 0

    for i, trade_date in enumerate(dates_to_backfill):
        print(f"  [{i+1}/{len(dates_to_backfill)}] {trade_date}...", end=" ", flush=True)

        result = backfill_tpex_for_date(trade_date)
        total_flows += result["flows"]
        total_foreign += result["foreign"]

        if result["errors"]:
            total_errors += len(result["errors"])
            print(f"flows={result['flows']}, foreign={result['foreign']}, ERRORS: {result['errors']}")
        else:
            print(f"flows={result['flows']}, foreign={result['foreign']}")

        # Rate limiting
        if delay_seconds > 0 and i < len(dates_to_backfill) - 1:
            time.sleep(delay_seconds)

        # Progress checkpoint every batch_size dates
        if (i + 1) % batch_size == 0:
            print(f"\n  --- Checkpoint: {i+1} dates processed, "
                  f"{total_flows} flows, {total_foreign} foreign ---\n")

    print("\n" + "=" * 60)
    print(f"[COMPLETE] Backfill finished!")
    print(f"  Total flows upserted: {total_flows}")
    print(f"  Total foreign holdings upserted: {total_foreign}")
    print(f"  Total errors: {total_errors}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Backfill TPEX historical data")
    parser.add_argument("--start-date", type=str, default="2020-01-02",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None,
                        help="End date (YYYY-MM-DD), defaults to yesterday")
    parser.add_argument("--batch-size", type=int, default=30,
                        help="Number of dates per checkpoint")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="Delay between API requests in seconds")
    parser.add_argument("--all-dates", action="store_true",
                        help="Process all dates, not just missing ones")

    args = parser.parse_args()

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date) if args.end_date else date.today() - timedelta(days=1)

    run_backfill(
        start_date=start,
        end_date=end,
        batch_size=args.batch_size,
        delay_seconds=args.delay,
        find_missing=not args.all_dates
    )


if __name__ == "__main__":
    main()
