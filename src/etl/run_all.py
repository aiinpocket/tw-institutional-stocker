"""Main ETL orchestrator - fetch institutional data, prices, and compute ratios.

This script:
1. Fetches TWSE/TPEX institutional flows
2. Fetches TWSE/TPEX foreign holdings
3. Fetches TWSE/TPEX stock prices (NEW)
4. Computes institutional ratios with baseline correction
5. Stores everything to PostgreSQL
"""
import os
import sys
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Optional
import pandas as pd

from src.common.config import settings
from src.common.database import get_db_session
from src.common.utils import iter_trading_days

from src.etl.fetchers.twse_flows import fetch_twse_t86
from src.etl.fetchers.twse_foreign import fetch_twse_mi_qfiis
from src.etl.fetchers.twse_prices import fetch_twse_stock_day_all
from src.etl.fetchers.tpex_flows import fetch_tpex_flows
from src.etl.fetchers.tpex_foreign import fetch_tpex_qfii
from src.etl.fetchers.tpex_prices import fetch_tpex_quotes
from src.etl.fetchers.twse_margin import fetch_twse_margin
from src.etl.fetchers.tpex_margin import fetch_tpex_margin
from src.etl.fetchers.revenue import fetch_all_revenue

from src.etl.loaders.db_loader import (
    upsert_flows,
    upsert_foreign_holdings,
    upsert_prices,
    upsert_ratios,
    upsert_margin_trading,
    upsert_revenue,
)
from src.etl.processors.holdings import build_estimated_holdings, build_foreign_master
from src.etl.processors.ratios import add_change_metrics


def update_etl_status(status: str, message: str, is_start: bool = False, is_end: bool = False):
    """Update ETL status in database for frontend notification."""
    from sqlalchemy import text
    import sys

    print(f"[ETL STATUS] Updating to: {status} - {message}", flush=True)

    try:
        with get_db_session() as session:
            # 確保 system_status 表存在
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS system_status (
                    id SERIAL PRIMARY KEY,
                    status_key VARCHAR(50) UNIQUE NOT NULL,
                    status_value VARCHAR(50) NOT NULL,
                    message TEXT,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))

            if is_start:
                query = text("""
                    INSERT INTO system_status (status_key, status_value, message, started_at, updated_at)
                    VALUES ('etl_status', :status, :message, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (status_key) DO UPDATE SET
                        status_value = :status,
                        message = :message,
                        started_at = CURRENT_TIMESTAMP,
                        completed_at = NULL,
                        updated_at = CURRENT_TIMESTAMP
                """)
            elif is_end:
                query = text("""
                    UPDATE system_status SET
                        status_value = :status,
                        message = :message,
                        completed_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE status_key = 'etl_status'
                """)
            else:
                query = text("""
                    UPDATE system_status SET
                        status_value = :status,
                        message = :message,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE status_key = 'etl_status'
                """)
            session.execute(query, {"status": status, "message": message})
            # Note: get_db_session context manager will commit
        print(f"[ETL STATUS] Successfully updated to: {status}", flush=True)
    except Exception as e:
        print(f"[WARN] Failed to update ETL status: {e}", flush=True)
        import traceback
        traceback.print_exc()
        sys.stdout.flush()


def get_taipei_today() -> date:
    """Get current date in Taipei timezone."""
    tz = ZoneInfo("Asia/Taipei")
    return datetime.now(tz).date()


def is_weekend(d: date) -> bool:
    return d.weekday() >= 5


def get_target_trade_date() -> date:
    """Get today's date as target (skip weekends).

    ETL will fetch data from (last_db_date + 1) to target_date.
    If exchange has no data yet (holiday or not published), fetch returns empty.
    """
    today = get_taipei_today()
    # If today is weekend, use last Friday
    target = today
    while is_weekend(target):
        target -= timedelta(days=1)
    return target


def get_last_date_from_db(table_name: str) -> Optional[date]:
    """Get the most recent date from a database table."""
    from sqlalchemy import text
    query = text(f"SELECT MAX(trade_date) FROM {table_name}")
    with get_db_session() as session:
        result = session.execute(query).scalar()
        return result


def fetch_flows_for_date(trade_date: date) -> pd.DataFrame:
    """Fetch institutional flows for a single date from both exchanges."""
    all_flows = []

    print(f"  Fetching TWSE T86 for {trade_date}...")
    try:
        twse_df = fetch_twse_t86(trade_date)
        if not twse_df.empty:
            all_flows.append(twse_df)
            print(f"    Got {len(twse_df)} TWSE records")
    except Exception as e:
        print(f"    [WARN] TWSE T86 failed: {e}")

    print(f"  Fetching TPEX flows for {trade_date}...")
    try:
        tpex_df = fetch_tpex_flows(trade_date)
        if not tpex_df.empty:
            all_flows.append(tpex_df)
            print(f"    Got {len(tpex_df)} TPEX records")
    except Exception as e:
        print(f"    [WARN] TPEX flows failed: {e}")

    if all_flows:
        return pd.concat(all_flows, ignore_index=True)
    return pd.DataFrame()


def fetch_foreign_for_date(trade_date: date) -> pd.DataFrame:
    """Fetch foreign holdings for a single date from both exchanges."""
    all_foreign = []

    print(f"  Fetching TWSE MI_QFIIS for {trade_date}...")
    try:
        twse_df = fetch_twse_mi_qfiis(trade_date)
        if not twse_df.empty:
            all_foreign.append(twse_df)
            print(f"    Got {len(twse_df)} TWSE records")
    except Exception as e:
        print(f"    [WARN] TWSE MI_QFIIS failed: {e}")

    print(f"  Fetching TPEX QFII for {trade_date}...")
    try:
        tpex_df = fetch_tpex_qfii(trade_date)
        if not tpex_df.empty:
            all_foreign.append(tpex_df)
            print(f"    Got {len(tpex_df)} TPEX records")
    except Exception as e:
        print(f"    [WARN] TPEX QFII failed: {e}")

    if all_foreign:
        return pd.concat(all_foreign, ignore_index=True)
    return pd.DataFrame()


def fetch_prices_for_today() -> pd.DataFrame:
    """Fetch today's stock prices from both exchanges."""
    all_prices = []

    print("  Fetching TWSE stock prices...")
    try:
        twse_df = fetch_twse_stock_day_all()
        if not twse_df.empty:
            all_prices.append(twse_df)
            print(f"    Got {len(twse_df)} TWSE records")
    except Exception as e:
        print(f"    [WARN] TWSE prices failed: {e}")

    print("  Fetching TPEX stock prices...")
    try:
        tpex_df = fetch_tpex_quotes()
        if not tpex_df.empty:
            all_prices.append(tpex_df)
            print(f"    Got {len(tpex_df)} TPEX records")
    except Exception as e:
        print(f"    [WARN] TPEX prices failed: {e}")

    if all_prices:
        return pd.concat(all_prices, ignore_index=True)
    return pd.DataFrame()


def load_baseline() -> Optional[pd.DataFrame]:
    """Load baseline calibration data if available."""
    baseline_path = os.path.join("data", "inst_baseline.csv")
    if os.path.exists(baseline_path):
        try:
            df = pd.read_csv(baseline_path, comment="#")
            if not df.empty:
                return df
        except Exception as e:
            print(f"[WARN] Failed to load baseline: {e}")
    return None


def clear_all_caches():
    """Clear all cache tables at the start of ETL."""
    from sqlalchemy import text
    print("\n[STEP 0] Clearing all cache tables...")

    cache_tables = [
        "strategy_rankings",
        "stock_technicals",
        "daily_summary_cache",
        "ai_analysis_cache",
    ]

    for table_name in cache_tables:
        try:
            with get_db_session() as session:
                # 先確保表存在
                session.execute(text(f"DELETE FROM {table_name}"))
            print(f"  Cleared {table_name}")
        except Exception as e:
            # 表可能不存在，這是正常的
            print(f"  [SKIP] {table_name}: {str(e)[:50]}")


def force_refetch_flows(days: int = 30):
    """Force re-fetch by clearing recent flow data."""
    from sqlalchemy import text
    print(f"\n[FORCE REFETCH] Clearing last {days} days of institutional flows...")
    try:
        with get_db_session() as session:
            # 刪除最近 N 天的 flows 資料
            result = session.execute(text(f"""
                DELETE FROM institutional_flows
                WHERE trade_date >= CURRENT_DATE - {days}
            """))
            deleted = result.rowcount
            print(f"  Deleted {deleted} flow records")
    except Exception as e:
        print(f"  [WARN] Failed to clear flows: {e}")


def force_clear_prices(days: int = 30):
    """Force clear recent stock price data to allow re-fetch with correct dates."""
    from sqlalchemy import text
    print(f"\n[FORCE CLEAR PRICES] Clearing last {days} days of stock prices...")
    try:
        with get_db_session() as session:
            result = session.execute(text(f"""
                DELETE FROM stock_prices
                WHERE trade_date >= CURRENT_DATE - {days}
            """))
            deleted = result.rowcount
            print(f"  Deleted {deleted} price records")
    except Exception as e:
        print(f"  [WARN] Failed to clear prices: {e}")


def verify_etl_results(target_date: date) -> list:
    """Verify ETL results after completion.

    Returns list of error messages. Empty list means all checks passed.
    """
    from sqlalchemy import text
    errors = []

    with get_db_session() as session:
        # Check 1: Price data for target date
        price_count = session.execute(text("""
            SELECT COUNT(*) FROM stock_prices WHERE trade_date = :date
        """), {"date": target_date}).scalar()

        if price_count == 0:
            errors.append(f"No price data for {target_date}")
        else:
            print(f"    Price records: {price_count}")

        # Check 2: Flow data for target date
        flow_count = session.execute(text("""
            SELECT COUNT(*) FROM institutional_flows WHERE trade_date = :date
        """), {"date": target_date}).scalar()

        if flow_count == 0:
            errors.append(f"No flow data for {target_date}")
        else:
            print(f"    Flow records: {flow_count}")

        # Check 3: Margin trading data (should have data on trading days)
        margin_count = session.execute(text("""
            SELECT COUNT(*) FROM margin_trading WHERE trade_date = :date
        """), {"date": target_date}).scalar()

        # Only warn if margin table exists but has no data (not critical)
        print(f"    Margin records: {margin_count}")
        if margin_count == 0:
            errors.append(f"No margin data for {target_date} (check API availability)")

        # Check 4: Revenue data (only if after 10th of month)
        # Check for either previous month or 2 months ago (whichever is available)
        today = get_taipei_today()
        if today.day >= 10:
            months_to_check = []
            if today.month == 1:
                months_to_check.append((today.year - 1, 12))
                months_to_check.append((today.year - 1, 11))
            elif today.month == 2:
                months_to_check.append((today.year, 1))
                months_to_check.append((today.year - 1, 12))
            else:
                months_to_check.append((today.year, today.month - 1))
                months_to_check.append((today.year, today.month - 2))

            has_revenue = False
            for rev_year, rev_month in months_to_check:
                revenue_count = session.execute(text("""
                    SELECT COUNT(*) FROM monthly_revenue WHERE year = :year AND month = :month
                """), {"year": rev_year, "month": rev_month}).scalar()
                print(f"    Revenue records ({rev_year}/{rev_month}): {revenue_count}")
                if revenue_count > 0:
                    has_revenue = True
                    break

            if not has_revenue:
                errors.append(f"No revenue data for recent months")

        # Check 5: Strategy rankings exist
        ranking_count = session.execute(text("""
            SELECT COUNT(*) FROM strategy_rankings
        """)).scalar()

        if ranking_count == 0:
            errors.append("No strategy rankings computed")
        else:
            print(f"    Strategy rankings: {ranking_count}")

        # Check 6: Verify some key stocks have data (2330, 2317)
        key_stocks = ['2330', '2317']
        for code in key_stocks:
            stock_price = session.execute(text("""
                SELECT COUNT(*) FROM stock_prices sp
                JOIN stocks s ON sp.stock_id = s.id
                WHERE s.code = :code AND sp.trade_date = :date
            """), {"code": code, "date": target_date}).scalar()

            if stock_price == 0:
                errors.append(f"Key stock {code} has no price data for {target_date}")

    return errors


def run_tpex_backfill():
    """Run TPEX historical data backfill."""
    from src.etl.backfill_tpex import run_backfill

    start_str = os.environ.get("BACKFILL_TPEX_START", "2020-01-02")
    end_str = os.environ.get("BACKFILL_TPEX_END")

    start_date = date.fromisoformat(start_str)
    end_date = date.fromisoformat(end_str) if end_str else get_target_trade_date()

    print("=" * 60)
    print("TPEX Historical Data Backfill Mode")
    print("=" * 60)

    run_backfill(
        start_date=start_date,
        end_date=end_date,
        batch_size=30,
        delay_seconds=1.0,
        find_missing=True
    )


def run_etl():
    """Run the complete ETL pipeline."""
    # 檢查是否為 TPEX 回補模式
    if os.environ.get("BACKFILL_TPEX_START"):
        run_tpex_backfill()
        return

    print("=" * 60)
    print("Taiwan Institutional Stock Tracker - ETL Pipeline")
    print("=" * 60)

    # 檢查是否需要修復股票名稱
    if os.environ.get("FIX_STOCK_NAMES"):
        print("\n[INFO] Fixing corrupted stock names...")
        from src.etl.fix_stock_names import fix_stock_names
        fix_stock_names()
        print("[INFO] Stock names fixed\n")

    # 檢查是否需要強制重新抓取
    if os.environ.get("FORCE_REFETCH_DAYS"):
        days = int(os.environ.get("FORCE_REFETCH_DAYS", "30"))
        force_refetch_flows(days)

    # 檢查是否需要清除錯誤的股價資料
    if os.environ.get("FORCE_CLEAR_PRICES_DAYS"):
        days = int(os.environ.get("FORCE_CLEAR_PRICES_DAYS", "30"))
        force_clear_prices(days)

    # 更新狀態：開始執行
    update_etl_status("running", "資料更新中...", is_start=True)

    # 清除所有快取
    clear_all_caches()

    # 確保所有資料表存在
    print("\n[STEP 0.4] Ensuring all database tables exist...")
    try:
        from src.common.database import create_tables
        create_tables()
        print("  Database tables verified")
    except Exception as e:
        print(f"  [WARN] Table creation failed: {e}")

    # 建立資料庫索引 (如果不存在)
    print("\n[STEP 0.5] Ensuring database indexes exist...")
    try:
        from src.etl.create_indexes import create_indexes
        create_indexes()
    except Exception as e:
        print(f"  [WARN] Index creation failed: {e}")

    # 清理下市/失效股票
    print("\n[STEP 0.6] Cleaning up inactive stocks...")
    try:
        from src.etl.processors.cleanup_stocks import cleanup_inactive_stocks
        with get_db_session() as session:
            cleanup_inactive_stocks(session)
    except Exception as e:
        print(f"  [WARN] Stock cleanup failed: {e}")

    target_date = get_target_trade_date()
    print(f"\n[INFO] Target trade date: {target_date}")

    # Determine date range to fetch
    last_flow_date = get_last_date_from_db("institutional_flows")
    last_foreign_date = get_last_date_from_db("foreign_holdings")
    last_price_date = get_last_date_from_db("stock_prices")

    def calc_start(last_date: Optional[date]) -> date:
        if last_date is None:
            # If no data, start from 60 days ago
            start = target_date - timedelta(days=60)
            while is_weekend(start):
                start += timedelta(days=1)
            return start
        return last_date + timedelta(days=1)

    start_flows = calc_start(last_flow_date)
    start_foreign = calc_start(last_foreign_date)

    print(f"[INFO] Flows update range: {start_flows} -> {target_date}")
    print(f"[INFO] Foreign update range: {start_foreign} -> {target_date}")
    print(f"[INFO] Last price date: {last_price_date}")

    # Fetch and store flows
    print("\n[STEP 1] Fetching institutional flows...")
    all_flows = []
    for d in iter_trading_days(start_flows, target_date):
        df = fetch_flows_for_date(d)
        if not df.empty:
            all_flows.append(df)

    if all_flows:
        flows_df = pd.concat(all_flows, ignore_index=True)
        count = upsert_flows(flows_df)
        print(f"  Upserted {count} flow records to database")
    else:
        print("  No new flows to upsert")

    # Fetch and store foreign holdings
    print("\n[STEP 2] Fetching foreign holdings...")
    all_foreign = []
    for d in iter_trading_days(start_foreign, target_date):
        df = fetch_foreign_for_date(d)
        if not df.empty:
            all_foreign.append(df)

    if all_foreign:
        foreign_df = pd.concat(all_foreign, ignore_index=True)
        count = upsert_foreign_holdings(foreign_df)
        print(f"  Upserted {count} foreign holding records to database")
    else:
        print("  No new foreign holdings to upsert")

    # Fetch and store prices
    print("\n[STEP 3] Fetching stock prices...")
    prices_df = fetch_prices_for_today()
    if not prices_df.empty:
        count = upsert_prices(prices_df)
        print(f"  Upserted {count} price records to database")
    else:
        print("  No prices to upsert")

    # Fetch and store margin trading data
    print("\n[STEP 3.5] Fetching margin trading data...")
    try:
        all_margin = []
        print(f"  Fetching TWSE margin for {target_date}...")
        twse_margin = fetch_twse_margin(target_date)
        if not twse_margin.empty:
            all_margin.append(twse_margin)
            print(f"    Got {len(twse_margin)} TWSE records")

        print(f"  Fetching TPEX margin for {target_date}...")
        tpex_margin = fetch_tpex_margin(target_date)
        if not tpex_margin.empty:
            all_margin.append(tpex_margin)
            print(f"    Got {len(tpex_margin)} TPEX records")

        if all_margin:
            margin_df = pd.concat(all_margin, ignore_index=True)
            count = upsert_margin_trading(margin_df)
            print(f"  Upserted {count} margin trading records to database")
        else:
            print("  No margin data to upsert")
    except Exception as e:
        print(f"  [WARN] Margin trading fetch failed: {e}")

    # Fetch and store monthly revenue (only on specific days or when needed)
    print("\n[STEP 3.6] Checking for monthly revenue updates...")
    try:
        today = get_taipei_today()
        # Fetch revenue if it's after the 10th of the month (revenue data is usually available)
        if today.day >= 10:
            from sqlalchemy import text

            # Try to fetch previous month's revenue first, then fallback to 2 months ago
            # This handles cases where the latest month's data isn't published yet
            months_to_try = []

            # Previous month
            if today.month == 1:
                months_to_try.append((today.year - 1, 12))
            else:
                months_to_try.append((today.year, today.month - 1))

            # Two months ago (fallback)
            if today.month <= 2:
                months_to_try.append((today.year - 1, 12 + today.month - 2))
            else:
                months_to_try.append((today.year, today.month - 2))

            for rev_year, rev_month in months_to_try:
                # Check if we already have this month's revenue
                with get_db_session() as session:
                    existing = session.execute(text("""
                        SELECT COUNT(*) FROM monthly_revenue
                        WHERE year = :year AND month = :month
                    """), {"year": rev_year, "month": rev_month}).scalar()

                if existing == 0:
                    print(f"  Fetching revenue for {rev_year}/{rev_month}...")
                    revenue_df = fetch_all_revenue(rev_year, rev_month)
                    if not revenue_df.empty:
                        count = upsert_revenue(revenue_df)
                        print(f"  Upserted {count} revenue records to database")
                        break  # Stop after successfully fetching one month
                    else:
                        print(f"  No revenue data available for {rev_year}/{rev_month}, trying earlier month...")
                else:
                    print(f"  Revenue for {rev_year}/{rev_month} already exists ({existing} records)")
                    break  # Data exists, no need to try earlier months
        else:
            print("  Skipping revenue fetch (data not yet available)")
    except Exception as e:
        print(f"  [WARN] Revenue fetch failed: {e}")

    # Compute and store ratios - now using pure PostgreSQL for speed
    print("\n[STEP 4] Computing institutional ratios (PostgreSQL mode)...")
    try:
        from src.etl.processors.compute_ratios_sql import compute_ratios_in_postgresql
        with get_db_session() as session:
            count = compute_ratios_in_postgresql(session, lookback_days=180)
        print(f"  Computed {count} ratio records in PostgreSQL")
    except Exception as e:
        print(f"  [WARN] PostgreSQL ratio computation failed: {e}")
        import traceback
        traceback.print_exc()

    # Compute pre-calculated strategies
    print("\n[STEP 5] Computing strategy rankings...")
    try:
        from src.etl.processors.compute_strategy import run_all_computations
        with get_db_session() as session:
            run_all_computations(session)
        print("  Strategy rankings computed successfully")
    except Exception as e:
        print(f"  [WARN] Strategy computation failed: {e}")

    # Compute daily summary cache
    print("\n[STEP 6] Computing daily summary cache...")
    try:
        from src.etl.processors.compute_daily_summary import compute_daily_summary
        with get_db_session() as session:
            compute_daily_summary(session)
        print("  Daily summary cached successfully")
    except Exception as e:
        print(f"  [WARN] Daily summary computation failed: {e}")

    # Pre-compute AI analysis
    print("\n[STEP 7] Pre-computing AI analysis...")
    try:
        from src.etl.processors.precompute_ai import run_precompute_ai
        with get_db_session() as session:
            run_precompute_ai(session)
        print("  AI analysis pre-computed successfully")
    except Exception as e:
        print(f"  [WARN] AI pre-computation failed: {e}")

    # Post-ETL verification
    print("\n[STEP 8] Verifying ETL results...")
    verification_errors = verify_etl_results(target_date)
    if verification_errors:
        print("  [WARN] Verification issues found:")
        for err in verification_errors:
            print(f"    - {err}")
    else:
        print("  All verifications passed!")

    # 更新狀態：完成
    update_etl_status("completed", f"資料更新完成 ({target_date})", is_end=True)

    print("\n" + "=" * 60)
    print("[SUCCESS] ETL pipeline completed!")
    print("=" * 60)


if __name__ == "__main__":
    etl_started = False
    etl_error = None
    try:
        # Check if this is a backfill job (doesn't need status tracking)
        if os.environ.get("BACKFILL_TPEX_START"):
            run_tpex_backfill()
        else:
            etl_started = True
            run_etl()
    except Exception as e:
        etl_error = e
        raise
    finally:
        # Always ensure status is updated at the end
        if etl_started:
            try:
                if etl_error:
                    update_etl_status("error", f"更新失敗: {str(etl_error)[:100]}", is_end=True)
                else:
                    # Double-check status is set to completed
                    update_etl_status("completed", "資料更新完成", is_end=True)
            except Exception as status_err:
                print(f"[CRITICAL] Failed to update final ETL status: {status_err}")
