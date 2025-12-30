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

from src.etl.loaders.db_loader import (
    upsert_flows,
    upsert_foreign_holdings,
    upsert_prices,
    upsert_ratios,
)
from src.etl.processors.holdings import build_estimated_holdings, build_foreign_master
from src.etl.processors.ratios import add_change_metrics


def update_etl_status(status: str, message: str, is_start: bool = False, is_end: bool = False):
    """Update ETL status in database for frontend notification."""
    from sqlalchemy import text
    try:
        with get_db_session() as session:
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
            session.commit()
    except Exception as e:
        print(f"[WARN] Failed to update ETL status: {e}")


def get_taipei_today() -> date:
    """Get current date in Taipei timezone."""
    tz = ZoneInfo("Asia/Taipei")
    return datetime.now(tz).date()


def is_weekend(d: date) -> bool:
    return d.weekday() >= 5


def get_target_trade_date() -> date:
    """Get yesterday's trading date (skip weekends)."""
    today = get_taipei_today()
    target = today - timedelta(days=1)
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


def run_etl():
    """Run the complete ETL pipeline."""
    print("=" * 60)
    print("Taiwan Institutional Stock Tracker - ETL Pipeline")
    print("=" * 60)

    # 更新狀態：開始執行
    update_etl_status("running", "資料更新中...", is_start=True)

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

    # Compute and store ratios
    print("\n[STEP 4] Computing institutional ratios...")

    # Only load recent data for ratio computation (last 180 days for memory efficiency)
    ratio_start_date = target_date - timedelta(days=180)
    print(f"  Loading data from {ratio_start_date} to {target_date}...")

    from sqlalchemy import text
    with get_db_session() as session:
        # Load flows (recent only)
        flows_query = text("""
            SELECT f.trade_date as date, s.code, s.name, s.market,
                   f.foreign_net, f.trust_net, f.dealer_net
            FROM institutional_flows f
            JOIN stocks s ON f.stock_id = s.id
            WHERE f.trade_date >= :start_date
            ORDER BY s.code, f.trade_date
        """)
        flows_result = session.execute(flows_query, {"start_date": ratio_start_date})
        flows_data = pd.DataFrame(flows_result.fetchall(), columns=[
            "date", "code", "name", "market", "foreign_net", "trust_net", "dealer_net"
        ])
        print(f"  Loaded {len(flows_data)} flow records")

        # Load foreign holdings (recent only)
        foreign_query = text("""
            SELECT h.trade_date as date, s.code, s.name, s.market,
                   h.total_shares, h.foreign_shares, h.foreign_ratio
            FROM foreign_holdings h
            JOIN stocks s ON h.stock_id = s.id
            WHERE h.trade_date >= :start_date
            ORDER BY s.code, h.trade_date
        """)
        foreign_result = session.execute(foreign_query, {"start_date": ratio_start_date})
        foreign_data = pd.DataFrame(foreign_result.fetchall(), columns=[
            "date", "code", "name", "market", "total_shares", "foreign_shares", "foreign_ratio"
        ])
        print(f"  Loaded {len(foreign_data)} foreign holding records")

    if flows_data.empty or foreign_data.empty:
        print("  [WARN] Insufficient data for ratio computation")
        update_etl_status("completed", f"資料更新完成，但無足夠資料計算比率 ({target_date})", is_end=True)
        return

    # Build foreign master with forward-fill
    foreign_master = build_foreign_master(
        foreign_data[foreign_data["market"] == "TWSE"],
        foreign_data[foreign_data["market"] == "TPEX"]
    )

    # Load baseline
    baseline = load_baseline()
    if baseline is not None:
        print(f"  Loaded {len(baseline)} baseline records")

    # Compute estimated holdings
    merged = build_estimated_holdings(flows_data, foreign_master, baseline=baseline)

    # Add change metrics
    merged = add_change_metrics(merged, windows=settings.windows)

    # Upsert ratios
    count = upsert_ratios(merged)
    print(f"  Upserted {count} ratio records to database")

    # Compute pre-calculated strategies
    print("\n[STEP 5] Computing strategy rankings...")
    try:
        from src.etl.processors.compute_strategy import run_all_computations
        with get_db_session() as session:
            run_all_computations(session)
        print("  Strategy rankings computed successfully")
    except Exception as e:
        print(f"  [WARN] Strategy computation failed: {e}")

    # 更新狀態：完成
    update_etl_status("completed", f"資料更新完成 ({target_date})", is_end=True)

    print("\n" + "=" * 60)
    print("[SUCCESS] ETL pipeline completed!")
    print("=" * 60)


if __name__ == "__main__":
    try:
        run_etl()
    except Exception as e:
        # 發生錯誤時更新狀態
        update_etl_status("error", f"更新失敗: {str(e)[:100]}", is_end=True)
        raise
