"""Historical data backfill orchestrator."""
import argparse
import logging
import time
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.common.database import get_db_session, engine
from src.common.models import Stock, InstitutionalFlow, ForeignHolding, StockPrice, InstitutionalRatio
from src.etl.backfill.historical_institutional import (
    fetch_twse_t86_date,
    fetch_twse_qfiis_date,
    fetch_tpex_inst_date,
    fetch_tpex_qfii_date,
    get_trading_dates,
)
from src.etl.backfill.historical_prices import (
    fetch_twse_stock_month,
    fetch_tpex_stock_month,
)
from src.etl.loaders.db_loader import upsert_stocks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Rate limiting
REQUEST_DELAY = 3.0  # seconds between requests


def ensure_stock_exists(session: Session, code: str, market: str, name: str = None) -> int:
    """Ensure stock exists in database, return stock_id."""
    stock = session.query(Stock).filter(Stock.code == code).first()
    if stock:
        return stock.id

    # Create new stock
    stock = Stock(
        code=code,
        name=name or code,
        market=market,
        is_active=True,
    )
    session.add(stock)
    session.flush()
    return stock.id


def backfill_institutional_flows(
    start_date: date,
    end_date: date,
    batch_size: int = 30,
) -> None:
    """Backfill institutional flows from start_date to end_date."""
    logger.info(f"Backfilling institutional flows from {start_date} to {end_date}")

    trading_dates = get_trading_dates(start_date, end_date)
    total_dates = len(trading_dates)
    processed = 0
    skipped = 0

    for trade_date in trading_dates:
        processed += 1
        logger.info(f"Processing flows {trade_date} ({processed}/{total_dates})")

        with get_db_session() as session:
            # Check if data already exists for this date
            existing = session.query(InstitutionalFlow).filter(
                InstitutionalFlow.trade_date == trade_date
            ).first()
            if existing:
                logger.info(f"  Skipping {trade_date} - data already exists")
                skipped += 1
                continue

            # Fetch TWSE
            twse_flows = fetch_twse_t86_date(trade_date)
            time.sleep(REQUEST_DELAY)

            # Fetch TPEX
            tpex_flows = fetch_tpex_inst_date(trade_date)
            time.sleep(REQUEST_DELAY)

            # Combine
            all_flows = []
            if twse_flows is not None and len(twse_flows) > 0:
                all_flows.append(twse_flows)
            if tpex_flows is not None and len(tpex_flows) > 0:
                all_flows.append(tpex_flows)

            if not all_flows:
                logger.info(f"  No data for {trade_date}")
                continue

            df = pd.concat(all_flows, ignore_index=True)
            logger.info(f"  Got {len(df)} flow records")

            # Insert to database
            for _, row in df.iterrows():
                stock_id = ensure_stock_exists(
                    session, row["code"], row["market"]
                )
                flow = InstitutionalFlow(
                    stock_id=stock_id,
                    trade_date=trade_date,
                    foreign_net=row["foreign_net"],
                    trust_net=row["trust_net"],
                    dealer_net=row["dealer_net"],
                )
                session.merge(flow)

            session.commit()
            logger.info(f"  Saved {len(df)} records")

    logger.info(f"Backfill complete. Processed: {processed}, Skipped: {skipped}")


def backfill_foreign_holdings(
    start_date: date,
    end_date: date,
) -> None:
    """Backfill foreign holdings from start_date to end_date."""
    logger.info(f"Backfilling foreign holdings from {start_date} to {end_date}")

    trading_dates = get_trading_dates(start_date, end_date)
    total_dates = len(trading_dates)
    processed = 0

    for trade_date in trading_dates:
        processed += 1
        logger.info(f"Processing holdings {trade_date} ({processed}/{total_dates})")

        with get_db_session() as session:
            # Check if data already exists
            existing = session.query(ForeignHolding).filter(
                ForeignHolding.trade_date == trade_date
            ).first()
            if existing:
                logger.info(f"  Skipping {trade_date} - data already exists")
                continue

            # Fetch TWSE
            twse_holdings = fetch_twse_qfiis_date(trade_date)
            time.sleep(REQUEST_DELAY)

            # Fetch TPEX
            tpex_holdings = fetch_tpex_qfii_date(trade_date)
            time.sleep(REQUEST_DELAY)

            all_holdings = []
            if twse_holdings is not None and len(twse_holdings) > 0:
                all_holdings.append(twse_holdings)
            if tpex_holdings is not None and len(tpex_holdings) > 0:
                all_holdings.append(tpex_holdings)

            if not all_holdings:
                logger.info(f"  No data for {trade_date}")
                continue

            df = pd.concat(all_holdings, ignore_index=True)
            logger.info(f"  Got {len(df)} holding records")

            for _, row in df.iterrows():
                stock_id = ensure_stock_exists(
                    session, row["code"], row["market"]
                )
                holding = ForeignHolding(
                    stock_id=stock_id,
                    trade_date=trade_date,
                    total_shares=row["total_shares"],
                    foreign_shares=row["foreign_shares"],
                    foreign_ratio=row["foreign_ratio"],
                )
                session.merge(holding)

            session.commit()
            logger.info(f"  Saved {len(df)} records")


def backfill_prices_for_stock(
    stock_code: str,
    market: str,
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
) -> int:
    """Backfill prices for a single stock. Returns number of records inserted."""
    fetch_func = fetch_twse_stock_month if market == "TWSE" else fetch_tpex_stock_month
    total_inserted = 0

    current_year = start_year
    current_month = start_month

    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        df = fetch_func(stock_code, current_year, current_month)

        if df is not None and len(df) > 0:
            with get_db_session() as session:
                stock_id = ensure_stock_exists(session, stock_code, market)

                for _, row in df.iterrows():
                    # Check if exists
                    existing = session.query(StockPrice).filter(
                        StockPrice.stock_id == stock_id,
                        StockPrice.trade_date == row["date"],
                    ).first()

                    if not existing:
                        price = StockPrice(
                            stock_id=stock_id,
                            trade_date=row["date"],
                            open_price=row["open_price"],
                            high_price=row["high_price"],
                            low_price=row["low_price"],
                            close_price=row["close_price"],
                            volume=row["volume"],
                            turnover=row["turnover"],
                            change_amount=row["change_amount"],
                        )
                        session.add(price)
                        total_inserted += 1

                session.commit()

        # Next month
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1

        time.sleep(REQUEST_DELAY)

    return total_inserted


def backfill_prices_all_stocks(
    start_year: int = 2008,
    start_month: int = 1,
    end_year: int = None,
    end_month: int = None,
) -> None:
    """Backfill prices for all stocks in database."""
    if end_year is None:
        end_year = datetime.now().year
    if end_month is None:
        end_month = datetime.now().month

    logger.info(f"Backfilling prices from {start_year}/{start_month} to {end_year}/{end_month}")

    with get_db_session() as session:
        stocks = session.query(Stock).filter(Stock.is_active == True).all()
        stock_list = [(s.code, s.market) for s in stocks]

    total_stocks = len(stock_list)
    logger.info(f"Found {total_stocks} stocks to backfill")

    for idx, (code, market) in enumerate(stock_list):
        logger.info(f"Processing {code} ({market}) [{idx+1}/{total_stocks}]")
        count = backfill_prices_for_stock(
            code, market, start_year, start_month, end_year, end_month
        )
        logger.info(f"  Inserted {count} price records for {code}")


def compute_institutional_ratios(
    start_date: date = None,
    end_date: date = None,
) -> None:
    """Compute institutional ratios from flows and holdings."""
    logger.info("Computing institutional ratios...")

    with get_db_session() as session:
        # Get date range
        if start_date is None:
            start_date = session.query(
                InstitutionalFlow.trade_date
            ).order_by(InstitutionalFlow.trade_date).first()
            if start_date:
                start_date = start_date[0]
            else:
                logger.warning("No flow data found")
                return

        if end_date is None:
            end_date = session.query(
                InstitutionalFlow.trade_date
            ).order_by(InstitutionalFlow.trade_date.desc()).first()
            if end_date:
                end_date = end_date[0]

        logger.info(f"Computing ratios from {start_date} to {end_date}")

        # Get all stocks
        stocks = session.query(Stock).filter(Stock.is_active == True).all()
        total_stocks = len(stocks)

        for idx, stock in enumerate(stocks):
            if idx % 100 == 0:
                logger.info(f"Processing stock {idx+1}/{total_stocks}")

            # Get flows for this stock
            flows = session.query(InstitutionalFlow).filter(
                InstitutionalFlow.stock_id == stock.id,
                InstitutionalFlow.trade_date >= start_date,
                InstitutionalFlow.trade_date <= end_date,
            ).order_by(InstitutionalFlow.trade_date).all()

            if not flows:
                continue

            # Get holdings for this stock
            holdings_map = {}
            holdings = session.query(ForeignHolding).filter(
                ForeignHolding.stock_id == stock.id,
                ForeignHolding.trade_date >= start_date,
                ForeignHolding.trade_date <= end_date,
            ).all()
            for h in holdings:
                holdings_map[h.trade_date] = h

            # Cumulative trust/dealer estimation
            cum_trust = 0
            cum_dealer = 0

            ratio_records = []

            for flow in flows:
                cum_trust += flow.trust_net or 0
                cum_dealer += flow.dealer_net or 0

                holding = holdings_map.get(flow.trade_date)
                total_shares = holding.total_shares if holding else None
                foreign_ratio = holding.foreign_ratio if holding else None

                # Estimate ratios
                if total_shares and total_shares > 0:
                    trust_ratio = (cum_trust / total_shares) * 100
                    dealer_ratio = (cum_dealer / total_shares) * 100
                    three_inst = (foreign_ratio or 0) + trust_ratio + dealer_ratio
                else:
                    trust_ratio = None
                    dealer_ratio = None
                    three_inst = None

                ratio_records.append({
                    "stock_id": stock.id,
                    "trade_date": flow.trade_date,
                    "foreign_ratio": foreign_ratio,
                    "trust_ratio_est": trust_ratio,
                    "dealer_ratio_est": dealer_ratio,
                    "three_inst_ratio_est": three_inst,
                })

            # Calculate change metrics
            for i, record in enumerate(ratio_records):
                for window in [5, 20, 60, 120]:
                    if i >= window:
                        prev = ratio_records[i - window]
                        if record["three_inst_ratio_est"] is not None and prev["three_inst_ratio_est"] is not None:
                            record[f"change_{window}d"] = record["three_inst_ratio_est"] - prev["three_inst_ratio_est"]
                        else:
                            record[f"change_{window}d"] = None
                    else:
                        record[f"change_{window}d"] = None

            # Upsert ratios
            for record in ratio_records:
                ratio = InstitutionalRatio(
                    stock_id=record["stock_id"],
                    trade_date=record["trade_date"],
                    foreign_ratio=record["foreign_ratio"],
                    trust_ratio_est=record["trust_ratio_est"],
                    dealer_ratio_est=record["dealer_ratio_est"],
                    three_inst_ratio_est=record["three_inst_ratio_est"],
                    change_5d=record.get("change_5d"),
                    change_20d=record.get("change_20d"),
                    change_60d=record.get("change_60d"),
                    change_120d=record.get("change_120d"),
                )
                session.merge(ratio)

        session.commit()

    logger.info("Ratio computation complete")


def main():
    parser = argparse.ArgumentParser(description="Historical data backfill")
    parser.add_argument(
        "--mode",
        choices=["flows", "holdings", "prices", "ratios", "all"],
        default="all",
        help="What data to backfill",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2008-01-01",
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD), defaults to today",
    )
    parser.add_argument(
        "--stock",
        type=str,
        default=None,
        help="Specific stock code to backfill (for prices only)",
    )

    args = parser.parse_args()

    start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else date.today()

    if args.mode in ("flows", "all"):
        backfill_institutional_flows(start, end)

    if args.mode in ("holdings", "all"):
        backfill_foreign_holdings(start, end)

    if args.mode in ("prices", "all"):
        if args.stock:
            # Single stock
            with get_db_session() as session:
                stock = session.query(Stock).filter(Stock.code == args.stock).first()
                if stock:
                    backfill_prices_for_stock(
                        args.stock, stock.market, start.year, start.month, end.year, end.month
                    )
                else:
                    logger.error(f"Stock {args.stock} not found")
        else:
            backfill_prices_all_stocks(start.year, start.month, end.year, end.month)

    if args.mode in ("ratios", "all"):
        compute_institutional_ratios(start, end)

    logger.info("Backfill complete!")


if __name__ == "__main__":
    main()
