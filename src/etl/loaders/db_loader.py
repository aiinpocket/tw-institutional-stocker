"""Database loader functions - upsert data to PostgreSQL."""
from datetime import date
from typing import Dict, Optional
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.common.database import get_db_session
from src.common.models import (
    Stock, InstitutionalFlow, ForeignHolding, StockPrice,
    InstitutionalRatio, BrokerTrade, InstitutionalBaseline
)


def get_or_create_stock(session: Session, code: str, name: str, market: str, total_shares: Optional[int] = None) -> Stock:
    """Get existing stock or create new one."""
    stock = session.query(Stock).filter_by(code=code).first()
    if stock:
        # Update name and total_shares if changed
        if name and stock.name != name:
            stock.name = name
        if total_shares and stock.total_shares != total_shares:
            stock.total_shares = total_shares
        return stock

    stock = Stock(code=code, name=name, market=market, total_shares=total_shares)
    session.add(stock)
    session.flush()
    return stock


def upsert_stocks(df: pd.DataFrame) -> int:
    """Upsert stocks from DataFrame.

    Expected columns: code, name, market, total_shares (optional)

    Returns:
        Number of stocks upserted
    """
    if df.empty:
        return 0

    count = 0
    with get_db_session() as session:
        for _, row in df.iterrows():
            code = str(row["code"]).strip()
            name = str(row.get("name", "")).strip()
            market = str(row.get("market", "TWSE")).strip()
            total_shares = int(row["total_shares"]) if pd.notna(row.get("total_shares")) else None

            stmt = insert(Stock).values(
                code=code,
                name=name,
                market=market,
                total_shares=total_shares
            ).on_conflict_do_update(
                index_elements=["code"],
                set_=dict(name=name, market=market, total_shares=total_shares)
            )
            session.execute(stmt)
            count += 1

    return count


def upsert_flows(df: pd.DataFrame) -> int:
    """Upsert institutional flows from DataFrame.

    Expected columns: date, code, foreign_net, trust_net, dealer_net, market (optional), name (optional)

    Returns:
        Number of flows upserted
    """
    if df.empty:
        return 0

    count = 0
    with get_db_session() as session:
        # Build stock code to id mapping
        stock_map: Dict[str, int] = {}

        for _, row in df.iterrows():
            code = str(row["code"]).strip()
            trade_date = row["date"] if isinstance(row["date"], date) else pd.to_datetime(row["date"]).date()

            if code not in stock_map:
                name = str(row.get("name", "")).strip() or code
                market = str(row.get("market", "TWSE")).strip()
                stock = get_or_create_stock(session, code, name, market)
                stock_map[code] = stock.id

            stock_id = stock_map[code]

            stmt = insert(InstitutionalFlow).values(
                stock_id=stock_id,
                trade_date=trade_date,
                foreign_net=int(row.get("foreign_net", 0) or 0),
                trust_net=int(row.get("trust_net", 0) or 0),
                dealer_net=int(row.get("dealer_net", 0) or 0),
            ).on_conflict_do_update(
                index_elements=["stock_id", "trade_date"],
                set_=dict(
                    foreign_net=int(row.get("foreign_net", 0) or 0),
                    trust_net=int(row.get("trust_net", 0) or 0),
                    dealer_net=int(row.get("dealer_net", 0) or 0),
                )
            )
            session.execute(stmt)
            count += 1

    return count


def upsert_foreign_holdings(df: pd.DataFrame) -> int:
    """Upsert foreign holdings from DataFrame.

    Expected columns: date, code, total_shares, foreign_shares, foreign_ratio, market (optional), name (optional)

    Returns:
        Number of holdings upserted
    """
    if df.empty:
        return 0

    count = 0
    with get_db_session() as session:
        stock_map: Dict[str, int] = {}

        for _, row in df.iterrows():
            code = str(row["code"]).strip()
            trade_date = row["date"] if isinstance(row["date"], date) else pd.to_datetime(row["date"]).date()

            if code not in stock_map:
                name = str(row.get("name", "")).strip() or code
                market = str(row.get("market", "TWSE")).strip()
                total_shares = int(row["total_shares"]) if pd.notna(row.get("total_shares")) else None
                stock = get_or_create_stock(session, code, name, market, total_shares)
                stock_map[code] = stock.id

            stock_id = stock_map[code]

            # Handle NaN values properly
            total_shares = int(row["total_shares"]) if pd.notna(row.get("total_shares")) else 0
            foreign_shares = int(row["foreign_shares"]) if pd.notna(row.get("foreign_shares")) else 0
            foreign_ratio = float(row["foreign_ratio"]) if pd.notna(row.get("foreign_ratio")) else 0.0

            stmt = insert(ForeignHolding).values(
                stock_id=stock_id,
                trade_date=trade_date,
                total_shares=total_shares,
                foreign_shares=foreign_shares,
                foreign_ratio=foreign_ratio,
            ).on_conflict_do_update(
                index_elements=["stock_id", "trade_date"],
                set_=dict(
                    total_shares=total_shares,
                    foreign_shares=foreign_shares,
                    foreign_ratio=foreign_ratio,
                )
            )
            session.execute(stmt)
            count += 1

    return count


def upsert_prices(df: pd.DataFrame) -> int:
    """Upsert stock prices from DataFrame.

    Expected columns: date, code, open_price, high_price, low_price, close_price, volume, turnover, ...

    Returns:
        Number of prices upserted
    """
    if df.empty:
        return 0

    count = 0
    with get_db_session() as session:
        stock_map: Dict[str, int] = {}

        for _, row in df.iterrows():
            code = str(row["code"]).strip()
            trade_date = row["date"] if isinstance(row["date"], date) else pd.to_datetime(row["date"]).date()

            if code not in stock_map:
                name = str(row.get("name", "")).strip() or code
                market = str(row.get("market", "TWSE")).strip()
                stock = get_or_create_stock(session, code, name, market)
                stock_map[code] = stock.id

            stock_id = stock_map[code]

            def safe_float(val):
                if pd.isna(val):
                    return None
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return None

            def safe_int(val):
                if pd.isna(val):
                    return None
                try:
                    return int(val)
                except (ValueError, TypeError):
                    return None

            stmt = insert(StockPrice).values(
                stock_id=stock_id,
                trade_date=trade_date,
                open_price=safe_float(row.get("open_price")),
                high_price=safe_float(row.get("high_price")),
                low_price=safe_float(row.get("low_price")),
                close_price=safe_float(row.get("close_price")),
                volume=safe_int(row.get("volume")),
                turnover=safe_int(row.get("turnover")),
                change_amount=safe_float(row.get("change_amount")),
                change_percent=safe_float(row.get("change_percent")),
                transactions=safe_int(row.get("transactions")),
            ).on_conflict_do_update(
                index_elements=["stock_id", "trade_date"],
                set_=dict(
                    open_price=safe_float(row.get("open_price")),
                    high_price=safe_float(row.get("high_price")),
                    low_price=safe_float(row.get("low_price")),
                    close_price=safe_float(row.get("close_price")),
                    volume=safe_int(row.get("volume")),
                    turnover=safe_int(row.get("turnover")),
                    change_amount=safe_float(row.get("change_amount")),
                    change_percent=safe_float(row.get("change_percent")),
                    transactions=safe_int(row.get("transactions")),
                )
            )
            session.execute(stmt)
            count += 1

    return count


def upsert_ratios(df: pd.DataFrame) -> int:
    """Upsert institutional ratios from DataFrame.

    Returns:
        Number of ratios upserted
    """
    if df.empty:
        return 0

    count = 0
    with get_db_session() as session:
        stock_map: Dict[str, int] = {}

        for _, row in df.iterrows():
            code = str(row["code"]).strip()
            trade_date = row["date"] if isinstance(row["date"], date) else pd.to_datetime(row["date"]).date()

            if code not in stock_map:
                stock = session.query(Stock).filter_by(code=code).first()
                if not stock:
                    continue
                stock_map[code] = stock.id

            stock_id = stock_map[code]

            def safe_float(val, default=None):
                if pd.isna(val):
                    return default
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return default

            def safe_int(val, default=None):
                if pd.isna(val):
                    return default
                try:
                    return int(val)
                except (ValueError, TypeError):
                    return default

            stmt = insert(InstitutionalRatio).values(
                stock_id=stock_id,
                trade_date=trade_date,
                foreign_ratio=safe_float(row.get("foreign_ratio")),
                trust_ratio_est=safe_float(row.get("trust_ratio_est")),
                dealer_ratio_est=safe_float(row.get("dealer_ratio_est")),
                three_inst_ratio_est=safe_float(row.get("three_inst_ratio_est")),
                trust_shares_est=safe_int(row.get("trust_shares_est")),
                dealer_shares_est=safe_int(row.get("dealer_shares_est")),
                change_5d=safe_float(row.get("three_inst_ratio_change_5")),
                change_20d=safe_float(row.get("three_inst_ratio_change_20")),
                change_60d=safe_float(row.get("three_inst_ratio_change_60")),
                change_120d=safe_float(row.get("three_inst_ratio_change_120")),
            ).on_conflict_do_update(
                index_elements=["stock_id", "trade_date"],
                set_=dict(
                    foreign_ratio=safe_float(row.get("foreign_ratio")),
                    trust_ratio_est=safe_float(row.get("trust_ratio_est")),
                    dealer_ratio_est=safe_float(row.get("dealer_ratio_est")),
                    three_inst_ratio_est=safe_float(row.get("three_inst_ratio_est")),
                    trust_shares_est=safe_int(row.get("trust_shares_est")),
                    dealer_shares_est=safe_int(row.get("dealer_shares_est")),
                    change_5d=safe_float(row.get("three_inst_ratio_change_5")),
                    change_20d=safe_float(row.get("three_inst_ratio_change_20")),
                    change_60d=safe_float(row.get("three_inst_ratio_change_60")),
                    change_120d=safe_float(row.get("three_inst_ratio_change_120")),
                )
            )
            session.execute(stmt)
            count += 1

    return count


def upsert_broker_trades(df: pd.DataFrame, trade_date: date) -> int:
    """Upsert broker trades from DataFrame.

    Returns:
        Number of broker trades upserted
    """
    if df.empty:
        return 0

    count = 0
    with get_db_session() as session:
        stock_map: Dict[str, int] = {}

        for _, row in df.iterrows():
            code = str(row.get("stock_code", "")).strip()
            if not code:
                continue

            if code not in stock_map:
                stock = session.query(Stock).filter_by(code=code).first()
                if not stock:
                    # Create stock if not exists
                    stock = Stock(code=code, name=code, market="TWSE")
                    session.add(stock)
                    session.flush()
                stock_map[code] = stock.id

            stock_id = stock_map[code]

            broker_trade = BrokerTrade(
                stock_id=stock_id,
                trade_date=trade_date,
                broker_name=str(row.get("broker_name", "")).strip(),
                broker_id=str(row.get("broker_id", "")).strip() or None,
                buy_vol=int(row.get("buy_vol", 0) or 0),
                sell_vol=int(row.get("sell_vol", 0) or 0),
                net_vol=int(row.get("net_vol", 0) or 0),
                pct=float(row.get("pct", 0) or 0),
                rank=int(row.get("rank", 0) or 0),
                side=str(row.get("side", "")).strip() or None,
            )
            session.add(broker_trade)
            count += 1

    return count
