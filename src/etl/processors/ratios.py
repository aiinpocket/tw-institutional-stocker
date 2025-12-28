"""Ratio computation and change metrics processor."""
from typing import List
import pandas as pd
from sqlalchemy import text

from src.common.database import get_db_session
from src.common.config import settings


def add_change_metrics(merged: pd.DataFrame, windows: List[int] = None) -> pd.DataFrame:
    """Add change metrics for multiple windows.

    Args:
        merged: DataFrame with three_inst_ratio_est column
        windows: List of window sizes (default: [5, 20, 60, 120])

    Returns:
        DataFrame with change columns added
    """
    if windows is None:
        windows = settings.windows

    merged = merged.sort_values(["code", "date"])

    def add_all(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()
        for w in windows:
            col = f"three_inst_ratio_change_{w}"
            g[col] = g["three_inst_ratio_est"].diff(periods=w)
        return g

    merged = merged.groupby("code", group_keys=False).apply(add_all)
    return merged


def compute_ratios_from_db() -> pd.DataFrame:
    """Compute institutional ratios directly from database.

    Queries flows and foreign_holdings tables and computes estimated ratios.

    Returns:
        DataFrame with computed ratios ready for upsert
    """
    query = text("""
        WITH flows_data AS (
            SELECT
                f.stock_id,
                f.trade_date,
                s.code,
                s.name,
                s.market,
                f.foreign_net,
                f.trust_net,
                f.dealer_net
            FROM institutional_flows f
            JOIN stocks s ON f.stock_id = s.id
        ),
        holdings_data AS (
            SELECT
                h.stock_id,
                h.trade_date,
                h.total_shares,
                h.foreign_ratio
            FROM foreign_holdings h
        ),
        merged AS (
            SELECT
                f.stock_id,
                f.trade_date,
                f.code,
                f.name,
                f.market,
                f.foreign_net,
                f.trust_net,
                f.dealer_net,
                COALESCE(h.total_shares, 0) as total_shares,
                COALESCE(h.foreign_ratio, 0) as foreign_ratio
            FROM flows_data f
            LEFT JOIN holdings_data h ON f.stock_id = h.stock_id AND f.trade_date = h.trade_date
        )
        SELECT * FROM merged
        ORDER BY code, trade_date
    """)

    with get_db_session() as session:
        result = session.execute(query)
        rows = result.fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[
        "stock_id", "trade_date", "code", "name", "market",
        "foreign_net", "trust_net", "dealer_net",
        "total_shares", "foreign_ratio"
    ])

    df["date"] = df["trade_date"]

    # Compute cumulative holdings estimation
    df = df.sort_values(["code", "date"])

    def compute_cumsum(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()
        g["trust_net"] = g["trust_net"].astype(float)
        g["dealer_net"] = g["dealer_net"].astype(float)

        g["trust_shares_est"] = g["trust_net"].cumsum()
        g["dealer_shares_est"] = g["dealer_net"].cumsum()

        denom = g["total_shares"].astype(float)
        valid = denom > 0

        g["trust_ratio_est"] = 0.0
        g["dealer_ratio_est"] = 0.0

        g.loc[valid, "trust_ratio_est"] = g.loc[valid, "trust_shares_est"] / denom[valid] * 100.0
        g.loc[valid, "dealer_ratio_est"] = g.loc[valid, "dealer_shares_est"] / denom[valid] * 100.0

        g["foreign_ratio"] = g["foreign_ratio"].fillna(0.0)
        g["three_inst_ratio_est"] = g["foreign_ratio"] + g["trust_ratio_est"] + g["dealer_ratio_est"]

        # Add change metrics
        for w in settings.windows:
            col = f"three_inst_ratio_change_{w}"
            g[col] = g["three_inst_ratio_est"].diff(periods=w)

        return g

    df = df.groupby("code", group_keys=False).apply(compute_cumsum)

    return df
