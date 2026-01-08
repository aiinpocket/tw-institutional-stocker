"""Compute institutional ratios entirely in PostgreSQL.

This replaces the Python-based holdings.py and ratios.py with pure SQL,
eliminating the need to transfer 250k+ records between DB and Python.
"""
import logging
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def compute_ratios_in_postgresql(db, lookback_days: int = 180):
    """
    Compute institutional ratios entirely in PostgreSQL using window functions.

    This is much faster than the Python approach because:
    1. No data transfer overhead (250k+ rows each way)
    2. PostgreSQL window functions are highly optimized
    3. Single query execution with proper indexing

    Args:
        db: SQLAlchemy session
        lookback_days: Number of days to compute ratios for (default 180)
    """
    logger.info(f"Computing ratios in PostgreSQL (last {lookback_days} days)...")

    # Step 1: Create temp table with cumulative holdings
    logger.info("  Creating temp table with cumulative holdings...")
    db.execute(text("""
        DROP TABLE IF EXISTS temp_holdings_calc;
        CREATE TEMP TABLE temp_holdings_calc AS
        WITH base_data AS (
            SELECT
                f.stock_id,
                f.trade_date,
                f.foreign_net,
                f.trust_net,
                f.dealer_net,
                COALESCE(h.total_shares, 0) as total_shares,
                COALESCE(h.foreign_ratio, 0) as foreign_ratio
            FROM institutional_flows f
            LEFT JOIN foreign_holdings h
                ON f.stock_id = h.stock_id AND f.trade_date = h.trade_date
            WHERE f.trade_date >= CURRENT_DATE - :lookback_days
        ),
        cumulative AS (
            SELECT
                stock_id,
                trade_date,
                foreign_net,
                trust_net,
                dealer_net,
                total_shares,
                foreign_ratio,
                -- Cumulative sum for trust and dealer
                SUM(trust_net) OVER (PARTITION BY stock_id ORDER BY trade_date) as trust_shares_est,
                SUM(dealer_net) OVER (PARTITION BY stock_id ORDER BY trade_date) as dealer_shares_est
            FROM base_data
        )
        SELECT
            stock_id,
            trade_date,
            foreign_net,
            trust_net,
            dealer_net,
            total_shares,
            foreign_ratio,
            trust_shares_est,
            dealer_shares_est,
            -- Calculate ratios
            CASE WHEN total_shares > 0
                THEN ROUND((trust_shares_est::numeric / total_shares * 100), 4)
                ELSE 0
            END as trust_ratio_est,
            CASE WHEN total_shares > 0
                THEN ROUND((dealer_shares_est::numeric / total_shares * 100), 4)
                ELSE 0
            END as dealer_ratio_est,
            -- Three institutional ratio
            foreign_ratio +
            CASE WHEN total_shares > 0 THEN ROUND((trust_shares_est::numeric / total_shares * 100), 4) ELSE 0 END +
            CASE WHEN total_shares > 0 THEN ROUND((dealer_shares_est::numeric / total_shares * 100), 4) ELSE 0 END
            as three_inst_ratio_est
        FROM cumulative
    """), {"lookback_days": lookback_days})
    db.commit()

    # Check row count
    count = db.execute(text("SELECT COUNT(*) FROM temp_holdings_calc")).scalar()
    logger.info(f"  Created temp_holdings_calc with {count} rows")

    # Step 2: Add change metrics using window functions
    logger.info("  Computing change metrics...")
    db.execute(text("""
        DROP TABLE IF EXISTS temp_ratios_final;
        CREATE TEMP TABLE temp_ratios_final AS
        SELECT
            stock_id,
            trade_date,
            foreign_net,
            trust_net,
            dealer_net,
            total_shares,
            foreign_ratio,
            trust_shares_est,
            dealer_shares_est,
            trust_ratio_est,
            dealer_ratio_est,
            three_inst_ratio_est,
            -- Change metrics using LAG window function
            three_inst_ratio_est - LAG(three_inst_ratio_est, 5) OVER (PARTITION BY stock_id ORDER BY trade_date)
                as three_inst_ratio_change_5,
            three_inst_ratio_est - LAG(three_inst_ratio_est, 20) OVER (PARTITION BY stock_id ORDER BY trade_date)
                as three_inst_ratio_change_20,
            three_inst_ratio_est - LAG(three_inst_ratio_est, 60) OVER (PARTITION BY stock_id ORDER BY trade_date)
                as three_inst_ratio_change_60,
            three_inst_ratio_est - LAG(three_inst_ratio_est, 120) OVER (PARTITION BY stock_id ORDER BY trade_date)
                as three_inst_ratio_change_120
        FROM temp_holdings_calc
    """))
    db.commit()

    count = db.execute(text("SELECT COUNT(*) FROM temp_ratios_final")).scalar()
    logger.info(f"  Created temp_ratios_final with {count} rows")

    # Step 3: Upsert to institutional_ratios table
    logger.info("  Upserting to institutional_ratios...")

    # First ensure the table exists with all required columns
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS institutional_ratios (
            id SERIAL PRIMARY KEY,
            stock_id INTEGER NOT NULL REFERENCES stocks(id),
            trade_date DATE NOT NULL,
            trust_shares_est BIGINT,
            dealer_shares_est BIGINT,
            trust_ratio_est NUMERIC(10, 4),
            dealer_ratio_est NUMERIC(10, 4),
            three_inst_ratio_est NUMERIC(10, 4),
            three_inst_ratio_change_5 NUMERIC(10, 4),
            three_inst_ratio_change_20 NUMERIC(10, 4),
            three_inst_ratio_change_60 NUMERIC(10, 4),
            three_inst_ratio_change_120 NUMERIC(10, 4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(stock_id, trade_date)
        )
    """))
    db.commit()

    # Add missing columns if they don't exist (for existing tables)
    alter_statements = [
        "ALTER TABLE institutional_ratios ADD COLUMN IF NOT EXISTS three_inst_ratio_change_5 NUMERIC(10, 4)",
        "ALTER TABLE institutional_ratios ADD COLUMN IF NOT EXISTS three_inst_ratio_change_20 NUMERIC(10, 4)",
        "ALTER TABLE institutional_ratios ADD COLUMN IF NOT EXISTS three_inst_ratio_change_60 NUMERIC(10, 4)",
        "ALTER TABLE institutional_ratios ADD COLUMN IF NOT EXISTS three_inst_ratio_change_120 NUMERIC(10, 4)",
        "ALTER TABLE institutional_ratios ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "ALTER TABLE institutional_ratios ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    ]
    for stmt in alter_statements:
        try:
            db.execute(text(stmt))
        except Exception:
            pass  # Column already exists
    db.commit()

    # Delete existing records in date range first (faster than UPSERT, avoids lock contention)
    logger.info("  Deleting existing records in date range...")
    delete_result = db.execute(text("""
        DELETE FROM institutional_ratios
        WHERE trade_date >= CURRENT_DATE - :lookback_days
    """), {"lookback_days": lookback_days})
    deleted = delete_result.rowcount
    logger.info(f"  Deleted {deleted} existing records")

    # Insert new records (no conflict handling needed)
    logger.info("  Inserting new ratio records...")
    result = db.execute(text("""
        INSERT INTO institutional_ratios (
            stock_id, trade_date,
            trust_shares_est, dealer_shares_est,
            trust_ratio_est, dealer_ratio_est, three_inst_ratio_est,
            three_inst_ratio_change_5, three_inst_ratio_change_20,
            three_inst_ratio_change_60, three_inst_ratio_change_120,
            updated_at
        )
        SELECT
            stock_id, trade_date,
            trust_shares_est, dealer_shares_est,
            trust_ratio_est, dealer_ratio_est, three_inst_ratio_est,
            three_inst_ratio_change_5, three_inst_ratio_change_20,
            three_inst_ratio_change_60, three_inst_ratio_change_120,
            CURRENT_TIMESTAMP
        FROM temp_ratios_final
    """))
    db.commit()

    upserted = result.rowcount
    logger.info(f"  Upserted {upserted} ratio records")

    # Cleanup
    db.execute(text("DROP TABLE IF EXISTS temp_holdings_calc"))
    db.execute(text("DROP TABLE IF EXISTS temp_ratios_final"))
    db.commit()

    logger.info("  Ratio computation completed")
    return upserted


if __name__ == "__main__":
    from src.common.database import SessionLocal
    db = SessionLocal()
    try:
        compute_ratios_in_postgresql(db)
    finally:
        db.close()
