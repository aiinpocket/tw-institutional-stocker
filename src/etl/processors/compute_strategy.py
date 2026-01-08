"""Compute and store pre-calculated strategy rankings.

Optimized version: Uses window functions and simplified queries for better performance.
Now with parallel execution for independent strategy computations.
"""
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text

from src.common.database import SessionLocal

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def compute_win_rate_rankings(db, holding_days: int = 10, min_signals: int = 2):
    """
    Compute win rate rankings using a simplified approach.

    Instead of looking up exact exit prices, we use a pre-computed price table
    with future returns calculated via window functions.
    """
    metric_type = f"win_rate_{holding_days}d"
    logger.info(f"Computing {metric_type}...")

    # Clear old data
    db.execute(text("DELETE FROM strategy_rankings WHERE metric_type = :metric_type"),
               {"metric_type": metric_type})
    db.commit()

    # Step 1: Create temp table with price returns
    db.execute(text("""
    DROP TABLE IF EXISTS temp_price_returns;
    CREATE TEMP TABLE temp_price_returns AS
    SELECT
        stock_id,
        trade_date,
        close_price,
        LEAD(close_price, :holding_days) OVER (PARTITION BY stock_id ORDER BY trade_date) as future_price
    FROM stock_prices
    WHERE trade_date >= CURRENT_DATE - 200
      AND close_price > 0
    """), {"holding_days": holding_days})
    db.commit()
    logger.info(f"  Created temp_price_returns")

    # Step 2: Create temp table with buy signals (simplified: just foreign net > 0 for 3+ days in 5)
    db.execute(text("""
    DROP TABLE IF EXISTS temp_buy_signals;
    CREATE TEMP TABLE temp_buy_signals AS
    WITH recent_flows AS (
        SELECT
            stock_id,
            trade_date,
            foreign_net,
            SUM(CASE WHEN foreign_net > 0 THEN 1 ELSE 0 END)
                OVER (PARTITION BY stock_id ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as buy_count_5d
        FROM institutional_flows
        WHERE trade_date >= CURRENT_DATE - 180
    )
    SELECT DISTINCT stock_id, trade_date as signal_date
    FROM recent_flows
    WHERE buy_count_5d >= 3
    """))
    db.commit()
    logger.info(f"  Created temp_buy_signals")

    # Step 3: Calculate returns and insert rankings
    query = text("""
    WITH latest_prices AS (
        SELECT DISTINCT ON (stock_id)
            stock_id, close_price
        FROM stock_prices
        ORDER BY stock_id, trade_date DESC
    ),
    signal_returns AS (
        SELECT
            bs.stock_id,
            bs.signal_date,
            pr.close_price as entry_price,
            pr.future_price as exit_price,
            ROUND((pr.future_price - pr.close_price) / pr.close_price * 100, 2) as return_pct
        FROM temp_buy_signals bs
        JOIN temp_price_returns pr ON bs.stock_id = pr.stock_id AND bs.signal_date = pr.trade_date
        WHERE pr.future_price IS NOT NULL
    ),
    stock_stats AS (
        SELECT
            sr.stock_id,
            lp.close_price as current_price,
            COUNT(*) as signal_count,
            ROUND(AVG(sr.return_pct), 2) as avg_return,
            ROUND(SUM(CASE WHEN sr.return_pct > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as win_rate,
            CASE
                WHEN lp.close_price >= 500 THEN 'high'
                WHEN lp.close_price >= 200 THEN 'mid'
                ELSE 'low'
            END as price_tier
        FROM signal_returns sr
        JOIN latest_prices lp ON sr.stock_id = lp.stock_id
        GROUP BY sr.stock_id, lp.close_price
        HAVING COUNT(*) >= :min_signals
    ),
    ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY price_tier ORDER BY win_rate DESC, avg_return DESC) as rank
        FROM stock_stats
    )
    INSERT INTO strategy_rankings (stock_id, price_tier, metric_type, signal_count, avg_return, win_rate, current_price, rank_in_tier)
    SELECT stock_id, price_tier, :metric_type, signal_count, avg_return, win_rate, current_price, rank
    FROM ranked
    WHERE rank <= 10
    """)

    result = db.execute(query, {"min_signals": min_signals, "metric_type": metric_type})
    db.commit()

    # Cleanup
    db.execute(text("DROP TABLE IF EXISTS temp_price_returns"))
    db.execute(text("DROP TABLE IF EXISTS temp_buy_signals"))
    db.commit()

    logger.info(f"  Inserted {result.rowcount} rankings for {metric_type}")
    return result.rowcount


def compute_correlation_rankings(db, min_data_points: int = 20):
    """Compute correlation between foreign net buying and stock returns."""
    metric_type = "correlation"
    logger.info(f"Computing {metric_type}...")

    db.execute(text("DELETE FROM strategy_rankings WHERE metric_type = :metric_type"),
               {"metric_type": metric_type})
    db.commit()

    # Simplified query with limited date range
    query = text("""
    WITH latest_prices AS (
        SELECT DISTINCT ON (stock_id)
            stock_id, close_price
        FROM stock_prices
        ORDER BY stock_id, trade_date DESC
    ),
    daily_data AS (
        SELECT
            f.stock_id,
            f.foreign_net,
            (p.close_price - LAG(p.close_price) OVER (PARTITION BY f.stock_id ORDER BY f.trade_date))
                / NULLIF(LAG(p.close_price) OVER (PARTITION BY f.stock_id ORDER BY f.trade_date), 0) * 100 as daily_return
        FROM institutional_flows f
        JOIN stock_prices p ON f.stock_id = p.stock_id AND f.trade_date = p.trade_date
        WHERE f.trade_date >= CURRENT_DATE - 90
          AND p.close_price > 0
    ),
    correlations AS (
        SELECT
            dd.stock_id,
            lp.close_price as current_price,
            COUNT(*) as data_points,
            ROUND(CORR(dd.foreign_net, dd.daily_return)::numeric, 4) as correlation,
            CASE
                WHEN lp.close_price >= 500 THEN 'high'
                WHEN lp.close_price >= 200 THEN 'mid'
                ELSE 'low'
            END as price_tier
        FROM daily_data dd
        JOIN latest_prices lp ON dd.stock_id = lp.stock_id
        WHERE dd.daily_return IS NOT NULL
        GROUP BY dd.stock_id, lp.close_price
        HAVING COUNT(*) >= :min_data_points
    ),
    ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY price_tier ORDER BY correlation DESC NULLS LAST) as rank
        FROM correlations
        WHERE correlation IS NOT NULL
    )
    INSERT INTO strategy_rankings (stock_id, price_tier, metric_type, correlation, data_points, current_price, rank_in_tier)
    SELECT stock_id, price_tier, :metric_type, correlation, data_points, current_price, rank
    FROM ranked
    WHERE rank <= 10
    """)

    result = db.execute(query, {"min_data_points": min_data_points, "metric_type": metric_type})
    db.commit()
    logger.info(f"  Inserted {result.rowcount} rankings for {metric_type}")
    return result.rowcount


def compute_below_cost_rankings(db, lookback_days: int = 60):
    """Compute stocks trading below institutional average cost."""
    metric_type = "below_cost"
    logger.info(f"Computing {metric_type}...")

    db.execute(text("DELETE FROM strategy_rankings WHERE metric_type = :metric_type"),
               {"metric_type": metric_type})
    db.commit()

    query = text("""
    WITH latest_prices AS (
        SELECT DISTINCT ON (stock_id)
            stock_id, close_price
        FROM stock_prices
        ORDER BY stock_id, trade_date DESC
    ),
    inst_cost AS (
        SELECT
            f.stock_id,
            SUM(CASE WHEN (f.foreign_net + f.trust_net + f.dealer_net) > 0
                THEN (f.foreign_net + f.trust_net + f.dealer_net) * p.close_price ELSE 0 END) as weighted_cost,
            SUM(CASE WHEN (f.foreign_net + f.trust_net + f.dealer_net) > 0
                THEN (f.foreign_net + f.trust_net + f.dealer_net) ELSE 0 END) as total_shares,
            COUNT(*) FILTER (WHERE (f.foreign_net + f.trust_net + f.dealer_net) > 0) as buy_days
        FROM institutional_flows f
        JOIN stock_prices p ON f.stock_id = p.stock_id AND f.trade_date = p.trade_date
        WHERE f.trade_date >= CURRENT_DATE - :lookback_days AND p.close_price > 0
        GROUP BY f.stock_id
        HAVING SUM(CASE WHEN (f.foreign_net + f.trust_net + f.dealer_net) > 0
                   THEN (f.foreign_net + f.trust_net + f.dealer_net) ELSE 0 END) > 0
    ),
    below_cost AS (
        SELECT
            ic.stock_id,
            lp.close_price as current_price,
            ROUND(ic.weighted_cost / ic.total_shares, 2) as avg_cost,
            ic.buy_days,
            ROUND((lp.close_price - ic.weighted_cost / ic.total_shares) / (ic.weighted_cost / ic.total_shares) * 100, 2) as discount_pct,
            CASE WHEN lp.close_price >= 500 THEN 'high' WHEN lp.close_price >= 200 THEN 'mid' ELSE 'low' END as price_tier
        FROM inst_cost ic
        JOIN latest_prices lp ON ic.stock_id = lp.stock_id
        WHERE lp.close_price < (ic.weighted_cost / ic.total_shares) AND ic.buy_days >= 3
    ),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY price_tier ORDER BY discount_pct ASC) as rank
        FROM below_cost
    )
    INSERT INTO strategy_rankings (stock_id, price_tier, metric_type, avg_return, win_rate, signal_count, current_price, rank_in_tier)
    SELECT stock_id, price_tier, :metric_type, avg_cost, discount_pct, buy_days, current_price, rank
    FROM ranked WHERE rank <= 15
    """)

    result = db.execute(query, {"lookback_days": lookback_days, "metric_type": metric_type})
    db.commit()
    logger.info(f"  Inserted {result.rowcount} rankings for {metric_type}")
    return result.rowcount


def compute_consecutive_buying(db, min_days: int = 3):
    """Compute stocks with consecutive foreign buying."""
    metric_type = "consecutive_buying"
    logger.info(f"Computing {metric_type}...")

    db.execute(text("DELETE FROM strategy_rankings WHERE metric_type = :metric_type"),
               {"metric_type": metric_type})
    db.commit()

    query = text("""
    WITH latest_prices AS (
        SELECT DISTINCT ON (stock_id) stock_id, close_price
        FROM stock_prices ORDER BY stock_id, trade_date DESC
    ),
    consecutive AS (
        SELECT stock_id, trade_date, foreign_net,
            CASE WHEN foreign_net > 0 THEN 1 ELSE 0 END as is_buy,
            ROW_NUMBER() OVER (PARTITION BY stock_id ORDER BY trade_date DESC) as rn
        FROM institutional_flows
        WHERE trade_date >= CURRENT_DATE - 30
    ),
    streak_calc AS (
        SELECT stock_id,
            COUNT(*) FILTER (WHERE is_buy = 1) as consecutive_days,
            SUM(foreign_net) FILTER (WHERE is_buy = 1) as total_net_buy
        FROM (
            SELECT *, SUM(CASE WHEN is_buy = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY stock_id ORDER BY rn) as grp
            FROM consecutive
        ) sub
        WHERE grp = 0 AND is_buy = 1
        GROUP BY stock_id
        HAVING COUNT(*) >= :min_days
    ),
    ranked AS (
        SELECT sc.stock_id, lp.close_price as current_price, sc.consecutive_days, sc.total_net_buy,
            CASE WHEN lp.close_price >= 500 THEN 'high' WHEN lp.close_price >= 200 THEN 'mid' ELSE 'low' END as price_tier,
            ROW_NUMBER() OVER (
                PARTITION BY CASE WHEN lp.close_price >= 500 THEN 'high' WHEN lp.close_price >= 200 THEN 'mid' ELSE 'low' END
                ORDER BY sc.consecutive_days DESC, sc.total_net_buy DESC
            ) as rank
        FROM streak_calc sc JOIN latest_prices lp ON sc.stock_id = lp.stock_id
    )
    INSERT INTO strategy_rankings (stock_id, price_tier, metric_type, signal_count, avg_return, current_price, rank_in_tier)
    SELECT stock_id, price_tier, :metric_type, consecutive_days, ROUND(total_net_buy / 100000.0, 2), current_price, rank
    FROM ranked WHERE rank <= 15
    """)

    result = db.execute(query, {"min_days": min_days, "metric_type": metric_type})
    db.commit()
    logger.info(f"  Inserted {result.rowcount} rankings for {metric_type}")
    return result.rowcount


def compute_trust_accumulation(db, lookback_days: int = 20):
    """Compute stocks with trust accumulation."""
    metric_type = "trust_accumulation"
    logger.info(f"Computing {metric_type}...")

    db.execute(text("DELETE FROM strategy_rankings WHERE metric_type = :metric_type"),
               {"metric_type": metric_type})
    db.commit()

    query = text("""
    WITH latest_prices AS (
        SELECT DISTINCT ON (stock_id) stock_id, close_price
        FROM stock_prices ORDER BY stock_id, trade_date DESC
    ),
    trust_activity AS (
        SELECT f.stock_id,
            SUM(f.trust_net) as total_trust_net,
            COUNT(*) FILTER (WHERE f.trust_net > 0) as buy_days,
            COUNT(*) as total_days
        FROM institutional_flows f
        WHERE f.trade_date >= CURRENT_DATE - :lookback_days
        GROUP BY f.stock_id
        HAVING SUM(f.trust_net) > 0 AND COUNT(*) FILTER (WHERE f.trust_net > 0) >= 3
    ),
    combined AS (
        SELECT ta.stock_id, lp.close_price as current_price, ta.total_trust_net, ta.buy_days,
            ROUND(ta.buy_days * 100.0 / NULLIF(ta.total_days, 0), 1) as buy_ratio,
            CASE WHEN lp.close_price >= 500 THEN 'high' WHEN lp.close_price >= 200 THEN 'mid' ELSE 'low' END as price_tier
        FROM trust_activity ta JOIN latest_prices lp ON ta.stock_id = lp.stock_id
    ),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY price_tier ORDER BY buy_ratio DESC, total_trust_net DESC) as rank
        FROM combined
    )
    INSERT INTO strategy_rankings (stock_id, price_tier, metric_type, signal_count, avg_return, win_rate, current_price, rank_in_tier)
    SELECT stock_id, price_tier, :metric_type, buy_days, ROUND(total_trust_net / 100000.0, 2), buy_ratio, current_price, rank
    FROM ranked WHERE rank <= 15
    """)

    result = db.execute(query, {"lookback_days": lookback_days, "metric_type": metric_type})
    db.commit()
    logger.info(f"  Inserted {result.rowcount} rankings for {metric_type}")
    return result.rowcount


def compute_synchronized_buying(db, lookback_days: int = 10):
    """Compute stocks with synchronized buying from all three institutional types."""
    metric_type = "synchronized_buying"
    logger.info(f"Computing {metric_type}...")

    db.execute(text("DELETE FROM strategy_rankings WHERE metric_type = :metric_type"),
               {"metric_type": metric_type})
    db.commit()

    query = text("""
    WITH latest_prices AS (
        SELECT DISTINCT ON (stock_id) stock_id, close_price
        FROM stock_prices ORDER BY stock_id, trade_date DESC
    ),
    sync_stats AS (
        SELECT stock_id, COUNT(*) as sync_days_count,
            SUM(foreign_net + trust_net + dealer_net) as total_sync_amount,
            SUM(foreign_net) as foreign_total, SUM(trust_net) as trust_total
        FROM institutional_flows
        WHERE trade_date >= CURRENT_DATE - :lookback_days
          AND foreign_net > 0 AND trust_net > 0 AND dealer_net > 0
        GROUP BY stock_id HAVING COUNT(*) >= 2
    ),
    combined AS (
        SELECT ss.stock_id, lp.close_price as current_price, ss.sync_days_count, ss.total_sync_amount, ss.foreign_total, ss.trust_total,
            CASE WHEN lp.close_price >= 500 THEN 'high' WHEN lp.close_price >= 200 THEN 'mid' ELSE 'low' END as price_tier
        FROM sync_stats ss JOIN latest_prices lp ON ss.stock_id = lp.stock_id
    ),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY price_tier ORDER BY sync_days_count DESC, total_sync_amount DESC) as rank
        FROM combined
    )
    INSERT INTO strategy_rankings (stock_id, price_tier, metric_type, signal_count, avg_return, correlation, data_points, current_price, rank_in_tier)
    SELECT stock_id, price_tier, :metric_type, sync_days_count, ROUND(total_sync_amount / 100000.0, 2),
           ROUND(foreign_total / 100000.0, 2), ROUND(trust_total / 100000.0, 2)::integer, current_price, rank
    FROM ranked WHERE rank <= 15
    """)

    result = db.execute(query, {"lookback_days": lookback_days, "metric_type": metric_type})
    db.commit()
    logger.info(f"  Inserted {result.rowcount} rankings for {metric_type}")
    return result.rowcount


def compute_price_deviation(db, lookback_days: int = 60):
    """Compute stocks with significant price deviation from institutional cost."""
    metric_type = "price_deviation"
    logger.info(f"Computing {metric_type}...")

    db.execute(text("DELETE FROM strategy_rankings WHERE metric_type = :metric_type"),
               {"metric_type": metric_type})
    db.commit()

    query = text("""
    WITH latest_prices AS (
        SELECT DISTINCT ON (stock_id) stock_id, close_price
        FROM stock_prices ORDER BY stock_id, trade_date DESC
    ),
    inst_cost AS (
        SELECT f.stock_id,
            SUM(CASE WHEN (f.foreign_net + f.trust_net + f.dealer_net) > 0
                THEN (f.foreign_net + f.trust_net + f.dealer_net) * p.close_price ELSE 0 END) as weighted_cost,
            SUM(CASE WHEN (f.foreign_net + f.trust_net + f.dealer_net) > 0
                THEN (f.foreign_net + f.trust_net + f.dealer_net) ELSE 0 END) as total_shares
        FROM institutional_flows f
        JOIN stock_prices p ON f.stock_id = p.stock_id AND f.trade_date = p.trade_date
        WHERE f.trade_date >= CURRENT_DATE - :lookback_days AND p.close_price > 0
        GROUP BY f.stock_id
        HAVING SUM(CASE WHEN (f.foreign_net + f.trust_net + f.dealer_net) > 0
                   THEN (f.foreign_net + f.trust_net + f.dealer_net) ELSE 0 END) > 0
    ),
    deviation_calc AS (
        SELECT ic.stock_id, lp.close_price as current_price,
            ROUND(ic.weighted_cost / ic.total_shares, 2) as avg_cost,
            ROUND((lp.close_price - ic.weighted_cost / ic.total_shares) / (ic.weighted_cost / ic.total_shares) * 100, 2) as deviation_pct,
            CASE WHEN lp.close_price >= 500 THEN 'high' WHEN lp.close_price >= 200 THEN 'mid' ELSE 'low' END as price_tier
        FROM inst_cost ic JOIN latest_prices lp ON ic.stock_id = lp.stock_id
        WHERE ABS((lp.close_price - ic.weighted_cost / ic.total_shares) / (ic.weighted_cost / ic.total_shares) * 100) >= 10
    ),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY price_tier ORDER BY ABS(deviation_pct) DESC) as rank
        FROM deviation_calc
    )
    INSERT INTO strategy_rankings (stock_id, price_tier, metric_type, avg_return, win_rate, current_price, rank_in_tier)
    SELECT stock_id, price_tier, :metric_type, avg_cost, deviation_pct, current_price, rank
    FROM ranked WHERE rank <= 15
    """)

    result = db.execute(query, {"lookback_days": lookback_days, "metric_type": metric_type})
    db.commit()
    logger.info(f"  Inserted {result.rowcount} rankings for {metric_type}")
    return result.rowcount


def compute_stock_technicals(db):
    """Compute technical indicators for all stocks."""
    logger.info("Computing stock technicals...")

    db.execute(text("DELETE FROM stock_technicals"))
    db.commit()

    query = text("""
    WITH price_data AS (
        SELECT stock_id, close_price, high_price, low_price,
            ROW_NUMBER() OVER (PARTITION BY stock_id ORDER BY trade_date DESC) as rn
        FROM stock_prices WHERE close_price IS NOT NULL
    ),
    ma_data AS (
        SELECT stock_id,
            AVG(CASE WHEN rn <= 5 THEN close_price END) as ma5,
            AVG(CASE WHEN rn <= 10 THEN close_price END) as ma10,
            AVG(CASE WHEN rn <= 20 THEN close_price END) as ma20,
            AVG(CASE WHEN rn <= 60 THEN close_price END) as ma60,
            AVG(CASE WHEN rn <= 120 THEN close_price END) as ma120,
            MAX(CASE WHEN rn <= 20 THEN high_price END) as high_20,
            MIN(CASE WHEN rn <= 20 THEN low_price END) as low_20
        FROM price_data WHERE rn <= 120
        GROUP BY stock_id HAVING COUNT(*) >= 5
    )
    INSERT INTO stock_technicals (stock_id, ma5, ma10, ma20, ma60, ma120, support1, resistance1)
    SELECT stock_id, ROUND(ma5, 2), ROUND(ma10, 2), ROUND(ma20, 2), ROUND(ma60, 2), ROUND(ma120, 2),
           ROUND(low_20, 2), ROUND(high_20, 2)
    FROM ma_data
    """)

    result = db.execute(query)
    db.commit()
    logger.info(f"  Updated {result.rowcount} stock technicals")
    return result.rowcount


def _run_computation_with_new_session(name: str, compute_func_factory):
    """Run a single computation with its own database session (for thread safety)."""
    db = SessionLocal()
    try:
        compute_func_factory(db)
        return (name, True, None)
    except Exception as e:
        logger.error(f"Failed to compute {name}: {e}")
        db.rollback()
        return (name, False, str(e))
    finally:
        db.close()


def run_all_computations(db):
    """Run all strategy computations in parallel with error handling.

    Each computation runs in its own thread with a separate database session
    for thread safety. This provides ~5-6x speedup over sequential execution.
    """
    logger.info("Starting strategy computations (parallel mode)...")

    # Define computations as (name, factory_function) pairs
    # Factory functions create the actual computation with a db session
    computations = [
        ("win_rate_5d", lambda db: compute_win_rate_rankings(db, holding_days=5, min_signals=2)),
        ("win_rate_10d", lambda db: compute_win_rate_rankings(db, holding_days=10, min_signals=2)),
        ("win_rate_30d", lambda db: compute_win_rate_rankings(db, holding_days=30, min_signals=2)),
        ("correlation", lambda db: compute_correlation_rankings(db, min_data_points=20)),
        ("below_cost", lambda db: compute_below_cost_rankings(db, lookback_days=60)),
        ("consecutive_buying", lambda db: compute_consecutive_buying(db, min_days=3)),
        ("trust_accumulation", lambda db: compute_trust_accumulation(db, lookback_days=20)),
        ("synchronized_buying", lambda db: compute_synchronized_buying(db, lookback_days=10)),
        ("price_deviation", lambda db: compute_price_deviation(db, lookback_days=60)),
        ("technicals", lambda db: compute_stock_technicals(db)),
    ]

    # Run computations in parallel using ThreadPoolExecutor
    # Use max_workers=5 to avoid overwhelming the database
    successful = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all tasks
        futures = {
            executor.submit(_run_computation_with_new_session, name, func): name
            for name, func in computations
        }

        # Collect results as they complete
        for future in as_completed(futures):
            name, success, error = future.result()
            if success:
                successful += 1
                logger.info(f"  ✓ {name} completed")
            else:
                failed += 1
                logger.error(f"  ✗ {name} failed: {error}")

    logger.info(f"Strategy computations completed: {successful} successful, {failed} failed")


if __name__ == "__main__":
    from src.common.database import SessionLocal
    db = SessionLocal()
    try:
        run_all_computations(db)
    finally:
        db.close()
