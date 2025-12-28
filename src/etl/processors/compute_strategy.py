"""Compute and store pre-calculated strategy rankings."""
import logging
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def compute_win_rate_rankings(db, holding_days: int = 10, min_signals: int = 2):
    """Compute and store win rate rankings for a specific holding period."""
    metric_type = f"win_rate_{holding_days}d"
    logger.info(f"Computing {metric_type}...")

    # Clear old data for this metric
    db.execute(text("DELETE FROM strategy_rankings WHERE metric_type = :metric_type"),
               {"metric_type": metric_type})

    # Compute and insert new rankings
    query = text("""
    WITH latest_prices AS (
        SELECT DISTINCT ON (stock_id)
            stock_id,
            close_price,
            trade_date
        FROM stock_prices
        ORDER BY stock_id, trade_date DESC
    ),
    consecutive_buying AS (
        SELECT
            f.stock_id,
            f.trade_date,
            f.foreign_net,
            SUM(CASE WHEN f.foreign_net > 0 THEN 1 ELSE 0 END)
                OVER (PARTITION BY f.stock_id ORDER BY f.trade_date
                      ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as buy_streak_5
        FROM institutional_flows f
        WHERE f.trade_date >= '2024-01-01'
    ),
    buy_signals AS (
        SELECT stock_id, trade_date as signal_date
        FROM consecutive_buying
        WHERE buy_streak_5 >= 3
    ),
    returns AS (
        SELECT
            bs.stock_id,
            bs.signal_date,
            p1.close_price as entry_price,
            p2.close_price as exit_price,
            ROUND((p2.close_price - p1.close_price) / NULLIF(p1.close_price, 0) * 100, 2) as return_pct
        FROM buy_signals bs
        JOIN stock_prices p1 ON bs.stock_id = p1.stock_id AND p1.trade_date = bs.signal_date
        JOIN stock_prices p2 ON bs.stock_id = p2.stock_id AND p2.trade_date = (
            SELECT MIN(trade_date) FROM stock_prices
            WHERE stock_id = bs.stock_id AND trade_date > bs.signal_date + :holding_days - 1
        )
        WHERE p1.close_price > 0 AND p2.close_price IS NOT NULL
    ),
    stock_stats AS (
        SELECT
            r.stock_id,
            lp.close_price as current_price,
            COUNT(*) as signal_count,
            ROUND(AVG(r.return_pct), 2) as avg_return,
            ROUND(SUM(CASE WHEN r.return_pct > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) as win_rate,
            CASE
                WHEN lp.close_price >= 500 THEN 'high'
                WHEN lp.close_price >= 200 THEN 'mid'
                ELSE 'low'
            END as price_tier
        FROM returns r
        LEFT JOIN latest_prices lp ON r.stock_id = lp.stock_id
        GROUP BY r.stock_id, lp.close_price
        HAVING COUNT(*) >= :min_signals
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY price_tier ORDER BY win_rate DESC, avg_return DESC) as rank
        FROM stock_stats
    )
    INSERT INTO strategy_rankings (stock_id, price_tier, metric_type, signal_count, avg_return, win_rate, current_price, rank_in_tier)
    SELECT stock_id, price_tier, :metric_type, signal_count, avg_return, win_rate, current_price, rank
    FROM ranked
    WHERE rank <= 10
    """)

    result = db.execute(query, {
        "holding_days": holding_days,
        "min_signals": min_signals,
        "metric_type": metric_type
    })
    db.commit()
    logger.info(f"  Inserted {result.rowcount} rankings for {metric_type}")
    return result.rowcount


def compute_correlation_rankings(db, min_data_points: int = 5):
    """Compute and store correlation rankings."""
    metric_type = "correlation"
    logger.info(f"Computing {metric_type}...")

    db.execute(text("DELETE FROM strategy_rankings WHERE metric_type = :metric_type"),
               {"metric_type": metric_type})

    query = text("""
    WITH latest_prices AS (
        SELECT DISTINCT ON (stock_id)
            stock_id,
            close_price
        FROM stock_prices
        ORDER BY stock_id, trade_date DESC
    ),
    daily_data AS (
        SELECT
            f.stock_id,
            f.trade_date,
            f.foreign_net,
            p.close_price,
            LAG(p.close_price) OVER (PARTITION BY f.stock_id ORDER BY f.trade_date) as prev_close
        FROM institutional_flows f
        JOIN stock_prices p ON f.stock_id = p.stock_id AND f.trade_date = p.trade_date
        WHERE f.trade_date >= '2024-01-01'
    ),
    returns_data AS (
        SELECT
            stock_id,
            trade_date,
            foreign_net,
            CASE WHEN prev_close > 0
                THEN (close_price - prev_close) / prev_close * 100
                ELSE 0
            END as daily_return
        FROM daily_data
        WHERE prev_close IS NOT NULL AND prev_close > 0
    ),
    correlations AS (
        SELECT
            rd.stock_id,
            lp.close_price as current_price,
            COUNT(*) as data_points,
            ROUND((
                COUNT(*) * SUM(rd.foreign_net * rd.daily_return) - SUM(rd.foreign_net) * SUM(rd.daily_return)
            ) / NULLIF(
                SQRT(
                    (COUNT(*) * SUM(rd.foreign_net * rd.foreign_net) - SUM(rd.foreign_net) * SUM(rd.foreign_net)) *
                    (COUNT(*) * SUM(rd.daily_return * rd.daily_return) - SUM(rd.daily_return) * SUM(rd.daily_return))
                ), 0
            ), 4) as correlation,
            CASE
                WHEN lp.close_price >= 500 THEN 'high'
                WHEN lp.close_price >= 200 THEN 'mid'
                ELSE 'low'
            END as price_tier
        FROM returns_data rd
        LEFT JOIN latest_prices lp ON rd.stock_id = lp.stock_id
        GROUP BY rd.stock_id, lp.close_price
        HAVING COUNT(*) >= :min_data_points
    ),
    ranked AS (
        SELECT
            *,
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


def compute_stock_technicals(db):
    """Compute and store technical indicators for all stocks with sufficient data."""
    logger.info("Computing stock technicals...")

    # Clear old data
    db.execute(text("DELETE FROM stock_technicals"))

    query = text("""
    WITH price_data AS (
        SELECT
            p.stock_id,
            p.trade_date,
            p.close_price,
            p.high_price,
            p.low_price,
            ROW_NUMBER() OVER (PARTITION BY p.stock_id ORDER BY p.trade_date DESC) as rn
        FROM stock_prices p
        WHERE p.close_price IS NOT NULL
    ),
    ma_data AS (
        SELECT
            stock_id,
            AVG(CASE WHEN rn <= 5 THEN close_price END) as ma5,
            AVG(CASE WHEN rn <= 10 THEN close_price END) as ma10,
            AVG(CASE WHEN rn <= 20 THEN close_price END) as ma20,
            AVG(CASE WHEN rn <= 60 THEN close_price END) as ma60,
            AVG(CASE WHEN rn <= 120 THEN close_price END) as ma120,
            MAX(CASE WHEN rn <= 20 THEN high_price END) as high_20,
            MIN(CASE WHEN rn <= 20 THEN low_price END) as low_20,
            MAX(CASE WHEN rn = 1 THEN close_price END) as current_close,
            COUNT(*) as price_count
        FROM price_data
        WHERE rn <= 120
        GROUP BY stock_id
        HAVING COUNT(*) >= 5
    )
    INSERT INTO stock_technicals (stock_id, ma5, ma10, ma20, ma60, ma120, support1, resistance1)
    SELECT
        stock_id,
        ROUND(ma5, 2),
        ROUND(ma10, 2),
        ROUND(ma20, 2),
        ROUND(ma60, 2),
        ROUND(ma120, 2),
        ROUND(low_20, 2) as support1,
        ROUND(high_20, 2) as resistance1
    FROM ma_data
    """)

    result = db.execute(query)
    db.commit()
    logger.info(f"  Updated {result.rowcount} stock technicals")
    return result.rowcount


def run_all_computations(db):
    """Run all strategy computations."""
    logger.info("Starting strategy computations...")

    # Win rate rankings for different periods
    for days in [5, 10, 30]:
        compute_win_rate_rankings(db, holding_days=days, min_signals=2)

    # Correlation rankings
    compute_correlation_rankings(db, min_data_points=5)

    # Technical indicators
    compute_stock_technicals(db)

    logger.info("Strategy computations completed")


if __name__ == "__main__":
    from src.common.database import SessionLocal
    db = SessionLocal()
    try:
        run_all_computations(db)
    finally:
        db.close()
