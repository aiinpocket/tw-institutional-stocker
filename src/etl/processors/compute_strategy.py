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


def compute_below_cost_rankings(db, lookback_days: int = 60):
    """
    Compute stocks where current price is below institutional 3-month average cost.

    Average cost = Σ(net_buy * close_price) / Σ(net_buy) for days where net > 0
    """
    metric_type = "below_cost"
    logger.info(f"Computing {metric_type}...")

    db.execute(text("DELETE FROM strategy_rankings WHERE metric_type = :metric_type"),
               {"metric_type": metric_type})

    query = text("""
    WITH latest_prices AS (
        SELECT DISTINCT ON (stock_id)
            stock_id,
            close_price,
            trade_date
        FROM stock_prices
        ORDER BY stock_id, trade_date DESC
    ),
    -- 計算三大法人合計淨買超
    inst_flows AS (
        SELECT
            f.stock_id,
            f.trade_date,
            f.foreign_net + f.trust_net + f.dealer_net as total_net,
            p.close_price
        FROM institutional_flows f
        JOIN stock_prices p ON f.stock_id = p.stock_id AND f.trade_date = p.trade_date
        WHERE f.trade_date >= CURRENT_DATE - :lookback_days
          AND p.close_price > 0
    ),
    -- 只計算淨買入日的加權平均成本
    cost_calc AS (
        SELECT
            stock_id,
            SUM(CASE WHEN total_net > 0 THEN total_net * close_price ELSE 0 END) as weighted_cost,
            SUM(CASE WHEN total_net > 0 THEN total_net ELSE 0 END) as total_shares,
            COUNT(CASE WHEN total_net > 0 THEN 1 END) as buy_days
        FROM inst_flows
        GROUP BY stock_id
        HAVING SUM(CASE WHEN total_net > 0 THEN total_net ELSE 0 END) > 0
    ),
    -- 計算平均成本與現價差距
    below_cost AS (
        SELECT
            c.stock_id,
            lp.close_price as current_price,
            ROUND(c.weighted_cost / c.total_shares, 2) as avg_cost,
            c.buy_days,
            c.total_shares,
            ROUND((lp.close_price - c.weighted_cost / c.total_shares) / (c.weighted_cost / c.total_shares) * 100, 2) as discount_pct,
            CASE
                WHEN lp.close_price >= 500 THEN 'high'
                WHEN lp.close_price >= 200 THEN 'mid'
                ELSE 'low'
            END as price_tier
        FROM cost_calc c
        JOIN latest_prices lp ON c.stock_id = lp.stock_id
        WHERE lp.close_price < (c.weighted_cost / c.total_shares)  -- 現價低於平均成本
          AND c.buy_days >= 3  -- 至少有3天買進記錄
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY price_tier ORDER BY discount_pct ASC) as rank
        FROM below_cost
    )
    INSERT INTO strategy_rankings (
        stock_id, price_tier, metric_type,
        avg_return, win_rate, signal_count, current_price, rank_in_tier
    )
    SELECT
        stock_id, price_tier, :metric_type,
        avg_cost,       -- 借用 avg_return 欄位存平均成本
        discount_pct,   -- 借用 win_rate 欄位存折價率
        buy_days,       -- 借用 signal_count 欄位存買進天數
        current_price,
        rank
    FROM ranked
    WHERE rank <= 15
    """)

    result = db.execute(query, {
        "lookback_days": lookback_days,
        "metric_type": metric_type
    })
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


def compute_consecutive_buying(db, min_days: int = 5):
    """
    計算外資連續買超排行。
    找出外資連續買超天數最多的股票。
    """
    metric_type = "consecutive_buying"
    logger.info(f"Computing {metric_type}...")

    db.execute(text("DELETE FROM strategy_rankings WHERE metric_type = :metric_type"),
               {"metric_type": metric_type})

    query = text("""
    WITH latest_prices AS (
        SELECT DISTINCT ON (stock_id)
            stock_id, close_price, trade_date
        FROM stock_prices
        ORDER BY stock_id, trade_date DESC
    ),
    -- 計算每個股票最近的連續買超天數
    consecutive AS (
        SELECT
            f.stock_id,
            f.trade_date,
            f.foreign_net,
            CASE WHEN f.foreign_net > 0 THEN 1 ELSE 0 END as is_buy,
            ROW_NUMBER() OVER (PARTITION BY f.stock_id ORDER BY f.trade_date DESC) as rn
        FROM institutional_flows f
        WHERE f.trade_date >= CURRENT_DATE - 30
    ),
    -- 找出連續買超的起點
    streak_calc AS (
        SELECT
            stock_id,
            COUNT(*) FILTER (WHERE is_buy = 1) as consecutive_days,
            SUM(foreign_net) FILTER (WHERE is_buy = 1) as total_net_buy
        FROM (
            SELECT *,
                SUM(CASE WHEN is_buy = 0 THEN 1 ELSE 0 END) OVER (
                    PARTITION BY stock_id ORDER BY rn
                ) as grp
            FROM consecutive
        ) sub
        WHERE grp = 0 AND is_buy = 1
        GROUP BY stock_id
        HAVING COUNT(*) >= :min_days
    ),
    ranked AS (
        SELECT
            sc.stock_id,
            lp.close_price as current_price,
            sc.consecutive_days,
            sc.total_net_buy,
            CASE
                WHEN lp.close_price >= 500 THEN 'high'
                WHEN lp.close_price >= 200 THEN 'mid'
                ELSE 'low'
            END as price_tier,
            ROW_NUMBER() OVER (
                PARTITION BY CASE
                    WHEN lp.close_price >= 500 THEN 'high'
                    WHEN lp.close_price >= 200 THEN 'mid'
                    ELSE 'low'
                END
                ORDER BY sc.consecutive_days DESC, sc.total_net_buy DESC
            ) as rank
        FROM streak_calc sc
        JOIN latest_prices lp ON sc.stock_id = lp.stock_id
    )
    INSERT INTO strategy_rankings (
        stock_id, price_tier, metric_type,
        signal_count, avg_return, current_price, rank_in_tier
    )
    SELECT
        stock_id, price_tier, :metric_type,
        consecutive_days,  -- 借用 signal_count 存連續天數
        total_net_buy,     -- 借用 avg_return 存總買超量
        current_price, rank
    FROM ranked
    WHERE rank <= 15
    """)

    result = db.execute(query, {"min_days": min_days, "metric_type": metric_type})
    db.commit()
    logger.info(f"  Inserted {result.rowcount} rankings for {metric_type}")
    return result.rowcount


def compute_trust_accumulation(db, lookback_days: int = 20):
    """
    計算投信認養股排行。
    找出投信近期持續加碼、持股比例創新高的股票。
    """
    metric_type = "trust_accumulation"
    logger.info(f"Computing {metric_type}...")

    db.execute(text("DELETE FROM strategy_rankings WHERE metric_type = :metric_type"),
               {"metric_type": metric_type})

    query = text("""
    WITH latest_prices AS (
        SELECT DISTINCT ON (stock_id)
            stock_id, close_price
        FROM stock_prices
        ORDER BY stock_id, trade_date DESC
    ),
    -- 計算投信近期買超情況
    trust_activity AS (
        SELECT
            f.stock_id,
            SUM(f.trust_net) as total_trust_net,
            COUNT(*) FILTER (WHERE f.trust_net > 0) as buy_days,
            COUNT(*) as total_days,
            SUM(f.trust_net) FILTER (WHERE f.trust_net > 0) as total_buy_amount
        FROM institutional_flows f
        WHERE f.trade_date >= CURRENT_DATE - :lookback_days
        GROUP BY f.stock_id
        HAVING SUM(f.trust_net) > 0
           AND COUNT(*) FILTER (WHERE f.trust_net > 0) >= 3
    ),
    -- 計算投信持股比例變化
    ratio_change AS (
        SELECT
            r.stock_id,
            MAX(r.trust_ratio_est) FILTER (WHERE r.trade_date >= CURRENT_DATE - 5) as recent_ratio,
            AVG(r.trust_ratio_est) FILTER (WHERE r.trade_date < CURRENT_DATE - 5) as prev_ratio
        FROM institutional_ratios r
        WHERE r.trade_date >= CURRENT_DATE - :lookback_days
        GROUP BY r.stock_id
    ),
    combined AS (
        SELECT
            ta.stock_id,
            lp.close_price as current_price,
            ta.total_trust_net,
            ta.buy_days,
            ta.total_days,
            ROUND(ta.buy_days * 100.0 / NULLIF(ta.total_days, 0), 1) as buy_ratio,
            ROUND((rc.recent_ratio - rc.prev_ratio), 4) as ratio_increase,
            CASE
                WHEN lp.close_price >= 500 THEN 'high'
                WHEN lp.close_price >= 200 THEN 'mid'
                ELSE 'low'
            END as price_tier
        FROM trust_activity ta
        JOIN latest_prices lp ON ta.stock_id = lp.stock_id
        LEFT JOIN ratio_change rc ON ta.stock_id = rc.stock_id
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY price_tier
                ORDER BY buy_ratio DESC, total_trust_net DESC
            ) as rank
        FROM combined
    )
    INSERT INTO strategy_rankings (
        stock_id, price_tier, metric_type,
        signal_count, avg_return, win_rate, current_price, rank_in_tier
    )
    SELECT
        stock_id, price_tier, :metric_type,
        buy_days,           -- 買超天數
        total_trust_net,    -- 總買超量
        buy_ratio,          -- 買超比例
        current_price, rank
    FROM ranked
    WHERE rank <= 15
    """)

    result = db.execute(query, {"lookback_days": lookback_days, "metric_type": metric_type})
    db.commit()
    logger.info(f"  Inserted {result.rowcount} rankings for {metric_type}")
    return result.rowcount


def compute_synchronized_buying(db, lookback_days: int = 10):
    """
    計算三大法人同步買超排行。
    找出外資、投信、自營商同時買超的股票。
    """
    metric_type = "synchronized_buying"
    logger.info(f"Computing {metric_type}...")

    db.execute(text("DELETE FROM strategy_rankings WHERE metric_type = :metric_type"),
               {"metric_type": metric_type})

    query = text("""
    WITH latest_prices AS (
        SELECT DISTINCT ON (stock_id)
            stock_id, close_price
        FROM stock_prices
        ORDER BY stock_id, trade_date DESC
    ),
    -- 找出三大法人同步買超的日子
    sync_days AS (
        SELECT
            f.stock_id,
            f.trade_date,
            f.foreign_net,
            f.trust_net,
            f.dealer_net,
            f.foreign_net + f.trust_net + f.dealer_net as total_net
        FROM institutional_flows f
        WHERE f.trade_date >= CURRENT_DATE - :lookback_days
          AND f.foreign_net > 0
          AND f.trust_net > 0
          AND f.dealer_net > 0
    ),
    -- 統計同步買超情況
    sync_stats AS (
        SELECT
            stock_id,
            COUNT(*) as sync_days_count,
            SUM(total_net) as total_sync_amount,
            SUM(foreign_net) as foreign_total,
            SUM(trust_net) as trust_total,
            SUM(dealer_net) as dealer_total
        FROM sync_days
        GROUP BY stock_id
        HAVING COUNT(*) >= 2
    ),
    combined AS (
        SELECT
            ss.stock_id,
            lp.close_price as current_price,
            ss.sync_days_count,
            ss.total_sync_amount,
            ss.foreign_total,
            ss.trust_total,
            ss.dealer_total,
            CASE
                WHEN lp.close_price >= 500 THEN 'high'
                WHEN lp.close_price >= 200 THEN 'mid'
                ELSE 'low'
            END as price_tier
        FROM sync_stats ss
        JOIN latest_prices lp ON ss.stock_id = lp.stock_id
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY price_tier
                ORDER BY sync_days_count DESC, total_sync_amount DESC
            ) as rank
        FROM combined
    )
    INSERT INTO strategy_rankings (
        stock_id, price_tier, metric_type,
        signal_count, avg_return, correlation, data_points, current_price, rank_in_tier
    )
    SELECT
        stock_id, price_tier, :metric_type,
        sync_days_count,      -- 同步天數
        total_sync_amount,    -- 總買超量
        foreign_total,        -- 外資買超
        trust_total,          -- 投信買超 (借用 data_points)
        current_price, rank
    FROM ranked
    WHERE rank <= 15
    """)

    result = db.execute(query, {"lookback_days": lookback_days, "metric_type": metric_type})
    db.commit()
    logger.info(f"  Inserted {result.rowcount} rankings for {metric_type}")
    return result.rowcount


def compute_price_deviation(db, lookback_days: int = 60):
    """
    計算股價乖離過大排行。
    找出股價大幅偏離法人平均成本的股票（可能超漲或超跌）。
    """
    metric_type = "price_deviation"
    logger.info(f"Computing {metric_type}...")

    db.execute(text("DELETE FROM strategy_rankings WHERE metric_type = :metric_type"),
               {"metric_type": metric_type})

    query = text("""
    WITH latest_prices AS (
        SELECT DISTINCT ON (stock_id)
            stock_id, close_price, trade_date
        FROM stock_prices
        ORDER BY stock_id, trade_date DESC
    ),
    -- 計算法人平均成本
    inst_cost AS (
        SELECT
            f.stock_id,
            SUM(CASE WHEN (f.foreign_net + f.trust_net + f.dealer_net) > 0
                THEN (f.foreign_net + f.trust_net + f.dealer_net) * p.close_price
                ELSE 0 END) as weighted_cost,
            SUM(CASE WHEN (f.foreign_net + f.trust_net + f.dealer_net) > 0
                THEN (f.foreign_net + f.trust_net + f.dealer_net)
                ELSE 0 END) as total_shares
        FROM institutional_flows f
        JOIN stock_prices p ON f.stock_id = p.stock_id AND f.trade_date = p.trade_date
        WHERE f.trade_date >= CURRENT_DATE - :lookback_days
          AND p.close_price > 0
        GROUP BY f.stock_id
        HAVING SUM(CASE WHEN (f.foreign_net + f.trust_net + f.dealer_net) > 0
                   THEN (f.foreign_net + f.trust_net + f.dealer_net) ELSE 0 END) > 0
    ),
    deviation_calc AS (
        SELECT
            ic.stock_id,
            lp.close_price as current_price,
            ROUND(ic.weighted_cost / ic.total_shares, 2) as avg_cost,
            ROUND((lp.close_price - ic.weighted_cost / ic.total_shares)
                  / (ic.weighted_cost / ic.total_shares) * 100, 2) as deviation_pct,
            CASE
                WHEN lp.close_price >= 500 THEN 'high'
                WHEN lp.close_price >= 200 THEN 'mid'
                ELSE 'low'
            END as price_tier
        FROM inst_cost ic
        JOIN latest_prices lp ON ic.stock_id = lp.stock_id
        WHERE ABS((lp.close_price - ic.weighted_cost / ic.total_shares)
              / (ic.weighted_cost / ic.total_shares) * 100) >= 10  -- 乖離超過10%
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY price_tier
                ORDER BY ABS(deviation_pct) DESC
            ) as rank
        FROM deviation_calc
    )
    INSERT INTO strategy_rankings (
        stock_id, price_tier, metric_type,
        avg_return, win_rate, current_price, rank_in_tier
    )
    SELECT
        stock_id, price_tier, :metric_type,
        avg_cost,        -- 法人成本
        deviation_pct,   -- 乖離率
        current_price, rank
    FROM ranked
    WHERE rank <= 15
    """)

    result = db.execute(query, {"lookback_days": lookback_days, "metric_type": metric_type})
    db.commit()
    logger.info(f"  Inserted {result.rowcount} rankings for {metric_type}")
    return result.rowcount


def run_all_computations(db):
    """Run all strategy computations."""
    logger.info("Starting strategy computations...")

    # Win rate rankings for different periods
    for days in [5, 10, 30]:
        compute_win_rate_rankings(db, holding_days=days, min_signals=2)

    # Correlation rankings
    compute_correlation_rankings(db, min_data_points=5)

    # Below cost rankings (現價低於法人成本)
    compute_below_cost_rankings(db, lookback_days=60)

    # 新增策略
    # 外資連續買超
    compute_consecutive_buying(db, min_days=3)

    # 投信認養股
    compute_trust_accumulation(db, lookback_days=20)

    # 三大法人同步買超
    compute_synchronized_buying(db, lookback_days=10)

    # 股價乖離過大
    compute_price_deviation(db, lookback_days=60)

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
