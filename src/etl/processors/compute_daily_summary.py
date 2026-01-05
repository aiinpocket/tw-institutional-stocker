"""Compute and cache daily summary for the live dashboard."""
import json
import logging
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def compute_daily_summary(db):
    """
    Compute and cache daily market summary.
    This avoids repeated expensive queries when users visit the live dashboard.
    """
    logger.info("Computing daily summary cache...")

    # Get the latest trade date
    latest_date = db.execute(text("""
        SELECT MAX(trade_date) FROM stock_prices
    """)).scalar()

    if not latest_date:
        logger.warning("No price data found, skipping summary computation")
        return

    logger.info(f"Computing summary for {latest_date}")

    # 1. Market summary (up/down/unchanged)
    market_summary = db.execute(text("""
        SELECT
            COUNT(*) as total,
            SUM(CASE
                WHEN change_percent > 0 THEN 1
                WHEN change_percent IS NULL AND close_price > open_price THEN 1
                ELSE 0
            END) as up_count,
            SUM(CASE
                WHEN change_percent < 0 THEN 1
                WHEN change_percent IS NULL AND close_price < open_price THEN 1
                ELSE 0
            END) as down_count,
            SUM(CASE
                WHEN change_percent = 0 THEN 1
                WHEN change_percent IS NULL AND close_price = open_price THEN 1
                ELSE 0
            END) as unchanged_count
        FROM stock_prices
        WHERE trade_date = :trade_date
          AND open_price IS NOT NULL
          AND close_price IS NOT NULL
    """), {"trade_date": latest_date}).fetchone()

    # 2. Institutional flow summary
    flow_summary = db.execute(text("""
        SELECT
            SUM(foreign_net) as foreign_total,
            SUM(trust_net) as trust_total,
            SUM(dealer_net) as dealer_total
        FROM institutional_flows
        WHERE trade_date = :trade_date
    """), {"trade_date": latest_date}).fetchone()

    # 3. Top 10 foreign buy/sell
    foreign_buy = db.execute(text("""
        SELECT s.code, s.name, f.foreign_net
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date = :trade_date AND f.foreign_net > 0
        ORDER BY f.foreign_net DESC
        LIMIT 10
    """), {"trade_date": latest_date}).fetchall()

    foreign_sell = db.execute(text("""
        SELECT s.code, s.name, f.foreign_net
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date = :trade_date AND f.foreign_net < 0
        ORDER BY f.foreign_net ASC
        LIMIT 10
    """), {"trade_date": latest_date}).fetchall()

    # 4. Top 10 trust buy/sell
    trust_buy = db.execute(text("""
        SELECT s.code, s.name, f.trust_net
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date = :trade_date AND f.trust_net > 0
        ORDER BY f.trust_net DESC
        LIMIT 10
    """), {"trade_date": latest_date}).fetchall()

    trust_sell = db.execute(text("""
        SELECT s.code, s.name, f.trust_net
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date = :trade_date AND f.trust_net < 0
        ORDER BY f.trust_net ASC
        LIMIT 10
    """), {"trade_date": latest_date}).fetchall()

    # 5. Industry summary (hot industries)
    industry_summary = db.execute(text("""
        SELECT
            s.industry,
            SUM(f.foreign_net + f.trust_net + f.dealer_net) as total_net,
            COUNT(*) as stock_count
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date = :trade_date
          AND s.industry IS NOT NULL
          AND s.industry != '其他業'
        GROUP BY s.industry
        ORDER BY ABS(SUM(f.foreign_net + f.trust_net + f.dealer_net)) DESC
        LIMIT 10
    """), {"trade_date": latest_date}).fetchall()

    # Build summary JSON
    summary_data = {
        "date": str(latest_date),
        "market": {
            "total": market_summary.total or 0,
            "up": market_summary.up_count or 0,
            "down": market_summary.down_count or 0,
            "unchanged": market_summary.unchanged_count or 0,
        },
        "institutional_flow": {
            "foreign": flow_summary.foreign_total or 0 if flow_summary else 0,
            "trust": flow_summary.trust_total or 0 if flow_summary else 0,
            "dealer": flow_summary.dealer_total or 0 if flow_summary else 0,
        },
        "foreign_buy_top10": [
            {"code": r.code, "name": r.name, "net": r.foreign_net}
            for r in foreign_buy
        ],
        "foreign_sell_top10": [
            {"code": r.code, "name": r.name, "net": r.foreign_net}
            for r in foreign_sell
        ],
        "trust_buy_top10": [
            {"code": r.code, "name": r.name, "net": r.trust_net}
            for r in trust_buy
        ],
        "trust_sell_top10": [
            {"code": r.code, "name": r.name, "net": r.trust_net}
            for r in trust_sell
        ],
        "hot_industries": [
            {"industry": r.industry, "total_net": r.total_net, "count": r.stock_count}
            for r in industry_summary
        ],
    }

    # Upsert into cache table
    db.execute(text("""
        INSERT INTO daily_summary_cache (trade_date, summary_data)
        VALUES (:trade_date, :summary_data)
        ON CONFLICT (trade_date) DO UPDATE SET
            summary_data = :summary_data,
            created_at = CURRENT_TIMESTAMP
    """), {
        "trade_date": latest_date,
        "summary_data": json.dumps(summary_data, ensure_ascii=False)
    })
    db.commit()

    logger.info(f"Daily summary cached for {latest_date}")
    return summary_data


if __name__ == "__main__":
    from src.common.database import SessionLocal
    db = SessionLocal()
    try:
        result = compute_daily_summary(db)
        print(json.dumps(result, indent=2, ensure_ascii=False))
    finally:
        db.close()
