"""Industry analysis routes - sector flows and heatmap data."""
from typing import Optional
from datetime import date
from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api.dependencies import get_db

router = APIRouter()


def ensure_industry_column(db: Session):
    """確保 industry 欄位存在。"""
    try:
        check_query = text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'stocks' AND column_name = 'industry'
        """)
        result = db.execute(check_query).fetchone()

        if not result:
            # 新增欄位
            db.execute(text("ALTER TABLE stocks ADD COLUMN IF NOT EXISTS industry VARCHAR(50)"))
            db.execute(text("CREATE INDEX IF NOT EXISTS idx_stocks_industry ON stocks(industry)"))
            db.commit()

        # 更新產業資料
        from src.etl.fetchers.industry import update_stock_industries
        update_stock_industries(db)

    except Exception as e:
        print(f"[WARN] Failed to ensure industry column: {e}")
        db.rollback()


@router.get("/summary")
def get_industry_summary(
    days: int = Query(5, description="Look back days", ge=1, le=30),
    db: Session = Depends(get_db),
):
    """
    取得產業資金流向摘要。
    統計各產業近 N 天的三大法人買賣超情況。
    """
    ensure_industry_column(db)

    query = text("""
    WITH industry_flows AS (
        SELECT
            COALESCE(s.industry, '其他業') as industry,
            SUM(f.foreign_net) as foreign_net,
            SUM(f.trust_net) as trust_net,
            SUM(f.dealer_net) as dealer_net,
            SUM(f.foreign_net + f.trust_net + f.dealer_net) as total_net,
            COUNT(DISTINCT s.code) as stock_count
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date >= CURRENT_DATE - :days
        GROUP BY COALESCE(s.industry, '其他業')
    )
    SELECT
        industry,
        foreign_net,
        trust_net,
        dealer_net,
        total_net,
        stock_count,
        CASE
            WHEN total_net > 0 THEN '買超'
            WHEN total_net < 0 THEN '賣超'
            ELSE '持平'
        END as direction
    FROM industry_flows
    ORDER BY ABS(total_net) DESC
    """)

    results = db.execute(query, {"days": days}).fetchall()

    items = [
        {
            "industry": r.industry,
            "foreign_net": r.foreign_net or 0,
            "trust_net": r.trust_net or 0,
            "dealer_net": r.dealer_net or 0,
            "total_net": r.total_net or 0,
            "stock_count": r.stock_count or 0,
            "direction": r.direction,
        }
        for r in results
    ]

    return {
        "days": days,
        "total": len(items),
        "items": items
    }


@router.get("/heatmap")
def get_industry_heatmap(
    days: int = Query(5, description="Look back days", ge=1, le=30),
    db: Session = Depends(get_db),
):
    """
    取得產業熱力圖資料。
    顯示各產業的法人買賣超強度，用於視覺化熱力圖。
    """
    ensure_industry_column(db)

    query = text("""
    WITH daily_flows AS (
        SELECT
            f.trade_date,
            COALESCE(s.industry, '其他業') as industry,
            SUM(f.foreign_net + f.trust_net + f.dealer_net) as daily_net
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date >= CURRENT_DATE - :days
        GROUP BY f.trade_date, COALESCE(s.industry, '其他業')
    ),
    industry_stats AS (
        SELECT
            industry,
            SUM(daily_net) as total_net,
            AVG(daily_net) as avg_daily_net,
            COUNT(*) as trading_days,
            STDDEV(daily_net) as volatility
        FROM daily_flows
        GROUP BY industry
    ),
    normalized AS (
        SELECT
            *,
            -- 計算標準化分數 (-100 to 100)
            CASE
                WHEN (SELECT MAX(ABS(total_net)) FROM industry_stats) > 0
                THEN ROUND(total_net * 100.0 / (SELECT MAX(ABS(total_net)) FROM industry_stats), 1)
                ELSE 0
            END as intensity
        FROM industry_stats
    )
    SELECT * FROM normalized
    ORDER BY intensity DESC
    """)

    results = db.execute(query, {"days": days}).fetchall()

    items = [
        {
            "industry": r.industry,
            "total_net": r.total_net or 0,
            "avg_daily_net": round(r.avg_daily_net or 0, 0),
            "intensity": float(r.intensity or 0),
            "trading_days": r.trading_days or 0,
        }
        for r in results
    ]

    return {
        "days": days,
        "total": len(items),
        "items": items
    }


@router.get("/rotation")
def get_industry_rotation(
    db: Session = Depends(get_db),
):
    """
    取得產業輪動分析。
    比較各產業短期(5天)與中期(20天)的資金流向變化。
    """
    ensure_industry_column(db)

    query = text("""
    WITH short_term AS (
        SELECT
            COALESCE(s.industry, '其他業') as industry,
            SUM(f.foreign_net + f.trust_net + f.dealer_net) as net_5d
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date >= CURRENT_DATE - 5
        GROUP BY COALESCE(s.industry, '其他業')
    ),
    mid_term AS (
        SELECT
            COALESCE(s.industry, '其他業') as industry,
            SUM(f.foreign_net + f.trust_net + f.dealer_net) as net_20d
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date >= CURRENT_DATE - 20
        GROUP BY COALESCE(s.industry, '其他業')
    ),
    combined AS (
        SELECT
            COALESCE(st.industry, mt.industry) as industry,
            COALESCE(st.net_5d, 0) as net_5d,
            COALESCE(mt.net_20d, 0) as net_20d,
            COALESCE(st.net_5d, 0) - COALESCE(mt.net_20d, 0) / 4 as momentum
        FROM short_term st
        FULL OUTER JOIN mid_term mt ON st.industry = mt.industry
    )
    SELECT
        industry,
        net_5d,
        net_20d,
        momentum,
        CASE
            WHEN net_5d > 0 AND net_20d > 0 AND momentum > 0 THEN '強勢加碼'
            WHEN net_5d > 0 AND net_20d < 0 THEN '轉強'
            WHEN net_5d < 0 AND net_20d > 0 THEN '轉弱'
            WHEN net_5d < 0 AND net_20d < 0 AND momentum < 0 THEN '持續弱勢'
            WHEN net_5d > 0 THEN '短期買超'
            WHEN net_5d < 0 THEN '短期賣超'
            ELSE '觀望'
        END as status
    FROM combined
    ORDER BY momentum DESC
    """)

    results = db.execute(query).fetchall()

    items = [
        {
            "industry": r.industry,
            "net_5d": r.net_5d or 0,
            "net_20d": r.net_20d or 0,
            "momentum": r.momentum or 0,
            "status": r.status,
        }
        for r in results
    ]

    return {
        "periods": {"short": 5, "mid": 20},
        "total": len(items),
        "items": items
    }


@router.get("/{industry}/stocks")
def get_industry_stocks(
    industry: str,
    days: int = Query(5, description="Look back days", ge=1, le=30),
    limit: int = Query(30, le=100),
    db: Session = Depends(get_db),
):
    """
    取得特定產業的股票列表及法人動向。
    """
    ensure_industry_column(db)

    query = text("""
    WITH stock_flows AS (
        SELECT
            s.code,
            s.name,
            SUM(f.foreign_net) as foreign_net,
            SUM(f.trust_net) as trust_net,
            SUM(f.dealer_net) as dealer_net,
            SUM(f.foreign_net + f.trust_net + f.dealer_net) as total_net
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE s.industry = :industry
          AND f.trade_date >= CURRENT_DATE - :days
        GROUP BY s.code, s.name
    ),
    with_prices AS (
        SELECT
            sf.*,
            lp.close_price as current_price,
            lp.change_percent
        FROM stock_flows sf
        LEFT JOIN LATERAL (
            SELECT close_price, change_percent
            FROM stock_prices sp
            JOIN stocks s ON sp.stock_id = s.id
            WHERE s.code = sf.code
            ORDER BY sp.trade_date DESC
            LIMIT 1
        ) lp ON true
    )
    SELECT * FROM with_prices
    ORDER BY ABS(total_net) DESC
    LIMIT :limit
    """)

    results = db.execute(query, {
        "industry": industry,
        "days": days,
        "limit": limit
    }).fetchall()

    items = [
        {
            "code": r.code,
            "name": r.name,
            "current_price": float(r.current_price) if r.current_price else None,
            "change_percent": float(r.change_percent) if r.change_percent else None,
            "foreign_net": r.foreign_net or 0,
            "trust_net": r.trust_net or 0,
            "dealer_net": r.dealer_net or 0,
            "total_net": r.total_net or 0,
        }
        for r in results
    ]

    return {
        "industry": industry,
        "days": days,
        "total": len(items),
        "items": items
    }


@router.get("/list")
def get_industry_list(db: Session = Depends(get_db)):
    """
    取得所有產業類別列表。
    """
    ensure_industry_column(db)

    query = text("""
    SELECT
        COALESCE(industry, '其他業') as industry,
        COUNT(*) as stock_count
    FROM stocks
    WHERE is_active = true
    GROUP BY COALESCE(industry, '其他業')
    ORDER BY stock_count DESC
    """)

    results = db.execute(query).fetchall()

    items = [
        {
            "industry": r.industry,
            "stock_count": r.stock_count,
        }
        for r in results
    ]

    return {"total": len(items), "items": items}
