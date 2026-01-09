"""月營收 API endpoints."""
from datetime import date
from typing import Optional, List
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api.dependencies import get_db

router = APIRouter()


@router.get("/latest")
def get_latest_revenue(
    year: Optional[int] = None,
    month: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """Get latest monthly revenue data."""
    if year is None or month is None:
        # Get the latest year/month from database
        result = db.execute(text("""
            SELECT year, month FROM monthly_revenue
            ORDER BY year DESC, month DESC
            LIMIT 1
        """)).mappings().first()

        if result is None:
            return {"error": "No revenue data available", "data": []}

        year = result["year"]
        month = result["month"]

    query = text("""
        SELECT
            s.code,
            s.name,
            r.year,
            r.month,
            r.revenue,
            r.mom_change,
            r.yoy_change,
            r.cumulative_revenue,
            r.cumulative_yoy_change
        FROM monthly_revenue r
        JOIN stocks s ON r.stock_id = s.id
        WHERE r.year = :year AND r.month = :month
          AND s.is_active = true
        ORDER BY r.revenue DESC NULLS LAST
    """)

    results = db.execute(query, {"year": year, "month": month}).mappings().all()

    return {
        "year": year,
        "month": month,
        "count": len(results),
        "data": [
            {
                "code": r["code"],
                "name": r["name"],
                "revenue": r["revenue"],
                "mom_change": float(r["mom_change"]) if r["mom_change"] else None,
                "yoy_change": float(r["yoy_change"]) if r["yoy_change"] else None,
                "cumulative_revenue": r["cumulative_revenue"],
                "cumulative_yoy_change": float(r["cumulative_yoy_change"]) if r["cumulative_yoy_change"] else None,
            }
            for r in results
        ]
    }


@router.get("/rankings")
def get_revenue_rankings(
    sort_by: str = Query("yoy_change", description="Sort by: revenue, mom_change, yoy_change, cumulative_yoy_change"),
    order: str = Query("desc", description="Sort order: asc or desc"),
    limit: int = Query(50, ge=1, le=200),
    year: Optional[int] = None,
    month: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """Get stocks ranked by revenue metrics."""
    if year is None or month is None:
        result = db.execute(text("""
            SELECT year, month FROM monthly_revenue
            ORDER BY year DESC, month DESC
            LIMIT 1
        """)).mappings().first()

        if result is None:
            return {"error": "No revenue data available", "data": []}

        year = result["year"]
        month = result["month"]

    valid_sort_fields = {
        "revenue": "r.revenue",
        "mom_change": "r.mom_change",
        "yoy_change": "r.yoy_change",
        "cumulative_yoy_change": "r.cumulative_yoy_change",
    }

    sort_field = valid_sort_fields.get(sort_by, "r.yoy_change")
    sort_order = "DESC" if order.lower() == "desc" else "ASC"

    query = text(f"""
        SELECT
            s.code,
            s.name,
            r.revenue,
            r.mom_change,
            r.yoy_change,
            r.cumulative_revenue,
            r.cumulative_yoy_change,
            p.close_price
        FROM monthly_revenue r
        JOIN stocks s ON r.stock_id = s.id
        LEFT JOIN LATERAL (
            SELECT close_price FROM stock_prices
            WHERE stock_id = s.id
            ORDER BY trade_date DESC
            LIMIT 1
        ) p ON true
        WHERE r.year = :year AND r.month = :month
          AND s.is_active = true
          AND r.revenue > 0
        ORDER BY {sort_field} {sort_order} NULLS LAST
        LIMIT :limit
    """)

    results = db.execute(query, {"year": year, "month": month, "limit": limit}).mappings().all()

    return {
        "year": year,
        "month": month,
        "sort_by": sort_by,
        "order": order,
        "data": [
            {
                "code": r["code"],
                "name": r["name"],
                "close_price": float(r["close_price"]) if r["close_price"] else None,
                "revenue": r["revenue"],
                "mom_change": float(r["mom_change"]) if r["mom_change"] else None,
                "yoy_change": float(r["yoy_change"]) if r["yoy_change"] else None,
                "cumulative_revenue": r["cumulative_revenue"],
                "cumulative_yoy_change": float(r["cumulative_yoy_change"]) if r["cumulative_yoy_change"] else None,
            }
            for r in results
        ]
    }


@router.get("/stock/{stock_code}")
def get_stock_revenue(
    stock_code: str,
    months: int = Query(24, ge=1, le=120, description="Number of months of history"),
    db: Session = Depends(get_db)
):
    """Get revenue history for a specific stock."""
    query = text("""
        SELECT
            r.year,
            r.month,
            r.revenue,
            r.mom_change,
            r.yoy_change,
            r.cumulative_revenue,
            r.cumulative_yoy_change
        FROM monthly_revenue r
        JOIN stocks s ON r.stock_id = s.id
        WHERE s.code = :code
        ORDER BY r.year DESC, r.month DESC
        LIMIT :months
    """)

    results = db.execute(query, {"code": stock_code, "months": months}).mappings().all()

    if not results:
        # Check if stock exists
        stock_check = db.execute(
            text("SELECT code, name FROM stocks WHERE code = :code"),
            {"code": stock_code}
        ).mappings().first()

        if not stock_check:
            raise HTTPException(status_code=404, detail="Stock not found")

        return {
            "code": stock_code,
            "name": stock_check["name"],
            "data": []
        }

    # Get stock name
    stock_name = db.execute(
        text("SELECT name FROM stocks WHERE code = :code"),
        {"code": stock_code}
    ).scalar()

    return {
        "code": stock_code,
        "name": stock_name,
        "data": [
            {
                "year": r["year"],
                "month": r["month"],
                "revenue": r["revenue"],
                "mom_change": float(r["mom_change"]) if r["mom_change"] else None,
                "yoy_change": float(r["yoy_change"]) if r["yoy_change"] else None,
                "cumulative_revenue": r["cumulative_revenue"],
                "cumulative_yoy_change": float(r["cumulative_yoy_change"]) if r["cumulative_yoy_change"] else None,
            }
            for r in results
        ]
    }


@router.get("/alerts")
def get_revenue_alerts(
    yoy_min: float = Query(50.0, description="Minimum YoY growth (%) for positive alert"),
    yoy_max: float = Query(-30.0, description="Maximum YoY growth (%) for negative alert"),
    db: Session = Depends(get_db)
):
    """Get stocks with significant revenue changes (growth or decline)."""
    # Get latest year/month
    result = db.execute(text("""
        SELECT year, month FROM monthly_revenue
        ORDER BY year DESC, month DESC
        LIMIT 1
    """)).mappings().first()

    if result is None:
        return {"error": "No revenue data available", "data": {"growth": [], "decline": []}}

    year = result["year"]
    month = result["month"]

    # High growth stocks
    growth_query = text("""
        SELECT
            s.code,
            s.name,
            r.revenue,
            r.yoy_change,
            r.mom_change
        FROM monthly_revenue r
        JOIN stocks s ON r.stock_id = s.id
        WHERE r.year = :year AND r.month = :month
          AND s.is_active = true
          AND r.yoy_change >= :yoy_min
        ORDER BY r.yoy_change DESC
        LIMIT 30
    """)

    growth_results = db.execute(growth_query, {
        "year": year,
        "month": month,
        "yoy_min": yoy_min
    }).mappings().all()

    # Declining stocks
    decline_query = text("""
        SELECT
            s.code,
            s.name,
            r.revenue,
            r.yoy_change,
            r.mom_change
        FROM monthly_revenue r
        JOIN stocks s ON r.stock_id = s.id
        WHERE r.year = :year AND r.month = :month
          AND s.is_active = true
          AND r.yoy_change <= :yoy_max
        ORDER BY r.yoy_change ASC
        LIMIT 30
    """)

    decline_results = db.execute(decline_query, {
        "year": year,
        "month": month,
        "yoy_max": yoy_max
    }).mappings().all()

    return {
        "year": year,
        "month": month,
        "thresholds": {
            "growth": yoy_min,
            "decline": yoy_max
        },
        "data": {
            "growth": [
                {
                    "code": r["code"],
                    "name": r["name"],
                    "revenue": r["revenue"],
                    "yoy_change": float(r["yoy_change"]) if r["yoy_change"] else None,
                    "mom_change": float(r["mom_change"]) if r["mom_change"] else None,
                }
                for r in growth_results
            ],
            "decline": [
                {
                    "code": r["code"],
                    "name": r["name"],
                    "revenue": r["revenue"],
                    "yoy_change": float(r["yoy_change"]) if r["yoy_change"] else None,
                    "mom_change": float(r["mom_change"]) if r["mom_change"] else None,
                }
                for r in decline_results
            ]
        }
    }
