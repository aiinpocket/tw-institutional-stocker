"""融資融券 API endpoints."""
from datetime import date, timedelta
from typing import Optional, List
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api.dependencies import get_db

router = APIRouter()


@router.get("/summary")
def get_margin_summary(
    trade_date: Optional[date] = None,
    db: Session = Depends(get_db)
):
    """Get market-wide margin trading summary for a given date."""
    if trade_date is None:
        # Get the latest date from database
        result = db.execute(text("SELECT MAX(trade_date) FROM margin_trading")).scalar()
        if result is None:
            return {"error": "No margin data available", "data": None}
        trade_date = result

    query = text("""
        SELECT
            COUNT(DISTINCT stock_id) as stock_count,
            SUM(margin_buy) as total_margin_buy,
            SUM(margin_sell) as total_margin_sell,
            SUM(margin_balance) as total_margin_balance,
            SUM(short_sell) as total_short_sell,
            SUM(short_buy) as total_short_buy,
            SUM(short_balance) as total_short_balance,
            SUM(offset) as total_offset
        FROM margin_trading
        WHERE trade_date = :trade_date
    """)

    result = db.execute(query, {"trade_date": trade_date}).mappings().first()

    if not result or result["stock_count"] == 0:
        return {"error": "No margin data for this date", "data": None, "date": str(trade_date)}

    return {
        "date": str(trade_date),
        "data": {
            "stock_count": result["stock_count"],
            "margin": {
                "buy": result["total_margin_buy"] or 0,
                "sell": result["total_margin_sell"] or 0,
                "balance": result["total_margin_balance"] or 0,
                "net": (result["total_margin_buy"] or 0) - (result["total_margin_sell"] or 0),
            },
            "short": {
                "sell": result["total_short_sell"] or 0,
                "buy": result["total_short_buy"] or 0,
                "balance": result["total_short_balance"] or 0,
                "net": (result["total_short_sell"] or 0) - (result["total_short_buy"] or 0),
            },
            "offset": result["total_offset"] or 0,
        }
    }


@router.get("/rankings")
def get_margin_rankings(
    sort_by: str = Query("short_margin_ratio", description="Sort by: short_margin_ratio, margin_balance, short_balance, margin_change, short_change"),
    order: str = Query("desc", description="Sort order: asc or desc"),
    limit: int = Query(50, ge=1, le=200),
    trade_date: Optional[date] = None,
    db: Session = Depends(get_db)
):
    """Get stocks ranked by margin trading metrics."""
    if trade_date is None:
        result = db.execute(text("SELECT MAX(trade_date) FROM margin_trading")).scalar()
        if result is None:
            return {"error": "No margin data available", "data": []}
        trade_date = result

    valid_sort_fields = {
        "short_margin_ratio": "m.short_margin_ratio",
        "margin_balance": "m.margin_balance",
        "short_balance": "m.short_balance",
        "margin_utilization": "m.margin_utilization",
        "short_utilization": "m.short_utilization",
    }

    sort_field = valid_sort_fields.get(sort_by, "m.short_margin_ratio")
    sort_order = "DESC" if order.lower() == "desc" else "ASC"

    query = text(f"""
        SELECT
            s.code,
            s.name,
            m.trade_date,
            m.margin_buy,
            m.margin_sell,
            m.margin_balance,
            m.margin_limit,
            m.margin_utilization,
            m.short_sell,
            m.short_buy,
            m.short_balance,
            m.short_limit,
            m.short_utilization,
            m.short_margin_ratio,
            m.offset,
            p.close_price
        FROM margin_trading m
        JOIN stocks s ON m.stock_id = s.id
        LEFT JOIN stock_prices p ON m.stock_id = p.stock_id AND m.trade_date = p.trade_date
        WHERE m.trade_date = :trade_date
          AND s.is_active = true
          AND m.margin_balance > 0
        ORDER BY {sort_field} {sort_order} NULLS LAST
        LIMIT :limit
    """)

    results = db.execute(query, {"trade_date": trade_date, "limit": limit}).mappings().all()

    return {
        "date": str(trade_date),
        "sort_by": sort_by,
        "order": order,
        "data": [
            {
                "code": r["code"],
                "name": r["name"],
                "close_price": float(r["close_price"]) if r["close_price"] else None,
                "margin": {
                    "buy": r["margin_buy"],
                    "sell": r["margin_sell"],
                    "balance": r["margin_balance"],
                    "limit": r["margin_limit"],
                    "utilization": float(r["margin_utilization"]) if r["margin_utilization"] else 0,
                },
                "short": {
                    "sell": r["short_sell"],
                    "buy": r["short_buy"],
                    "balance": r["short_balance"],
                    "limit": r["short_limit"],
                    "utilization": float(r["short_utilization"]) if r["short_utilization"] else 0,
                },
                "short_margin_ratio": float(r["short_margin_ratio"]) if r["short_margin_ratio"] else 0,
                "offset": r["offset"],
            }
            for r in results
        ]
    }


@router.get("/stock/{stock_code}")
def get_stock_margin(
    stock_code: str,
    days: int = Query(60, ge=1, le=365),
    db: Session = Depends(get_db)
):
    """Get margin trading history for a specific stock."""
    query = text("""
        SELECT
            m.trade_date,
            m.margin_buy,
            m.margin_sell,
            m.margin_cash_repay,
            m.margin_balance,
            m.margin_limit,
            m.margin_utilization,
            m.short_sell,
            m.short_buy,
            m.short_stock_repay,
            m.short_balance,
            m.short_limit,
            m.short_utilization,
            m.short_margin_ratio,
            m.offset
        FROM margin_trading m
        JOIN stocks s ON m.stock_id = s.id
        WHERE s.code = :code
        ORDER BY m.trade_date DESC
        LIMIT :days
    """)

    results = db.execute(query, {"code": stock_code, "days": days}).mappings().all()

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
                "date": str(r["trade_date"]),
                "margin": {
                    "buy": r["margin_buy"],
                    "sell": r["margin_sell"],
                    "cash_repay": r["margin_cash_repay"],
                    "balance": r["margin_balance"],
                    "limit": r["margin_limit"],
                    "utilization": float(r["margin_utilization"]) if r["margin_utilization"] else 0,
                },
                "short": {
                    "sell": r["short_sell"],
                    "buy": r["short_buy"],
                    "stock_repay": r["short_stock_repay"],
                    "balance": r["short_balance"],
                    "limit": r["short_limit"],
                    "utilization": float(r["short_utilization"]) if r["short_utilization"] else 0,
                },
                "short_margin_ratio": float(r["short_margin_ratio"]) if r["short_margin_ratio"] else 0,
                "offset": r["offset"],
            }
            for r in results
        ]
    }


@router.get("/alerts")
def get_margin_alerts(
    min_short_margin_ratio: float = Query(30.0, description="Minimum short/margin ratio (%) to trigger alert"),
    db: Session = Depends(get_db)
):
    """Get stocks with high short/margin ratio (potential short squeeze candidates)."""
    # Get latest date
    latest_date = db.execute(text("SELECT MAX(trade_date) FROM margin_trading")).scalar()
    if latest_date is None:
        return {"error": "No margin data available", "data": []}

    query = text("""
        SELECT
            s.code,
            s.name,
            m.trade_date,
            m.margin_balance,
            m.short_balance,
            m.short_margin_ratio,
            p.close_price,
            p.volume
        FROM margin_trading m
        JOIN stocks s ON m.stock_id = s.id
        LEFT JOIN stock_prices p ON m.stock_id = p.stock_id AND m.trade_date = p.trade_date
        WHERE m.trade_date = :trade_date
          AND s.is_active = true
          AND m.short_margin_ratio >= :min_ratio
          AND m.margin_balance > 0
        ORDER BY m.short_margin_ratio DESC
        LIMIT 50
    """)

    results = db.execute(query, {
        "trade_date": latest_date,
        "min_ratio": min_short_margin_ratio
    }).mappings().all()

    return {
        "date": str(latest_date),
        "alert_threshold": min_short_margin_ratio,
        "count": len(results),
        "data": [
            {
                "code": r["code"],
                "name": r["name"],
                "close_price": float(r["close_price"]) if r["close_price"] else None,
                "volume": r["volume"],
                "margin_balance": r["margin_balance"],
                "short_balance": r["short_balance"],
                "short_margin_ratio": float(r["short_margin_ratio"]) if r["short_margin_ratio"] else 0,
            }
            for r in results
        ]
    }
