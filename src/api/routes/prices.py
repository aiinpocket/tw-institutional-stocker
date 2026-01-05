"""Price routes."""
from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, case, text

from src.api.dependencies import get_db
from src.common.models import Stock, StockPrice

router = APIRouter()


@router.get("/summary")
def get_market_summary(
    db: Session = Depends(get_db),
):
    """Get market summary with up/down/unchanged counts."""
    latest_date = db.query(func.max(StockPrice.trade_date)).scalar()

    if latest_date is None:
        return {
            "date": None,
            "total": 0,
            "up": 0,
            "down": 0,
            "unchanged": 0,
        }

    # 計算上漲/下跌/平盤數量
    # change_percent 可能是 null，所以我們也用 close_price vs open_price 來判斷
    result = db.execute(text("""
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

    return {
        "date": latest_date,
        "total": result.total or 0,
        "up": result.up_count or 0,
        "down": result.down_count or 0,
        "unchanged": result.unchanged_count or 0,
    }


@router.get("/latest")
def get_latest_prices(
    market: Optional[str] = Query(None, description="Filter by market"),
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
    db: Session = Depends(get_db),
):
    """Get latest stock prices."""
    latest_date = db.query(func.max(StockPrice.trade_date)).scalar()

    if latest_date is None:
        return {"date": None, "total": 0, "items": []}

    query = (
        db.query(StockPrice, Stock)
        .join(Stock, StockPrice.stock_id == Stock.id)
        .filter(StockPrice.trade_date == latest_date)
    )

    if market:
        query = query.filter(Stock.market == market.upper())

    total = query.count()
    results = query.order_by(Stock.code).offset(offset).limit(limit).all()

    items = [
        {
            "code": stock.code,
            "name": stock.name,
            "market": stock.market,
            "trade_date": price.trade_date,
            "open_price": float(price.open_price) if price.open_price else None,
            "high_price": float(price.high_price) if price.high_price else None,
            "low_price": float(price.low_price) if price.low_price else None,
            "close_price": float(price.close_price) if price.close_price else None,
            "volume": price.volume,
            "turnover": price.turnover,
            "change_amount": float(price.change_amount) if price.change_amount else None,
            "change_percent": float(price.change_percent) if price.change_percent else None,
        }
        for price, stock in results
    ]

    return {"date": latest_date, "total": total, "items": items}


@router.get("/date/{trade_date}")
def get_prices_by_date(
    trade_date: date,
    market: Optional[str] = Query(None, description="Filter by market"),
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
    db: Session = Depends(get_db),
):
    """Get stock prices for a specific date."""
    query = (
        db.query(StockPrice, Stock)
        .join(Stock, StockPrice.stock_id == Stock.id)
        .filter(StockPrice.trade_date == trade_date)
    )

    if market:
        query = query.filter(Stock.market == market.upper())

    total = query.count()
    results = query.order_by(Stock.code).offset(offset).limit(limit).all()

    items = [
        {
            "code": stock.code,
            "name": stock.name,
            "market": stock.market,
            "trade_date": price.trade_date,
            "open_price": float(price.open_price) if price.open_price else None,
            "high_price": float(price.high_price) if price.high_price else None,
            "low_price": float(price.low_price) if price.low_price else None,
            "close_price": float(price.close_price) if price.close_price else None,
            "volume": price.volume,
            "turnover": price.turnover,
            "change_amount": float(price.change_amount) if price.change_amount else None,
            "change_percent": float(price.change_percent) if price.change_percent else None,
        }
        for price, stock in results
    ]

    return {"date": trade_date, "total": total, "items": items}
