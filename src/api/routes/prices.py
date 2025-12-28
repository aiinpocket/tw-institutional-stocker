"""Price routes."""
from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from src.api.dependencies import get_db
from src.common.models import Stock, StockPrice

router = APIRouter()


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
