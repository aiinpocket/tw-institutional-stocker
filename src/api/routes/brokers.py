"""Broker routes."""
from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from src.api.dependencies import get_db
from src.common.models import Stock, BrokerTrade

router = APIRouter()


@router.get("/trades")
def get_broker_trades(
    trade_date: Optional[date] = Query(None, description="Trade date (default: latest)"),
    stock_code: Optional[str] = Query(None, description="Filter by stock code"),
    broker_name: Optional[str] = Query(None, description="Filter by broker name"),
    side: Optional[str] = Query(None, description="Filter by side (buy/sell)"),
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
    db: Session = Depends(get_db),
):
    """Get broker trading data."""
    if trade_date is None:
        trade_date = db.query(func.max(BrokerTrade.trade_date)).scalar()

    if trade_date is None:
        return {"date": None, "total": 0, "items": []}

    query = (
        db.query(BrokerTrade, Stock)
        .join(Stock, BrokerTrade.stock_id == Stock.id)
        .filter(BrokerTrade.trade_date == trade_date)
    )

    if stock_code:
        query = query.filter(Stock.code == stock_code)

    if broker_name:
        query = query.filter(BrokerTrade.broker_name.ilike(f"%{broker_name}%"))

    if side:
        query = query.filter(BrokerTrade.side == side.lower())

    total = query.count()
    results = query.order_by(BrokerTrade.rank).offset(offset).limit(limit).all()

    items = [
        {
            "code": stock.code,
            "name": stock.name,
            "trade_date": trade.trade_date,
            "broker_name": trade.broker_name,
            "broker_id": trade.broker_id,
            "buy_vol": trade.buy_vol,
            "sell_vol": trade.sell_vol,
            "net_vol": trade.net_vol,
            "pct": float(trade.pct) if trade.pct else None,
            "rank": trade.rank,
            "side": trade.side,
        }
        for trade, stock in results
    ]

    return {"date": trade_date, "total": total, "items": items}


@router.get("/ranking")
def get_broker_ranking(
    trade_date: Optional[date] = Query(None, description="Trade date (default: latest)"),
    limit: int = Query(50, le=200),
    db: Session = Depends(get_db),
):
    """Get broker ranking by total trading volume."""
    if trade_date is None:
        trade_date = db.query(func.max(BrokerTrade.trade_date)).scalar()

    if trade_date is None:
        return {"date": None, "total": 0, "items": []}

    # Aggregate by broker
    results = (
        db.query(
            BrokerTrade.broker_name,
            func.sum(BrokerTrade.buy_vol).label("total_buy"),
            func.sum(BrokerTrade.sell_vol).label("total_sell"),
            func.sum(BrokerTrade.net_vol).label("total_net"),
            func.count(Stock.code.distinct()).label("stock_count"),
        )
        .join(Stock, BrokerTrade.stock_id == Stock.id)
        .filter(BrokerTrade.trade_date == trade_date)
        .group_by(BrokerTrade.broker_name)
        .order_by(func.sum(func.abs(BrokerTrade.net_vol)).desc())
        .limit(limit)
        .all()
    )

    items = [
        {
            "broker_name": r.broker_name,
            "total_buy": r.total_buy or 0,
            "total_sell": r.total_sell or 0,
            "total_net": r.total_net or 0,
            "stock_count": r.stock_count or 0,
        }
        for r in results
    ]

    return {"date": trade_date, "total": len(items), "items": items}


@router.get("/{broker_name}/history")
def get_broker_history(
    broker_name: str,
    stock_code: Optional[str] = Query(None, description="Filter by stock code"),
    limit: int = Query(100, le=500),
    db: Session = Depends(get_db),
):
    """Get trading history for a specific broker."""
    query = (
        db.query(BrokerTrade, Stock)
        .join(Stock, BrokerTrade.stock_id == Stock.id)
        .filter(BrokerTrade.broker_name.ilike(f"%{broker_name}%"))
    )

    if stock_code:
        query = query.filter(Stock.code == stock_code)

    results = (
        query.order_by(BrokerTrade.trade_date.desc(), Stock.code)
        .limit(limit)
        .all()
    )

    items = [
        {
            "code": stock.code,
            "name": stock.name,
            "trade_date": trade.trade_date,
            "broker_name": trade.broker_name,
            "buy_vol": trade.buy_vol,
            "sell_vol": trade.sell_vol,
            "net_vol": trade.net_vol,
            "side": trade.side,
        }
        for trade, stock in results
    ]

    return {"broker_name": broker_name, "total": len(items), "items": items}
