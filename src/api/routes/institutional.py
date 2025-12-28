"""Institutional data routes."""
from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from src.api.dependencies import get_db
from src.common.models import Stock, InstitutionalFlow, ForeignHolding, InstitutionalRatio

router = APIRouter()


@router.get("/flows")
def get_flows(
    trade_date: Optional[date] = Query(None, description="Trade date (default: latest)"),
    market: Optional[str] = Query(None, description="Filter by market"),
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
    db: Session = Depends(get_db),
):
    """Get institutional flows for a date."""
    if trade_date is None:
        # Get latest date
        trade_date = db.query(func.max(InstitutionalFlow.trade_date)).scalar()

    if trade_date is None:
        return {"date": None, "total": 0, "items": []}

    query = (
        db.query(InstitutionalFlow, Stock)
        .join(Stock, InstitutionalFlow.stock_id == Stock.id)
        .filter(InstitutionalFlow.trade_date == trade_date)
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
            "trade_date": flow.trade_date,
            "foreign_net": flow.foreign_net,
            "trust_net": flow.trust_net,
            "dealer_net": flow.dealer_net,
        }
        for flow, stock in results
    ]

    return {"date": trade_date, "total": total, "items": items}


@router.get("/holdings")
def get_holdings(
    trade_date: Optional[date] = Query(None, description="Trade date (default: latest)"),
    market: Optional[str] = Query(None, description="Filter by market"),
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
    db: Session = Depends(get_db),
):
    """Get foreign holdings for a date."""
    if trade_date is None:
        trade_date = db.query(func.max(ForeignHolding.trade_date)).scalar()

    if trade_date is None:
        return {"date": None, "total": 0, "items": []}

    query = (
        db.query(ForeignHolding, Stock)
        .join(Stock, ForeignHolding.stock_id == Stock.id)
        .filter(ForeignHolding.trade_date == trade_date)
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
            "trade_date": holding.trade_date,
            "total_shares": holding.total_shares,
            "foreign_shares": holding.foreign_shares,
            "foreign_ratio": float(holding.foreign_ratio) if holding.foreign_ratio else None,
        }
        for holding, stock in results
    ]

    return {"date": trade_date, "total": total, "items": items}


@router.get("/ratios")
def get_ratios(
    trade_date: Optional[date] = Query(None, description="Trade date (default: latest)"),
    market: Optional[str] = Query(None, description="Filter by market"),
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
    db: Session = Depends(get_db),
):
    """Get institutional ratios for a date."""
    if trade_date is None:
        trade_date = db.query(func.max(InstitutionalRatio.trade_date)).scalar()

    if trade_date is None:
        return {"date": None, "total": 0, "items": []}

    query = (
        db.query(InstitutionalRatio, Stock)
        .join(Stock, InstitutionalRatio.stock_id == Stock.id)
        .filter(InstitutionalRatio.trade_date == trade_date)
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
            "trade_date": ratio.trade_date,
            "foreign_ratio": float(ratio.foreign_ratio) if ratio.foreign_ratio else None,
            "trust_ratio_est": float(ratio.trust_ratio_est) if ratio.trust_ratio_est else None,
            "dealer_ratio_est": float(ratio.dealer_ratio_est) if ratio.dealer_ratio_est else None,
            "three_inst_ratio_est": float(ratio.three_inst_ratio_est) if ratio.three_inst_ratio_est else None,
            "change_5d": float(ratio.change_5d) if ratio.change_5d else None,
            "change_20d": float(ratio.change_20d) if ratio.change_20d else None,
            "change_60d": float(ratio.change_60d) if ratio.change_60d else None,
            "change_120d": float(ratio.change_120d) if ratio.change_120d else None,
        }
        for ratio, stock in results
    ]

    return {"date": trade_date, "total": total, "items": items}
