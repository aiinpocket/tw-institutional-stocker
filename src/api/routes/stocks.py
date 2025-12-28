"""Stock routes."""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.api.dependencies import get_db
from src.api.schemas.stock import StockResponse, StockListResponse
from src.common.models import Stock

router = APIRouter()


@router.get("", response_model=StockListResponse)
def list_stocks(
    market: Optional[str] = Query(None, description="Filter by market (TWSE or TPEX)"),
    search: Optional[str] = Query(None, description="Search by code or name"),
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
    db: Session = Depends(get_db),
):
    """List all stocks with optional filtering."""
    query = db.query(Stock).filter(Stock.is_active == True)

    if market:
        query = query.filter(Stock.market == market.upper())

    if search:
        query = query.filter(
            (Stock.code.ilike(f"%{search}%")) | (Stock.name.ilike(f"%{search}%"))
        )

    total = query.count()
    items = query.order_by(Stock.code).offset(offset).limit(limit).all()

    return StockListResponse(total=total, items=items)


@router.get("/{code}", response_model=StockResponse)
def get_stock(code: str, db: Session = Depends(get_db)):
    """Get a stock by code."""
    stock = db.query(Stock).filter(Stock.code == code).first()
    if not stock:
        raise HTTPException(status_code=404, detail=f"Stock {code} not found")
    return stock


@router.get("/{code}/institutional")
def get_stock_institutional(
    code: str,
    limit: int = Query(120, le=500),
    db: Session = Depends(get_db),
):
    """Get institutional holdings history for a stock."""
    from src.common.models import InstitutionalRatio

    stock = db.query(Stock).filter(Stock.code == code).first()
    if not stock:
        raise HTTPException(status_code=404, detail=f"Stock {code} not found")

    ratios = (
        db.query(InstitutionalRatio)
        .filter(InstitutionalRatio.stock_id == stock.id)
        .order_by(InstitutionalRatio.trade_date.desc())
        .limit(limit)
        .all()
    )

    return {
        "code": stock.code,
        "name": stock.name,
        "market": stock.market,
        "data": [
            {
                "trade_date": r.trade_date,
                "foreign_ratio": float(r.foreign_ratio) if r.foreign_ratio else None,
                "trust_ratio_est": float(r.trust_ratio_est) if r.trust_ratio_est else None,
                "dealer_ratio_est": float(r.dealer_ratio_est) if r.dealer_ratio_est else None,
                "three_inst_ratio_est": float(r.three_inst_ratio_est) if r.three_inst_ratio_est else None,
                "change_5d": float(r.change_5d) if r.change_5d else None,
                "change_20d": float(r.change_20d) if r.change_20d else None,
                "change_60d": float(r.change_60d) if r.change_60d else None,
                "change_120d": float(r.change_120d) if r.change_120d else None,
            }
            for r in reversed(ratios)
        ],
    }


@router.get("/{code}/prices")
def get_stock_prices(
    code: str,
    limit: int = Query(120, le=500),
    db: Session = Depends(get_db),
):
    """Get price history for a stock."""
    from src.common.models import StockPrice

    stock = db.query(Stock).filter(Stock.code == code).first()
    if not stock:
        raise HTTPException(status_code=404, detail=f"Stock {code} not found")

    prices = (
        db.query(StockPrice)
        .filter(StockPrice.stock_id == stock.id)
        .order_by(StockPrice.trade_date.desc())
        .limit(limit)
        .all()
    )

    return {
        "code": stock.code,
        "name": stock.name,
        "market": stock.market,
        "data": [
            {
                "trade_date": p.trade_date,
                "open_price": float(p.open_price) if p.open_price else None,
                "high_price": float(p.high_price) if p.high_price else None,
                "low_price": float(p.low_price) if p.low_price else None,
                "close_price": float(p.close_price) if p.close_price else None,
                "volume": p.volume,
                "turnover": p.turnover,
                "change_amount": float(p.change_amount) if p.change_amount else None,
                "change_percent": float(p.change_percent) if p.change_percent else None,
            }
            for p in reversed(prices)
        ],
    }
