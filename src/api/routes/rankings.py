"""Rankings routes."""
from typing import Optional, Literal
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, asc

from src.api.dependencies import get_db
from src.common.models import Stock, InstitutionalRatio

router = APIRouter()


@router.get("/{window}")
def get_rankings(
    window: int,
    direction: Literal["up", "down"] = Query("up", description="Ranking direction"),
    market: Optional[str] = Query(None, description="Filter by market"),
    limit: int = Query(200, le=500),
    db: Session = Depends(get_db),
):
    """Get top movers by institutional ratio change.

    Args:
        window: Change window (5, 20, 60, or 120 days)
        direction: 'up' for top gainers, 'down' for top losers
        market: Optional market filter (TWSE or TPEX)
        limit: Maximum number of results
    """
    if window not in [5, 20, 60, 120]:
        raise HTTPException(status_code=400, detail="Window must be 5, 20, 60, or 120")

    # Get latest date
    latest_date = db.query(func.max(InstitutionalRatio.trade_date)).scalar()
    if latest_date is None:
        return {"window": window, "direction": direction, "total": 0, "items": []}

    # Build query
    change_col = getattr(InstitutionalRatio, f"change_{window}d")

    query = (
        db.query(InstitutionalRatio, Stock)
        .join(Stock, InstitutionalRatio.stock_id == Stock.id)
        .filter(InstitutionalRatio.trade_date == latest_date)
        .filter(change_col.isnot(None))
    )

    if market:
        query = query.filter(Stock.market == market.upper())

    # Order by change
    if direction == "up":
        query = query.order_by(desc(change_col))
    else:
        query = query.order_by(asc(change_col))

    results = query.limit(limit).all()

    items = [
        {
            "code": stock.code,
            "name": stock.name,
            "market": stock.market,
            "three_inst_ratio": float(ratio.three_inst_ratio_est) if ratio.three_inst_ratio_est else None,
            "change": float(getattr(ratio, f"change_{window}d")) if getattr(ratio, f"change_{window}d") else None,
        }
        for ratio, stock in results
    ]

    return {
        "window": window,
        "direction": direction,
        "date": latest_date,
        "total": len(items),
        "items": items,
    }


@router.get("/{window}/up")
def get_rankings_up(
    window: int,
    market: Optional[str] = Query(None),
    limit: int = Query(200, le=500),
    db: Session = Depends(get_db),
):
    """Get top gainers by institutional ratio change."""
    return get_rankings(window, "up", market, limit, db)


@router.get("/{window}/down")
def get_rankings_down(
    window: int,
    market: Optional[str] = Query(None),
    limit: int = Query(200, le=500),
    db: Session = Depends(get_db),
):
    """Get top losers by institutional ratio change."""
    return get_rankings(window, "down", market, limit, db)
