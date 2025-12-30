"""Strategy analysis routes - Win rate and correlation rankings."""
import math
from typing import Optional
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api.dependencies import get_db


def safe_float(value, default=0.0):
    """Convert to float safely, handling NaN and Inf."""
    if value is None:
        return default
    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except (ValueError, TypeError):
        return default

router = APIRouter()


def get_rankings_from_cache(db, metric_type: str):
    """Get pre-computed rankings from cache table."""
    query = text("""
        SELECT
            s.code, s.name,
            sr.current_price, sr.signal_count, sr.avg_return,
            sr.win_rate, sr.correlation, sr.data_points,
            sr.price_tier, sr.rank_in_tier
        FROM strategy_rankings sr
        JOIN stocks s ON sr.stock_id = s.id
        WHERE sr.metric_type = :metric_type
        ORDER BY sr.price_tier, sr.rank_in_tier
    """)
    return db.execute(query, {"metric_type": metric_type}).fetchall()


@router.get("/win-rate-rankings")
def get_win_rate_rankings(
    holding_days: int = 10,
    db: Session = Depends(get_db),
):
    """
    Get top stocks by win rate after foreign consecutive buying,
    segmented by price range. Uses pre-computed data for fast response.
    """
    metric_type = f"win_rate_{holding_days}d"
    rows = get_rankings_from_cache(db, metric_type)

    rankings = {"high": [], "mid": [], "low": []}

    for row in rows:
        tier = row.price_tier
        if tier in rankings:
            rankings[tier].append({
                "code": row.code,
                "name": row.name,
                "current_price": safe_float(row.current_price, None),
                "signal_count": row.signal_count or 0,
                "avg_return": safe_float(row.avg_return),
                "win_rate": safe_float(row.win_rate),
            })

    return {
        "holding_days": holding_days,
        "rankings": rankings,
    }


@router.get("/correlation-rankings")
def get_correlation_rankings(
    db: Session = Depends(get_db),
):
    """
    Get top stocks by correlation between foreign net buying and stock returns.
    Uses pre-computed data for fast response.
    """
    rows = get_rankings_from_cache(db, "correlation")

    rankings = {"high": [], "mid": [], "low": []}

    for row in rows:
        tier = row.price_tier
        if tier in rankings:
            rankings[tier].append({
                "code": row.code,
                "name": row.name,
                "current_price": safe_float(row.current_price, None),
                "data_points": row.data_points or 0,
                "correlation": safe_float(row.correlation),
            })

    return {
        "rankings": rankings,
    }


@router.get("/below-cost-rankings")
def get_below_cost_rankings(
    db: Session = Depends(get_db),
):
    """
    取得現價低於三大法人三個月平均成本的股票。
    顯示折價幅度最大的股票，按股價區間分類。
    """
    rows = get_rankings_from_cache(db, "below_cost")

    rankings = {"high": [], "mid": [], "low": []}

    for row in rows:
        tier = row.price_tier
        if tier in rankings:
            rankings[tier].append({
                "code": row.code,
                "name": row.name,
                "current_price": safe_float(row.current_price, None),
                "avg_cost": safe_float(row.avg_return, None),  # 借用欄位
                "discount_pct": safe_float(row.win_rate, None),  # 借用欄位
                "buy_days": row.signal_count or 0,  # 借用欄位
            })

    return {
        "description": "現價低於法人三個月平均成本",
        "rankings": rankings,
    }


@router.get("/consecutive-buying")
def get_consecutive_buying_rankings(db: Session = Depends(get_db)):
    """
    取得外資連續買超排行。
    顯示外資連續買超天數最多的股票。
    """
    rows = get_rankings_from_cache(db, "consecutive_buying")

    rankings = {"high": [], "mid": [], "low": []}

    for row in rows:
        tier = row.price_tier
        if tier in rankings:
            rankings[tier].append({
                "code": row.code,
                "name": row.name,
                "current_price": safe_float(row.current_price, None),
                "consecutive_days": row.signal_count or 0,
                "total_net_buy": safe_float(row.avg_return, 0),
            })

    return {
        "description": "外資連續買超",
        "rankings": rankings,
    }


@router.get("/trust-accumulation")
def get_trust_accumulation_rankings(db: Session = Depends(get_db)):
    """
    取得投信認養股排行。
    顯示投信近期持續加碼的股票。
    """
    rows = get_rankings_from_cache(db, "trust_accumulation")

    rankings = {"high": [], "mid": [], "low": []}

    for row in rows:
        tier = row.price_tier
        if tier in rankings:
            rankings[tier].append({
                "code": row.code,
                "name": row.name,
                "current_price": safe_float(row.current_price, None),
                "buy_days": row.signal_count or 0,
                "total_trust_net": safe_float(row.avg_return, 0),
                "buy_ratio": safe_float(row.win_rate, 0),
            })

    return {
        "description": "投信認養股",
        "rankings": rankings,
    }


@router.get("/synchronized-buying")
def get_synchronized_buying_rankings(db: Session = Depends(get_db)):
    """
    取得三大法人同步買超排行。
    顯示外資、投信、自營商同時買超的股票。
    """
    rows = get_rankings_from_cache(db, "synchronized_buying")

    rankings = {"high": [], "mid": [], "low": []}

    for row in rows:
        tier = row.price_tier
        if tier in rankings:
            rankings[tier].append({
                "code": row.code,
                "name": row.name,
                "current_price": safe_float(row.current_price, None),
                "sync_days": row.signal_count or 0,
                "total_amount": safe_float(row.avg_return, 0),
                "foreign_total": safe_float(row.correlation, 0),
                "trust_total": row.data_points or 0,
            })

    return {
        "description": "三大法人同步買超",
        "rankings": rankings,
    }


@router.get("/price-deviation")
def get_price_deviation_rankings(db: Session = Depends(get_db)):
    """
    取得股價乖離過大排行。
    顯示股價大幅偏離法人平均成本的股票。
    """
    rows = get_rankings_from_cache(db, "price_deviation")

    rankings = {"high": [], "mid": [], "low": []}

    for row in rows:
        tier = row.price_tier
        if tier in rankings:
            rankings[tier].append({
                "code": row.code,
                "name": row.name,
                "current_price": safe_float(row.current_price, None),
                "avg_cost": safe_float(row.avg_return, None),
                "deviation_pct": safe_float(row.win_rate, 0),
            })

    return {
        "description": "股價乖離過大",
        "rankings": rankings,
    }


@router.get("/summary")
def get_strategy_summary(db: Session = Depends(get_db)):
    """Get summary of all strategy rankings for display. Uses pre-computed data."""
    results = {}

    for days in [5, 10, 30]:
        win_data = get_win_rate_rankings(holding_days=days, db=db)
        results[f"win_rate_{days}d"] = win_data["rankings"]

    corr_data = get_correlation_rankings(db=db)
    results["correlation"] = corr_data["rankings"]

    below_cost_data = get_below_cost_rankings(db=db)
    results["below_cost"] = below_cost_data["rankings"]

    # 新增策略
    consecutive_data = get_consecutive_buying_rankings(db=db)
    results["consecutive_buying"] = consecutive_data["rankings"]

    trust_data = get_trust_accumulation_rankings(db=db)
    results["trust_accumulation"] = trust_data["rankings"]

    sync_data = get_synchronized_buying_rankings(db=db)
    results["synchronized_buying"] = sync_data["rankings"]

    deviation_data = get_price_deviation_rankings(db=db)
    results["price_deviation"] = deviation_data["rankings"]

    return results


@router.post("/recompute")
def recompute_strategy(db: Session = Depends(get_db)):
    """Manually trigger strategy recomputation (for admin use)."""
    from src.etl.processors.compute_strategy import run_all_computations
    run_all_computations(db)
    return {"status": "ok", "message": "Strategy rankings recomputed"}
