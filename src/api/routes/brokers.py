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


@router.get("/top-active")
def get_top_active_brokers(
    days: int = Query(5, description="Look back days", ge=1, le=30),
    limit: int = Query(20, le=50),
    db: Session = Depends(get_db),
):
    """
    取得最活躍主力券商排行。
    統計近 N 天交易量最大的券商及其主要操作標的。
    """
    from sqlalchemy import text

    query = text("""
    WITH recent_trades AS (
        SELECT
            bt.broker_name,
            s.code,
            s.name as stock_name,
            SUM(bt.buy_vol) as total_buy,
            SUM(bt.sell_vol) as total_sell,
            SUM(bt.net_vol) as net_vol,
            COUNT(DISTINCT bt.trade_date) as active_days
        FROM broker_trades bt
        JOIN stocks s ON bt.stock_id = s.id
        WHERE bt.trade_date >= CURRENT_DATE - :days
        GROUP BY bt.broker_name, s.code, s.name
    ),
    broker_summary AS (
        SELECT
            broker_name,
            SUM(ABS(net_vol)) as total_volume,
            COUNT(DISTINCT code) as stock_count,
            SUM(CASE WHEN net_vol > 0 THEN net_vol ELSE 0 END) as total_net_buy,
            SUM(CASE WHEN net_vol < 0 THEN ABS(net_vol) ELSE 0 END) as total_net_sell,
            MAX(active_days) as max_active_days
        FROM recent_trades
        GROUP BY broker_name
        ORDER BY total_volume DESC
        LIMIT :limit
    ),
    top_stocks AS (
        SELECT DISTINCT ON (rt.broker_name)
            rt.broker_name,
            rt.code as top_stock_code,
            rt.stock_name as top_stock_name,
            rt.net_vol as top_stock_net
        FROM recent_trades rt
        JOIN broker_summary bs ON rt.broker_name = bs.broker_name
        ORDER BY rt.broker_name, ABS(rt.net_vol) DESC
    )
    SELECT
        bs.*,
        ts.top_stock_code,
        ts.top_stock_name,
        ts.top_stock_net
    FROM broker_summary bs
    LEFT JOIN top_stocks ts ON bs.broker_name = ts.broker_name
    ORDER BY bs.total_volume DESC
    """)

    results = db.execute(query, {"days": days, "limit": limit}).fetchall()

    items = [
        {
            "broker_name": r.broker_name,
            "total_volume": r.total_volume or 0,
            "stock_count": r.stock_count or 0,
            "total_net_buy": r.total_net_buy or 0,
            "total_net_sell": r.total_net_sell or 0,
            "bias": "買超" if (r.total_net_buy or 0) > (r.total_net_sell or 0) else "賣超",
            "top_stock": {
                "code": r.top_stock_code,
                "name": r.top_stock_name,
                "net_vol": r.top_stock_net,
            } if r.top_stock_code else None,
        }
        for r in results
    ]

    return {"days": days, "total": len(items), "items": items}


@router.get("/unusual-volume")
def get_unusual_volume(
    threshold: float = Query(3.0, description="Volume threshold multiplier", ge=1.5, le=10.0),
    limit: int = Query(30, le=100),
    db: Session = Depends(get_db),
):
    """
    偵測異常大單。
    找出當日交易量遠超過該券商平均交易量的記錄。
    """
    from sqlalchemy import text

    # 取得最新交易日
    latest_date = db.query(func.max(BrokerTrade.trade_date)).scalar()
    if not latest_date:
        return {"date": None, "threshold": threshold, "total": 0, "items": []}

    query = text("""
    WITH broker_avg AS (
        -- 計算每個券商在每支股票的歷史平均交易量
        SELECT
            broker_name,
            stock_id,
            AVG(ABS(net_vol)) as avg_vol,
            STDDEV(ABS(net_vol)) as std_vol,
            COUNT(*) as history_count
        FROM broker_trades
        WHERE trade_date < :latest_date
          AND trade_date >= :latest_date - 30
        GROUP BY broker_name, stock_id
        HAVING COUNT(*) >= 3
    ),
    today_trades AS (
        SELECT
            bt.broker_name,
            bt.stock_id,
            s.code,
            s.name as stock_name,
            bt.buy_vol,
            bt.sell_vol,
            bt.net_vol,
            bt.side
        FROM broker_trades bt
        JOIN stocks s ON bt.stock_id = s.id
        WHERE bt.trade_date = :latest_date
    ),
    unusual AS (
        SELECT
            tt.*,
            ba.avg_vol,
            ba.std_vol,
            ba.history_count,
            CASE
                WHEN ba.avg_vol > 0 THEN ABS(tt.net_vol) / ba.avg_vol
                ELSE 0
            END as volume_ratio
        FROM today_trades tt
        JOIN broker_avg ba ON tt.broker_name = ba.broker_name AND tt.stock_id = ba.stock_id
        WHERE ABS(tt.net_vol) > ba.avg_vol * :threshold
    )
    SELECT *
    FROM unusual
    ORDER BY volume_ratio DESC
    LIMIT :limit
    """)

    results = db.execute(query, {
        "latest_date": latest_date,
        "threshold": threshold,
        "limit": limit
    }).fetchall()

    items = [
        {
            "broker_name": r.broker_name,
            "code": r.code,
            "name": r.stock_name,
            "net_vol": r.net_vol,
            "side": r.side,
            "avg_vol": round(r.avg_vol, 0) if r.avg_vol else 0,
            "volume_ratio": round(r.volume_ratio, 1) if r.volume_ratio else 0,
            "history_count": r.history_count,
        }
        for r in results
    ]

    return {
        "date": latest_date,
        "threshold": threshold,
        "total": len(items),
        "items": items
    }


@router.get("/stock/{stock_code}/top-brokers")
def get_stock_top_brokers(
    stock_code: str,
    days: int = Query(10, description="Look back days", ge=1, le=60),
    db: Session = Depends(get_db),
):
    """
    取得特定股票的主力券商分析。
    顯示近期在該股票上最活躍的券商。
    """
    from sqlalchemy import text

    # 先確認股票存在
    stock = db.query(Stock).filter(Stock.code == stock_code).first()
    if not stock:
        return {"code": stock_code, "error": "Stock not found"}

    query = text("""
    WITH broker_activity AS (
        SELECT
            bt.broker_name,
            SUM(bt.buy_vol) as total_buy,
            SUM(bt.sell_vol) as total_sell,
            SUM(bt.net_vol) as net_vol,
            COUNT(*) as trade_days,
            COUNT(*) FILTER (WHERE bt.net_vol > 0) as buy_days,
            COUNT(*) FILTER (WHERE bt.net_vol < 0) as sell_days,
            MIN(bt.trade_date) as first_date,
            MAX(bt.trade_date) as last_date
        FROM broker_trades bt
        WHERE bt.stock_id = :stock_id
          AND bt.trade_date >= CURRENT_DATE - :days
        GROUP BY bt.broker_name
        HAVING SUM(ABS(bt.net_vol)) > 0
        ORDER BY SUM(ABS(bt.net_vol)) DESC
        LIMIT 20
    )
    SELECT
        *,
        CASE
            WHEN net_vol > 0 THEN '買超'
            WHEN net_vol < 0 THEN '賣超'
            ELSE '持平'
        END as position
    FROM broker_activity
    """)

    results = db.execute(query, {"stock_id": stock.id, "days": days}).fetchall()

    items = [
        {
            "broker_name": r.broker_name,
            "total_buy": r.total_buy or 0,
            "total_sell": r.total_sell or 0,
            "net_vol": r.net_vol or 0,
            "position": r.position,
            "trade_days": r.trade_days,
            "buy_days": r.buy_days,
            "sell_days": r.sell_days,
            "first_date": r.first_date,
            "last_date": r.last_date,
        }
        for r in results
    ]

    return {
        "code": stock_code,
        "name": stock.name,
        "days": days,
        "total": len(items),
        "items": items
    }
