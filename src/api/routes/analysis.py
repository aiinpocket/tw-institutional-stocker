"""Stock analysis routes - Detailed technical and institutional analysis."""
import math
from datetime import date, timedelta
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session
import statistics

from src.api.dependencies import get_db


def safe_float(value, default=None):
    """Convert to float safely, handling NaN and Infinity."""
    if value is None:
        return default
    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default
        return round(f, 2)
    except (ValueError, TypeError):
        return default

router = APIRouter()

# Max days allowed per query (3 months)
MAX_QUERY_DAYS = 92


def calculate_support_resistance(prices: List[dict], window: int = 20) -> dict:
    """Calculate support and resistance levels using pivot points and price clusters."""
    if len(prices) < window:
        return {"supports": [], "resistances": []}

    highs = [p["high"] for p in prices[-window:] if p["high"]]
    lows = [p["low"] for p in prices[-window:] if p["low"]]
    closes = [p["close"] for p in prices[-window:] if p["close"]]

    if not highs or not lows or not closes:
        return {"supports": [], "resistances": []}

    # Classic Pivot Points
    pivot = safe_float((max(highs) + min(lows) + closes[-1]) / 3, 0)
    r1 = safe_float(2 * pivot - min(lows), 0)
    r2 = safe_float(pivot + (max(highs) - min(lows)), 0)
    s1 = safe_float(2 * pivot - max(highs), 0)
    s2 = safe_float(pivot - (max(highs) - min(lows)), 0)

    # Recent swing highs/lows
    swing_highs = []
    swing_lows = []
    for i in range(2, len(prices) - 2):
        # Check all 5 values exist for high comparison
        h = [prices[j]["high"] for j in range(i-2, i+3)]
        if all(v is not None for v in h):
            if h[2] > h[1] and h[2] > h[3] and h[2] > h[0] and h[2] > h[4]:
                swing_highs.append(h[2])
        # Check all 5 values exist for low comparison
        l = [prices[j]["low"] for j in range(i-2, i+3)]
        if all(v is not None for v in l):
            if l[2] < l[1] and l[2] < l[3] and l[2] < l[0] and l[2] < l[4]:
                swing_lows.append(l[2])

    # Combine and deduplicate levels (filter out None/0 values)
    r_values = [v for v in [r1, r2] + swing_highs[-3:] if v and v > 0]
    s_values = [v for v in [s1, s2] + swing_lows[-3:] if v and v > 0]
    resistances = sorted(set([round(v, 1) for v in r_values]), reverse=True)[:3]
    supports = sorted(set([round(v, 1) for v in s_values]))[:3]

    current_price = safe_float(closes[-1], 0) if closes else 0

    # Filter to only relevant levels
    resistances = [r for r in resistances if r > current_price][:3]
    supports = [s for s in supports if s < current_price][-3:]

    return {
        "pivot": round(pivot, 2),
        "supports": supports,
        "resistances": resistances,
    }


def calculate_moving_averages(prices: List[dict]) -> dict:
    """Calculate common moving averages."""
    closes = [p["close"] for p in prices if p["close"]]

    def ma(data, period):
        if len(data) < period:
            return None
        return safe_float(sum(data[-period:]) / period)

    return {
        "ma5": ma(closes, 5),
        "ma10": ma(closes, 10),
        "ma20": ma(closes, 20),
        "ma60": ma(closes, 60),
        "ma120": ma(closes, 120),
    }


def calculate_rsi(prices: List[dict], period: int = 14) -> Optional[float]:
    """Calculate Relative Strength Index."""
    closes = [p["close"] for p in prices if p["close"]]
    if len(closes) < period + 1:
        return None

    gains = []
    losses = []
    for i in range(1, len(closes)):
        change = closes[i] - closes[i-1]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    if len(gains) < period:
        return None

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return safe_float(rsi, 50.0)


def calculate_macd(prices: List[dict]) -> dict:
    """Calculate MACD indicator."""
    closes = [p["close"] for p in prices if p["close"]]

    def ema(data, period):
        if len(data) < period:
            return None
        multiplier = 2 / (period + 1)
        ema_val = sum(data[:period]) / period
        for price in data[period:]:
            ema_val = (price - ema_val) * multiplier + ema_val
        return ema_val

    ema12 = ema(closes, 12)
    ema26 = ema(closes, 26)

    if ema12 is None or ema26 is None:
        return {"macd": None, "signal": None, "histogram": None}

    macd_line = ema12 - ema26

    # Calculate signal line (9-day EMA of MACD)
    # Simplified: just return current MACD
    return {
        "macd": safe_float(macd_line),
        "signal": None,  # Would need full MACD history
        "histogram": None,
    }


def generate_signals(prices: List[dict], flows: List[dict], ma: dict, rsi: float) -> List[dict]:
    """Generate trading signals based on technical and institutional data."""
    signals = []

    if not prices:
        return signals

    current = prices[-1]
    current_price = current.get("close", 0)

    # MA signals
    if ma.get("ma5") and ma.get("ma20"):
        if ma["ma5"] > ma["ma20"] and current_price > ma["ma5"]:
            signals.append({
                "type": "bullish",
                "source": "MA",
                "message": "短期均線在長期均線上方，多頭排列",
                "strength": "medium"
            })
        elif ma["ma5"] < ma["ma20"] and current_price < ma["ma5"]:
            signals.append({
                "type": "bearish",
                "source": "MA",
                "message": "短期均線在長期均線下方，空頭排列",
                "strength": "medium"
            })

    # RSI signals
    if rsi:
        if rsi > 70:
            signals.append({
                "type": "bearish",
                "source": "RSI",
                "message": f"RSI {rsi} 超買區，注意回檔風險",
                "strength": "high"
            })
        elif rsi < 30:
            signals.append({
                "type": "bullish",
                "source": "RSI",
                "message": f"RSI {rsi} 超賣區，可能反彈",
                "strength": "high"
            })

    # Institutional flow signals
    if flows and len(flows) >= 3:
        recent_foreign = sum(f.get("foreign_net", 0) for f in flows[-3:])
        recent_trust = sum(f.get("trust_net", 0) for f in flows[-3:])

        if recent_foreign > 0:
            signals.append({
                "type": "bullish",
                "source": "籌碼",
                "message": f"近3日外資買超 {recent_foreign:,} 張",
                "strength": "high" if recent_foreign > 1000 else "medium"
            })
        elif recent_foreign < 0:
            signals.append({
                "type": "bearish",
                "source": "籌碼",
                "message": f"近3日外資賣超 {abs(recent_foreign):,} 張",
                "strength": "high" if recent_foreign < -1000 else "medium"
            })

        if recent_trust > 0:
            signals.append({
                "type": "bullish",
                "source": "籌碼",
                "message": f"近3日投信買超 {recent_trust:,} 張",
                "strength": "medium"
            })

    # Volume signal
    if len(prices) >= 5:
        recent_vol = sum(p.get("volume", 0) for p in prices[-5:]) / 5
        avg_vol = sum(p.get("volume", 0) for p in prices[-20:]) / 20 if len(prices) >= 20 else recent_vol
        if avg_vol > 0 and recent_vol > avg_vol * 1.5:
            signals.append({
                "type": "neutral",
                "source": "成交量",
                "message": "近期成交量放大，關注突破方向",
                "strength": "medium"
            })

    return signals


@router.get("/{stock_code}")
def get_stock_analysis(
    stock_code: str,
    start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
    days: int = Query(90, description="Days of data (default 90, max 92). Ignored if start_date/end_date provided."),
    db: Session = Depends(get_db),
):
    """Get comprehensive stock analysis including technicals, flows, and signals.

    - Default: last 90 days (3 months)
    - Can specify start_date and end_date for custom range
    - Max query range: 92 days (3 months)
    """

    # Get stock info
    stock_query = text("SELECT id, code, name, market FROM stocks WHERE code = :code")
    stock_result = db.execute(stock_query, {"code": stock_code}).fetchone()

    if not stock_result:
        raise HTTPException(status_code=404, detail=f"Stock {stock_code} not found")

    stock_id = stock_result.id

    # Determine date range
    if start_date and end_date:
        # Validate date range
        if end_date < start_date:
            raise HTTPException(status_code=400, detail="end_date must be >= start_date")
        delta_days = (end_date - start_date).days
        if delta_days > MAX_QUERY_DAYS:
            raise HTTPException(
                status_code=400,
                detail=f"Date range exceeds maximum of {MAX_QUERY_DAYS} days (3 months)"
            )
        query_start = start_date
        query_end = end_date
    else:
        # Default: last N days
        effective_days = min(days, MAX_QUERY_DAYS)
        query_end = date.today()
        query_start = query_end - timedelta(days=effective_days)

    # Get price data with date range
    price_query = text("""
        SELECT trade_date, open_price, high_price, low_price, close_price, volume
        FROM stock_prices
        WHERE stock_id = :stock_id
          AND trade_date >= :start_date
          AND trade_date <= :end_date
        ORDER BY trade_date ASC
    """)
    price_rows = db.execute(price_query, {
        "stock_id": stock_id,
        "start_date": query_start,
        "end_date": query_end
    }).fetchall()

    prices = [{
        "date": str(row.trade_date),
        "open": safe_float(row.open_price),
        "high": safe_float(row.high_price),
        "low": safe_float(row.low_price),
        "close": safe_float(row.close_price),
        "volume": int(row.volume) if row.volume else 0,
    } for row in price_rows]

    # Get institutional flows with date range
    flow_query = text("""
        SELECT trade_date, foreign_net, trust_net, dealer_net
        FROM institutional_flows
        WHERE stock_id = :stock_id
          AND trade_date >= :start_date
          AND trade_date <= :end_date
        ORDER BY trade_date ASC
    """)
    flow_rows = db.execute(flow_query, {
        "stock_id": stock_id,
        "start_date": query_start,
        "end_date": query_end
    }).fetchall()

    flows = [{
        "date": str(row.trade_date),
        "foreign_net": int(row.foreign_net) if row.foreign_net else 0,
        "trust_net": int(row.trust_net) if row.trust_net else 0,
        "dealer_net": int(row.dealer_net) if row.dealer_net else 0,
    } for row in reversed(flow_rows)]

    # Calculate technicals
    ma = calculate_moving_averages(prices)
    rsi = calculate_rsi(prices)
    macd = calculate_macd(prices)
    support_resistance = calculate_support_resistance(prices)

    # Generate signals
    signals = generate_signals(prices, flows, ma, rsi)

    # Calculate summary stats
    current_price = prices[-1]["close"] if prices else None
    price_change = None
    price_change_pct = None
    if len(prices) >= 2 and prices[-1]["close"] and prices[-2]["close"] and prices[-2]["close"] != 0:
        price_change = safe_float(prices[-1]["close"] - prices[-2]["close"])
        price_change_pct = safe_float((price_change / prices[-2]["close"]) * 100) if price_change else None

    # Institutional summary
    foreign_5d = sum(f["foreign_net"] for f in flows[-5:]) if len(flows) >= 5 else None
    foreign_20d = sum(f["foreign_net"] for f in flows[-20:]) if len(flows) >= 20 else None
    trust_5d = sum(f["trust_net"] for f in flows[-5:]) if len(flows) >= 5 else None
    trust_20d = sum(f["trust_net"] for f in flows[-20:]) if len(flows) >= 20 else None

    return {
        "stock": {
            "code": stock_result.code,
            "name": stock_result.name,
            "market": stock_result.market,
        },
        "query_range": {
            "start_date": str(query_start),
            "end_date": str(query_end),
            "days": (query_end - query_start).days,
        },
        "current": {
            "price": current_price,
            "change": price_change,
            "change_pct": price_change_pct,
            "volume": prices[-1]["volume"] if prices else None,
        },
        "technicals": {
            "ma": ma,
            "rsi": rsi,
            "macd": macd,
            "support_resistance": support_resistance,
        },
        "institutional": {
            "foreign_5d": foreign_5d,
            "foreign_20d": foreign_20d,
            "trust_5d": trust_5d,
            "trust_20d": trust_20d,
            "latest_flows": flows[-5:] if flows else [],
        },
        "signals": signals,
        "chart_data": {
            "prices": prices,
            "flows": flows,
        },
    }


@router.get("/{stock_code}/brokers")
def get_stock_brokers(
    stock_code: str,
    days: int = Query(10, description="Days of broker data to fetch (default 10)"),
    db: Session = Depends(get_db),
):
    """Get broker trading data for a stock (last N days)."""

    # Get stock info
    stock_query = text("SELECT id, name FROM stocks WHERE code = :code")
    stock_result = db.execute(stock_query, {"code": stock_code}).fetchone()

    if not stock_result:
        raise HTTPException(status_code=404, detail=f"Stock {stock_code} not found")

    stock_id = stock_result.id
    stock_name = stock_result.name

    # Get broker data for the last N days
    broker_query = text("""
        SELECT trade_date, broker_name, broker_id, buy_vol, sell_vol, net_vol, pct
        FROM broker_trades
        WHERE stock_id = :stock_id
          AND trade_date >= CURRENT_DATE - :days
        ORDER BY trade_date DESC, ABS(net_vol) DESC
    """)
    broker_rows = db.execute(broker_query, {"stock_id": stock_id, "days": days}).fetchall()

    brokers = [{
        "date": str(row.trade_date),
        "name": row.broker_name,
        "id": row.broker_id,
        "buy": row.buy_vol,
        "sell": row.sell_vol,
        "net": row.net_vol,
        "pct": safe_float(row.pct, 0),
    } for row in broker_rows]

    # Group by date with sorted dates
    by_date = {}
    for b in brokers:
        d = b["date"]
        if d not in by_date:
            by_date[d] = {"date": d, "buy": [], "sell": []}
        if b["net"] > 0:
            by_date[d]["buy"].append(b)
        else:
            by_date[d]["sell"].append(b)

    # Get date range
    dates = sorted(by_date.keys(), reverse=True)
    date_range = {
        "start_date": dates[-1] if dates else None,
        "end_date": dates[0] if dates else None,
        "total_days": len(dates),
    }

    return {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "date_range": date_range,
        "broker_data": by_date,
        "top_buyers": sorted([b for b in brokers if b["net"] > 0], key=lambda x: x["net"], reverse=True)[:10],
        "top_sellers": sorted([b for b in brokers if b["net"] < 0], key=lambda x: x["net"])[:10],
    }


@router.get("/{stock_code}/date-range")
def get_stock_date_range(
    stock_code: str,
    db: Session = Depends(get_db),
):
    """Get available date range for a stock's historical data."""

    # Get stock info
    stock_query = text("SELECT id, code, name FROM stocks WHERE code = :code")
    stock_result = db.execute(stock_query, {"code": stock_code}).fetchone()

    if not stock_result:
        raise HTTPException(status_code=404, detail=f"Stock {stock_code} not found")

    stock_id = stock_result.id

    # Get date range from stock_prices
    range_query = text("""
        SELECT MIN(trade_date) as min_date, MAX(trade_date) as max_date, COUNT(*) as total_days
        FROM stock_prices
        WHERE stock_id = :stock_id
    """)
    result = db.execute(range_query, {"stock_id": stock_id}).fetchone()

    if not result.min_date:
        return {
            "stock_code": stock_code,
            "stock_name": stock_result.name,
            "has_data": False,
            "min_date": None,
            "max_date": None,
            "total_days": 0,
        }

    return {
        "stock_code": stock_code,
        "stock_name": stock_result.name,
        "has_data": True,
        "min_date": str(result.min_date),
        "max_date": str(result.max_date),
        "total_days": result.total_days,
        "max_query_days": MAX_QUERY_DAYS,
    }
