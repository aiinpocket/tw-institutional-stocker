"""AI-powered stock analysis and recommendations."""
import os
import json
from typing import Optional, List, Dict, Any
from datetime import date, timedelta
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from openai import OpenAI

from src.api.dependencies import get_db

router = APIRouter()


def get_openai_client() -> Optional[OpenAI]:
    """Get OpenAI client with API key from environment or file."""
    api_key = os.environ.get("OPENAI_API_KEY")

    # Fallback: try to read from file (for local development)
    if not api_key:
        key_file = os.path.join(os.path.dirname(__file__), "..", "..", "..", "openaiKEY.txt")
        if os.path.exists(key_file):
            with open(key_file, "r") as f:
                api_key = f.read().strip()

    if not api_key:
        return None

    return OpenAI(api_key=api_key)


def get_stock_data(db: Session, stock_code: str, days: int = 20) -> Dict[str, Any]:
    """Gather comprehensive stock data for AI analysis."""

    # Basic stock info
    stock_query = text("""
        SELECT s.code, s.name, s.industry, s.market, s.total_shares
        FROM stocks s
        WHERE s.code = :code
    """)
    stock = db.execute(stock_query, {"code": stock_code}).fetchone()

    if not stock:
        return None

    # Recent price data
    price_query = text("""
        SELECT trade_date, open_price, high_price, low_price, close_price,
               volume, change_percent
        FROM stock_prices sp
        JOIN stocks s ON sp.stock_id = s.id
        WHERE s.code = :code
        ORDER BY trade_date DESC
        LIMIT :days
    """)
    prices = db.execute(price_query, {"code": stock_code, "days": days}).fetchall()

    # Institutional flows
    flow_query = text("""
        SELECT trade_date, foreign_net, trust_net, dealer_net
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE s.code = :code
        ORDER BY trade_date DESC
        LIMIT :days
    """)
    flows = db.execute(flow_query, {"code": stock_code, "days": days}).fetchall()

    # Cumulative institutional data
    cum_query = text("""
        SELECT
            SUM(CASE WHEN trade_date >= CURRENT_DATE - 5 THEN foreign_net ELSE 0 END) as foreign_5d,
            SUM(CASE WHEN trade_date >= CURRENT_DATE - 20 THEN foreign_net ELSE 0 END) as foreign_20d,
            SUM(CASE WHEN trade_date >= CURRENT_DATE - 5 THEN trust_net ELSE 0 END) as trust_5d,
            SUM(CASE WHEN trade_date >= CURRENT_DATE - 20 THEN trust_net ELSE 0 END) as trust_20d,
            SUM(CASE WHEN trade_date >= CURRENT_DATE - 5 THEN dealer_net ELSE 0 END) as dealer_5d,
            SUM(CASE WHEN trade_date >= CURRENT_DATE - 20 THEN dealer_net ELSE 0 END) as dealer_20d
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE s.code = :code
    """)
    cumulative = db.execute(cum_query, {"code": stock_code}).fetchone()

    # Foreign holding ratio
    holding_query = text("""
        SELECT foreign_ratio
        FROM foreign_holdings fh
        JOIN stocks s ON fh.stock_id = s.id
        WHERE s.code = :code
        ORDER BY trade_date DESC
        LIMIT 1
    """)
    holding = db.execute(holding_query, {"code": stock_code}).fetchone()

    # Top brokers
    broker_query = text("""
        SELECT broker_name, SUM(net_vol) as net_vol
        FROM broker_trades bt
        JOIN stocks s ON bt.stock_id = s.id
        WHERE s.code = :code AND trade_date >= CURRENT_DATE - 5
        GROUP BY broker_name
        ORDER BY ABS(SUM(net_vol)) DESC
        LIMIT 5
    """)
    brokers = db.execute(broker_query, {"code": stock_code}).fetchall()

    return {
        "stock": {
            "code": stock.code,
            "name": stock.name,
            "industry": stock.industry,
            "market": stock.market,
        },
        "prices": [
            {
                "date": str(p.trade_date),
                "close": float(p.close_price) if p.close_price else None,
                "volume": p.volume,
                "change_pct": float(p.change_percent) if p.change_percent else None,
            }
            for p in prices
        ],
        "flows": [
            {
                "date": str(f.trade_date),
                "foreign": f.foreign_net,
                "trust": f.trust_net,
                "dealer": f.dealer_net,
            }
            for f in flows
        ],
        "cumulative": {
            "foreign_5d": cumulative.foreign_5d or 0,
            "foreign_20d": cumulative.foreign_20d or 0,
            "trust_5d": cumulative.trust_5d or 0,
            "trust_20d": cumulative.trust_20d or 0,
            "dealer_5d": cumulative.dealer_5d or 0,
            "dealer_20d": cumulative.dealer_20d or 0,
        },
        "foreign_ratio": float(holding.foreign_ratio) if holding and holding.foreign_ratio else None,
        "top_brokers": [
            {"name": b.broker_name, "net_vol": b.net_vol}
            for b in brokers
        ],
    }


def get_market_overview(db: Session) -> Dict[str, Any]:
    """Get market overview data for AI recommendations."""

    # Industry flows
    industry_query = text("""
        SELECT
            COALESCE(s.industry, '其他業') as industry,
            SUM(f.foreign_net + f.trust_net + f.dealer_net) as total_net
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date >= CURRENT_DATE - 5
        GROUP BY COALESCE(s.industry, '其他業')
        ORDER BY total_net DESC
        LIMIT 10
    """)
    industries = db.execute(industry_query).fetchall()

    # Top foreign buying
    foreign_query = text("""
        SELECT s.code, s.name, s.industry,
               SUM(f.foreign_net) as foreign_net
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date >= CURRENT_DATE - 5
        GROUP BY s.code, s.name, s.industry
        ORDER BY SUM(f.foreign_net) DESC
        LIMIT 10
    """)
    foreign_top = db.execute(foreign_query).fetchall()

    # Top trust buying
    trust_query = text("""
        SELECT s.code, s.name, s.industry,
               SUM(f.trust_net) as trust_net
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date >= CURRENT_DATE - 5
        GROUP BY s.code, s.name, s.industry
        ORDER BY SUM(f.trust_net) DESC
        LIMIT 10
    """)
    trust_top = db.execute(trust_query).fetchall()

    # Consecutive buying stocks
    consecutive_query = text("""
        WITH daily_data AS (
            SELECT s.code, s.name, s.industry, f.trade_date, f.foreign_net
            FROM institutional_flows f
            JOIN stocks s ON f.stock_id = s.id
            WHERE f.trade_date >= CURRENT_DATE - 10
        ),
        with_streak AS (
            SELECT code, name, industry,
                   COUNT(*) FILTER (WHERE foreign_net > 0) as buy_days
            FROM daily_data
            GROUP BY code, name, industry
        )
        SELECT * FROM with_streak
        WHERE buy_days >= 5
        ORDER BY buy_days DESC
        LIMIT 10
    """)
    consecutive = db.execute(consecutive_query).fetchall()

    return {
        "hot_industries": [
            {"industry": i.industry, "net_flow": i.total_net}
            for i in industries
        ],
        "foreign_favorites": [
            {"code": f.code, "name": f.name, "industry": f.industry, "net": f.foreign_net}
            for f in foreign_top
        ],
        "trust_favorites": [
            {"code": t.code, "name": t.name, "industry": t.industry, "net": t.trust_net}
            for t in trust_top
        ],
        "consecutive_buying": [
            {"code": c.code, "name": c.name, "industry": c.industry, "days": c.buy_days}
            for c in consecutive
        ],
    }


@router.get("/stock/{stock_code}")
def analyze_stock(
    stock_code: str,
    db: Session = Depends(get_db),
):
    """
    AI 個股分析報告。
    綜合技術面與籌碼面資料，提供 AI 分析建議。
    """
    client = get_openai_client()
    if not client:
        raise HTTPException(status_code=503, detail="AI 服務暫時無法使用")

    # Gather stock data
    data = get_stock_data(db, stock_code)
    if not data:
        raise HTTPException(status_code=404, detail=f"找不到股票 {stock_code}")

    # Build prompt
    prompt = f"""你是專業的台灣股票分析師。請根據以下數據分析這檔股票，並給出投資建議。

**股票資訊**
- 代碼：{data['stock']['code']}
- 名稱：{data['stock']['name']}
- 產業：{data['stock']['industry']}
- 市場：{data['stock']['market']}
- 外資持股比例：{data['foreign_ratio']:.2f}%

**近期股價走勢**（最近 5 天）
{json.dumps(data['prices'][:5], ensure_ascii=False, indent=2)}

**三大法人動向**
- 外資 5 日累計：{data['cumulative']['foreign_5d']:,} 張
- 外資 20 日累計：{data['cumulative']['foreign_20d']:,} 張
- 投信 5 日累計：{data['cumulative']['trust_5d']:,} 張
- 投信 20 日累計：{data['cumulative']['trust_20d']:,} 張
- 自營商 5 日累計：{data['cumulative']['dealer_5d']:,} 張
- 自營商 20 日累計：{data['cumulative']['dealer_20d']:,} 張

**主力券商動向**（近 5 日）
{json.dumps(data['top_brokers'], ensure_ascii=False, indent=2)}

請提供：
1. 籌碼面分析（法人動向解讀）
2. 技術面觀察（價量關係）
3. 風險提示
4. 操作建議（短期/中期觀點）

請用繁體中文回答，語氣專業但易懂。
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "你是專業的台灣股票分析師，擅長籌碼分析和技術分析。回答要專業、客觀、謹慎，並提醒投資風險。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=2000,
        )

        analysis = response.choices[0].message.content

        return {
            "stock_code": stock_code,
            "stock_name": data['stock']['name'],
            "industry": data['stock']['industry'],
            "analysis": analysis,
            "data_summary": {
                "foreign_ratio": data['foreign_ratio'],
                "foreign_5d": data['cumulative']['foreign_5d'],
                "foreign_20d": data['cumulative']['foreign_20d'],
                "trust_5d": data['cumulative']['trust_5d'],
                "trust_20d": data['cumulative']['trust_20d'],
            },
            "disclaimer": "本分析僅供參考，不構成投資建議。投資有風險，請審慎評估。"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI 分析失敗: {str(e)}")


@router.get("/recommendations")
def get_recommendations(
    strategy: str = Query("balanced", description="投資策略：aggressive/balanced/conservative"),
    limit: int = Query(10, le=20),
    db: Session = Depends(get_db),
):
    """
    AI 智能選股建議。
    根據當前市場狀況和投資策略，推薦值得關注的股票。
    """
    client = get_openai_client()
    if not client:
        raise HTTPException(status_code=503, detail="AI 服務暫時無法使用")

    # Gather market data
    market_data = get_market_overview(db)

    strategy_desc = {
        "aggressive": "積極型：追求高報酬，可承受較高風險，偏好動能強勁的標的",
        "balanced": "穩健型：追求穩定成長，風險與報酬平衡，偏好法人認同的標的",
        "conservative": "保守型：以保本為主，偏好大型權值股和高外資持股標的",
    }

    prompt = f"""你是專業的台灣股票投資顧問。請根據以下市場數據，為「{strategy_desc.get(strategy, strategy_desc['balanced'])}」的投資者推薦 {limit} 檔值得關注的股票。

**產業資金流向**（近 5 日法人買賣超）
{json.dumps(market_data['hot_industries'], ensure_ascii=False, indent=2)}

**外資買超前 10 名**
{json.dumps(market_data['foreign_favorites'], ensure_ascii=False, indent=2)}

**投信買超前 10 名**
{json.dumps(market_data['trust_favorites'], ensure_ascii=False, indent=2)}

**外資連續買超股票**
{json.dumps(market_data['consecutive_buying'], ensure_ascii=False, indent=2)}

請根據上述數據，推薦 {limit} 檔股票，每檔股票請提供：
1. 股票代碼和名稱
2. 推薦理由（50字內）
3. 關注重點
4. 風險提示

請以 JSON 格式回覆，格式如下：
{{
    "market_view": "對當前市場的整體看法（100字內）",
    "recommendations": [
        {{
            "code": "2330",
            "name": "台積電",
            "reason": "外資連續買超，產業趨勢向上",
            "focus": "關注月營收和法說會",
            "risk": "估值偏高，注意回檔風險"
        }}
    ]
}}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "你是專業的台灣股票投資顧問，擅長根據籌碼面分析推薦股票。回答要專業、客觀，並提醒投資風險。只回傳 JSON 格式。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=2000,
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)

        return {
            "strategy": strategy,
            "strategy_description": strategy_desc.get(strategy, strategy_desc['balanced']),
            "market_view": result.get("market_view", ""),
            "recommendations": result.get("recommendations", []),
            "data_date": str(date.today()),
            "disclaimer": "本推薦僅供參考，不構成投資建議。投資有風險，請審慎評估並自行判斷。"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI 推薦失敗: {str(e)}")


@router.get("/market-summary")
def get_market_summary(
    db: Session = Depends(get_db),
):
    """
    AI 市場摘要。
    分析當前市場狀況，提供整體市場觀點。
    """
    client = get_openai_client()
    if not client:
        raise HTTPException(status_code=503, detail="AI 服務暫時無法使用")

    # Gather market data
    market_data = get_market_overview(db)

    # Calculate some statistics
    total_foreign = sum(i['net'] for i in market_data['foreign_favorites'])
    total_trust = sum(i['net'] for i in market_data['trust_favorites'])

    prompt = f"""你是專業的台灣股市分析師。請根據以下法人動向數據，提供今日市場摘要分析。

**產業資金流向**（近 5 日）
{json.dumps(market_data['hot_industries'], ensure_ascii=False, indent=2)}

**外資動向**
- 買超前 10 名合計：{total_foreign:,} 張
- 主要買超標的：{', '.join([f"{s['name']}({s['code']})" for s in market_data['foreign_favorites'][:5]])}

**投信動向**
- 買超前 10 名合計：{total_trust:,} 張
- 主要買超標的：{', '.join([f"{s['name']}({s['code']})" for s in market_data['trust_favorites'][:5]])}

**連續買超觀察**
{json.dumps(market_data['consecutive_buying'][:5], ensure_ascii=False, indent=2)}

請提供：
1. 市場氛圍評估（多/空/盤整）
2. 資金流向解讀（哪些產業受青睞）
3. 法人態度分析
4. 後市展望
5. 操作建議

請用繁體中文回答，約 300-500 字。
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "你是專業的台灣股市分析師，擅長解讀法人籌碼和市場趨勢。語氣專業但易懂。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=1500,
        )

        summary = response.choices[0].message.content

        return {
            "date": str(date.today()),
            "summary": summary,
            "hot_industries": market_data['hot_industries'][:5],
            "foreign_top5": market_data['foreign_favorites'][:5],
            "trust_top5": market_data['trust_favorites'][:5],
            "disclaimer": "本分析僅供參考，不構成投資建議。"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI 分析失敗: {str(e)}")


@router.get("/compare")
def compare_stocks(
    codes: str = Query(..., description="股票代碼，用逗號分隔，例如：2330,2317,2454"),
    db: Session = Depends(get_db),
):
    """
    AI 股票比較分析。
    比較多檔股票的籌碼面表現，提供相對強弱分析。
    """
    client = get_openai_client()
    if not client:
        raise HTTPException(status_code=503, detail="AI 服務暫時無法使用")

    stock_codes = [c.strip() for c in codes.split(",")]
    if len(stock_codes) < 2:
        raise HTTPException(status_code=400, detail="請提供至少 2 檔股票進行比較")
    if len(stock_codes) > 5:
        raise HTTPException(status_code=400, detail="最多比較 5 檔股票")

    # Gather data for each stock
    stocks_data = []
    for code in stock_codes:
        data = get_stock_data(db, code, days=10)
        if data:
            stocks_data.append(data)

    if len(stocks_data) < 2:
        raise HTTPException(status_code=404, detail="找不到足夠的股票資料進行比較")

    # Build comparison table
    comparison = []
    for s in stocks_data:
        latest_price = s['prices'][0] if s['prices'] else {}
        comparison.append({
            "code": s['stock']['code'],
            "name": s['stock']['name'],
            "industry": s['stock']['industry'],
            "price": latest_price.get('close'),
            "change_pct": latest_price.get('change_pct'),
            "foreign_5d": s['cumulative']['foreign_5d'],
            "foreign_20d": s['cumulative']['foreign_20d'],
            "trust_5d": s['cumulative']['trust_5d'],
            "trust_20d": s['cumulative']['trust_20d'],
            "foreign_ratio": s['foreign_ratio'],
        })

    prompt = f"""你是專業的台灣股票分析師。請比較以下股票的籌碼面表現，分析相對強弱。

**股票比較表**
{json.dumps(comparison, ensure_ascii=False, indent=2)}

請提供：
1. 籌碼面強弱排名（根據法人買賣超）
2. 各股票的相對優劣勢
3. 如果只能選一檔，你會選哪一檔？為什麼？
4. 風險提示

請用繁體中文回答。
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "你是專業的台灣股票分析師，擅長比較分析和籌碼解讀。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=1500,
        )

        analysis = response.choices[0].message.content

        return {
            "stocks": comparison,
            "analysis": analysis,
            "disclaimer": "本分析僅供參考，不構成投資建議。投資有風險，請審慎評估。"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI 比較分析失敗: {str(e)}")
