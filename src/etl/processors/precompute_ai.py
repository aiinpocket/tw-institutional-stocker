"""Pre-compute AI analysis results after ETL completion.

Optimized version with parallel execution using ThreadPoolExecutor.
"""
import os
import json
import logging
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_openai_client():
    """Get OpenAI client."""
    try:
        from openai import OpenAI
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            key_file = "/app/openaiKEY.txt"
            if os.path.exists(key_file):
                with open(key_file, "r") as f:
                    api_key = f.read().strip()
        if api_key:
            return OpenAI(api_key=api_key)
    except Exception as e:
        logger.warning(f"Failed to get OpenAI client: {e}")
    return None


def get_latest_data_date(db) -> date:
    """Get the latest data date."""
    result = db.execute(text("SELECT MAX(trade_date) FROM stock_prices")).scalar()
    return result


def set_cache(db, cache_key: str, cache_type: str, cache_data: dict, data_date: date):
    """Store AI analysis result in cache."""
    try:
        query = text("""
            INSERT INTO ai_analysis_cache (cache_key, cache_type, cache_data, data_date)
            VALUES (:cache_key, :cache_type, :cache_data, :data_date)
            ON CONFLICT (cache_key) DO UPDATE SET
                cache_data = :cache_data,
                data_date = :data_date,
                created_at = CURRENT_TIMESTAMP
        """)
        db.execute(query, {
            "cache_key": cache_key,
            "cache_type": cache_type,
            "cache_data": json.dumps(cache_data, ensure_ascii=False),
            "data_date": data_date
        })
        db.commit()
    except Exception as e:
        logger.warning(f"Failed to set cache for {cache_key}: {e}")
        db.rollback()


def compute_market_summary(db, client, data_date: date):
    """Pre-compute market summary AI analysis."""
    logger.info("Pre-computing market summary...")

    # Gather market data
    industry_query = text("""
        SELECT
            COALESCE(s.industry, '其他業') as industry,
            SUM(f.foreign_net + f.trust_net + f.dealer_net) as total_net
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date >= :data_date - 5
        GROUP BY COALESCE(s.industry, '其他業')
        ORDER BY total_net DESC
        LIMIT 10
    """)
    industries = db.execute(industry_query, {"data_date": data_date}).fetchall()

    foreign_query = text("""
        SELECT s.code, s.name, s.industry, SUM(f.foreign_net) as foreign_net
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date >= :data_date - 5
        GROUP BY s.code, s.name, s.industry
        ORDER BY SUM(f.foreign_net) DESC
        LIMIT 10
    """)
    foreign_top = db.execute(foreign_query, {"data_date": data_date}).fetchall()

    trust_query = text("""
        SELECT s.code, s.name, s.industry, SUM(f.trust_net) as trust_net
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date >= :data_date - 5
        GROUP BY s.code, s.name, s.industry
        ORDER BY SUM(f.trust_net) DESC
        LIMIT 10
    """)
    trust_top = db.execute(trust_query, {"data_date": data_date}).fetchall()

    hot_industries = [{"industry": i.industry, "net_flow": int(i.total_net) if i.total_net else 0} for i in industries]
    foreign_favorites = [{"code": f.code, "name": f.name, "industry": f.industry, "net": int(f.foreign_net) if f.foreign_net else 0} for f in foreign_top]
    trust_favorites = [{"code": t.code, "name": t.name, "industry": t.industry, "net": int(t.trust_net) if t.trust_net else 0} for t in trust_top]

    total_foreign = sum(f['net'] for f in foreign_favorites)
    total_trust = sum(t['net'] for t in trust_favorites)

    prompt = f"""你是專業的台灣股市分析師。請根據以下法人動向數據，提供今日市場摘要分析。

**產業資金流向**（近 5 日）
{json.dumps(hot_industries, ensure_ascii=False, indent=2)}

**外資動向**
- 買超前 10 名合計：{total_foreign:,} 張
- 主要買超標的：{', '.join([f"{s['name']}({s['code']})" for s in foreign_favorites[:5]])}

**投信動向**
- 買超前 10 名合計：{total_trust:,} 張
- 主要買超標的：{', '.join([f"{s['name']}({s['code']})" for s in trust_favorites[:5]])}

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

        result = {
            "date": str(data_date),
            "summary": summary,
            "hot_industries": hot_industries[:5],
            "foreign_top5": foreign_favorites[:5],
            "trust_top5": trust_favorites[:5],
            "disclaimer": "本分析僅供參考，不構成投資建議。",
            "cached": False
        }

        set_cache(db, "market_summary", "market_summary", result, data_date)
        logger.info("  Market summary cached successfully")
        return True

    except Exception as e:
        logger.error(f"  Failed to compute market summary: {e}")
        return False


def compute_recommendations(db, client, data_date: date, strategy: str = "balanced"):
    """Pre-compute AI recommendations."""
    logger.info(f"Pre-computing recommendations ({strategy})...")

    # Similar market data gathering as market summary
    industry_query = text("""
        SELECT COALESCE(s.industry, '其他業') as industry, SUM(f.foreign_net + f.trust_net + f.dealer_net) as total_net
        FROM institutional_flows f
        JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date >= :data_date - 5
        GROUP BY COALESCE(s.industry, '其他業')
        ORDER BY total_net DESC LIMIT 10
    """)
    industries = db.execute(industry_query, {"data_date": data_date}).fetchall()

    foreign_query = text("""
        SELECT s.code, s.name, s.industry, SUM(f.foreign_net) as foreign_net
        FROM institutional_flows f JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date >= :data_date - 5
        GROUP BY s.code, s.name, s.industry
        ORDER BY SUM(f.foreign_net) DESC LIMIT 10
    """)
    foreign_top = db.execute(foreign_query, {"data_date": data_date}).fetchall()

    trust_query = text("""
        SELECT s.code, s.name, s.industry, SUM(f.trust_net) as trust_net
        FROM institutional_flows f JOIN stocks s ON f.stock_id = s.id
        WHERE f.trade_date >= :data_date - 5
        GROUP BY s.code, s.name, s.industry
        ORDER BY SUM(f.trust_net) DESC LIMIT 10
    """)
    trust_top = db.execute(trust_query, {"data_date": data_date}).fetchall()

    consecutive_query = text("""
        WITH daily_data AS (
            SELECT s.code, s.name, s.industry, f.trade_date, f.foreign_net
            FROM institutional_flows f JOIN stocks s ON f.stock_id = s.id
            WHERE f.trade_date >= :data_date - 10
        )
        SELECT code, name, industry, COUNT(*) FILTER (WHERE foreign_net > 0) as buy_days
        FROM daily_data GROUP BY code, name, industry
        HAVING COUNT(*) FILTER (WHERE foreign_net > 0) >= 5
        ORDER BY buy_days DESC LIMIT 10
    """)
    consecutive = db.execute(consecutive_query, {"data_date": data_date}).fetchall()

    market_data = {
        "hot_industries": [{"industry": i.industry, "net_flow": int(i.total_net) if i.total_net else 0} for i in industries],
        "foreign_favorites": [{"code": f.code, "name": f.name, "industry": f.industry, "net": int(f.foreign_net) if f.foreign_net else 0} for f in foreign_top],
        "trust_favorites": [{"code": t.code, "name": t.name, "industry": t.industry, "net": int(t.trust_net) if t.trust_net else 0} for t in trust_top],
        "consecutive_buying": [{"code": c.code, "name": c.name, "industry": c.industry, "days": int(c.buy_days)} for c in consecutive],
    }

    strategy_desc = {
        "aggressive": "積極型：追求高報酬，可承受較高風險，偏好動能強勁的標的",
        "balanced": "穩健型：追求穩定成長，風險與報酬平衡，偏好法人認同的標的",
        "conservative": "保守型：以保本為主，偏好大型權值股和高外資持股標的",
    }

    prompt = f"""你是專業的台灣股票投資顧問。請根據以下市場數據，為「{strategy_desc.get(strategy)}」的投資者推薦 10 檔值得關注的股票。

**產業資金流向**（近 5 日法人買賣超）
{json.dumps(market_data['hot_industries'], ensure_ascii=False, indent=2)}

**外資買超前 10 名**
{json.dumps(market_data['foreign_favorites'], ensure_ascii=False, indent=2)}

**投信買超前 10 名**
{json.dumps(market_data['trust_favorites'], ensure_ascii=False, indent=2)}

**外資連續買超股票**
{json.dumps(market_data['consecutive_buying'], ensure_ascii=False, indent=2)}

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

        result_json = json.loads(response.choices[0].message.content)

        result = {
            "strategy": strategy,
            "strategy_description": strategy_desc.get(strategy),
            "market_view": result_json.get("market_view", ""),
            "recommendations": result_json.get("recommendations", []),
            "data_date": str(data_date),
            "disclaimer": "本推薦僅供參考，不構成投資建議。投資有風險，請審慎評估並自行判斷。",
            "cached": False
        }

        cache_key = f"recommendations_{strategy}_10"
        set_cache(db, cache_key, "recommendations", result, data_date)
        logger.info(f"  Recommendations ({strategy}) cached successfully")
        return True

    except Exception as e:
        logger.error(f"  Failed to compute recommendations ({strategy}): {e}")
        return False


def _run_market_summary_task(client, data_date: date) -> tuple:
    """Wrapper to run market summary with its own DB session."""
    from src.common.database import SessionLocal
    db = SessionLocal()
    try:
        success = compute_market_summary(db, client, data_date)
        return ("market_summary", success)
    except Exception as e:
        logger.error(f"Market summary task failed: {e}")
        return ("market_summary", False)
    finally:
        db.close()


def _run_recommendations_task(client, data_date: date, strategy: str) -> tuple:
    """Wrapper to run recommendations with its own DB session."""
    from src.common.database import SessionLocal
    db = SessionLocal()
    try:
        success = compute_recommendations(db, client, data_date, strategy)
        return (f"recommendations_{strategy}", success)
    except Exception as e:
        logger.error(f"Recommendations ({strategy}) task failed: {e}")
        return (f"recommendations_{strategy}", False)
    finally:
        db.close()


def run_precompute_ai(db):
    """Run all pre-compute AI tasks in parallel.

    Uses ThreadPoolExecutor to run 4 AI tasks concurrently:
    - 1 market summary
    - 3 strategy recommendations (balanced, aggressive, conservative)

    Each task uses its own database session for thread safety.
    """
    import time
    start_time = time.time()
    logger.info("Starting AI pre-computation (parallel mode)...")

    # 確保 ai_analysis_cache 表存在
    try:
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS ai_analysis_cache (
                id SERIAL PRIMARY KEY,
                cache_key VARCHAR(100) UNIQUE NOT NULL,
                cache_type VARCHAR(50) NOT NULL,
                cache_data JSONB NOT NULL,
                data_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.commit()
    except Exception as e:
        logger.warning(f"Failed to ensure ai_analysis_cache table: {e}")
        db.rollback()

    client = get_openai_client()
    if not client:
        logger.warning("OpenAI client not available, skipping AI pre-computation")
        return

    data_date = get_latest_data_date(db)
    if not data_date:
        logger.warning("No data date found, skipping AI pre-computation")
        return

    logger.info(f"Pre-computing AI analysis for {data_date} with 4 parallel tasks...")

    # Run all 4 tasks in parallel using ThreadPoolExecutor
    strategies = ["balanced", "aggressive", "conservative"]
    results = {}

    with ThreadPoolExecutor(max_workers=4) as executor:
        # Submit all tasks
        futures = {}

        # Market summary task
        futures[executor.submit(_run_market_summary_task, client, data_date)] = "market_summary"

        # Recommendations tasks for each strategy
        for strategy in strategies:
            futures[executor.submit(_run_recommendations_task, client, data_date, strategy)] = f"recommendations_{strategy}"

        # Collect results as they complete
        for future in as_completed(futures):
            task_name = futures[future]
            try:
                result_name, success = future.result()
                results[result_name] = success
                status = "✓" if success else "✗"
                logger.info(f"  {status} {result_name} completed")
            except Exception as e:
                results[task_name] = False
                logger.error(f"  ✗ {task_name} failed with exception: {e}")

    # Summary
    elapsed = time.time() - start_time
    success_count = sum(1 for v in results.values() if v)
    total_count = len(results)

    logger.info(f"AI pre-computation completed: {success_count}/{total_count} tasks successful in {elapsed:.1f}s")


if __name__ == "__main__":
    from src.common.database import SessionLocal
    db = SessionLocal()
    try:
        run_precompute_ai(db)
    finally:
        db.close()
