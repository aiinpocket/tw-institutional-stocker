"""Industry classification fetcher with AI-powered auto-classification."""
import os
import json
from typing import List, Dict, Optional
from openai import OpenAI

from src.common.config import settings


# 標準產業分類清單 (基於台灣證交所官方分類)
STANDARD_INDUSTRIES = [
    "水泥工業",
    "食品工業",
    "塑膠工業",
    "紡織纖維",
    "電機機械",
    "電器電纜",
    "化學工業",
    "生技醫療業",
    "玻璃陶瓷",
    "造紙工業",
    "鋼鐵工業",
    "橡膠工業",
    "汽車工業",
    "半導體業",
    "電腦及週邊設備業",
    "光電業",
    "通信網路業",
    "電子零組件業",
    "電子通路業",
    "資訊服務業",
    "其他電子業",
    "建材營造業",
    "航運業",
    "觀光餐旅",
    "金融保險業",
    "貿易百貨業",
    "油電燃氣業",
    "綜合",
    "其他業",
]


# 常見股票的產業分類（手動維護的主要股票，用於加速分類）
STOCK_INDUSTRY_MAP = {
    # 半導體
    "2330": "半導體業", "2303": "半導體業", "2454": "半導體業",
    "3711": "半導體業", "2379": "半導體業", "3034": "半導體業",
    "2408": "半導體業", "6415": "半導體業", "3529": "半導體業",
    "2449": "半導體業", "5347": "半導體業", "6770": "半導體業",
    # 電腦及週邊
    "2382": "電腦及週邊設備業", "2357": "電腦及週邊設備業",
    "2301": "電腦及週邊設備業", "3231": "電腦及週邊設備業",
    "2353": "電腦及週邊設備業", "2324": "電腦及週邊設備業",
    "2356": "電腦及週邊設備業", "6669": "電腦及週邊設備業",
    # 光電
    "2409": "光電業", "3008": "光電業", "2393": "光電業",
    "3481": "光電業", "2426": "光電業", "6176": "光電業",
    # 通信網路
    "2412": "通信網路業", "3045": "通信網路業", "4904": "通信網路業",
    "2498": "通信網路業", "6285": "通信網路業",
    # 電子零組件
    "2317": "電子零組件業", "2327": "電子零組件業", "3037": "電子零組件業",
    "2308": "電子零組件業", "2345": "電子零組件業", "2474": "電子零組件業",
    "3533": "電子零組件業", "2377": "電子零組件業",
    # 金融保險
    "2881": "金融保險業", "2882": "金融保險業", "2891": "金融保險業",
    "2886": "金融保險業", "2884": "金融保險業", "2892": "金融保險業",
    "2880": "金融保險業", "5880": "金融保險業", "2883": "金融保險業",
    "2801": "金融保險業", "2890": "金融保險業", "2887": "金融保險業",
    "2885": "金融保險業", "5876": "金融保險業", "2834": "金融保險業",
    # 航運
    "2603": "航運業", "2609": "航運業", "2615": "航運業",
    "2606": "航運業", "2618": "航運業", "2634": "航運業",
    # 鋼鐵
    "2002": "鋼鐵工業", "2006": "鋼鐵工業", "2014": "鋼鐵工業",
    # 塑膠
    "1301": "塑膠工業", "1303": "塑膠工業", "1326": "塑膠工業",
    "6505": "塑膠工業", "1304": "塑膠工業",
    # 紡織
    "1402": "紡織纖維", "1476": "紡織纖維", "9910": "紡織纖維",
    # 食品
    "1216": "食品工業", "1229": "食品工業", "1227": "食品工業",
    # 汽車
    "2207": "汽車工業", "2201": "汽車工業", "2204": "汽車工業",
    # 電機機械
    "1503": "電機機械", "2049": "電機機械", "1504": "電機機械",
    # 生技醫療
    "4743": "生技醫療業", "6446": "生技醫療業", "1795": "生技醫療業",
    # 貿易百貨
    "2912": "貿易百貨業", "2915": "貿易百貨業", "2903": "貿易百貨業",
    # 建材營造
    "2504": "建材營造業", "2542": "建材營造業", "5522": "建材營造業",
    # 觀光餐旅
    "2707": "觀光餐旅", "2727": "觀光餐旅",
    # 其他電子
    "2395": "其他電子業", "2354": "其他電子業",
    # 電子通路
    "3702": "電子通路業", "2347": "電子通路業", "6294": "電子通路業",
    # 資訊服務
    "2468": "資訊服務業", "6214": "資訊服務業",
    # 化學工業
    "1710": "化學工業", "1722": "化學工業", "4763": "化學工業",
    # 油電燃氣
    "9926": "油電燃氣業",
}


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
        print("[WARN] OpenAI API key not found")
        return None

    return OpenAI(api_key=api_key)


def classify_stocks_with_ai(stocks: List[Dict[str, str]]) -> Dict[str, str]:
    """Use OpenAI to classify multiple stocks at once.

    Args:
        stocks: List of dicts with 'code' and 'name' keys

    Returns:
        Dict mapping stock code to industry name
    """
    if not stocks:
        return {}

    client = get_openai_client()
    if not client:
        print("[WARN] No OpenAI client available, using default classification")
        return {s["code"]: "其他業" for s in stocks}

    # Build the prompt with stock list
    stock_list = "\n".join([f"- {s['code']}: {s['name']}" for s in stocks])
    industries_list = "\n".join([f"- {ind}" for ind in STANDARD_INDUSTRIES])

    prompt = f"""你是台灣股票產業分類專家。請根據股票代碼和公司名稱，將以下股票分類到對應的產業。

**重要規則：**
1. 你只能從以下產業清單中選擇，不能創造新的產業分類
2. 如果無法確定，請選擇「其他業」
3. 請回傳 JSON 格式，key 是股票代碼，value 是產業名稱

**可用的產業分類：**
{industries_list}

**待分類的股票：**
{stock_list}

請以 JSON 格式回覆，例如：
{{"2330": "半導體業", "2317": "電子零組件業"}}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "你是專業的台灣股票產業分類專家，只回傳 JSON 格式的分類結果。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,  # Low temperature for consistent classification
            max_tokens=2000,
            response_format={"type": "json_object"}
        )

        result_text = response.choices[0].message.content
        result = json.loads(result_text)

        # Validate that all industries are in the standard list
        validated_result = {}
        for code, industry in result.items():
            if industry in STANDARD_INDUSTRIES:
                validated_result[code] = industry
            else:
                print(f"[WARN] AI returned invalid industry '{industry}' for {code}, using '其他業'")
                validated_result[code] = "其他業"

        return validated_result

    except Exception as e:
        print(f"[ERROR] OpenAI classification failed: {e}")
        return {s["code"]: "其他業" for s in stocks}


def get_stock_industry(code: str) -> str:
    """Get industry for a specific stock code.

    Args:
        code: Stock code

    Returns:
        Industry name or "其他業" if not found
    """
    return STOCK_INDUSTRY_MAP.get(code, "其他業")


def update_stock_industries(db_session, use_ai: bool = True, batch_size: int = 50):
    """Update industry field for all stocks in database.

    Args:
        db_session: Database session
        use_ai: Whether to use AI for unclassified stocks
        batch_size: Number of stocks to classify per AI call

    Returns:
        Number of stocks updated
    """
    from sqlalchemy import text

    updated = 0

    # Step 1: Apply manual mapping for known stocks
    for code, industry in STOCK_INDUSTRY_MAP.items():
        query = text("""
            UPDATE stocks SET industry = :industry
            WHERE code = :code AND (industry IS NULL OR industry != :industry)
        """)
        result = db_session.execute(query, {"code": code, "industry": industry})
        updated += result.rowcount

    db_session.commit()
    print(f"[INFO] Updated {updated} stocks from manual mapping")

    # Step 2: Find stocks without industry classification
    if use_ai:
        query = text("""
            SELECT code, name FROM stocks
            WHERE industry IS NULL AND is_active = true
        """)
        unclassified = db_session.execute(query).fetchall()

        if unclassified:
            print(f"[INFO] Found {len(unclassified)} unclassified stocks, using AI...")

            # Process in batches
            stocks_to_classify = [{"code": r.code, "name": r.name} for r in unclassified]

            for i in range(0, len(stocks_to_classify), batch_size):
                batch = stocks_to_classify[i:i + batch_size]
                print(f"[INFO] Classifying batch {i // batch_size + 1} ({len(batch)} stocks)...")

                classifications = classify_stocks_with_ai(batch)

                # Update database with AI classifications
                for code, industry in classifications.items():
                    update_query = text("""
                        UPDATE stocks SET industry = :industry
                        WHERE code = :code
                    """)
                    result = db_session.execute(update_query, {"code": code, "industry": industry})
                    updated += result.rowcount

                db_session.commit()
                print(f"[INFO] Batch classified: {len(classifications)} stocks")

    # Step 3: Set remaining unclassified to "其他業"
    query = text("""
        UPDATE stocks SET industry = '其他業'
        WHERE industry IS NULL
    """)
    result = db_session.execute(query)
    if result.rowcount > 0:
        print(f"[INFO] Set {result.rowcount} remaining stocks to '其他業'")
    updated += result.rowcount

    db_session.commit()
    return updated


def get_standard_industries() -> List[str]:
    """Get the list of standard industry classifications.

    Returns:
        List of industry names
    """
    return STANDARD_INDUSTRIES.copy()
