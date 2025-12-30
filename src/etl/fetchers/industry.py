"""Industry classification fetcher from TWSE/TPEX."""
import requests
import pandas as pd
from io import StringIO

from src.common.config import settings


# TWSE 產業分類對照
TWSE_INDUSTRY_MAP = {
    "01": "水泥工業",
    "02": "食品工業",
    "03": "塑膠工業",
    "04": "紡織纖維",
    "05": "電機機械",
    "06": "電器電纜",
    "21": "化學工業",
    "22": "生技醫療業",
    "08": "玻璃陶瓷",
    "09": "造紙工業",
    "10": "鋼鐵工業",
    "11": "橡膠工業",
    "12": "汽車工業",
    "24": "半導體業",
    "25": "電腦及週邊設備業",
    "26": "光電業",
    "27": "通信網路業",
    "28": "電子零組件業",
    "29": "電子通路業",
    "30": "資訊服務業",
    "31": "其他電子業",
    "14": "建材營造業",
    "15": "航運業",
    "16": "觀光餐旅",
    "17": "金融保險業",
    "18": "貿易百貨業",
    "23": "油電燃氣業",
    "19": "綜合",
    "20": "其他業",
    "32": "文化創意業",
    "33": "農業科技業",
    "34": "電子商務",
    "80": "管理股票",
}


def fetch_twse_industry() -> pd.DataFrame:
    """Fetch industry classification from TWSE.

    Returns:
        DataFrame with columns: code, industry
    """
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    params = {"response": "json"}

    try:
        resp = requests.get(url, params=params, timeout=settings.request_timeout)
        data = resp.json()

        if "data" not in data:
            return pd.DataFrame(columns=["code", "industry"])

        records = []
        for row in data["data"]:
            code = str(row[0]).strip()
            # TWSE API 不直接提供產業別，需要從其他來源
            records.append({"code": code})

        return pd.DataFrame(records)
    except Exception as e:
        print(f"[WARN] Failed to fetch TWSE industry: {e}")
        return pd.DataFrame(columns=["code", "industry"])


def fetch_twse_industry_list() -> pd.DataFrame:
    """Fetch complete industry classification list from TWSE.

    Returns:
        DataFrame with columns: code, name, industry
    """
    # 使用 BWIBBU_ALL 來取得產業分類
    url = "https://www.twse.com.tw/exchangeReport/BWIBBU_ALL"
    params = {"response": "json"}

    try:
        resp = requests.get(url, params=params, timeout=settings.request_timeout)
        data = resp.json()

        if "data" not in data:
            return pd.DataFrame(columns=["code", "name", "industry"])

        records = []
        current_industry = "其他業"

        for row in data["data"]:
            code = str(row[0]).strip()
            name = str(row[1]).strip()

            # 嘗試從股票代號推斷產業
            # 實際上需要用其他 API
            records.append({
                "code": code,
                "name": name,
                "industry": current_industry
            })

        return pd.DataFrame(records)
    except Exception as e:
        print(f"[WARN] Failed to fetch TWSE industry list: {e}")
        return pd.DataFrame(columns=["code", "name", "industry"])


# 常見股票的產業分類（手動維護的主要股票）
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
    "6505": "油電燃氣業", "9926": "油電燃氣業",
}


def get_stock_industry(code: str) -> str:
    """Get industry for a specific stock code.

    Args:
        code: Stock code

    Returns:
        Industry name or "其他業" if not found
    """
    return STOCK_INDUSTRY_MAP.get(code, "其他業")


def update_stock_industries(db_session):
    """Update industry field for all stocks in database.

    Args:
        db_session: Database session
    """
    from sqlalchemy import text

    updated = 0
    for code, industry in STOCK_INDUSTRY_MAP.items():
        query = text("""
            UPDATE stocks SET industry = :industry
            WHERE code = :code AND (industry IS NULL OR industry != :industry)
        """)
        result = db_session.execute(query, {"code": code, "industry": industry})
        updated += result.rowcount

    # 設定其他股票為 "其他業"
    query = text("""
        UPDATE stocks SET industry = '其他業'
        WHERE industry IS NULL
    """)
    result = db_session.execute(query)
    updated += result.rowcount

    db_session.commit()
    return updated
