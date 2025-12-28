"""Broker data ETL - fetch broker branch trading data.

This script fetches broker trading data from Fubon e-Broker website
and stores it in PostgreSQL.
"""
import argparse
from datetime import datetime, date
from zoneinfo import ZoneInfo

from src.etl.fetchers.broker import fetch_multiple_stocks, close_browser
from src.etl.loaders.db_loader import upsert_broker_trades


# Hot stocks to track by default
HOT_STOCKS = [
    "2330",  # 台積電
    "2317",  # 鴻海
    "2454",  # 聯發科
    "2412",  # 中華電
    "2308",  # 台達電
    "2881",  # 富邦金
    "2882",  # 國泰金
    "2891",  # 中信金
    "2886",  # 兆豐金
    "2884",  # 玉山金
    "1301",  # 台塑
    "1303",  # 南亞
    "2303",  # 聯電
    "2382",  # 廣達
    "3008",  # 大立光
    "2357",  # 華碩
    "2603",  # 長榮
    "2609",  # 陽明
    "2615",  # 萬海
    "3711",  # 日月光投控
]

# Top 50 stocks
TOP_50_STOCKS = HOT_STOCKS + [
    "2345",  # 智邦
    "3034",  # 聯詠
    "2379",  # 瑞昱
    "3231",  # 緯創
    "2395",  # 研華
    "2327",  # 國巨
    "3037",  # 欣興
    "2049",  # 上銀
    "2207",  # 和泰車
    "1216",  # 統一
    "2912",  # 統一超
    "9910",  # 豐泰
    "2474",  # 可成
    "6669",  # 緯穎
    "2301",  # 光寶科
    "5871",  # 中租-KY
    "2377",  # 微星
    "3045",  # 台灣大
    "4904",  # 遠傳
    "2892",  # 第一金
    "2880",  # 華南金
    "5880",  # 合庫金
    "2883",  # 開發金
    "6505",  # 台塑化
    "1326",  # 台化
    "2002",  # 中鋼
    "1402",  # 遠東新
    "2801",  # 彰銀
    "2890",  # 永豐金
    "2887",  # 台新金
]


def get_taipei_today() -> date:
    """Get current date in Taipei timezone."""
    tz = ZoneInfo("Asia/Taipei")
    return datetime.now(tz).date()


def run_broker_etl(stock_list: list[str] = None, delay: float = 1.5):
    """Run broker data ETL.

    Args:
        stock_list: List of stock codes to fetch (default: HOT_STOCKS)
        delay: Delay between requests in seconds
    """
    if stock_list is None:
        stock_list = HOT_STOCKS

    print("=" * 60)
    print("Taiwan Stock Tracker - Broker Data ETL")
    print("=" * 60)

    today = get_taipei_today()
    print(f"\n[INFO] Trade date: {today}")
    print(f"[INFO] Fetching broker data for {len(stock_list)} stocks")
    print(f"[INFO] Stocks: {', '.join(stock_list[:10])}{'...' if len(stock_list) > 10 else ''}")

    try:
        print("\n[STEP 1] Fetching broker trading data...")
        df = fetch_multiple_stocks(stock_list, delay=delay)

        if df.empty:
            print("  [WARN] No broker data fetched")
            return

        print(f"  Got {len(df)} broker records")

        print("\n[STEP 2] Storing to database...")
        count = upsert_broker_trades(df, today)
        print(f"  Inserted {count} broker trade records")

        print("\n" + "=" * 60)
        print("[SUCCESS] Broker ETL completed!")
        print("=" * 60)

    finally:
        print("\n[INFO] Closing browser...")
        close_browser()


def main():
    parser = argparse.ArgumentParser(description="Fetch broker trading data")
    parser.add_argument(
        "--top50",
        action="store_true",
        help="Fetch top 50 stocks instead of default 20"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Fetch all stocks from database (slow)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Delay between requests in seconds (default: 1.5)"
    )
    parser.add_argument(
        "--stocks",
        type=str,
        nargs="+",
        help="Specific stock codes to fetch"
    )

    args = parser.parse_args()

    if args.stocks:
        stock_list = args.stocks
    elif args.all:
        # Load all stocks from database
        from src.common.database import get_db_session
        from src.common.models import Stock
        with get_db_session() as session:
            stocks = session.query(Stock.code).filter(Stock.is_active == True).all()
            stock_list = [s[0] for s in stocks]
    elif args.top50:
        stock_list = TOP_50_STOCKS
    else:
        stock_list = HOT_STOCKS

    run_broker_etl(stock_list=stock_list, delay=args.delay)


if __name__ == "__main__":
    main()
