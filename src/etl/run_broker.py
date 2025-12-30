"""Broker data ETL - fetch broker branch trading data.

This script fetches broker trading data from Fubon e-Broker website
and stores it in PostgreSQL.

Waits for Main ETL to complete before starting (dependency check).
"""
import argparse
import time
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import text

from src.common.database import get_db_session
from src.etl.fetchers.broker import fetch_multiple_stocks, close_browser
from src.etl.loaders.db_loader import upsert_broker_trades


def wait_for_main_etl(max_wait_minutes: int = 30, check_interval: int = 30) -> bool:
    """等待 Main ETL 完成後才開始執行。

    Args:
        max_wait_minutes: 最長等待時間（分鐘）
        check_interval: 檢查間隔（秒）

    Returns:
        True 表示可以繼續執行，False 表示應該跳過
    """
    print("[INFO] 檢查 Main ETL 狀態...")

    max_wait_seconds = max_wait_minutes * 60
    waited = 0

    while waited < max_wait_seconds:
        try:
            with get_db_session() as session:
                query = text("""
                    SELECT status_value, message, completed_at, updated_at
                    FROM system_status
                    WHERE status_key = 'etl_status'
                """)
                result = session.execute(query).fetchone()

                if not result:
                    print("  [WARN] 找不到 ETL 狀態記錄，直接執行")
                    return True

                status = result.status_value
                message = result.message
                completed_at = result.completed_at

                if status == "running":
                    print(f"  [WAIT] Main ETL 執行中: {message}，等待 {check_interval} 秒...")
                    time.sleep(check_interval)
                    waited += check_interval
                    continue

                if status == "completed":
                    # 檢查是否是今天完成的
                    if completed_at:
                        taipei_tz = ZoneInfo("Asia/Taipei")
                        today = datetime.now(taipei_tz).date()
                        completed_date = completed_at.date() if hasattr(completed_at, 'date') else completed_at

                        if completed_date == today:
                            print(f"  [OK] Main ETL 今日已完成: {message}")
                            return True
                        else:
                            print(f"  [WARN] Main ETL 完成時間為 {completed_date}，非今日，直接執行")
                            return True
                    else:
                        print(f"  [OK] Main ETL 已完成: {message}")
                        return True

                if status == "error":
                    print(f"  [WARN] Main ETL 執行失敗: {message}")
                    print("  [INFO] 仍然繼續執行 Broker ETL（可能使用舊的股票資料）")
                    return True

                # 其他狀態（idle 等）
                print(f"  [INFO] Main ETL 狀態: {status}，直接執行")
                return True

        except Exception as e:
            print(f"  [WARN] 檢查 ETL 狀態失敗: {e}，直接執行")
            return True

    print(f"  [TIMEOUT] 等待 Main ETL 超過 {max_wait_minutes} 分鐘，跳過本次執行")
    return False


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


def run_broker_etl(stock_list: list[str] = None, delay: float = 1.5, skip_wait: bool = False):
    """Run broker data ETL.

    Args:
        stock_list: List of stock codes to fetch (default: HOT_STOCKS)
        delay: Delay between requests in seconds
        skip_wait: Skip waiting for Main ETL (for manual runs)
    """
    if stock_list is None:
        stock_list = HOT_STOCKS

    print("=" * 60)
    print("Taiwan Stock Tracker - Broker Data ETL")
    print("=" * 60)

    # 檢查 Main ETL 是否完成（排程執行時）
    if not skip_wait:
        if not wait_for_main_etl():
            print("\n[SKIP] Broker ETL 因 Main ETL 未完成而跳過")
            return

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
    parser.add_argument(
        "--skip-wait",
        action="store_true",
        help="Skip waiting for Main ETL to complete (for manual runs)"
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

    run_broker_etl(stock_list=stock_list, delay=args.delay, skip_wait=args.skip_wait)


if __name__ == "__main__":
    main()
