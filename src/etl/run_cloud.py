"""Cloud Run ETL entry point - runs all ETL tasks.

Combines:
1. run_all.py - Institutional flows, foreign holdings, prices, ratios
2. run_broker.py - Broker branch trading data (top 50 stocks)
"""
import sys
import traceback

def main():
    print("=" * 60)
    print("Taiwan Stock Tracker - Cloud Run ETL")
    print("=" * 60)

    success = True

    # Step 1: Run main ETL (flows, holdings, prices, ratios)
    print("\n" + "=" * 60)
    print("[PART 1] Running main ETL (flows, prices, ratios)...")
    print("=" * 60)
    try:
        from src.etl.run_all import run_etl
        run_etl()
    except Exception as e:
        print(f"[ERROR] Main ETL failed: {e}")
        traceback.print_exc()
        success = False

    # Step 2: Run broker ETL (top 50 stocks only for speed)
    print("\n" + "=" * 60)
    print("[PART 2] Running broker ETL (top 50 stocks)...")
    print("=" * 60)
    try:
        from src.etl.run_broker import run_broker_etl, TOP_50_STOCKS, close_browser
        run_broker_etl(stock_list=TOP_50_STOCKS, delay=1.0)
    except Exception as e:
        print(f"[ERROR] Broker ETL failed: {e}")
        traceback.print_exc()
        # Don't fail entire job for broker ETL failure
    finally:
        try:
            close_browser()
        except:
            pass

    print("\n" + "=" * 60)
    if success:
        print("[SUCCESS] Cloud ETL completed!")
    else:
        print("[WARNING] ETL completed with errors")
    print("=" * 60)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
