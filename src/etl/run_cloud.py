"""Cloud Run ETL entry point - runs all ETL tasks.

Combines:
1. run_all.py - Institutional flows, foreign holdings, prices, ratios
2. run_broker.py - Broker branch trading data (top 50 stocks)
3. verify_etl.py - API verification and notification
"""
import sys
import time
import traceback

def main():
    start_time = time.time()

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

    # Step 3: Verify ETL results and send notification on failure
    print("\n" + "=" * 60)
    print("[PART 3] Verifying ETL results...")
    print("=" * 60)
    try:
        from src.etl.verify_etl import run_verification_with_notification
        report = run_verification_with_notification()

        if not report.all_passed:
            print(f"[WARNING] {report.failed} verification tests failed!")
            success = False
        else:
            print(f"[OK] All {report.total_tests} verification tests passed!")
    except Exception as e:
        print(f"[ERROR] Verification failed: {e}")
        traceback.print_exc()
        # Don't fail entire job for verification failure, but log it

    # Summary
    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print(f"Total ETL Time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    if success:
        print("[SUCCESS] Cloud ETL completed!")
    else:
        print("[WARNING] ETL completed with errors")
    print("=" * 60)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
