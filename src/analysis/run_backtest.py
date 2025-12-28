"""Run backtesting analysis."""
import argparse
import logging
from datetime import date, datetime

from src.analysis.backtest import (
    run_all_strategies,
    InstitutionalMomentumStrategy,
    InstitutionalAccumulationStrategy,
    ForeignFollowingStrategy,
    print_result,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Run backtesting analysis")
    parser.add_argument(
        "--start-date",
        type=str,
        default="2010-01-01",
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD), defaults to today",
    )
    parser.add_argument(
        "--strategy",
        type=str,
        choices=["all", "momentum", "accumulation", "foreign"],
        default="all",
        help="Strategy to run",
    )

    args = parser.parse_args()

    start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else date.today()

    logger.info(f"Running backtest from {start} to {end}")
    logger.info(f"Strategy: {args.strategy}")

    if args.strategy == "all":
        results = run_all_strategies(start, end)

    elif args.strategy == "momentum":
        strategies = [
            InstitutionalMomentumStrategy(window=5, min_change=3.0, holding_days=10),
            InstitutionalMomentumStrategy(window=5, min_change=5.0, holding_days=20),
            InstitutionalMomentumStrategy(window=20, min_change=5.0, holding_days=20),
            InstitutionalMomentumStrategy(window=20, min_change=10.0, holding_days=40),
            InstitutionalMomentumStrategy(window=60, min_change=10.0, holding_days=60),
        ]
        for s in strategies:
            result = s.run(start, end)
            print_result(result)

    elif args.strategy == "accumulation":
        strategies = [
            InstitutionalAccumulationStrategy(consecutive_days=3, min_net_per_day=100, holding_days=10),
            InstitutionalAccumulationStrategy(consecutive_days=5, min_net_per_day=100, holding_days=20),
            InstitutionalAccumulationStrategy(consecutive_days=5, min_net_per_day=500, holding_days=20),
        ]
        for s in strategies:
            result = s.run(start, end)
            print_result(result)

    elif args.strategy == "foreign":
        strategies = [
            ForeignFollowingStrategy(min_foreign_net=500, holding_days=10),
            ForeignFollowingStrategy(min_foreign_net=1000, holding_days=20),
            ForeignFollowingStrategy(min_foreign_net=2000, holding_days=40),
        ]
        for s in strategies:
            result = s.run(start, end)
            print_result(result)

    logger.info("Backtest complete!")


if __name__ == "__main__":
    main()
