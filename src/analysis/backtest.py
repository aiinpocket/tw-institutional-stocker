"""Backtesting framework for institutional holdings strategies."""
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Optional, Dict, Tuple

import pandas as pd
import numpy as np
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.common.database import get_db_session
from src.common.models import Stock, StockPrice, InstitutionalRatio, InstitutionalFlow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class Trade:
    """Represents a single trade."""
    stock_code: str
    stock_name: str
    entry_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    holding_days: int
    return_pct: float
    signal_value: float  # The institutional signal that triggered entry


@dataclass
class BacktestResult:
    """Backtest results summary."""
    strategy_name: str
    start_date: date
    end_date: date
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    avg_return: float
    avg_holding_days: float
    total_return: float
    max_drawdown: float
    sharpe_ratio: float
    trades: List[Trade]


def get_price_on_date(session: Session, stock_id: int, trade_date: date) -> Optional[float]:
    """Get closing price for a stock on or after a specific date."""
    price = session.query(StockPrice).filter(
        StockPrice.stock_id == stock_id,
        StockPrice.trade_date >= trade_date,
    ).order_by(StockPrice.trade_date).first()

    if price and price.close_price:
        return float(price.close_price)
    return None


def get_future_price(session: Session, stock_id: int, from_date: date, days: int) -> Tuple[Optional[float], Optional[date]]:
    """Get closing price N trading days after a date."""
    prices = session.query(StockPrice).filter(
        StockPrice.stock_id == stock_id,
        StockPrice.trade_date > from_date,
    ).order_by(StockPrice.trade_date).limit(days).all()

    if prices and len(prices) >= days:
        last_price = prices[-1]
        return float(last_price.close_price) if last_price.close_price else None, last_price.trade_date

    return None, None


class InstitutionalMomentumStrategy:
    """Strategy based on institutional momentum (ratio changes)."""

    def __init__(
        self,
        window: int = 20,  # 5, 20, 60, or 120
        min_change: float = 5.0,  # Minimum change to trigger signal
        holding_days: int = 20,  # Days to hold after signal
        top_n: int = 20,  # Pick top N stocks by signal
    ):
        self.window = window
        self.min_change = min_change
        self.holding_days = holding_days
        self.top_n = top_n
        self.name = f"Inst_Momentum_{window}d_min{min_change}_hold{holding_days}"

    def run(self, start_date: date, end_date: date) -> BacktestResult:
        """Run backtest for this strategy."""
        logger.info(f"Running backtest: {self.name}")
        logger.info(f"Period: {start_date} to {end_date}")

        trades = []
        change_col = f"change_{self.window}d"

        with get_db_session() as session:
            # Get all unique trading dates in range
            dates = session.query(InstitutionalRatio.trade_date).filter(
                InstitutionalRatio.trade_date >= start_date,
                InstitutionalRatio.trade_date <= end_date,
            ).distinct().order_by(InstitutionalRatio.trade_date).all()

            trade_dates = [d[0] for d in dates]
            logger.info(f"Found {len(trade_dates)} trading dates")

            # Sample every N days to avoid overlapping trades
            sample_dates = trade_dates[::self.holding_days]

            for signal_date in sample_dates:
                # Find stocks with strong institutional momentum
                signals = session.query(
                    InstitutionalRatio,
                    Stock,
                ).join(
                    Stock, InstitutionalRatio.stock_id == Stock.id
                ).filter(
                    InstitutionalRatio.trade_date == signal_date,
                    getattr(InstitutionalRatio, change_col) >= self.min_change,
                ).order_by(
                    getattr(InstitutionalRatio, change_col).desc()
                ).limit(self.top_n).all()

                for ratio, stock in signals:
                    signal_value = getattr(ratio, change_col)
                    if signal_value is None:
                        continue

                    # Get entry price (next trading day)
                    entry_price = get_price_on_date(session, stock.id, signal_date)
                    if not entry_price:
                        continue

                    # Get exit price
                    exit_price, exit_date = get_future_price(
                        session, stock.id, signal_date, self.holding_days
                    )
                    if not exit_price or not exit_date:
                        continue

                    return_pct = (exit_price - entry_price) / entry_price * 100
                    holding_days = (exit_date - signal_date).days

                    trade = Trade(
                        stock_code=stock.code,
                        stock_name=stock.name,
                        entry_date=signal_date,
                        exit_date=exit_date,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        holding_days=holding_days,
                        return_pct=return_pct,
                        signal_value=float(signal_value),
                    )
                    trades.append(trade)

        return self._calculate_results(trades, start_date, end_date)

    def _calculate_results(self, trades: List[Trade], start_date: date, end_date: date) -> BacktestResult:
        """Calculate backtest statistics."""
        if not trades:
            return BacktestResult(
                strategy_name=self.name,
                start_date=start_date,
                end_date=end_date,
                total_trades=0,
                winning_trades=0,
                losing_trades=0,
                win_rate=0,
                avg_return=0,
                avg_holding_days=0,
                total_return=0,
                max_drawdown=0,
                sharpe_ratio=0,
                trades=[],
            )

        returns = [t.return_pct for t in trades]
        winning = [t for t in trades if t.return_pct > 0]
        losing = [t for t in trades if t.return_pct <= 0]

        # Calculate drawdown
        cumulative = np.cumsum(returns)
        running_max = np.maximum.accumulate(cumulative)
        drawdowns = running_max - cumulative
        max_drawdown = np.max(drawdowns) if len(drawdowns) > 0 else 0

        # Calculate Sharpe ratio (simplified)
        avg_return = np.mean(returns)
        std_return = np.std(returns) if len(returns) > 1 else 1
        sharpe = avg_return / std_return if std_return > 0 else 0

        return BacktestResult(
            strategy_name=self.name,
            start_date=start_date,
            end_date=end_date,
            total_trades=len(trades),
            winning_trades=len(winning),
            losing_trades=len(losing),
            win_rate=len(winning) / len(trades) * 100,
            avg_return=avg_return,
            avg_holding_days=np.mean([t.holding_days for t in trades]),
            total_return=np.sum(returns),
            max_drawdown=max_drawdown,
            sharpe_ratio=sharpe,
            trades=trades,
        )


class InstitutionalAccumulationStrategy:
    """Strategy: Buy when institutions are accumulating (multiple positive days)."""

    def __init__(
        self,
        consecutive_days: int = 5,  # N consecutive days of buying
        min_net_per_day: int = 100,  # Minimum net buy per day (in 1000 shares)
        holding_days: int = 20,
        top_n: int = 10,
    ):
        self.consecutive_days = consecutive_days
        self.min_net_per_day = min_net_per_day * 1000
        self.holding_days = holding_days
        self.top_n = top_n
        self.name = f"Inst_Accumulation_{consecutive_days}d_net{min_net_per_day}k_hold{holding_days}"

    def run(self, start_date: date, end_date: date) -> BacktestResult:
        """Run backtest."""
        logger.info(f"Running backtest: {self.name}")

        trades = []

        with get_db_session() as session:
            # Get all stocks
            stocks = session.query(Stock).filter(Stock.is_active == True).all()

            for stock in stocks:
                # Get flows for this stock
                flows = session.query(InstitutionalFlow).filter(
                    InstitutionalFlow.stock_id == stock.id,
                    InstitutionalFlow.trade_date >= start_date,
                    InstitutionalFlow.trade_date <= end_date,
                ).order_by(InstitutionalFlow.trade_date).all()

                if len(flows) < self.consecutive_days:
                    continue

                # Find accumulation signals
                for i in range(self.consecutive_days - 1, len(flows)):
                    window = flows[i - self.consecutive_days + 1 : i + 1]

                    # Check if all days are positive
                    all_positive = all(
                        (f.foreign_net or 0) + (f.trust_net or 0) + (f.dealer_net or 0) >= self.min_net_per_day
                        for f in window
                    )

                    if not all_positive:
                        continue

                    signal_date = flows[i].trade_date
                    total_net = sum(
                        (f.foreign_net or 0) + (f.trust_net or 0) + (f.dealer_net or 0)
                        for f in window
                    )

                    # Get prices
                    entry_price = get_price_on_date(session, stock.id, signal_date)
                    if not entry_price:
                        continue

                    exit_price, exit_date = get_future_price(
                        session, stock.id, signal_date, self.holding_days
                    )
                    if not exit_price or not exit_date:
                        continue

                    return_pct = (exit_price - entry_price) / entry_price * 100

                    trade = Trade(
                        stock_code=stock.code,
                        stock_name=stock.name,
                        entry_date=signal_date,
                        exit_date=exit_date,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        holding_days=(exit_date - signal_date).days,
                        return_pct=return_pct,
                        signal_value=total_net / 1000,  # In thousands
                    )
                    trades.append(trade)

        # Sort by signal value and keep top N per date
        trades_df = pd.DataFrame([vars(t) for t in trades])
        if len(trades_df) > 0:
            trades_df = trades_df.sort_values(
                ["entry_date", "signal_value"], ascending=[True, False]
            )
            trades_df = trades_df.groupby("entry_date").head(self.top_n)
            trades = [Trade(**row) for row in trades_df.to_dict("records")]

        return self._calculate_results(trades, start_date, end_date)

    def _calculate_results(self, trades, start_date, end_date):
        """Same as InstitutionalMomentumStrategy."""
        return InstitutionalMomentumStrategy().__class__._calculate_results(
            self, trades, start_date, end_date
        )


class ForeignFollowingStrategy:
    """Strategy: Follow foreign institutional investors' large moves."""

    def __init__(
        self,
        min_foreign_net: int = 1000,  # Minimum foreign net buy (in 1000 shares)
        holding_days: int = 20,
        top_n: int = 10,
    ):
        self.min_foreign_net = min_foreign_net * 1000
        self.holding_days = holding_days
        self.top_n = top_n
        self.name = f"Foreign_Following_net{min_foreign_net}k_hold{holding_days}"

    def run(self, start_date: date, end_date: date) -> BacktestResult:
        """Run backtest."""
        logger.info(f"Running backtest: {self.name}")

        trades = []

        with get_db_session() as session:
            # Get all unique dates
            dates = session.query(InstitutionalFlow.trade_date).filter(
                InstitutionalFlow.trade_date >= start_date,
                InstitutionalFlow.trade_date <= end_date,
            ).distinct().order_by(InstitutionalFlow.trade_date).all()

            trade_dates = [d[0] for d in dates]
            sample_dates = trade_dates[::self.holding_days]

            for signal_date in sample_dates:
                # Find stocks with large foreign buying
                signals = session.query(
                    InstitutionalFlow,
                    Stock,
                ).join(
                    Stock, InstitutionalFlow.stock_id == Stock.id
                ).filter(
                    InstitutionalFlow.trade_date == signal_date,
                    InstitutionalFlow.foreign_net >= self.min_foreign_net,
                ).order_by(
                    InstitutionalFlow.foreign_net.desc()
                ).limit(self.top_n).all()

                for flow, stock in signals:
                    entry_price = get_price_on_date(session, stock.id, signal_date)
                    if not entry_price:
                        continue

                    exit_price, exit_date = get_future_price(
                        session, stock.id, signal_date, self.holding_days
                    )
                    if not exit_price or not exit_date:
                        continue

                    return_pct = (exit_price - entry_price) / entry_price * 100

                    trade = Trade(
                        stock_code=stock.code,
                        stock_name=stock.name,
                        entry_date=signal_date,
                        exit_date=exit_date,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        holding_days=(exit_date - signal_date).days,
                        return_pct=return_pct,
                        signal_value=flow.foreign_net / 1000,
                    )
                    trades.append(trade)

        return self._calculate_results(trades, start_date, end_date)

    def _calculate_results(self, trades, start_date, end_date):
        return InstitutionalMomentumStrategy()._calculate_results(trades, start_date, end_date)


def print_result(result: BacktestResult):
    """Print backtest result in a nice format."""
    print("\n" + "=" * 60)
    print(f"策略: {result.strategy_name}")
    print("=" * 60)
    print(f"回測期間: {result.start_date} ~ {result.end_date}")
    print(f"總交易次數: {result.total_trades}")
    print(f"獲利交易: {result.winning_trades} ({result.win_rate:.1f}%)")
    print(f"虧損交易: {result.losing_trades}")
    print(f"平均報酬: {result.avg_return:.2f}%")
    print(f"平均持有天數: {result.avg_holding_days:.1f}")
    print(f"總報酬: {result.total_return:.2f}%")
    print(f"最大回撤: {result.max_drawdown:.2f}%")
    print(f"夏普比率: {result.sharpe_ratio:.3f}")

    if result.trades:
        print("\n最佳10筆交易:")
        sorted_trades = sorted(result.trades, key=lambda t: t.return_pct, reverse=True)[:10]
        for t in sorted_trades:
            print(f"  {t.stock_code} {t.stock_name}: {t.return_pct:.1f}% "
                  f"({t.entry_date} @ {t.entry_price:.1f} → {t.exit_date} @ {t.exit_price:.1f})")

        print("\n最差10筆交易:")
        sorted_trades = sorted(result.trades, key=lambda t: t.return_pct)[:10]
        for t in sorted_trades:
            print(f"  {t.stock_code} {t.stock_name}: {t.return_pct:.1f}% "
                  f"({t.entry_date} @ {t.entry_price:.1f} → {t.exit_date} @ {t.exit_price:.1f})")


def run_all_strategies(start_date: date, end_date: date) -> List[BacktestResult]:
    """Run all backtesting strategies and compare."""
    strategies = [
        # Momentum strategies with different windows
        InstitutionalMomentumStrategy(window=5, min_change=3.0, holding_days=10),
        InstitutionalMomentumStrategy(window=5, min_change=5.0, holding_days=20),
        InstitutionalMomentumStrategy(window=20, min_change=5.0, holding_days=20),
        InstitutionalMomentumStrategy(window=20, min_change=10.0, holding_days=40),
        InstitutionalMomentumStrategy(window=60, min_change=10.0, holding_days=60),

        # Accumulation strategies
        InstitutionalAccumulationStrategy(consecutive_days=3, min_net_per_day=100, holding_days=10),
        InstitutionalAccumulationStrategy(consecutive_days=5, min_net_per_day=100, holding_days=20),
        InstitutionalAccumulationStrategy(consecutive_days=5, min_net_per_day=500, holding_days=20),

        # Foreign following
        ForeignFollowingStrategy(min_foreign_net=500, holding_days=10),
        ForeignFollowingStrategy(min_foreign_net=1000, holding_days=20),
        ForeignFollowingStrategy(min_foreign_net=2000, holding_days=40),
    ]

    results = []
    for strategy in strategies:
        try:
            result = strategy.run(start_date, end_date)
            results.append(result)
            print_result(result)
        except Exception as e:
            logger.error(f"Error running {strategy.name}: {e}")

    # Summary comparison
    print("\n" + "=" * 80)
    print("策略比較總結")
    print("=" * 80)
    print(f"{'策略名稱':<45} {'交易數':>8} {'勝率':>8} {'平均報酬':>10} {'夏普':>8}")
    print("-" * 80)
    for r in sorted(results, key=lambda x: x.sharpe_ratio, reverse=True):
        print(f"{r.strategy_name:<45} {r.total_trades:>8} {r.win_rate:>7.1f}% {r.avg_return:>9.2f}% {r.sharpe_ratio:>8.3f}")

    return results
