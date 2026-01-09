"""ETL 驗證案例 - 測試融資融券和營收 fetchers."""
import pytest
from datetime import date, timedelta
import pandas as pd


class TestMarginFetchers:
    """融資融券 fetcher 測試。"""

    def test_twse_margin_fetch(self):
        """測試 TWSE 融資融券資料抓取。"""
        from src.etl.fetchers.twse_margin import fetch_twse_margin

        # 使用最近的交易日
        trade_date = date.today()
        if trade_date.weekday() >= 5:  # 週末
            trade_date -= timedelta(days=trade_date.weekday() - 4)

        df = fetch_twse_margin(trade_date)

        # 驗證 DataFrame 結構
        expected_columns = [
            "date", "code", "name", "margin_buy", "margin_sell",
            "margin_balance", "short_sell", "short_buy", "short_balance", "market"
        ]
        for col in expected_columns:
            assert col in df.columns, f"Missing column: {col}"

        # 如果有資料，驗證資料格式
        if not df.empty:
            assert df["market"].iloc[0] == "TWSE"
            assert df["code"].str.match(r"^\d{4,5}").all()
            print(f"TWSE margin fetch: {len(df)} records for {trade_date}")

    def test_tpex_margin_fetch(self):
        """測試 TPEX 融資融券資料抓取。"""
        from src.etl.fetchers.tpex_margin import fetch_tpex_margin

        trade_date = date.today()
        if trade_date.weekday() >= 5:
            trade_date -= timedelta(days=trade_date.weekday() - 4)

        df = fetch_tpex_margin(trade_date)

        expected_columns = [
            "date", "code", "name", "margin_buy", "margin_sell",
            "margin_balance", "short_sell", "short_buy", "short_balance", "market"
        ]
        for col in expected_columns:
            assert col in df.columns, f"Missing column: {col}"

        if not df.empty:
            assert df["market"].iloc[0] == "TPEX"
            print(f"TPEX margin fetch: {len(df)} records for {trade_date}")

    def test_margin_data_values(self):
        """驗證融資融券資料數值合理性。"""
        from src.etl.fetchers.twse_margin import fetch_twse_margin

        trade_date = date.today()
        if trade_date.weekday() >= 5:
            trade_date -= timedelta(days=trade_date.weekday() - 4)

        df = fetch_twse_margin(trade_date)

        if not df.empty:
            # 驗證數值為非負數
            assert (df["margin_balance"] >= 0).all(), "margin_balance should be non-negative"
            assert (df["short_balance"] >= 0).all(), "short_balance should be non-negative"

            # 驗證餘額不超過限額（如果有限額資料）
            if "margin_limit" in df.columns:
                valid_limits = df["margin_limit"] > 0
                if valid_limits.any():
                    assert (df.loc[valid_limits, "margin_balance"] <= df.loc[valid_limits, "margin_limit"]).all()


class TestRevenueFetchers:
    """營收 fetcher 測試。"""

    def test_revenue_fetch_twse(self):
        """測試 TWSE 營收資料抓取。"""
        from src.etl.fetchers.revenue import fetch_monthly_revenue

        # 使用上個月的資料（營收通常在次月10號後公布）
        today = date.today()
        if today.month == 1:
            year, month = today.year - 1, 12
        else:
            year, month = today.year, today.month - 1

        # 如果是月初，使用上上個月
        if today.day < 10:
            if month == 1:
                year, month = year - 1, 12
            else:
                month -= 1

        df = fetch_monthly_revenue(year, month, "sii")

        expected_columns = ["code", "name", "year", "month", "revenue", "yoy_change"]
        for col in expected_columns:
            assert col in df.columns, f"Missing column: {col}"

        if not df.empty:
            assert df["market"].iloc[0] == "TWSE"
            assert (df["year"] == year).all()
            assert (df["month"] == month).all()
            print(f"TWSE revenue fetch: {len(df)} records for {year}/{month}")

    def test_revenue_fetch_tpex(self):
        """測試 TPEX 營收資料抓取。"""
        from src.etl.fetchers.revenue import fetch_monthly_revenue

        today = date.today()
        if today.month == 1:
            year, month = today.year - 1, 12
        else:
            year, month = today.year, today.month - 1

        if today.day < 10:
            if month == 1:
                year, month = year - 1, 12
            else:
                month -= 1

        df = fetch_monthly_revenue(year, month, "otc")

        if not df.empty:
            assert df["market"].iloc[0] == "TPEX"
            print(f"TPEX revenue fetch: {len(df)} records for {year}/{month}")

    def test_revenue_data_values(self):
        """驗證營收資料數值合理性。"""
        from src.etl.fetchers.revenue import fetch_all_revenue

        today = date.today()
        if today.month == 1:
            year, month = today.year - 1, 12
        else:
            year, month = today.year, today.month - 1

        if today.day < 10:
            if month == 1:
                year, month = year - 1, 12
            else:
                month -= 1

        df = fetch_all_revenue(year, month)

        if not df.empty:
            # 驗證營收為正數
            valid_revenue = df["revenue"].notna()
            if valid_revenue.any():
                assert (df.loc[valid_revenue, "revenue"] > 0).all(), "revenue should be positive"

            # 驗證年增率在合理範圍 (-100% to 10000%)
            valid_yoy = df["yoy_change"].notna()
            if valid_yoy.any():
                assert (df.loc[valid_yoy, "yoy_change"] >= -100).all()
                assert (df.loc[valid_yoy, "yoy_change"] <= 10000).all()


class TestAPIEndpoints:
    """API 端點測試。"""

    def test_margin_api_endpoints_exist(self):
        """驗證融資融券 API 端點存在。"""
        from src.api.routes import margin

        assert hasattr(margin, "router")
        # 驗證 router 有正確的路由
        routes = [r.path for r in margin.router.routes]
        assert "/summary" in routes or "summary" in str(routes)
        assert "/rankings" in routes or "rankings" in str(routes)

    def test_revenue_api_endpoints_exist(self):
        """驗證營收 API 端點存在。"""
        from src.api.routes import revenue

        assert hasattr(revenue, "router")
        routes = [r.path for r in revenue.router.routes]
        assert "/latest" in routes or "latest" in str(routes)
        assert "/rankings" in routes or "rankings" in str(routes)


class TestDatabaseModels:
    """資料庫 Model 測試。"""

    def test_margin_trading_model(self):
        """驗證 MarginTrading model 結構。"""
        from src.common.models import MarginTrading

        assert hasattr(MarginTrading, "stock_id")
        assert hasattr(MarginTrading, "trade_date")
        assert hasattr(MarginTrading, "margin_balance")
        assert hasattr(MarginTrading, "short_balance")
        assert hasattr(MarginTrading, "short_margin_ratio")

    def test_monthly_revenue_model(self):
        """驗證 MonthlyRevenue model 結構。"""
        from src.common.models import MonthlyRevenue

        assert hasattr(MonthlyRevenue, "stock_id")
        assert hasattr(MonthlyRevenue, "year")
        assert hasattr(MonthlyRevenue, "month")
        assert hasattr(MonthlyRevenue, "revenue")
        assert hasattr(MonthlyRevenue, "yoy_change")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
