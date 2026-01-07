"""ETL 結果驗證腳本 - 檢查所有頁面 API 是否正常運作。

每個畫面的 API 都會被測試，確保 ETL 完成後資料完整性。
驗證失敗時會發送通知。
"""
import os
import json
import logging
import requests
from datetime import datetime, date
from typing import Optional
from dataclasses import dataclass, field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# API 基礎 URL
API_BASE_URL = os.environ.get("API_BASE_URL", "https://stock-tw.aiinpocket.com")

# 通知設定
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
LINE_NOTIFY_TOKEN = os.environ.get("LINE_NOTIFY_TOKEN", "")


@dataclass
class TestResult:
    """測試結果"""
    name: str
    page: str
    endpoint: str
    success: bool
    message: str
    response_time_ms: float = 0
    data_count: int = 0


@dataclass
class VerificationReport:
    """驗證報告"""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    total_tests: int = 0
    passed: int = 0
    failed: int = 0
    results: list = field(default_factory=list)
    etl_duration_seconds: float = 0

    @property
    def success_rate(self) -> float:
        if self.total_tests == 0:
            return 0
        return (self.passed / self.total_tests) * 100

    @property
    def all_passed(self) -> bool:
        return self.failed == 0


def api_get(endpoint: str, timeout: int = 30) -> tuple:
    """
    執行 API GET 請求

    Returns:
        (response_json, response_time_ms, error_message)
    """
    url = f"{API_BASE_URL}{endpoint}"
    try:
        start = datetime.now()
        resp = requests.get(url, timeout=timeout)
        elapsed_ms = (datetime.now() - start).total_seconds() * 1000

        if resp.status_code != 200:
            return None, elapsed_ms, f"HTTP {resp.status_code}"

        return resp.json(), elapsed_ms, None
    except requests.exceptions.Timeout:
        return None, timeout * 1000, "Timeout"
    except requests.exceptions.RequestException as e:
        return None, 0, str(e)
    except json.JSONDecodeError:
        return None, 0, "Invalid JSON response"


def verify_health() -> TestResult:
    """驗證健康檢查 API"""
    data, ms, err = api_get("/health")
    if err:
        return TestResult("Health Check", "系統", "/health", False, err, ms)

    if data.get("status") == "healthy":
        return TestResult("Health Check", "系統", "/health", True, "OK", ms)

    return TestResult("Health Check", "系統", "/health", False, "Unhealthy status", ms)


def verify_etl_status() -> TestResult:
    """驗證 ETL 狀態 API"""
    data, ms, err = api_get("/api/v1/system/etl-status")
    if err:
        return TestResult("ETL Status", "系統", "/api/v1/system/etl-status", False, err, ms)

    status = data.get("status")
    if status == "completed":
        return TestResult("ETL Status", "系統", "/api/v1/system/etl-status", True,
                         f"completed: {data.get('message', '')}", ms)
    elif status == "running":
        return TestResult("ETL Status", "系統", "/api/v1/system/etl-status", False,
                         "ETL still running", ms)
    else:
        return TestResult("ETL Status", "系統", "/api/v1/system/etl-status", False,
                         f"Unexpected status: {status}", ms)


def verify_dashboard_strategy_summary() -> TestResult:
    """驗證策略儀表板 - /dashboard"""
    data, ms, err = api_get("/api/v1/strategy/summary")
    if err:
        return TestResult("Strategy Summary", "策略儀表板", "/api/v1/strategy/summary",
                         False, err, ms)

    # 檢查必要的策略類型
    required_strategies = [
        "win_rate_5d", "win_rate_10d", "win_rate_30d",
        "correlation", "below_cost", "consecutive_buying",
        "trust_accumulation", "synchronized_buying", "price_deviation"
    ]

    total_count = 0
    missing = []

    for strategy in required_strategies:
        if strategy not in data:
            missing.append(strategy)
            continue

        strategy_data = data[strategy]
        for tier in ["high", "mid", "low"]:
            count = len(strategy_data.get(tier, []))
            total_count += count

    if missing:
        return TestResult("Strategy Summary", "策略儀表板", "/api/v1/strategy/summary",
                         False, f"Missing strategies: {missing}", ms, total_count)

    if total_count == 0:
        return TestResult("Strategy Summary", "策略儀表板", "/api/v1/strategy/summary",
                         False, "No strategy rankings data", ms, 0)

    return TestResult("Strategy Summary", "策略儀表板", "/api/v1/strategy/summary",
                     True, f"{total_count} rankings", ms, total_count)


def verify_live_daily_summary() -> TestResult:
    """驗證今日總結 - /live"""
    data, ms, err = api_get("/api/v1/system/daily-summary")
    if err:
        return TestResult("Daily Summary", "今日總結", "/api/v1/system/daily-summary",
                         False, err, ms)

    # 檢查必要欄位
    if not data.get("date"):
        return TestResult("Daily Summary", "今日總結", "/api/v1/system/daily-summary",
                         False, "Missing date field", ms)

    # 檢查市場統計
    market = data.get("market", {})
    if not market.get("total"):
        return TestResult("Daily Summary", "今日總結", "/api/v1/system/daily-summary",
                         False, "Missing market data", ms)

    # 檢查法人資料
    foreign_buy = data.get("foreign_buy_top10", [])
    trust_buy = data.get("trust_buy_top10", [])

    if len(foreign_buy) == 0 and len(trust_buy) == 0:
        return TestResult("Daily Summary", "今日總結", "/api/v1/system/daily-summary",
                         False, "No institutional rankings data", ms)

    total_count = len(foreign_buy) + len(trust_buy)
    return TestResult("Daily Summary", "今日總結", "/api/v1/system/daily-summary",
                     True, f"Date: {data['date']}, {total_count} rankings", ms, total_count)


def verify_rankings(window: int = 5) -> TestResult:
    """驗證法人排行榜 - /rankings"""
    endpoint = f"/api/v1/rankings/{window}"
    data, ms, err = api_get(endpoint)
    if err:
        return TestResult(f"Rankings {window}D", "法人排行榜", endpoint, False, err, ms)

    # API 返回 items 陣列
    items = data.get("items", [])

    if len(items) == 0:
        return TestResult(f"Rankings {window}D", "法人排行榜", endpoint,
                         False, "No rankings data", ms)

    return TestResult(f"Rankings {window}D", "法人排行榜", endpoint,
                     True, f"{len(items)} stocks", ms, len(items))


def verify_industry_heatmap() -> TestResult:
    """驗證產業熱力圖 - /industry"""
    data, ms, err = api_get("/api/v1/industry/heatmap")
    if err:
        return TestResult("Industry Heatmap", "產業熱力圖", "/api/v1/industry/heatmap",
                         False, err, ms)

    # API 返回 items 陣列
    items = data.get("items", [])
    if len(items) == 0:
        return TestResult("Industry Heatmap", "產業熱力圖", "/api/v1/industry/heatmap",
                         False, "No industry data", ms)

    return TestResult("Industry Heatmap", "產業熱力圖", "/api/v1/industry/heatmap",
                     True, f"{len(items)} industries", ms, len(items))


def verify_industry_summary() -> TestResult:
    """驗證產業摘要 - /industry"""
    data, ms, err = api_get("/api/v1/industry/summary")
    if err:
        return TestResult("Industry Summary", "產業熱力圖", "/api/v1/industry/summary",
                         False, err, ms)

    # API 返回 items 陣列和 days 欄位
    items = data.get("items", [])
    days = data.get("days", 0)

    if len(items) == 0:
        return TestResult("Industry Summary", "產業熱力圖", "/api/v1/industry/summary",
                         False, "No industry data", ms)

    return TestResult("Industry Summary", "產業熱力圖", "/api/v1/industry/summary",
                     True, f"{len(items)} industries ({days}D)", ms, len(items))


def verify_ai_market_summary() -> TestResult:
    """驗證 AI 市場摘要 - /ai"""
    data, ms, err = api_get("/api/v1/ai/market-summary", timeout=60)
    if err:
        return TestResult("AI Market Summary", "AI 分析", "/api/v1/ai/market-summary",
                         False, err, ms)

    if not data.get("summary"):
        return TestResult("AI Market Summary", "AI 分析", "/api/v1/ai/market-summary",
                         False, "Missing summary content", ms)

    cached = data.get("cached", False)
    return TestResult("AI Market Summary", "AI 分析", "/api/v1/ai/market-summary",
                     True, f"Cached: {cached}", ms)


def verify_ai_recommendations() -> TestResult:
    """驗證 AI 推薦 - /ai"""
    data, ms, err = api_get("/api/v1/ai/recommendations?strategy=balanced", timeout=60)
    if err:
        return TestResult("AI Recommendations", "AI 分析", "/api/v1/ai/recommendations",
                         False, err, ms)

    recommendations = data.get("recommendations", [])
    if len(recommendations) == 0:
        return TestResult("AI Recommendations", "AI 分析", "/api/v1/ai/recommendations",
                         False, "No recommendations", ms)

    return TestResult("AI Recommendations", "AI 分析", "/api/v1/ai/recommendations",
                     True, f"{len(recommendations)} recommendations", ms, len(recommendations))


def verify_brokers_ranking() -> TestResult:
    """驗證券商排行 - /brokers"""
    data, ms, err = api_get("/api/v1/brokers/ranking?days=10&limit=20")
    if err:
        return TestResult("Broker Ranking", "券商追蹤", "/api/v1/brokers/ranking",
                         False, err, ms)

    items = data.get("items", [])
    if len(items) == 0:
        return TestResult("Broker Ranking", "券商追蹤", "/api/v1/brokers/ranking",
                         False, "No broker data", ms)

    return TestResult("Broker Ranking", "券商追蹤", "/api/v1/brokers/ranking",
                     True, f"{len(items)} brokers", ms, len(items))


def verify_brokers_concentration() -> TestResult:
    """驗證主力集中 - /brokers"""
    data, ms, err = api_get("/api/v1/brokers/concentration?days=10&limit=20")
    if err:
        return TestResult("Broker Concentration", "券商追蹤", "/api/v1/brokers/concentration",
                         False, err, ms)

    items = data.get("items", [])
    # 主力集中可能沒有資料，這是正常的
    return TestResult("Broker Concentration", "券商追蹤", "/api/v1/brokers/concentration",
                     True, f"{len(items)} stocks", ms, len(items))


def verify_stock_analysis(stock_code: str = "2330") -> TestResult:
    """驗證個股分析 - /stock/{code}"""
    endpoint = f"/api/v1/analysis/{stock_code}"
    data, ms, err = api_get(endpoint)
    if err:
        return TestResult(f"Stock Analysis ({stock_code})", "個股分析", endpoint, False, err, ms)

    # 檢查必要欄位
    stock = data.get("stock", {})
    if not stock.get("code"):
        return TestResult(f"Stock Analysis ({stock_code})", "個股分析", endpoint,
                         False, "Missing stock info", ms)

    chart_data = data.get("chart_data", {})
    prices = chart_data.get("prices", [])

    return TestResult(f"Stock Analysis ({stock_code})", "個股分析", endpoint,
                     True, f"{len(prices)} price records", ms, len(prices))


def verify_stock_brokers(stock_code: str = "2330") -> TestResult:
    """驗證個股券商分點 - /stock/{code}"""
    endpoint = f"/api/v1/analysis/{stock_code}/brokers"
    data, ms, err = api_get(endpoint)
    if err:
        return TestResult(f"Stock Brokers ({stock_code})", "個股分析", endpoint, False, err, ms)

    top_buyers = data.get("top_buyers", [])
    top_sellers = data.get("top_sellers", [])

    # 券商資料可能沒有，這是正常的
    total = len(top_buyers) + len(top_sellers)
    return TestResult(f"Stock Brokers ({stock_code})", "個股分析", endpoint,
                     True, f"{total} broker records", ms, total)


def run_all_verifications() -> VerificationReport:
    """執行所有驗證測試"""
    report = VerificationReport()

    # 定義所有測試
    tests = [
        # 系統
        verify_health,
        verify_etl_status,
        # 策略儀表板 (/dashboard)
        verify_dashboard_strategy_summary,
        # 今日總結 (/live)
        verify_live_daily_summary,
        # 法人排行榜 (/rankings)
        lambda: verify_rankings(5),
        lambda: verify_rankings(20),
        # 產業熱力圖 (/industry)
        verify_industry_heatmap,
        verify_industry_summary,
        # AI 分析 (/ai)
        verify_ai_market_summary,
        verify_ai_recommendations,
        # 券商追蹤 (/brokers)
        verify_brokers_ranking,
        verify_brokers_concentration,
        # 個股分析 (/stock)
        lambda: verify_stock_analysis("2330"),  # 台積電
        lambda: verify_stock_analysis("2317"),  # 鴻海
        lambda: verify_stock_brokers("2330"),
    ]

    logger.info(f"開始執行 {len(tests)} 個 API 驗證測試...")

    for test_func in tests:
        try:
            result = test_func()
            report.results.append(result)
            report.total_tests += 1

            if result.success:
                report.passed += 1
                logger.info(f"✓ {result.name}: {result.message} ({result.response_time_ms:.0f}ms)")
            else:
                report.failed += 1
                logger.error(f"✗ {result.name}: {result.message}")
        except Exception as e:
            report.total_tests += 1
            report.failed += 1
            logger.error(f"✗ Test exception: {e}")

    return report


def send_slack_notification(report: VerificationReport, webhook_url: str):
    """發送 Slack 通知"""
    if not webhook_url:
        logger.warning("Slack webhook URL not configured")
        return False

    # 構建訊息
    if report.all_passed:
        color = "good"
        title = "✅ ETL 驗證通過"
    else:
        color = "danger"
        title = "❌ ETL 驗證失敗"

    failed_tests = [r for r in report.results if not r.success]
    failed_text = "\n".join([f"• {r.name}: {r.message}" for r in failed_tests])

    payload = {
        "attachments": [{
            "color": color,
            "title": title,
            "fields": [
                {"title": "通過", "value": str(report.passed), "short": True},
                {"title": "失敗", "value": str(report.failed), "short": True},
                {"title": "成功率", "value": f"{report.success_rate:.1f}%", "short": True},
                {"title": "ETL 耗時", "value": f"{report.etl_duration_seconds:.0f}s", "short": True},
            ],
            "footer": f"驗證時間: {report.timestamp}",
        }]
    }

    if failed_tests:
        payload["attachments"][0]["fields"].append({
            "title": "失敗項目",
            "value": failed_text,
            "short": False
        })

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code == 200:
            logger.info("Slack notification sent successfully")
            return True
        else:
            logger.error(f"Slack notification failed: {resp.status_code}")
            return False
    except Exception as e:
        logger.error(f"Slack notification error: {e}")
        return False


def send_line_notification(report: VerificationReport, token: str):
    """發送 LINE Notify 通知"""
    if not token:
        logger.warning("LINE Notify token not configured")
        return False

    # 構建訊息
    if report.all_passed:
        emoji = "✅"
        status = "通過"
    else:
        emoji = "❌"
        status = "失敗"

    message = f"""
{emoji} ETL 驗證{status}

📊 測試結果: {report.passed}/{report.total_tests} 通過
⏱️ ETL 耗時: {report.etl_duration_seconds:.0f} 秒
📈 成功率: {report.success_rate:.1f}%
"""

    if report.failed > 0:
        failed_tests = [r for r in report.results if not r.success]
        message += "\n❌ 失敗項目:\n"
        for r in failed_tests:
            message += f"• {r.name}: {r.message}\n"

    try:
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.post(
            "https://notify-api.line.me/api/notify",
            headers=headers,
            data={"message": message},
            timeout=10
        )
        if resp.status_code == 200:
            logger.info("LINE notification sent successfully")
            return True
        else:
            logger.error(f"LINE notification failed: {resp.status_code}")
            return False
    except Exception as e:
        logger.error(f"LINE notification error: {e}")
        return False


def get_etl_duration() -> float:
    """取得 ETL 執行時間"""
    data, _, _ = api_get("/api/v1/system/etl-status")
    if not data:
        return 0

    started_at = data.get("started_at")
    completed_at = data.get("completed_at")

    if not started_at or not completed_at:
        return 0

    try:
        start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        end = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
        return (end - start).total_seconds()
    except:
        return 0


def run_verification_with_notification():
    """執行驗證並發送通知"""
    logger.info("=" * 60)
    logger.info("ETL 驗證開始")
    logger.info("=" * 60)

    # 執行驗證
    report = run_all_verifications()
    report.etl_duration_seconds = get_etl_duration()

    # 輸出摘要
    logger.info("=" * 60)
    logger.info(f"驗證完成: {report.passed}/{report.total_tests} 通過")
    logger.info(f"成功率: {report.success_rate:.1f}%")
    logger.info(f"ETL 耗時: {report.etl_duration_seconds:.0f} 秒")
    logger.info("=" * 60)

    # 發送通知（只在有失敗時，或設定了強制通知）
    always_notify = os.environ.get("ALWAYS_NOTIFY", "false").lower() == "true"

    if not report.all_passed or always_notify:
        if SLACK_WEBHOOK_URL:
            send_slack_notification(report, SLACK_WEBHOOK_URL)
        if LINE_NOTIFY_TOKEN:
            send_line_notification(report, LINE_NOTIFY_TOKEN)

    return report


if __name__ == "__main__":
    report = run_verification_with_notification()

    # 以 JSON 格式輸出詳細結果
    print("\n" + "=" * 60)
    print("詳細測試結果 (JSON)")
    print("=" * 60)

    results_json = {
        "timestamp": report.timestamp,
        "total_tests": report.total_tests,
        "passed": report.passed,
        "failed": report.failed,
        "success_rate": report.success_rate,
        "etl_duration_seconds": report.etl_duration_seconds,
        "results": [
            {
                "name": r.name,
                "page": r.page,
                "endpoint": r.endpoint,
                "success": r.success,
                "message": r.message,
                "response_time_ms": r.response_time_ms,
                "data_count": r.data_count
            }
            for r in report.results
        ]
    }
    print(json.dumps(results_json, indent=2, ensure_ascii=False))

    # 如果有失敗，以非零狀態碼退出
    if not report.all_passed:
        exit(1)
