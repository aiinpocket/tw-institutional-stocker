"""System status routes - ETL status and system health."""
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api.dependencies import get_db

TAIPEI_TZ = ZoneInfo("Asia/Taipei")

router = APIRouter()


@router.get("/config")
def get_public_config():
    """
    取得公開的前端設定。
    這些設定不含機敏資料，可安全提供給前端使用。
    """
    return {
        "ga_measurement_id": os.environ.get("GA_MEASUREMENT_ID", ""),
        "github_url": "https://github.com/aiinpocket/tw-institutional-stocker",
    }


@router.get("/daily-summary")
def get_daily_summary(db: Session = Depends(get_db)):
    """
    Get cached daily summary data.
    This endpoint returns pre-computed market summary to avoid expensive queries.
    Falls back to computing on-the-fly if no cache exists.
    """
    # Try to get from cache first
    try:
        # Check if cache table exists
        table_check = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'daily_summary_cache'
            )
        """)
        table_exists = db.execute(table_check).scalar()

        if table_exists:
            cache_query = text("""
                SELECT trade_date, summary_data, created_at
                FROM daily_summary_cache
                ORDER BY trade_date DESC
                LIMIT 1
            """)
            result = db.execute(cache_query).fetchone()
            if result and result.summary_data:
                return {
                    "cached": True,
                    "cached_at": result.created_at.isoformat() if result.created_at else None,
                    **result.summary_data
                }
    except Exception as e:
        # Rollback failed transaction
        db.rollback()

    # Fallback: compute on-the-fly (slower but works without cache)
    return compute_summary_fallback(db)


def compute_summary_fallback(db: Session):
    """Compute summary on-the-fly as fallback when cache is unavailable."""
    # Get latest trade date
    latest_date = db.execute(text("""
        SELECT MAX(trade_date) FROM stock_prices
    """)).scalar()

    if not latest_date:
        return {"cached": False, "date": None, "error": "No data available"}

    # Market summary
    market = db.execute(text("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN change_percent > 0 OR (change_percent IS NULL AND close_price > open_price) THEN 1 ELSE 0 END) as up,
            SUM(CASE WHEN change_percent < 0 OR (change_percent IS NULL AND close_price < open_price) THEN 1 ELSE 0 END) as down,
            SUM(CASE WHEN change_percent = 0 OR (change_percent IS NULL AND close_price = open_price) THEN 1 ELSE 0 END) as unchanged
        FROM stock_prices
        WHERE trade_date = :trade_date AND open_price IS NOT NULL AND close_price IS NOT NULL
    """), {"trade_date": latest_date}).fetchone()

    # Institutional flow
    flow = db.execute(text("""
        SELECT SUM(foreign_net) as foreign, SUM(trust_net) as trust, SUM(dealer_net) as dealer
        FROM institutional_flows WHERE trade_date = :trade_date
    """), {"trade_date": latest_date}).fetchone()

    return {
        "cached": False,
        "date": str(latest_date),
        "market": {
            "total": market.total or 0,
            "up": market.up or 0,
            "down": market.down or 0,
            "unchanged": market.unchanged or 0,
        },
        "institutional_flow": {
            "foreign": flow.foreign or 0 if flow else 0,
            "trust": flow.trust or 0 if flow else 0,
            "dealer": flow.dealer or 0 if flow else 0,
        }
    }


def ensure_system_status_table(db: Session):
    """確保 system_status 表存在，若不存在則創建。"""
    try:
        # 檢查表是否存在
        check_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'system_status'
            )
        """)
        exists = db.execute(check_query).scalar()

        if not exists:
            # 創建表
            create_query = text("""
                CREATE TABLE IF NOT EXISTS system_status (
                    id SERIAL PRIMARY KEY,
                    status_key VARCHAR(50) UNIQUE NOT NULL,
                    status_value VARCHAR(50) NOT NULL,
                    message TEXT,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            db.execute(create_query)

            # 初始化資料
            init_query = text("""
                INSERT INTO system_status (status_key, status_value, message)
                VALUES ('etl_status', 'idle', '系統待機中')
                ON CONFLICT (status_key) DO NOTHING
            """)
            db.execute(init_query)
            db.commit()
    except Exception as e:
        print(f"[WARN] Failed to ensure system_status table: {e}")


@router.get("/etl-status")
def get_etl_status(db: Session = Depends(get_db)):
    """
    取得 ETL 執行狀態，供前端輪詢使用。

    狀態值：
    - idle: 系統待機中
    - running: 資料更新中
    - completed: 更新完成
    - error: 更新失敗
    """
    # 確保表存在
    ensure_system_status_table(db)

    query = text("""
        SELECT status_value, message, started_at, completed_at, updated_at
        FROM system_status
        WHERE status_key = 'etl_status'
    """)

    result = db.execute(query).fetchone()

    if not result:
        return {
            "status": "idle",
            "message": "系統待機中",
            "started_at": None,
            "completed_at": None,
            "updated_at": None,
        }

    def format_time(dt) -> str | None:
        """Format datetime with Taipei timezone for frontend display."""
        if dt is None:
            return None
        # 確保有時區資訊，統一轉換為台北時間
        if hasattr(dt, 'tzinfo') and dt.tzinfo is None:
            # 無時區資訊，假設是台北時間
            dt = dt.replace(tzinfo=TAIPEI_TZ)
        return dt.astimezone(TAIPEI_TZ).isoformat()

    return {
        "status": result.status_value,
        "message": result.message,
        "started_at": format_time(result.started_at),
        "completed_at": format_time(result.completed_at),
        "updated_at": format_time(result.updated_at),
    }


@router.post("/etl-status/reset")
def reset_etl_status(db: Session = Depends(get_db)):
    """
    重置 ETL 狀態為 idle。
    用於處理 ETL 卡住或異常終止的情況。
    """
    ensure_system_status_table(db)

    query = text("""
        UPDATE system_status
        SET status_value = 'idle',
            message = '系統待機中（手動重置）',
            completed_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE status_key = 'etl_status'
    """)

    db.execute(query)
    db.commit()

    return {
        "success": True,
        "message": "ETL 狀態已重置為 idle"
    }


@router.post("/cleanup-stocks")
def cleanup_stocks(db: Session = Depends(get_db)):
    """
    清理下市/失效股票。
    標記不再交易的股票為非活躍狀態。
    """
    try:
        from src.etl.processors.cleanup_stocks import cleanup_inactive_stocks
        cleanup_inactive_stocks(db)
        return {
            "success": True,
            "message": "股票清理完成"
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"清理失敗: {str(e)}"
        }
