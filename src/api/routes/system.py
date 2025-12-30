"""System status routes - ETL status and system health."""
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api.dependencies import get_db

router = APIRouter()


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

    return {
        "status": result.status_value,
        "message": result.message,
        "started_at": str(result.started_at) if result.started_at else None,
        "completed_at": str(result.completed_at) if result.completed_at else None,
        "updated_at": str(result.updated_at) if result.updated_at else None,
    }
