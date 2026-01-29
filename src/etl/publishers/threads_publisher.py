"""Threads social media publisher for market summary.

Publishes daily AI market summary to Threads platform for traffic generation.
"""
import os
import time
import logging
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

TAIPEI_TZ = ZoneInfo("Asia/Taipei")
THREADS_API_BASE = "https://graph.threads.net/v1.0"
WEBSITE_URL = "https://stock-tw.aiinpocket.com"


class ThreadsPublisher:
    """Threads API publisher for market summary."""

    def __init__(self, db_session=None):
        self.app_id = os.environ.get("THREADS_APP_ID")
        self.app_secret = os.environ.get("THREADS_APP_SECRET")
        self.user_id = os.environ.get("THREADS_USER_ID")
        self.enabled = os.environ.get("THREADS_PUBLISH_ENABLED", "false").lower() == "true"
        self.db = db_session
        self._access_token = None

    def is_configured(self) -> bool:
        """Check if Threads publishing is properly configured."""
        return all([self.app_id, self.user_id, self.enabled])

    def get_access_token(self) -> Optional[str]:
        """Get access token from database, auto-refresh if needed."""
        if self._access_token:
            return self._access_token

        if not self.db:
            logger.warning("No DB session, cannot get access token")
            return None

        from sqlalchemy import text

        # Ensure social_tokens table exists
        try:
            self.db.execute(text("""
                CREATE TABLE IF NOT EXISTS social_tokens (
                    id SERIAL PRIMARY KEY,
                    platform VARCHAR(50) NOT NULL,
                    user_id VARCHAR(100) NOT NULL,
                    access_token TEXT NOT NULL,
                    expires_at TIMESTAMP,
                    scopes VARCHAR(200),
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(platform, user_id)
                )
            """))
            self.db.commit()
        except Exception as e:
            logger.warning(f"Failed to ensure social_tokens table: {e}")
            self.db.rollback()

        # Get token from database
        result = self.db.execute(text("""
            SELECT access_token, expires_at
            FROM social_tokens
            WHERE platform = 'threads' AND user_id = :user_id AND is_active = TRUE
        """), {"user_id": self.user_id}).fetchone()

        if not result:
            logger.warning("No active Threads token found in database")
            return None

        # Check if token needs refresh (7 days before expiration)
        if result.expires_at:
            now = datetime.now(TAIPEI_TZ)
            expires_at = result.expires_at
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=TAIPEI_TZ)

            refresh_threshold = now + timedelta(days=7)
            if expires_at < refresh_threshold:
                logger.info("Token expiring soon, attempting refresh...")
                new_token = self._refresh_token(result.access_token)
                if new_token:
                    self._access_token = new_token
                    return new_token

        self._access_token = result.access_token
        return self._access_token

    def _refresh_token(self, current_token: str) -> Optional[str]:
        """Refresh long-lived access token."""
        try:
            url = f"{THREADS_API_BASE}/refresh_access_token"
            params = {
                "grant_type": "th_refresh_token",
                "access_token": current_token
            }
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            new_token = data.get("access_token")
            expires_in = data.get("expires_in", 5184000)  # Default 60 days
            expires_at = datetime.now(TAIPEI_TZ) + timedelta(seconds=expires_in)

            # Update database
            from sqlalchemy import text
            self.db.execute(text("""
                UPDATE social_tokens
                SET access_token = :token, expires_at = :expires_at, updated_at = CURRENT_TIMESTAMP
                WHERE platform = 'threads' AND user_id = :user_id
            """), {
                "token": new_token,
                "expires_at": expires_at,
                "user_id": self.user_id
            })
            self.db.commit()

            logger.info(f"Threads token refreshed successfully, expires at {expires_at}")
            return new_token

        except Exception as e:
            logger.error(f"Failed to refresh Threads token: {e}")
            return None

    def format_market_summary(self, summary_data: Dict[str, Any]) -> str:
        """Format market summary for Threads post (max 500 chars)."""
        date_str = summary_data.get("date", "")
        threads_data = summary_data.get("threads_data", {})

        # 格式化日期 (2025-01-29 -> 01/29)
        if date_str and len(date_str) >= 10:
            date_display = f"{date_str[5:7]}/{date_str[8:10]}"
        else:
            date_display = date_str

        # 外資買賣超
        daily_foreign = threads_data.get("daily_foreign", 0)
        foreign_action = "買超" if daily_foreign >= 0 else "賣超"
        foreign_amount = abs(daily_foreign)

        # 買超王
        foreign_top1 = threads_data.get("foreign_top1")
        top1_line = ""
        if foreign_top1 and daily_foreign > 0:
            top1_line = f"🏆 買超王：{foreign_top1['name']} +{foreign_top1['net']:,}張\n"

        # 連續買超
        consecutive = threads_data.get("consecutive_buying", [])
        consecutive_line = ""
        if consecutive:
            consecutive_items = [f"{c['name']}({c['days']}天)" for c in consecutive[:3]]
            consecutive_line = f"📈 連續買超：{', '.join(consecutive_items)}\n"

        # 三大法人同步買超
        three_way = threads_data.get("three_way_sync", [])
        three_way_line = ""
        if three_way:
            three_way_names = [t['name'] for t in three_way[:5]]
            three_way_line = f"\n⚡ 三大法人同步買超：\n{', '.join(three_way_names)}\n"

        # 投信焦點
        trust_new = threads_data.get("trust_new", [])
        trust_line = ""
        if trust_new:
            trust_names = [t['name'] for t in trust_new[:3]]
            trust_line = f"\n💡 投信焦點：{', '.join(trust_names)}\n"

        # 組合貼文
        post_lines = [
            f"📊 {date_display} 台股法人快報\n",
            f"\n🔥 外資今日{foreign_action} {foreign_amount:,} 張\n",
            top1_line,
            consecutive_line,
            three_way_line,
            trust_line,
            f"\n👉 完整分析 {WEBSITE_URL}/dashboard\n",
            f"\n#台股",
        ]

        post_text = "".join(post_lines)

        # 確保不超過 500 字元
        if len(post_text) > 500:
            # 移除一些內容來縮短
            post_lines = [
                f"📊 {date_display} 台股法人快報\n",
                f"\n🔥 外資今日{foreign_action} {foreign_amount:,} 張\n",
                top1_line,
                consecutive_line if len(consecutive_line) < 50 else "",
                f"\n👉 {WEBSITE_URL}/dashboard\n",
                f"\n#台股",
            ]
            post_text = "".join(post_lines)

        return post_text

    def create_media_container(self, text: str) -> Optional[str]:
        """Step 1: Create media container for text post."""
        access_token = self.get_access_token()
        if not access_token:
            return None

        url = f"{THREADS_API_BASE}/{self.user_id}/threads"
        params = {
            "media_type": "TEXT",
            "text": text,
            "access_token": access_token
        }

        try:
            resp = requests.post(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            container_id = data.get("id")
            logger.info(f"Created Threads media container: {container_id}")
            return container_id
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create Threads container: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            return None

    def publish_container(self, container_id: str) -> Optional[str]:
        """Step 2: Publish the media container."""
        access_token = self.get_access_token()
        if not access_token:
            return None

        url = f"{THREADS_API_BASE}/{self.user_id}/threads_publish"
        params = {
            "creation_id": container_id,
            "access_token": access_token
        }

        try:
            resp = requests.post(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            post_id = data.get("id")
            logger.info(f"Published Threads post: {post_id}")
            return post_id
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to publish Threads post: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            return None

    def publish_market_summary(self, summary_data: Dict[str, Any]) -> bool:
        """Publish market summary to Threads."""
        if not self.is_configured():
            logger.info("Threads publishing not configured or disabled, skipping")
            return False

        # Format post
        post_text = self.format_market_summary(summary_data)
        logger.info(f"Preparing to publish to Threads ({len(post_text)} chars)")

        # Step 1: Create container
        container_id = self.create_media_container(post_text)
        if not container_id:
            return False

        # Wait for processing (Threads recommends a few seconds)
        time.sleep(3)

        # Step 2: Publish
        post_id = self.publish_container(container_id)
        return post_id is not None


def publish_to_threads(db, summary_data: Dict[str, Any]) -> bool:
    """Convenience function to publish market summary to Threads.

    Args:
        db: Database session
        summary_data: Market summary data dict with 'date' and 'summary' keys

    Returns:
        True if published successfully, False otherwise
    """
    try:
        publisher = ThreadsPublisher(db)
        return publisher.publish_market_summary(summary_data)
    except Exception as e:
        logger.error(f"Threads publishing failed: {e}")
        return False
