"""Threads engagement collector - fetch replies and insights for analysis.

Collects:
- Post engagement metrics (views, likes, replies, reposts, quotes)
- Reply content with sentiment analysis
- User engagement patterns
"""
import os
import logging
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

TAIPEI_TZ = ZoneInfo("Asia/Taipei")
THREADS_API_BASE = "https://graph.threads.net/v1.0"


class ThreadsEngagementCollector:
    """Collect engagement data from Threads posts."""

    def __init__(self, db_session):
        self.db = db_session
        self.user_id = os.environ.get("THREADS_USER_ID")
        self.enabled = os.environ.get("THREADS_PUBLISH_ENABLED", "false").lower() == "true"
        self._access_token = None

    def is_configured(self) -> bool:
        """Check if collector is properly configured."""
        return bool(self.user_id and self.enabled)

    def get_access_token(self) -> Optional[str]:
        """Get access token from database."""
        if self._access_token:
            return self._access_token

        from sqlalchemy import text
        result = self.db.execute(text("""
            SELECT access_token FROM social_tokens
            WHERE platform = 'threads' AND user_id = :user_id AND is_active = TRUE
        """), {"user_id": self.user_id}).fetchone()

        if result:
            self._access_token = result.access_token
        return self._access_token

    def ensure_tables(self):
        """Ensure engagement tables exist."""
        from sqlalchemy import text

        # Posts table - track our published posts
        self.db.execute(text("""
            CREATE TABLE IF NOT EXISTS threads_posts (
                id SERIAL PRIMARY KEY,
                post_id VARCHAR(50) UNIQUE NOT NULL,
                text TEXT,
                permalink VARCHAR(255),
                posted_at TIMESTAMP,
                data_date DATE,
                views INTEGER DEFAULT 0,
                likes INTEGER DEFAULT 0,
                replies_count INTEGER DEFAULT 0,
                reposts INTEGER DEFAULT 0,
                quotes INTEGER DEFAULT 0,
                last_fetched_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # Replies table - track replies to our posts
        self.db.execute(text("""
            CREATE TABLE IF NOT EXISTS threads_replies (
                id SERIAL PRIMARY KEY,
                reply_id VARCHAR(50) UNIQUE NOT NULL,
                post_id VARCHAR(50) NOT NULL,
                username VARCHAR(100),
                text TEXT,
                replied_at TIMESTAMP,
                sentiment VARCHAR(20),
                sentiment_score FLOAT,
                is_processed BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (post_id) REFERENCES threads_posts(post_id) ON DELETE CASCADE
            )
        """))

        # Create indexes
        self.db.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_threads_posts_data_date ON threads_posts(data_date);
            CREATE INDEX IF NOT EXISTS idx_threads_replies_post_id ON threads_replies(post_id);
            CREATE INDEX IF NOT EXISTS idx_threads_replies_sentiment ON threads_replies(sentiment);
        """))

        self.db.commit()

    def fetch_user_posts(self, limit: int = 10) -> List[Dict]:
        """Fetch recent posts from user's Threads account."""
        token = self.get_access_token()
        if not token:
            logger.warning("No access token available")
            return []

        url = f"{THREADS_API_BASE}/{self.user_id}/threads"
        params = {
            "fields": "id,text,timestamp,permalink,media_type",
            "limit": limit,
            "access_token": token
        }

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            return data.get("data", [])
        except Exception as e:
            logger.error(f"Failed to fetch posts: {e}")
            return []

    def fetch_post_insights(self, post_id: str) -> Dict[str, int]:
        """Fetch engagement metrics for a post."""
        token = self.get_access_token()
        if not token:
            return {}

        url = f"{THREADS_API_BASE}/{post_id}/insights"
        params = {
            "metric": "views,likes,replies,reposts,quotes",
            "access_token": token
        }

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            insights = {}
            for item in data.get("data", []):
                name = item.get("name")
                values = item.get("values", [{}])
                value = values[0].get("value", 0) if values else 0
                insights[name] = value

            return insights
        except Exception as e:
            logger.error(f"Failed to fetch insights for {post_id}: {e}")
            return {}

    def fetch_post_replies(self, post_id: str) -> List[Dict]:
        """Fetch replies to a post."""
        token = self.get_access_token()
        if not token:
            return []

        url = f"{THREADS_API_BASE}/{post_id}/replies"
        params = {
            "fields": "id,text,username,timestamp,media_type",
            "access_token": token
        }

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            return data.get("data", [])
        except Exception as e:
            logger.error(f"Failed to fetch replies for {post_id}: {e}")
            return []

    def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        """Analyze sentiment of reply text using OpenAI."""
        try:
            from openai import OpenAI
            api_key = os.environ.get("OPENAI_API_KEY")
            if not api_key:
                return {"sentiment": "unknown", "score": 0.0}

            client = OpenAI(api_key=api_key)

            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": """你是情感分析專家。分析以下留言的情感傾向。
回覆 JSON 格式：{"sentiment": "positive/negative/neutral", "score": 0.0-1.0, "reason": "簡短原因"}
- positive: 正向、支持、感謝、稱讚
- negative: 負面、批評、抱怨、質疑
- neutral: 中性、詢問、無明顯情感
score 表示情感強度，0.5 為中性，越高越正面，越低越負面"""
                    },
                    {"role": "user", "content": f"留言內容：{text}"}
                ],
                temperature=0.3,
                max_tokens=200,
                response_format={"type": "json_object"}
            )

            import json
            result = json.loads(response.choices[0].message.content)
            return {
                "sentiment": result.get("sentiment", "neutral"),
                "score": float(result.get("score", 0.5))
            }
        except Exception as e:
            logger.warning(f"Sentiment analysis failed: {e}")
            return {"sentiment": "unknown", "score": 0.0}

    def save_post(self, post: Dict, insights: Dict) -> bool:
        """Save or update post data."""
        from sqlalchemy import text

        try:
            # Parse timestamp
            timestamp_str = post.get("timestamp", "")
            posted_at = None
            if timestamp_str:
                posted_at = datetime.fromisoformat(timestamp_str.replace("+0000", "+00:00"))

            # Extract data_date from post text (format: MM/DD)
            data_date = None
            post_text = post.get("text", "")
            if post_text:
                import re
                match = re.search(r"(\d{2})/(\d{2})", post_text)
                if match:
                    month, day = int(match.group(1)), int(match.group(2))
                    year = posted_at.year if posted_at else datetime.now().year
                    data_date = datetime(year, month, day).date()

            self.db.execute(text("""
                INSERT INTO threads_posts (
                    post_id, text, permalink, posted_at, data_date,
                    views, likes, replies_count, reposts, quotes, last_fetched_at
                ) VALUES (
                    :post_id, :text, :permalink, :posted_at, :data_date,
                    :views, :likes, :replies_count, :reposts, :quotes, CURRENT_TIMESTAMP
                )
                ON CONFLICT (post_id) DO UPDATE SET
                    views = :views,
                    likes = :likes,
                    replies_count = :replies_count,
                    reposts = :reposts,
                    quotes = :quotes,
                    last_fetched_at = CURRENT_TIMESTAMP
            """), {
                "post_id": post.get("id"),
                "text": post_text,
                "permalink": post.get("permalink"),
                "posted_at": posted_at,
                "data_date": data_date,
                "views": insights.get("views", 0),
                "likes": insights.get("likes", 0),
                "replies_count": insights.get("replies", 0),
                "reposts": insights.get("reposts", 0),
                "quotes": insights.get("quotes", 0)
            })
            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save post: {e}")
            self.db.rollback()
            return False

    def save_reply(self, post_id: str, reply: Dict, sentiment: Dict) -> bool:
        """Save reply with sentiment analysis."""
        from sqlalchemy import text

        try:
            # Parse timestamp
            timestamp_str = reply.get("timestamp", "")
            replied_at = None
            if timestamp_str:
                replied_at = datetime.fromisoformat(timestamp_str.replace("+0000", "+00:00"))

            self.db.execute(text("""
                INSERT INTO threads_replies (
                    reply_id, post_id, username, text, replied_at,
                    sentiment, sentiment_score, is_processed
                ) VALUES (
                    :reply_id, :post_id, :username, :text, :replied_at,
                    :sentiment, :sentiment_score, TRUE
                )
                ON CONFLICT (reply_id) DO UPDATE SET
                    sentiment = :sentiment,
                    sentiment_score = :sentiment_score,
                    is_processed = TRUE
            """), {
                "reply_id": reply.get("id"),
                "post_id": post_id,
                "username": reply.get("username"),
                "text": reply.get("text"),
                "replied_at": replied_at,
                "sentiment": sentiment.get("sentiment", "unknown"),
                "sentiment_score": sentiment.get("score", 0.0)
            })
            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save reply: {e}")
            self.db.rollback()
            return False

    def collect_engagement(self, days_back: int = 7) -> Dict[str, Any]:
        """Collect engagement data for recent posts."""
        if not self.is_configured():
            logger.info("Threads engagement collector not configured, skipping")
            return {"success": False, "reason": "not_configured"}

        logger.info(f"Collecting Threads engagement data (last {days_back} days)...")

        self.ensure_tables()

        # Fetch recent posts
        posts = self.fetch_user_posts(limit=20)
        logger.info(f"Found {len(posts)} posts")

        stats = {
            "posts_processed": 0,
            "replies_collected": 0,
            "new_replies": 0,
            "sentiment_positive": 0,
            "sentiment_negative": 0,
            "sentiment_neutral": 0
        }

        cutoff_date = datetime.now(TAIPEI_TZ) - timedelta(days=days_back)

        for post in posts:
            # Parse timestamp
            timestamp_str = post.get("timestamp", "")
            if timestamp_str:
                posted_at = datetime.fromisoformat(timestamp_str.replace("+0000", "+00:00"))
                if posted_at.replace(tzinfo=None) < cutoff_date.replace(tzinfo=None):
                    continue  # Skip old posts

            post_id = post.get("id")
            logger.info(f"Processing post {post_id}")

            # Fetch insights
            insights = self.fetch_post_insights(post_id)
            self.save_post(post, insights)
            stats["posts_processed"] += 1

            # Fetch replies
            replies = self.fetch_post_replies(post_id)
            stats["replies_collected"] += len(replies)

            for reply in replies:
                # Check if already processed
                from sqlalchemy import text
                existing = self.db.execute(text("""
                    SELECT is_processed FROM threads_replies WHERE reply_id = :reply_id
                """), {"reply_id": reply.get("id")}).fetchone()

                if existing and existing.is_processed:
                    continue

                # Analyze sentiment
                reply_text = reply.get("text", "")
                if reply_text:
                    sentiment = self.analyze_sentiment(reply_text)
                else:
                    sentiment = {"sentiment": "neutral", "score": 0.5}

                self.save_reply(post_id, reply, sentiment)
                stats["new_replies"] += 1

                # Count sentiment
                s = sentiment.get("sentiment", "neutral")
                if s == "positive":
                    stats["sentiment_positive"] += 1
                elif s == "negative":
                    stats["sentiment_negative"] += 1
                else:
                    stats["sentiment_neutral"] += 1

        logger.info(f"Engagement collection complete: {stats}")
        return {"success": True, "stats": stats}


def collect_threads_engagement(db, days_back: int = 7) -> Dict[str, Any]:
    """Convenience function to collect Threads engagement data.

    Args:
        db: Database session
        days_back: Number of days to look back for posts

    Returns:
        Collection results with stats
    """
    try:
        collector = ThreadsEngagementCollector(db)
        return collector.collect_engagement(days_back)
    except Exception as e:
        logger.error(f"Threads engagement collection failed: {e}")
        return {"success": False, "error": str(e)}
