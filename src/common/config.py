import os
from dataclasses import dataclass, field
from typing import Optional


def build_database_url() -> str:
    """Build database URL from environment variables.

    Supports two modes:
    1. Direct DATABASE_URL (for Docker/local development)
    2. Individual DB_* variables (for Cloud SQL via Unix socket)
    """
    # If DATABASE_URL is provided, use it directly
    if os.environ.get("DATABASE_URL"):
        return os.environ["DATABASE_URL"]

    # Otherwise, build from individual components (Cloud SQL style)
    db_user = os.environ.get("DB_USER", "stocker")
    db_password = os.environ.get("DB_PASSWORD", "stockerpass123")
    db_name = os.environ.get("DB_NAME", "tw_stocker")
    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = os.environ.get("DB_PORT", "5432")

    # Cloud SQL Unix socket connection (DB_HOST starts with /cloudsql/)
    if db_host.startswith("/cloudsql/"):
        return f"postgresql://{db_user}:{db_password}@/{db_name}?host={db_host}"

    # Standard TCP connection
    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


@dataclass
class Settings:
    database_url: str = field(default_factory=build_database_url)
    api_host: str = field(default_factory=lambda: os.environ.get("API_HOST", "0.0.0.0"))
    api_port: int = field(default_factory=lambda: int(os.environ.get("API_PORT", "8000")))

    # Data fetching settings
    request_timeout: int = 30
    max_retries: int = 3

    # Analysis windows
    windows: Optional[list] = None

    def __post_init__(self):
        if self.windows is None:
            self.windows = [5, 20, 60, 120]


settings = Settings()
