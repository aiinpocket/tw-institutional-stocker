import os
from dataclasses import dataclass


@dataclass
class Settings:
    database_url: str = os.environ.get(
        "DATABASE_URL",
        "postgresql://stocker:stockerpass123@localhost:5432/tw_stocker"
    )
    api_host: str = os.environ.get("API_HOST", "0.0.0.0")
    api_port: int = int(os.environ.get("API_PORT", "8000"))

    # Data fetching settings
    request_timeout: int = 30
    max_retries: int = 3

    # Analysis windows
    windows: list = None

    def __post_init__(self):
        if self.windows is None:
            self.windows = [5, 20, 60, 120]


settings = Settings()
