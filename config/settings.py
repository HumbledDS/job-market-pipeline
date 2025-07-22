import os
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

class Settings:
    # API Settings
    ADZUNA_API_ID: Optional[str] = os.getenv("ADZUNA_API_ID")
    ADZUNA_API_KEY: Optional[str] = os.getenv("ADZUNA_API_KEY")
    ADZUNA_BASE_URL: str = "https://api.adzuna.com/v1/api"

    
    # Database (SQLite instead of DuckDB for Windows compatibility)
    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "./data/jobs.db")
    
    # Pipeline
    BATCH_SIZE: int = 100
    MAX_RETRIES: int = 3
    RATE_LIMIT_DELAY: int = 1  # seconds between API calls