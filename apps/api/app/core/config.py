from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parents[4]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    project_name: str = Field(default="Calcio Platform API", alias="PROJECT_NAME")
    environment: str = Field(default="local", alias="ENVIRONMENT")
    debug: bool = Field(default=True, alias="DEBUG")
    api_v1_prefix: str = Field(default="/api/v1", alias="API_V1_PREFIX")
    host: str = Field(default="0.0.0.0", alias="HOST")
    port: int = Field(default=8000, alias="PORT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_json: bool = Field(default=False, alias="LOG_JSON")

    database_url_override: str | None = Field(default=None, alias="DATABASE_URL")
    postgres_server: str = Field(default="localhost", alias="POSTGRES_SERVER")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_user: str = Field(default="calcio", alias="POSTGRES_USER")
    postgres_password: str = Field(default="calcio", alias="POSTGRES_PASSWORD")
    postgres_db: str = Field(default="calcio_platform", alias="POSTGRES_DB")

    database_pool_size: int = Field(default=10, alias="DATABASE_POOL_SIZE")
    database_max_overflow: int = Field(default=20, alias="DATABASE_MAX_OVERFLOW")
    database_pool_pre_ping: bool = Field(default=True, alias="DATABASE_POOL_PRE_PING")
    database_echo: bool = Field(default=False, alias="DATABASE_ECHO")

    cors_origins: list[str] = Field(default_factory=list, alias="CORS_ORIGINS")

    ingestion_provider: str = Field(default="football_data", alias="INGESTION_PROVIDER")
    raw_storage_path: str = Field(default="data/raw", alias="RAW_STORAGE_PATH")
    provider_timeout_seconds: float = Field(default=20.0, alias="PROVIDER_TIMEOUT_SECONDS")
    provider_retry_attempts: int = Field(default=3, alias="PROVIDER_RETRY_ATTEMPTS")
    provider_retry_backoff_seconds: float = Field(
        default=1.0,
        alias="PROVIDER_RETRY_BACKOFF_SECONDS",
    )

    monitoring_failed_jobs_window_hours: int = Field(
        default=72, alias="MONITORING_FAILED_JOBS_WINDOW_HOURS"
    )
    monitoring_raw_stale_after_hours: int = Field(
        default=24, alias="MONITORING_RAW_STALE_AFTER_HOURS"
    )
    monitoring_normalization_stale_after_hours: int = Field(
        default=24,
        alias="MONITORING_NORMALIZATION_STALE_AFTER_HOURS",
    )
    monitoring_feature_stale_after_hours: int = Field(
        default=24, alias="MONITORING_FEATURE_STALE_AFTER_HOURS"
    )
    monitoring_prediction_stale_after_hours: int = Field(
        default=24,
        alias="MONITORING_PREDICTION_STALE_AFTER_HOURS",
    )
    monitoring_evaluation_stale_after_hours: int = Field(
        default=168,
        alias="MONITORING_EVALUATION_STALE_AFTER_HOURS",
    )

    football_data_base_url: str = Field(
        default="https://api.football-data.org/v4",
        alias="FOOTBALL_DATA_BASE_URL",
    )
    football_data_api_key: str = Field(default="", alias="FOOTBALL_DATA_API_KEY")
    football_data_competition_codes: list[str] = Field(
        default_factory=list,
        alias="FOOTBALL_DATA_COMPETITION_CODES",
    )
    football_data_season_year: int | None = Field(
        default=None,
        alias="FOOTBALL_DATA_SEASON_YEAR",
    )

    @field_validator("football_data_season_year", mode="before")
    @classmethod
    def empty_string_to_none(cls, value: object) -> object:
        if value in ("", None):
            return None
        return value

    the_odds_api_base_url: str = Field(
        default="https://api.the-odds-api.com/v4",
        alias="THE_ODDS_API_BASE_URL",
    )
    the_odds_api_api_key: str = Field(default="", alias="THE_ODDS_API_API_KEY")
    the_odds_api_sport_keys: list[str] = Field(
        default_factory=lambda: ["soccer_italy_serie_a"],
        alias="THE_ODDS_API_SPORT_KEYS",
    )
    the_odds_api_regions: list[str] = Field(
        default_factory=lambda: ["eu"], alias="THE_ODDS_API_REGIONS"
    )
    the_odds_api_bookmakers: list[str] = Field(
        default_factory=list, alias="THE_ODDS_API_BOOKMAKERS"
    )

    @property
    def database_url(self) -> str:
        if self.database_url_override:
            return self.database_url_override
        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_server}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def raw_storage_abs_path(self) -> Path:
        configured_path = Path(self.raw_storage_path)
        if configured_path.is_absolute():
            return configured_path
        return PROJECT_ROOT / configured_path


@lru_cache
def get_settings() -> Settings:
    return Settings()
