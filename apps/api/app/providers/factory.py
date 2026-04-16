from __future__ import annotations

from app.core.config import Settings
from app.providers.base import BaseProvider
from app.providers.football_data_provider import FootballDataProvider
from app.providers.mock_provider import MockProvider
from app.providers.the_odds_api_provider import TheOddsApiProvider


def build_provider(settings: Settings) -> BaseProvider:
    if settings.ingestion_provider == "football_data":
        return FootballDataProvider(settings=settings)
    if settings.ingestion_provider == "the_odds_api":
        return TheOddsApiProvider(settings=settings)
    if settings.ingestion_provider == "mock_provider":
        return MockProvider(settings=settings)
    raise ValueError(f"Unsupported INGESTION_PROVIDER={settings.ingestion_provider}")
