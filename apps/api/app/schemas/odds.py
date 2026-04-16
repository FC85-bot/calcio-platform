from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class OddsSnapshotRowRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    match_id: UUID
    provider_id: UUID
    provider_name: str
    bookmaker_id: UUID
    bookmaker_name: str
    market_id: UUID
    market_code: str
    market_name: str
    selection_code: str
    line_value: float | None
    odds_value: float
    snapshot_timestamp: datetime
    ingested_at: datetime


class OddsBestRowRead(BaseModel):
    provider_id: UUID
    provider_name: str
    bookmaker_id: UUID
    bookmaker_name: str
    market_id: UUID
    market_code: str
    market_name: str
    selection_code: str
    line_value: float | None
    odds_value: float
    snapshot_timestamp: datetime
    ingested_at: datetime
    id: UUID
    match_id: UUID


class LatestOddsRead(BaseModel):
    provider_id: UUID
    provider_name: str
    bookmaker_id: UUID
    bookmaker_name: str
    snapshot_timestamp: datetime
    home_win: float | None
    draw: float | None
    away_win: float | None
    over_2_5: float | None
    under_2_5: float | None
    btts_yes: float | None
    btts_no: float | None


class OddsAdminSummaryRead(BaseModel):
    total_snapshot_rows: int
    matches_with_odds_count: int
    bookmakers_count: int
    markets_count: int
    latest_snapshot_timestamp: datetime | None


class OddsAdminQualityRead(BaseModel):
    duplicate_snapshot_count: int
    invalid_odds_value_count: int
    invalid_line_value_count: int
    missing_bookmaker_count: int
    missing_market_count: int
    missing_snapshot_timestamp_count: int
