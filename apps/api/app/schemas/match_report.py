from __future__ import annotations

from datetime import date, datetime
from uuid import UUID

from pydantic import BaseModel, Field

from app.schemas.common import (
    CompetitionSummaryRead,
    MatchScoreRead,
    SeasonSummaryRead,
    TeamSummaryRead,
)
from app.schemas.odds import OddsBestRowRead, OddsSnapshotRowRead
from app.schemas.prediction import PredictionSelectionRowRead
from app.schemas.team import TeamStatsRead, TeamStreakRead, TeamVenueStatsRead


class MatchReportContextRead(BaseModel):
    match_id: UUID
    competition: CompetitionSummaryRead
    season: SeasonSummaryRead | None = None
    season_label: str | None = None
    match_date: datetime
    home_team: TeamSummaryRead
    away_team: TeamSummaryRead
    status: str
    score: MatchScoreRead | None = None


class TeamReportBlockRead(BaseModel):
    team: TeamSummaryRead
    last_results: list[str] = Field(default_factory=list)
    stats: TeamStatsRead | None = None
    streak: TeamStreakRead | None = None
    venue_split_label: str | None = None
    venue_split: TeamVenueStatsRead | None = None


class StandingsReportBlockRead(BaseModel):
    position: int
    points: int
    goal_difference: int
    played: int


class StandingsContextRead(BaseModel):
    available: bool
    source: str | None = None
    snapshot_date: date | None = None
    home_team: StandingsReportBlockRead | None = None
    away_team: StandingsReportBlockRead | None = None


class OddsReportBlockRead(BaseModel):
    market_code: str
    available: bool
    latest_snapshot_timestamp: datetime | None = None
    latest: list[OddsSnapshotRowRead] = Field(default_factory=list)
    best: list[OddsBestRowRead] = Field(default_factory=list)
    opening: list[OddsSnapshotRowRead] = Field(default_factory=list)


class PredictionReportBlockRead(BaseModel):
    market_code: str
    available: bool
    prediction_id: UUID | None = None
    feature_snapshot_id: UUID | None = None
    feature_set_version: str | None = None
    model_version_id: UUID | None = None
    model_version: str | None = None
    model_code: str | None = None
    model_name: str | None = None
    prediction_horizon: str | None = None
    as_of_ts: datetime | None = None
    data_quality_score: float | None = None
    selections: list[PredictionSelectionRowRead] = Field(default_factory=list)


class WarningRowRead(BaseModel):
    code: str
    section: str
    severity: str = "warning"
    detail: str | None = None


class MatchReportRead(BaseModel):
    context: MatchReportContextRead
    home_team: TeamReportBlockRead
    away_team: TeamReportBlockRead
    standings_context: StandingsContextRead
    odds: list[OddsReportBlockRead] = Field(default_factory=list)
    predictions: list[PredictionReportBlockRead] = Field(default_factory=list)
    warnings: list[WarningRowRead] = Field(default_factory=list)
    generated_at: datetime
    report_version: str
    feature_set_version: str | None = None
