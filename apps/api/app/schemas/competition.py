from __future__ import annotations

from datetime import date, datetime
from uuid import UUID

from pydantic import BaseModel

from app.schemas.common import CompetitionSummaryRead, SeasonSummaryRead, TeamSummaryRead


class CompetitionRead(BaseModel):
    id: UUID
    name: str
    country: str
    created_at: datetime


class StandingRowRead(BaseModel):
    position: int
    team: TeamSummaryRead
    points: int
    played: int
    won: int
    drawn: int
    lost: int
    goals_for: int
    goals_against: int
    goal_difference: int


class CompetitionStandingsRead(BaseModel):
    competition: CompetitionSummaryRead
    season: SeasonSummaryRead | None = None
    season_name: str | None = None
    source: str
    snapshot_date: date | None = None
    standings: list[StandingRowRead]
