from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from app.schemas.common import CompetitionSummaryRead, SeasonSummaryRead


class TeamVenueStatsRead(BaseModel):
    matches: int
    wins: int
    draws: int
    losses: int
    goals_scored: int
    goals_conceded: int


class TeamStatsRead(BaseModel):
    team_id: UUID
    competition_id: UUID
    season: str
    matches_played: int
    wins: int
    draws: int
    losses: int
    goals_scored: int
    goals_conceded: int
    avg_goals_scored: float
    avg_goals_conceded: float
    home: TeamVenueStatsRead
    away: TeamVenueStatsRead


class TeamFormRead(BaseModel):
    team_id: UUID
    competition_id: UUID
    season: str
    last_n: int
    results: list[str]


class TeamStreakRead(BaseModel):
    team_id: UUID
    competition_id: UUID
    season: str
    current_streak_type: str | None
    current_streak_length: int


class TeamListItemRead(BaseModel):
    id: UUID
    name: str
    competition_id: UUID
    created_at: datetime
    competition: CompetitionSummaryRead


class TeamDetailRead(BaseModel):
    id: UUID
    name: str
    competition_id: UUID
    created_at: datetime
    competition: CompetitionSummaryRead
    season_id: UUID | None = None
    season: str | None = None
    season_detail: SeasonSummaryRead | None = None
    stats: TeamStatsRead | None = None
    form: TeamFormRead | None = None
    streak: TeamStreakRead | None = None
