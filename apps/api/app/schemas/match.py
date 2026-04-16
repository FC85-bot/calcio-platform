from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from app.schemas.common import (
    CompetitionSummaryRead,
    MatchScoreRead,
    SeasonSummaryRead,
    TeamSummaryRead,
)
from app.schemas.odds import LatestOddsRead


class MatchRead(BaseModel):
    id: UUID
    competition_id: UUID
    season_id: UUID | None = None
    season: str
    match_date: datetime
    home_team_id: UUID
    away_team_id: UUID
    home_goals: int | None = None
    away_goals: int | None = None
    status: str
    created_at: datetime | None = None
    competition: CompetitionSummaryRead
    season_detail: SeasonSummaryRead | None = None
    home_team: TeamSummaryRead
    away_team: TeamSummaryRead
    score: MatchScoreRead | None = None
    latest_odds: list[LatestOddsRead] | None = None
