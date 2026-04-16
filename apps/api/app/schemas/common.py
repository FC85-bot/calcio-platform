from __future__ import annotations

from datetime import date
from uuid import UUID

from pydantic import BaseModel


class CompetitionSummaryRead(BaseModel):
    id: UUID
    name: str
    country: str


class SeasonSummaryRead(BaseModel):
    id: UUID
    name: str
    start_date: date
    end_date: date


class TeamSummaryRead(BaseModel):
    id: UUID
    name: str


class MatchScoreRead(BaseModel):
    home: int | None
    away: int | None
