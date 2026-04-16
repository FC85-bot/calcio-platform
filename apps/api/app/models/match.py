from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Uuid

from app.db.base import Base


class Match(Base):
    __tablename__ = "matches"
    __table_args__ = (
        CheckConstraint(
            "status IN ('scheduled', 'live', 'finished', 'postponed', 'cancelled')",
            name="ck_matches_status",
        ),
        UniqueConstraint(
            "competition_id",
            "season",
            "home_team_id",
            "away_team_id",
            "match_date",
            name="uq_matches_competition_season_home_away_match_date",
        ),
        Index("ix_matches_competition_id", "competition_id"),
        Index("ix_matches_season_id", "season_id"),
        Index("ix_matches_home_team_id", "home_team_id"),
        Index("ix_matches_away_team_id", "away_team_id"),
        Index("ix_matches_match_date", "match_date"),
        Index("ix_matches_status_match_date", "status", "match_date"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    competition_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("competitions.id", ondelete="RESTRICT"), nullable=False
    )
    season_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("seasons.id", ondelete="SET NULL"), nullable=True
    )
    season: Mapped[str] = mapped_column(String(32), nullable=False)
    match_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    home_team_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False
    )
    away_team_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False
    )
    home_goals: Mapped[int | None] = mapped_column(Integer, nullable=True)
    away_goals: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
