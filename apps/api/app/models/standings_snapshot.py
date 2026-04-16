from __future__ import annotations

from datetime import date, datetime
from uuid import UUID, uuid4

from sqlalchemy import Date, DateTime, ForeignKey, Index, Integer, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Uuid

from app.db.base import Base


class StandingsSnapshot(Base):
    __tablename__ = "standings_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "competition_id",
            "season_id",
            "team_id",
            "snapshot_date",
            name="uq_standings_snapshots_competition_season_team_snapshot_date",
        ),
        Index("ix_standings_snapshots_competition_id", "competition_id"),
        Index("ix_standings_snapshots_season_id", "season_id"),
        Index("ix_standings_snapshots_team_id", "team_id"),
        Index("ix_standings_snapshots_snapshot_date", "snapshot_date"),
        Index(
            "ix_standings_snapshots_competition_season_snapshot_date",
            "competition_id",
            "season_id",
            "snapshot_date",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    competition_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("competitions.id", ondelete="RESTRICT"), nullable=False
    )
    season_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("seasons.id", ondelete="RESTRICT"), nullable=False
    )
    team_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False
    )
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False)
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    points: Mapped[int] = mapped_column(Integer, nullable=False)
    played: Mapped[int] = mapped_column(Integer, nullable=False)
    won: Mapped[int] = mapped_column(Integer, nullable=False)
    drawn: Mapped[int] = mapped_column(Integer, nullable=False)
    lost: Mapped[int] = mapped_column(Integer, nullable=False)
    goals_for: Mapped[int] = mapped_column(Integer, nullable=False)
    goals_against: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
