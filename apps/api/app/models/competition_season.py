from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import DateTime, ForeignKey, Index, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Uuid

from app.db.base import Base


class CompetitionSeason(Base):
    __tablename__ = "competition_seasons"
    __table_args__ = (
        UniqueConstraint(
            "competition_id",
            "season_id",
            name="uq_competition_seasons_competition_id_season_id",
        ),
        Index("ix_competition_seasons_competition_id", "competition_id"),
        Index("ix_competition_seasons_season_id", "season_id"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    competition_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("competitions.id", ondelete="RESTRICT"), nullable=False
    )
    season_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("seasons.id", ondelete="RESTRICT"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
