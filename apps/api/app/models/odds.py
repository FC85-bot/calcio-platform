from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Uuid

from app.db.base import Base


class Odds(Base):
    __tablename__ = "odds"
    __table_args__ = (
        CheckConstraint(
            "selection_code IN ('HOME', 'DRAW', 'AWAY', 'OVER', 'UNDER', 'YES', 'NO')",
            name="ck_odds_selection_code",
        ),
        CheckConstraint("odds_value >= 1.01 AND odds_value <= 1000", name="ck_odds_value_range"),
        UniqueConstraint(
            "match_id",
            "provider_id",
            "bookmaker_id",
            "market_id",
            "selection_code",
            "line_value",
            "odds_value",
            "snapshot_timestamp",
            name="uq_odds_snapshot_identity",
        ),
        Index("ix_odds_match_id", "match_id"),
        Index("ix_odds_provider_id", "provider_id"),
        Index("ix_odds_bookmaker_id", "bookmaker_id"),
        Index("ix_odds_market_id", "market_id"),
        Index("ix_odds_snapshot_timestamp", "snapshot_timestamp"),
        Index("ix_odds_match_market_snapshot", "match_id", "market_id", "snapshot_timestamp"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    match_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("matches.id", ondelete="CASCADE"), nullable=False
    )
    provider_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("providers.id", ondelete="CASCADE"), nullable=False
    )
    bookmaker_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("bookmakers.id", ondelete="RESTRICT"), nullable=False
    )
    market_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("markets.id", ondelete="RESTRICT"), nullable=False
    )
    selection_code: Mapped[str] = mapped_column(String(16), nullable=False)
    line_value: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), nullable=True)
    odds_value: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    snapshot_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
