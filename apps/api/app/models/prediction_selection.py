from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Uuid

from app.db.base import Base


class PredictionSelection(Base):
    __tablename__ = "prediction_selections"
    __table_args__ = (
        CheckConstraint(
            "selection_code IN ('HOME', 'DRAW', 'AWAY', 'OVER', 'UNDER', 'YES', 'NO')",
            name="ck_prediction_selections_selection_code",
        ),
        CheckConstraint(
            "predicted_probability >= 0 AND predicted_probability <= 1",
            name="ck_prediction_selections_probability_range",
        ),
        CheckConstraint(
            "fair_odds >= 1",
            name="ck_prediction_selections_fair_odds_min",
        ),
        UniqueConstraint(
            "prediction_id",
            "selection_code",
            name="uq_prediction_selections_prediction_selection",
        ),
        Index("ix_prediction_selections_prediction_id", "prediction_id"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    prediction_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("predictions.id", ondelete="CASCADE"),
        nullable=False,
    )
    selection_code: Mapped[str] = mapped_column(String(16), nullable=False)
    predicted_probability: Mapped[float] = mapped_column(Float, nullable=False)
    fair_odds: Mapped[float] = mapped_column(Float, nullable=False)
    market_best_odds: Mapped[float | None] = mapped_column(Float, nullable=True)
    edge_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
