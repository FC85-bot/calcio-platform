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


class Prediction(Base):
    __tablename__ = "predictions"
    __table_args__ = (
        CheckConstraint(
            "data_quality_score >= 0 AND data_quality_score <= 1",
            name="ck_predictions_data_quality_score_range",
        ),
        UniqueConstraint(
            "match_id",
            "feature_snapshot_id",
            "model_version_id",
            "market_code",
            "prediction_horizon",
            name="uq_predictions_identity",
        ),
        Index("ix_predictions_match_id", "match_id"),
        Index("ix_predictions_feature_snapshot_id", "feature_snapshot_id"),
        Index("ix_predictions_model_version_id", "model_version_id"),
        Index("ix_predictions_market_code", "market_code"),
        Index("ix_predictions_as_of_ts", "as_of_ts"),
        Index(
            "ix_predictions_match_horizon_market_asof",
            "match_id",
            "prediction_horizon",
            "market_code",
            "as_of_ts",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    match_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("matches.id", ondelete="CASCADE"),
        nullable=False,
    )
    feature_snapshot_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("feature_snapshots.id", ondelete="CASCADE"),
        nullable=False,
    )
    model_version_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("model_versions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    market_code: Mapped[str] = mapped_column(String(32), nullable=False)
    prediction_horizon: Mapped[str] = mapped_column(String(32), nullable=False)
    as_of_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    data_quality_score: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
