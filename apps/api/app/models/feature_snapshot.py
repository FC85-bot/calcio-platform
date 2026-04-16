from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    JSON,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Uuid

from app.db.base import Base


class FeatureSnapshot(Base):
    __tablename__ = "feature_snapshots"
    __table_args__ = (
        CheckConstraint(
            "completeness_score >= 0 AND completeness_score <= 1",
            name="ck_feature_snapshots_completeness_score_range",
        ),
        UniqueConstraint(
            "match_id",
            "as_of_ts",
            "prediction_horizon",
            "feature_set_version",
            name="uq_feature_snapshots_match_as_of_horizon_version",
        ),
        Index("ix_feature_snapshots_match_id", "match_id"),
        Index("ix_feature_snapshots_as_of_ts", "as_of_ts"),
        Index("ix_feature_snapshots_feature_set_version", "feature_set_version"),
        Index(
            "ix_feature_snapshots_match_horizon_version_asof",
            "match_id",
            "prediction_horizon",
            "feature_set_version",
            "as_of_ts",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    match_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("matches.id", ondelete="CASCADE"), nullable=False
    )
    as_of_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    prediction_horizon: Mapped[str] = mapped_column(String(32), nullable=False)
    feature_set_version: Mapped[str] = mapped_column(String(64), nullable=False)
    home_team_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False
    )
    away_team_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False
    )
    features_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    completeness_score: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
