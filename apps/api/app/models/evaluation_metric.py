from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import DateTime, Float, ForeignKey, Index, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Uuid

from app.db.base import Base


class EvaluationMetric(Base):
    __tablename__ = "evaluation_metrics"
    __table_args__ = (
        UniqueConstraint(
            "evaluation_run_id",
            "metric_code",
            "segment_key",
            name="uq_evaluation_metrics_run_metric_segment",
        ),
        Index("ix_evaluation_metrics_evaluation_run_id", "evaluation_run_id"),
        Index("ix_evaluation_metrics_metric_code", "metric_code"),
        Index("ix_evaluation_metrics_segment_key", "segment_key"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    evaluation_run_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("evaluation_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    metric_code: Mapped[str] = mapped_column(String(64), nullable=False)
    metric_value: Mapped[float] = mapped_column(Float, nullable=False)
    segment_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
