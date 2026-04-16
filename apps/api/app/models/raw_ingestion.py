from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Uuid

from app.db.base import Base


class RawIngestion(Base):
    __tablename__ = "raw_ingestion"
    __table_args__ = (
        CheckConstraint(
            "normalization_status IN ('pending', 'success', 'failed', 'skipped')",
            name="ck_raw_ingestion_normalization_status",
        ),
        Index("ix_raw_ingestion_ingested_at", "ingested_at"),
        Index("ix_raw_ingestion_run_id", "run_id"),
        Index("ix_raw_ingestion_normalization_run_id", "normalization_run_id"),
        Index("ix_raw_ingestion_normalization_status", "normalization_status"),
        Index(
            "ix_raw_ingestion_provider_entity_type_ingested_at",
            "provider",
            "entity_type",
            "ingested_at",
        ),
        Index(
            "ix_raw_ingestion_provider_entity_endpoint_sha",
            "provider",
            "entity_type",
            "endpoint",
            "payload_sha256",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    run_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("ingestion_runs.id", ondelete="SET NULL"),
        nullable=True,
    )
    normalization_run_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("ingestion_runs.id", ondelete="SET NULL"),
        nullable=True,
    )
    provider: Mapped[str] = mapped_column(String(255), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    endpoint: Mapped[str] = mapped_column(String(255), nullable=False)
    raw_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    payload_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    payload_size_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_params: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    response_metadata: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    normalization_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        server_default="pending",
        default="pending",
    )
    normalized_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    normalization_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
