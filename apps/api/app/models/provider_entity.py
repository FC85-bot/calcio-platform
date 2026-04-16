from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Uuid

from app.db.base import Base


class ProviderEntity(Base):
    __tablename__ = "provider_entities"
    __table_args__ = (
        CheckConstraint(
            "entity_type IN ('competition', 'season', 'team', 'match', 'bookmaker', 'market')",
            name="ck_provider_entities_entity_type",
        ),
        UniqueConstraint(
            "provider_id",
            "entity_type",
            "external_id",
            name="uq_provider_entities_provider_entity_external",
        ),
        Index("ix_provider_entities_provider_id", "provider_id"),
        Index("ix_provider_entities_internal_id", "internal_id"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    provider_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("providers.id", ondelete="CASCADE"),
        nullable=False,
    )
    entity_type: Mapped[str] = mapped_column(String(32), nullable=False)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    internal_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
