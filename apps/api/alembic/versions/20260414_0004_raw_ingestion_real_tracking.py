"""raw ingestion real tracking sprint6

Revision ID: 20260414_0004
Revises: 20260414_0003
Create Date: 2026-04-14 18:15:00.000000

"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20260414_0004"
down_revision: str | None = "20260414_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint("ck_provider_entities_entity_type", "provider_entities", type_="check")
    op.create_check_constraint(
        "ck_provider_entities_entity_type",
        "provider_entities",
        "entity_type IN ('competition', 'season', 'team', 'match')",
    )

    op.add_column("raw_ingestion", sa.Column("run_id", sa.Uuid(), nullable=True))
    op.add_column(
        "raw_ingestion",
        sa.Column(
            "entity_type",
            sa.String(length=64),
            server_default="legacy",
            nullable=False,
        ),
    )
    op.add_column("raw_ingestion", sa.Column("raw_path", sa.String(length=1024), nullable=True))
    op.add_column("raw_ingestion", sa.Column("payload_sha256", sa.String(length=64), nullable=True))
    op.add_column("raw_ingestion", sa.Column("payload_size_bytes", sa.Integer(), nullable=True))
    op.add_column("raw_ingestion", sa.Column("request_params", sa.JSON(), nullable=True))
    op.add_column("raw_ingestion", sa.Column("response_metadata", sa.JSON(), nullable=True))

    op.create_foreign_key(
        "fk_raw_ingestion_run_id_ingestion_runs",
        "raw_ingestion",
        "ingestion_runs",
        ["run_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_raw_ingestion_run_id", "raw_ingestion", ["run_id"], unique=False)
    op.create_index(
        "ix_raw_ingestion_provider_entity_type_ingested_at",
        "raw_ingestion",
        ["provider", "entity_type", "ingested_at"],
        unique=False,
    )

    op.alter_column("raw_ingestion", "entity_type", server_default=None)


def downgrade() -> None:
    op.drop_index("ix_raw_ingestion_provider_entity_type_ingested_at", table_name="raw_ingestion")
    op.drop_index("ix_raw_ingestion_run_id", table_name="raw_ingestion")
    op.drop_constraint("fk_raw_ingestion_run_id_ingestion_runs", "raw_ingestion", type_="foreignkey")

    op.drop_column("raw_ingestion", "response_metadata")
    op.drop_column("raw_ingestion", "request_params")
    op.drop_column("raw_ingestion", "payload_size_bytes")
    op.drop_column("raw_ingestion", "payload_sha256")
    op.drop_column("raw_ingestion", "raw_path")
    op.drop_column("raw_ingestion", "entity_type")
    op.drop_column("raw_ingestion", "run_id")

    op.drop_constraint("ck_provider_entities_entity_type", "provider_entities", type_="check")
    op.create_check_constraint(
        "ck_provider_entities_entity_type",
        "provider_entities",
        "entity_type IN ('competition', 'team', 'match')",
    )
