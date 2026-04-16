"""sprint7 normalization mapping and audit

Revision ID: 20260414_0005
Revises: 20260414_0004
Create Date: 2026-04-14 20:40:00.000000

"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20260414_0005"
down_revision: str | None = "20260414_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint("ck_provider_entities_entity_type", "provider_entities", type_="check")
    op.create_check_constraint(
        "ck_provider_entities_entity_type",
        "provider_entities",
        "entity_type IN ('competition', 'season', 'team', 'match', 'bookmaker', 'market')",
    )

    op.drop_constraint("ck_matches_status", "matches", type_="check")
    op.create_check_constraint(
        "ck_matches_status",
        "matches",
        "status IN ('scheduled', 'live', 'finished', 'postponed', 'cancelled')",
    )
    op.add_column("matches", sa.Column("season_id", sa.Uuid(), nullable=True))
    op.create_foreign_key(
        "fk_matches_season_id_seasons",
        "matches",
        "seasons",
        ["season_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_matches_season_id", "matches", ["season_id"], unique=False)

    op.add_column(
        "ingestion_runs",
        sa.Column("run_type", sa.String(length=32), server_default="raw_ingestion", nullable=False),
    )
    op.add_column(
        "ingestion_runs",
        sa.Column("raw_record_count", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "ingestion_runs",
        sa.Column("created_count", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "ingestion_runs",
        sa.Column("updated_count", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "ingestion_runs",
        sa.Column("skipped_count", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "ingestion_runs",
        sa.Column("error_count", sa.Integer(), server_default="0", nullable=False),
    )
    op.create_check_constraint(
        "ck_ingestion_runs_run_type",
        "ingestion_runs",
        "run_type IN ('raw_ingestion', 'normalization')",
    )
    op.create_index(
        "ix_ingestion_runs_run_type_started_at",
        "ingestion_runs",
        ["run_type", "started_at"],
        unique=False,
    )
    op.alter_column("ingestion_runs", "run_type", server_default=None)
    op.alter_column("ingestion_runs", "raw_record_count", server_default=None)
    op.alter_column("ingestion_runs", "created_count", server_default=None)
    op.alter_column("ingestion_runs", "updated_count", server_default=None)
    op.alter_column("ingestion_runs", "skipped_count", server_default=None)
    op.alter_column("ingestion_runs", "error_count", server_default=None)

    op.add_column("raw_ingestion", sa.Column("normalization_run_id", sa.Uuid(), nullable=True))
    op.add_column(
        "raw_ingestion",
        sa.Column(
            "normalization_status",
            sa.String(length=32),
            server_default="pending",
            nullable=False,
        ),
    )
    op.add_column(
        "raw_ingestion", sa.Column("normalized_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("raw_ingestion", sa.Column("normalization_error", sa.Text(), nullable=True))
    op.create_foreign_key(
        "fk_raw_ingestion_normalization_run_id_ingestion_runs",
        "raw_ingestion",
        "ingestion_runs",
        ["normalization_run_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_check_constraint(
        "ck_raw_ingestion_normalization_status",
        "raw_ingestion",
        "normalization_status IN ('pending', 'success', 'failed', 'skipped')",
    )
    op.create_index(
        "ix_raw_ingestion_normalization_run_id",
        "raw_ingestion",
        ["normalization_run_id"],
        unique=False,
    )
    op.create_index(
        "ix_raw_ingestion_normalization_status",
        "raw_ingestion",
        ["normalization_status"],
        unique=False,
    )
    op.alter_column("raw_ingestion", "normalization_status", server_default=None)


def downgrade() -> None:
    op.drop_index("ix_raw_ingestion_normalization_status", table_name="raw_ingestion")
    op.drop_index("ix_raw_ingestion_normalization_run_id", table_name="raw_ingestion")
    op.drop_constraint(
        "ck_raw_ingestion_normalization_status",
        "raw_ingestion",
        type_="check",
    )
    op.drop_constraint(
        "fk_raw_ingestion_normalization_run_id_ingestion_runs",
        "raw_ingestion",
        type_="foreignkey",
    )
    op.drop_column("raw_ingestion", "normalization_error")
    op.drop_column("raw_ingestion", "normalized_at")
    op.drop_column("raw_ingestion", "normalization_status")
    op.drop_column("raw_ingestion", "normalization_run_id")

    op.drop_index("ix_ingestion_runs_run_type_started_at", table_name="ingestion_runs")
    op.drop_constraint("ck_ingestion_runs_run_type", "ingestion_runs", type_="check")
    op.drop_column("ingestion_runs", "error_count")
    op.drop_column("ingestion_runs", "skipped_count")
    op.drop_column("ingestion_runs", "updated_count")
    op.drop_column("ingestion_runs", "created_count")
    op.drop_column("ingestion_runs", "raw_record_count")
    op.drop_column("ingestion_runs", "run_type")

    op.drop_index("ix_matches_season_id", table_name="matches")
    op.drop_constraint("fk_matches_season_id_seasons", "matches", type_="foreignkey")
    op.drop_column("matches", "season_id")
    op.drop_constraint("ck_matches_status", "matches", type_="check")
    op.create_check_constraint(
        "ck_matches_status",
        "matches",
        "status IN ('scheduled', 'finished')",
    )

    op.drop_constraint("ck_provider_entities_entity_type", "provider_entities", type_="check")
    op.create_check_constraint(
        "ck_provider_entities_entity_type",
        "provider_entities",
        "entity_type IN ('competition', 'season', 'team', 'match')",
    )
