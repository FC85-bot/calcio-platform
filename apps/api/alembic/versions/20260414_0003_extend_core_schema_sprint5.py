"""extend core schema sprint5

Revision ID: 20260414_0003
Revises: 20260414_0002
Create Date: 2026-04-14 16:10:00.000000

"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20260414_0003"
down_revision: str | None = "20260414_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "seasons",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=64), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_seasons_name"),
    )
    op.create_index("ix_seasons_end_date", "seasons", ["end_date"], unique=False)
    op.create_index("ix_seasons_start_date", "seasons", ["start_date"], unique=False)

    op.create_table(
        "bookmakers",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_bookmakers_name"),
    )

    op.create_table(
        "markets",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("code", sa.String(length=32), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("code", name="uq_markets_code"),
    )

    op.create_table(
        "competition_seasons",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("competition_id", sa.Uuid(), nullable=False),
        sa.Column("season_id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["competition_id"], ["competitions.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["season_id"], ["seasons.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "competition_id",
            "season_id",
            name="uq_competition_seasons_competition_id_season_id",
        ),
    )
    op.create_index(
        "ix_competition_seasons_competition_id",
        "competition_seasons",
        ["competition_id"],
        unique=False,
    )
    op.create_index(
        "ix_competition_seasons_season_id",
        "competition_seasons",
        ["season_id"],
        unique=False,
    )

    op.create_table(
        "ingestion_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("provider_id", sa.Uuid(), nullable=True),
        sa.Column("entity_type", sa.String(length=64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("row_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('running', 'success', 'failed')",
            name="ck_ingestion_runs_status",
        ),
        sa.ForeignKeyConstraint(["provider_id"], ["providers.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ingestion_runs_provider_id",
        "ingestion_runs",
        ["provider_id"],
        unique=False,
    )
    op.create_index("ix_ingestion_runs_started_at", "ingestion_runs", ["started_at"], unique=False)
    op.create_index(
        "ix_ingestion_runs_status_started_at",
        "ingestion_runs",
        ["status", "started_at"],
        unique=False,
    )

    op.create_table(
        "standings_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("competition_id", sa.Uuid(), nullable=False),
        sa.Column("season_id", sa.Uuid(), nullable=False),
        sa.Column("team_id", sa.Uuid(), nullable=False),
        sa.Column("snapshot_date", sa.Date(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("points", sa.Integer(), nullable=False),
        sa.Column("played", sa.Integer(), nullable=False),
        sa.Column("won", sa.Integer(), nullable=False),
        sa.Column("drawn", sa.Integer(), nullable=False),
        sa.Column("lost", sa.Integer(), nullable=False),
        sa.Column("goals_for", sa.Integer(), nullable=False),
        sa.Column("goals_against", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["competition_id"], ["competitions.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["season_id"], ["seasons.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["team_id"], ["teams.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "competition_id",
            "season_id",
            "team_id",
            "snapshot_date",
            name="uq_standings_snapshots_competition_season_team_snapshot_date",
        ),
    )
    op.create_index(
        "ix_standings_snapshots_competition_id",
        "standings_snapshots",
        ["competition_id"],
        unique=False,
    )
    op.create_index(
        "ix_standings_snapshots_competition_season_snapshot_date",
        "standings_snapshots",
        ["competition_id", "season_id", "snapshot_date"],
        unique=False,
    )
    op.create_index(
        "ix_standings_snapshots_season_id",
        "standings_snapshots",
        ["season_id"],
        unique=False,
    )
    op.create_index(
        "ix_standings_snapshots_snapshot_date",
        "standings_snapshots",
        ["snapshot_date"],
        unique=False,
    )
    op.create_index(
        "ix_standings_snapshots_team_id",
        "standings_snapshots",
        ["team_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_standings_snapshots_team_id", table_name="standings_snapshots")
    op.drop_index("ix_standings_snapshots_snapshot_date", table_name="standings_snapshots")
    op.drop_index(
        "ix_standings_snapshots_season_id",
        table_name="standings_snapshots",
    )
    op.drop_index(
        "ix_standings_snapshots_competition_season_snapshot_date",
        table_name="standings_snapshots",
    )
    op.drop_index("ix_standings_snapshots_competition_id", table_name="standings_snapshots")
    op.drop_table("standings_snapshots")

    op.drop_index("ix_ingestion_runs_status_started_at", table_name="ingestion_runs")
    op.drop_index("ix_ingestion_runs_started_at", table_name="ingestion_runs")
    op.drop_index("ix_ingestion_runs_provider_id", table_name="ingestion_runs")
    op.drop_table("ingestion_runs")

    op.drop_index("ix_competition_seasons_season_id", table_name="competition_seasons")
    op.drop_index("ix_competition_seasons_competition_id", table_name="competition_seasons")
    op.drop_table("competition_seasons")

    op.drop_table("markets")
    op.drop_table("bookmakers")

    op.drop_index("ix_seasons_start_date", table_name="seasons")
    op.drop_index("ix_seasons_end_date", table_name="seasons")
    op.drop_table("seasons")
