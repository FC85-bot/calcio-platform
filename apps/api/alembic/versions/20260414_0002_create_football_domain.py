"""create football domain

Revision ID: 20260414_0002
Revises: 20260414_0001
Create Date: 2026-04-14 12:30:00.000000

"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20260414_0002"
down_revision: str | None = "20260414_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "competitions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("country", sa.String(length=255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", "country", name="uq_competitions_name_country"),
    )

    op.create_table(
        "providers",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_providers_name"),
    )

    op.create_table(
        "raw_ingestion",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("provider", sa.String(length=255), nullable=False),
        sa.Column("endpoint", sa.String(length=255), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_raw_ingestion_ingested_at", "raw_ingestion", ["ingested_at"], unique=False)

    op.create_table(
        "teams",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("competition_id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["competition_id"], ["competitions.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", "competition_id", name="uq_teams_name_competition_id"),
    )
    op.create_index("ix_teams_competition_id", "teams", ["competition_id"], unique=False)

    op.create_table(
        "provider_entities",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("provider_id", sa.Uuid(), nullable=False),
        sa.Column("entity_type", sa.String(length=32), nullable=False),
        sa.Column("external_id", sa.String(length=255), nullable=False),
        sa.Column("internal_id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "entity_type IN ('competition', 'team', 'match')",
            name="ck_provider_entities_entity_type",
        ),
        sa.ForeignKeyConstraint(["provider_id"], ["providers.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "provider_id",
            "entity_type",
            "external_id",
            name="uq_provider_entities_provider_entity_external",
        ),
    )
    op.create_index(
        "ix_provider_entities_internal_id", "provider_entities", ["internal_id"], unique=False
    )
    op.create_index(
        "ix_provider_entities_provider_id", "provider_entities", ["provider_id"], unique=False
    )

    op.create_table(
        "matches",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("competition_id", sa.Uuid(), nullable=False),
        sa.Column("season", sa.String(length=32), nullable=False),
        sa.Column("match_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("home_team_id", sa.Uuid(), nullable=False),
        sa.Column("away_team_id", sa.Uuid(), nullable=False),
        sa.Column("home_goals", sa.Integer(), nullable=True),
        sa.Column("away_goals", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint("status IN ('scheduled', 'finished')", name="ck_matches_status"),
        sa.ForeignKeyConstraint(["away_team_id"], ["teams.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["competition_id"], ["competitions.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["home_team_id"], ["teams.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "competition_id",
            "season",
            "home_team_id",
            "away_team_id",
            "match_date",
            name="uq_matches_competition_season_home_away_match_date",
        ),
    )
    op.create_index("ix_matches_away_team_id", "matches", ["away_team_id"], unique=False)
    op.create_index("ix_matches_competition_id", "matches", ["competition_id"], unique=False)
    op.create_index("ix_matches_home_team_id", "matches", ["home_team_id"], unique=False)
    op.create_index("ix_matches_match_date", "matches", ["match_date"], unique=False)

    op.create_table(
        "odds",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("match_id", sa.Uuid(), nullable=False),
        sa.Column("provider_id", sa.Uuid(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("home_win", sa.Float(), nullable=False),
        sa.Column("draw", sa.Float(), nullable=False),
        sa.Column("away_win", sa.Float(), nullable=False),
        sa.Column("over_2_5", sa.Float(), nullable=True),
        sa.Column("under_2_5", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(["match_id"], ["matches.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["provider_id"], ["providers.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "match_id", "provider_id", "timestamp", name="uq_odds_match_provider_timestamp"
        ),
    )
    op.create_index("ix_odds_match_id", "odds", ["match_id"], unique=False)
    op.create_index("ix_odds_provider_id", "odds", ["provider_id"], unique=False)
    op.create_index("ix_odds_timestamp", "odds", ["timestamp"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_odds_timestamp", table_name="odds")
    op.drop_index("ix_odds_provider_id", table_name="odds")
    op.drop_index("ix_odds_match_id", table_name="odds")
    op.drop_table("odds")

    op.drop_index("ix_matches_match_date", table_name="matches")
    op.drop_index("ix_matches_home_team_id", table_name="matches")
    op.drop_index("ix_matches_competition_id", table_name="matches")
    op.drop_index("ix_matches_away_team_id", table_name="matches")
    op.drop_table("matches")

    op.drop_index("ix_provider_entities_provider_id", table_name="provider_entities")
    op.drop_index("ix_provider_entities_internal_id", table_name="provider_entities")
    op.drop_table("provider_entities")

    op.drop_index("ix_teams_competition_id", table_name="teams")
    op.drop_table("teams")

    op.drop_index("ix_raw_ingestion_ingested_at", table_name="raw_ingestion")
    op.drop_table("raw_ingestion")

    op.drop_table("providers")
    op.drop_table("competitions")
