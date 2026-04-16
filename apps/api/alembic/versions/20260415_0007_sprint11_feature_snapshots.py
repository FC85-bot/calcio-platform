"""sprint11 feature snapshots mvp

Revision ID: 20260415_0007
Revises: 20260415_0006
Create Date: 2026-04-15 10:45:00.000000

"""
from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20260415_0007"
down_revision: str | None = "20260415_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "feature_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("match_id", sa.Uuid(), nullable=False),
        sa.Column("as_of_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("prediction_horizon", sa.String(length=32), nullable=False),
        sa.Column("feature_set_version", sa.String(length=64), nullable=False),
        sa.Column("home_team_id", sa.Uuid(), nullable=False),
        sa.Column("away_team_id", sa.Uuid(), nullable=False),
        sa.Column("features_json", sa.JSON(), nullable=False),
        sa.Column("completeness_score", sa.Float(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "completeness_score >= 0 AND completeness_score <= 1",
            name="ck_feature_snapshots_completeness_score_range",
        ),
        sa.ForeignKeyConstraint(["away_team_id"], ["teams.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["home_team_id"], ["teams.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["match_id"], ["matches.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "match_id",
            "as_of_ts",
            "prediction_horizon",
            "feature_set_version",
            name="uq_feature_snapshots_match_as_of_horizon_version",
        ),
    )
    op.create_index("ix_feature_snapshots_match_id", "feature_snapshots", ["match_id"], unique=False)
    op.create_index("ix_feature_snapshots_as_of_ts", "feature_snapshots", ["as_of_ts"], unique=False)
    op.create_index(
        "ix_feature_snapshots_feature_set_version",
        "feature_snapshots",
        ["feature_set_version"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_feature_snapshots_feature_set_version", table_name="feature_snapshots")
    op.drop_index("ix_feature_snapshots_as_of_ts", table_name="feature_snapshots")
    op.drop_index("ix_feature_snapshots_match_id", table_name="feature_snapshots")
    op.drop_table("feature_snapshots")
