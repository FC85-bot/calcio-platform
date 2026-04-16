"""sprint12 prediction engine mvp

Revision ID: 20260415_0008
Revises: 20260415_0007
Create Date: 2026-04-15 14:20:00.000000

"""
from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20260415_0008"
down_revision: str | None = "20260415_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


MODEL_STATUS_CHECK = "status IN ('draft', 'active', 'archived')"
SELECTION_CODE_CHECK = "selection_code IN ('HOME', 'DRAW', 'AWAY', 'OVER', 'UNDER', 'YES', 'NO')"


def upgrade() -> None:
    op.create_table(
        "model_registry",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("code", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("market_code", sa.String(length=32), nullable=False),
        sa.Column("task_type", sa.String(length=32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("code", name="uq_model_registry_code"),
    )

    op.create_table(
        "model_versions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("model_registry_id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("artifact_path", sa.String(length=512), nullable=True),
        sa.Column("config_json", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(MODEL_STATUS_CHECK, name="ck_model_versions_status"),
        sa.ForeignKeyConstraint(["model_registry_id"], ["model_registry.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "model_registry_id",
            "version",
            name="uq_model_versions_model_registry_version",
        ),
    )
    op.create_index("ix_model_versions_model_registry_id", "model_versions", ["model_registry_id"], unique=False)
    op.create_index("ix_model_versions_is_active", "model_versions", ["is_active"], unique=False)

    op.create_table(
        "predictions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("match_id", sa.Uuid(), nullable=False),
        sa.Column("feature_snapshot_id", sa.Uuid(), nullable=False),
        sa.Column("model_version_id", sa.Uuid(), nullable=False),
        sa.Column("market_code", sa.String(length=32), nullable=False),
        sa.Column("prediction_horizon", sa.String(length=32), nullable=False),
        sa.Column("as_of_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("data_quality_score", sa.Float(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "data_quality_score >= 0 AND data_quality_score <= 1",
            name="ck_predictions_data_quality_score_range",
        ),
        sa.ForeignKeyConstraint(["feature_snapshot_id"], ["feature_snapshots.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["match_id"], ["matches.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["model_version_id"], ["model_versions.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "match_id",
            "feature_snapshot_id",
            "model_version_id",
            "market_code",
            "prediction_horizon",
            name="uq_predictions_identity",
        ),
    )
    op.create_index("ix_predictions_match_id", "predictions", ["match_id"], unique=False)
    op.create_index("ix_predictions_feature_snapshot_id", "predictions", ["feature_snapshot_id"], unique=False)
    op.create_index("ix_predictions_model_version_id", "predictions", ["model_version_id"], unique=False)
    op.create_index("ix_predictions_market_code", "predictions", ["market_code"], unique=False)
    op.create_index("ix_predictions_as_of_ts", "predictions", ["as_of_ts"], unique=False)

    op.create_table(
        "prediction_selections",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("prediction_id", sa.Uuid(), nullable=False),
        sa.Column("selection_code", sa.String(length=16), nullable=False),
        sa.Column("predicted_probability", sa.Float(), nullable=False),
        sa.Column("fair_odds", sa.Float(), nullable=False),
        sa.Column("market_best_odds", sa.Float(), nullable=True),
        sa.Column("edge_pct", sa.Float(), nullable=True),
        sa.Column("confidence_score", sa.Float(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(SELECTION_CODE_CHECK, name="ck_prediction_selections_selection_code"),
        sa.CheckConstraint(
            "predicted_probability >= 0 AND predicted_probability <= 1",
            name="ck_prediction_selections_probability_range",
        ),
        sa.CheckConstraint("fair_odds >= 1", name="ck_prediction_selections_fair_odds_min"),
        sa.ForeignKeyConstraint(["prediction_id"], ["predictions.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "prediction_id",
            "selection_code",
            name="uq_prediction_selections_prediction_selection",
        ),
    )
    op.create_index(
        "ix_prediction_selections_prediction_id",
        "prediction_selections",
        ["prediction_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_prediction_selections_prediction_id", table_name="prediction_selections")
    op.drop_table("prediction_selections")

    op.drop_index("ix_predictions_as_of_ts", table_name="predictions")
    op.drop_index("ix_predictions_market_code", table_name="predictions")
    op.drop_index("ix_predictions_model_version_id", table_name="predictions")
    op.drop_index("ix_predictions_feature_snapshot_id", table_name="predictions")
    op.drop_index("ix_predictions_match_id", table_name="predictions")
    op.drop_table("predictions")

    op.drop_index("ix_model_versions_is_active", table_name="model_versions")
    op.drop_index("ix_model_versions_model_registry_id", table_name="model_versions")
    op.drop_table("model_versions")

    op.drop_table("model_registry")
