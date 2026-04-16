"""sprint15 hardening indexes

Revision ID: 20260415_0010
Revises: 20260415_0009
Create Date: 2026-04-15 21:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260415_0010"
down_revision: str | None = "20260415_0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_raw_ingestion_provider_entity_endpoint_sha",
        "raw_ingestion",
        ["provider", "entity_type", "endpoint", "payload_sha256"],
        unique=False,
    )
    op.create_index(
        "ix_feature_snapshots_match_horizon_version_asof",
        "feature_snapshots",
        ["match_id", "prediction_horizon", "feature_set_version", "as_of_ts"],
        unique=False,
    )
    op.create_index(
        "ix_predictions_match_horizon_market_asof",
        "predictions",
        ["match_id", "prediction_horizon", "market_code", "as_of_ts"],
        unique=False,
    )
    op.create_index(
        "ix_matches_status_match_date",
        "matches",
        ["status", "match_date"],
        unique=False,
    )
    op.create_index(
        "ix_evaluation_runs_status_market_started_at",
        "evaluation_runs",
        ["status", "market_code", "started_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_evaluation_runs_status_market_started_at", table_name="evaluation_runs")
    op.drop_index("ix_matches_status_match_date", table_name="matches")
    op.drop_index("ix_predictions_match_horizon_market_asof", table_name="predictions")
    op.drop_index("ix_feature_snapshots_match_horizon_version_asof", table_name="feature_snapshots")
    op.drop_index("ix_raw_ingestion_provider_entity_endpoint_sha", table_name="raw_ingestion")
