"""sprint14 evaluation backtest mvp

Revision ID: 20260415_0009
Revises: 20260415_0008
Create Date: 2026-04-15 19:15:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20260415_0009"
down_revision: str | None = "20260415_0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


EVALUATION_STATUS_CHECK = "status IN ('running', 'success', 'failed')"
EVALUATION_TYPE_CHECK = "evaluation_type IN ('backtest', 'walk_forward')"


def upgrade() -> None:
    op.create_table(
        "evaluation_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("code", sa.String(length=128), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("evaluation_type", sa.String(length=32), nullable=False),
        sa.Column("model_version_id", sa.Uuid(), nullable=True),
        sa.Column("market_code", sa.String(length=32), nullable=False),
        sa.Column("period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("period_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("config_json", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(EVALUATION_STATUS_CHECK, name="ck_evaluation_runs_status"),
        sa.CheckConstraint(EVALUATION_TYPE_CHECK, name="ck_evaluation_runs_evaluation_type"),
        sa.ForeignKeyConstraint(["model_version_id"], ["model_versions.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("code", name="uq_evaluation_runs_code"),
    )
    op.create_index("ix_evaluation_runs_status", "evaluation_runs", ["status"], unique=False)
    op.create_index(
        "ix_evaluation_runs_market_code", "evaluation_runs", ["market_code"], unique=False
    )
    op.create_index(
        "ix_evaluation_runs_model_version_id",
        "evaluation_runs",
        ["model_version_id"],
        unique=False,
    )
    op.create_index(
        "ix_evaluation_runs_period_start",
        "evaluation_runs",
        ["period_start"],
        unique=False,
    )
    op.create_index(
        "ix_evaluation_runs_period_end",
        "evaluation_runs",
        ["period_end"],
        unique=False,
    )

    op.create_table(
        "evaluation_metrics",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("evaluation_run_id", sa.Uuid(), nullable=False),
        sa.Column("metric_code", sa.String(length=64), nullable=False),
        sa.Column("metric_value", sa.Float(), nullable=False),
        sa.Column("segment_key", sa.String(length=255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["evaluation_run_id"], ["evaluation_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "evaluation_run_id",
            "metric_code",
            "segment_key",
            name="uq_evaluation_metrics_run_metric_segment",
        ),
    )
    op.create_index(
        "ix_evaluation_metrics_evaluation_run_id",
        "evaluation_metrics",
        ["evaluation_run_id"],
        unique=False,
    )
    op.create_index(
        "ix_evaluation_metrics_metric_code",
        "evaluation_metrics",
        ["metric_code"],
        unique=False,
    )
    op.create_index(
        "ix_evaluation_metrics_segment_key",
        "evaluation_metrics",
        ["segment_key"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_evaluation_metrics_segment_key", table_name="evaluation_metrics")
    op.drop_index("ix_evaluation_metrics_metric_code", table_name="evaluation_metrics")
    op.drop_index("ix_evaluation_metrics_evaluation_run_id", table_name="evaluation_metrics")
    op.drop_table("evaluation_metrics")

    op.drop_index("ix_evaluation_runs_period_end", table_name="evaluation_runs")
    op.drop_index("ix_evaluation_runs_period_start", table_name="evaluation_runs")
    op.drop_index("ix_evaluation_runs_model_version_id", table_name="evaluation_runs")
    op.drop_index("ix_evaluation_runs_market_code", table_name="evaluation_runs")
    op.drop_index("ix_evaluation_runs_status", table_name="evaluation_runs")
    op.drop_table("evaluation_runs")
