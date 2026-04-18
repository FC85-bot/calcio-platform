from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.competition import Competition
from app.models.evaluation_run import EvaluationRun
from app.models.feature_snapshot import FeatureSnapshot
from app.models.ingestion_run import IngestionRun
from app.models.market import Market
from app.models.match import Match
from app.models.odds import Odds
from app.models.prediction import Prediction
from app.models.prediction_selection import PredictionSelection
from app.models.provider_entity import ProviderEntity
from app.models.raw_ingestion import RawIngestion
from app.models.team import Team

DATA_CONFIDENCE_SIGNAL_DEFINITIONS = (
    ("matches_missing_team_count", "normalized/core", "_count_matches_missing_team"),
    ("matches_missing_competition_count", "normalized/core", "_count_matches_missing_competition"),
    ("provider_mapping_missing_count", "normalization/mapping", "_count_provider_mapping_missing"),
    ("odds_inconsistent_count", "odds", "_count_odds_inconsistent"),
    (
        "predictions_without_selections_count",
        "predictions",
        "_count_predictions_without_selections",
    ),
)


class MonitoringService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.settings = get_settings()

    def get_summary(self) -> dict[str, Any]:
        generated_at = datetime.now(UTC)
        database_latency_ms = self._measure_database_latency_ms()
        failed_window_start = generated_at - timedelta(
            hours=self.settings.monitoring_failed_jobs_window_hours
        )
        raw_pending_count = self._count_raw_pending()

        pipelines = [
            self._build_ingestion_pipeline_state(
                pipeline="raw_ingestion",
                run_type="raw_ingestion",
                stale_after_hours=self.settings.monitoring_raw_stale_after_hours,
                failed_window_start=failed_window_start,
                lag_source="raw_ingested_at",
            ),
            self._build_ingestion_pipeline_state(
                pipeline="normalization",
                run_type="normalization",
                stale_after_hours=self.settings.monitoring_normalization_stale_after_hours,
                failed_window_start=failed_window_start,
                lag_source="run_finished_at",
            ),
            self._build_timestamp_pipeline_state(
                pipeline="feature_snapshots",
                model=FeatureSnapshot,
                timestamp_column=FeatureSnapshot.created_at,
                stale_after_hours=self.settings.monitoring_feature_stale_after_hours,
            ),
            self._build_timestamp_pipeline_state(
                pipeline="predictions",
                model=Prediction,
                timestamp_column=Prediction.created_at,
                stale_after_hours=self.settings.monitoring_prediction_stale_after_hours,
            ),
            self._build_timestamp_pipeline_state(
                pipeline="evaluation_runs",
                model=EvaluationRun,
                timestamp_column=EvaluationRun.created_at,
                stale_after_hours=self.settings.monitoring_evaluation_stale_after_hours,
                status_column=EvaluationRun.status,
                failed_window_start=failed_window_start,
            ),
        ]

        failed_runs_last_window = sum(item["failed_runs_last_window"] for item in pipelines)
        checks = [
            self._build_check_for_raw_pending(raw_pending_count),
            self._build_check_for_failed_runs(failed_runs_last_window),
        ]
        for pipeline in pipelines:
            if pipeline["status"] != "ok":
                checks.append(
                    {
                        "code": f"{pipeline['pipeline']}_status",
                        "status": pipeline["status"],
                        "detail": pipeline.get("detail"),
                        "observed_value": pipeline.get("lag_minutes"),
                        "threshold_value": self._pipeline_threshold_minutes(pipeline["pipeline"]),
                    }
                )

        return {
            "service": self.settings.project_name,
            "environment": self.settings.environment,
            "generated_at": generated_at,
            "database": f"ok ({database_latency_ms} ms)",
            "raw_pending_count": raw_pending_count,
            "failed_runs_last_window": failed_runs_last_window,
            "checks": checks,
            "pipelines": pipelines,
            "provider_latency": self._provider_latency_summary(),
            "data_confidence": self.get_data_confidence_summary(),
        }

    def get_data_confidence_summary(self) -> dict[str, Any]:
        detailed_signals = self._collect_data_confidence_signals()
        critical_signal_count = sum(1 for signal in detailed_signals if signal["value"] > 0)
        status = "BROKEN" if critical_signal_count > 0 else "OK"

        return {
            "status": status,
            "critical_signal_count": critical_signal_count,
            "signals": [
                {
                    "name": signal["name"],
                    "value": signal["value"],
                }
                for signal in detailed_signals
            ],
        }

    def _collect_data_confidence_signals(self) -> list[dict[str, Any]]:
        signals: list[dict[str, Any]] = []
        for signal_name, layer, method_name in DATA_CONFIDENCE_SIGNAL_DEFINITIONS:
            value = int(getattr(self, method_name)())
            signals.append(
                {
                    "name": signal_name,
                    "value": value,
                    "layer": layer,
                    "threshold": 0,
                    "status": "critical" if value > 0 else "ok",
                }
            )
        return signals

    def _measure_database_latency_ms(self) -> float:
        started_at = perf_counter()
        self.db.execute(text("SELECT 1"))
        return round((perf_counter() - started_at) * 1000, 2)

    def _count_raw_pending(self) -> int:
        return int(
            self.db.execute(
                select(func.count(RawIngestion.id)).where(
                    RawIngestion.normalization_status == "pending"
                )
            ).scalar_one()
            or 0
        )

    def _build_ingestion_pipeline_state(
        self,
        *,
        pipeline: str,
        run_type: str,
        stale_after_hours: int,
        failed_window_start: datetime,
        lag_source: str,
    ) -> dict[str, Any]:
        latest_success = self.db.execute(
            select(IngestionRun)
            .where(IngestionRun.run_type == run_type, IngestionRun.status == "success")
            .order_by(IngestionRun.finished_at.desc(), IngestionRun.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        latest_failure = self.db.execute(
            select(IngestionRun)
            .where(IngestionRun.run_type == run_type, IngestionRun.status == "failed")
            .order_by(IngestionRun.finished_at.desc(), IngestionRun.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        failed_runs_last_window = int(
            self.db.execute(
                select(func.count(IngestionRun.id)).where(
                    IngestionRun.run_type == run_type,
                    IngestionRun.status == "failed",
                    IngestionRun.started_at >= failed_window_start,
                )
            ).scalar_one()
            or 0
        )

        lag_reference = None
        if lag_source == "raw_ingested_at":
            lag_reference = self.db.execute(
                select(func.max(RawIngestion.ingested_at))
            ).scalar_one_or_none()
        elif latest_success is not None:
            lag_reference = latest_success.finished_at or latest_success.started_at

        return self._pipeline_state_payload(
            pipeline=pipeline,
            latest_success_at=latest_success.finished_at if latest_success is not None else None,
            latest_failure_at=latest_failure.finished_at if latest_failure is not None else None,
            lag_reference=lag_reference,
            stale_after_hours=stale_after_hours,
            failed_runs_last_window=failed_runs_last_window,
        )

    def _build_timestamp_pipeline_state(
        self,
        *,
        pipeline: str,
        model,
        timestamp_column,
        stale_after_hours: int,
        status_column=None,
        failed_window_start: datetime | None = None,
    ) -> dict[str, Any]:
        latest_success_at = self.db.execute(select(func.max(timestamp_column))).scalar_one_or_none()
        latest_failure_at = None
        failed_runs_last_window = 0
        if status_column is not None and failed_window_start is not None:
            latest_failure_at = self.db.execute(
                select(func.max(EvaluationRun.finished_at)).where(status_column == "failed")
            ).scalar_one_or_none()
            failed_runs_last_window = int(
                self.db.execute(
                    select(func.count(EvaluationRun.id)).where(
                        status_column == "failed",
                        EvaluationRun.started_at >= failed_window_start,
                    )
                ).scalar_one()
                or 0
            )

        return self._pipeline_state_payload(
            pipeline=pipeline,
            latest_success_at=latest_success_at,
            latest_failure_at=latest_failure_at,
            lag_reference=latest_success_at,
            stale_after_hours=stale_after_hours,
            failed_runs_last_window=failed_runs_last_window,
        )

    def _pipeline_state_payload(
        self,
        *,
        pipeline: str,
        latest_success_at: datetime | None,
        latest_failure_at: datetime | None,
        lag_reference: datetime | None,
        stale_after_hours: int,
        failed_runs_last_window: int,
    ) -> dict[str, Any]:
        if latest_success_at is None:
            status = "warning"
            detail = "no_successful_run_yet"
            lag_minutes = None
        else:
            lag_minutes = round(
                (
                    datetime.now(UTC) - self._coerce_utc(lag_reference or latest_success_at)
                ).total_seconds()
                / 60,
                2,
            )
            if lag_minutes > stale_after_hours * 60:
                status = "warning"
                detail = f"stale_after_{stale_after_hours}h"
            else:
                status = "ok"
                detail = None

        if failed_runs_last_window > 0 and status == "ok":
            status = "warning"
            detail = "recent_failed_runs_detected"

        return {
            "pipeline": pipeline,
            "status": status,
            "detail": detail,
            "last_success_at": latest_success_at,
            "last_failure_at": latest_failure_at,
            "lag_minutes": lag_minutes,
            "failed_runs_last_window": failed_runs_last_window,
        }

    def _provider_latency_summary(self) -> list[dict[str, Any]]:
        rows = self.db.execute(
            select(
                RawIngestion.provider,
                RawIngestion.entity_type,
                RawIngestion.response_metadata,
                RawIngestion.ingested_at,
            )
            .where(RawIngestion.response_metadata.is_not(None))
            .order_by(RawIngestion.ingested_at.desc())
            .limit(500)
        ).all()

        grouped: dict[tuple[str, str], list[tuple[float, datetime]]] = defaultdict(list)
        for provider, entity_type, response_metadata, ingested_at in rows:
            if not isinstance(response_metadata, dict):
                continue
            latency_ms = response_metadata.get("latency_ms")
            try:
                latency_value = float(latency_ms)
            except (TypeError, ValueError):
                continue
            grouped[(provider, entity_type)].append((latency_value, self._coerce_utc(ingested_at)))

        payload: list[dict[str, Any]] = []
        for (provider, entity_type), values in sorted(grouped.items()):
            latencies = [item[0] for item in values]
            payload.append(
                {
                    "provider": provider,
                    "entity_type": entity_type,
                    "sample_size": len(latencies),
                    "avg_latency_ms": round(sum(latencies) / len(latencies), 2),
                    "max_latency_ms": round(max(latencies), 2),
                    "last_latency_ms": round(values[0][0], 2),
                    "last_observed_at": values[0][1],
                }
            )
        return payload

    def _build_check_for_raw_pending(self, raw_pending_count: int) -> dict[str, Any]:
        if raw_pending_count == 0:
            return {
                "code": "raw_pending_count",
                "status": "ok",
                "observed_value": raw_pending_count,
                "threshold_value": 0,
            }
        return {
            "code": "raw_pending_count",
            "status": "warning",
            "detail": "pending_raw_records_present",
            "observed_value": raw_pending_count,
            "threshold_value": 0,
        }

    def _build_check_for_failed_runs(self, failed_runs_last_window: int) -> dict[str, Any]:
        if failed_runs_last_window == 0:
            return {
                "code": "failed_runs_last_window",
                "status": "ok",
                "observed_value": 0,
                "threshold_value": 0,
            }
        return {
            "code": "failed_runs_last_window",
            "status": "warning",
            "detail": "recent_failed_runs_detected",
            "observed_value": failed_runs_last_window,
            "threshold_value": 0,
        }

    def _pipeline_threshold_minutes(self, pipeline: str) -> int | None:
        mapping = {
            "raw_ingestion": self.settings.monitoring_raw_stale_after_hours * 60,
            "normalization": self.settings.monitoring_normalization_stale_after_hours * 60,
            "feature_snapshots": self.settings.monitoring_feature_stale_after_hours * 60,
            "predictions": self.settings.monitoring_prediction_stale_after_hours * 60,
            "evaluation_runs": self.settings.monitoring_evaluation_stale_after_hours * 60,
        }
        return mapping.get(pipeline)

    def _count_matches_missing_team(self) -> int:
        return int(
            self.db.execute(
                select(func.count(Match.id)).where(
                    (Match.home_team_id.is_(None)) | (Match.away_team_id.is_(None))
                )
            ).scalar_one()
            or 0
        )

    def _count_matches_missing_competition(self) -> int:
        return int(
            self.db.execute(
                select(func.count(Match.id)).where(Match.competition_id.is_(None))
            ).scalar_one()
            or 0
        )

    def _count_provider_mapping_missing(self) -> int:
        total = 0
        total += self._count_missing_mappings_for_model(Competition, "competition")
        total += self._count_missing_mappings_for_model(Team, "team")
        total += self._count_missing_mappings_for_model(Match, "match")
        return total

    def _count_missing_mappings_for_model(self, model, entity_type: str) -> int:
        return int(
            self.db.execute(
                select(func.count())
                .select_from(model)
                .where(
                    ~select(ProviderEntity.id)
                    .where(
                        ProviderEntity.entity_type == entity_type,
                        ProviderEntity.internal_id == model.id,
                    )
                    .exists()
                )
            ).scalar_one()
            or 0
        )

    def _count_odds_inconsistent(self) -> int:
        return int(
            self.db.execute(
                select(func.count(Odds.id))
                .select_from(Odds)
                .outerjoin(Market, Market.id == Odds.market_id)
                .where(
                    (Odds.market_id.is_(None))
                    | (Odds.odds_value <= 1)
                    | ((Market.code == "OU") & (Odds.line_value.is_(None)))
                )
            ).scalar_one()
            or 0
        )

    def _count_predictions_without_selections(self) -> int:
        return int(
            self.db.execute(
                select(func.count(Prediction.id)).where(
                    ~select(PredictionSelection.id)
                    .where(PredictionSelection.prediction_id == Prediction.id)
                    .exists()
                )
            ).scalar_one()
            or 0
        )

    def _coerce_utc(self, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
