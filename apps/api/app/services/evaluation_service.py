from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Select, case, func, select
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.models.competition import Competition
from app.models.evaluation_metric import EvaluationMetric
from app.models.evaluation_run import EvaluationRun
from app.models.feature_snapshot import FeatureSnapshot
from app.models.match import Match
from app.models.model_registry import ModelRegistry
from app.models.model_version import ModelVersion
from app.models.prediction import Prediction
from app.models.prediction_selection import PredictionSelection
from app.models.season import Season
from app.schemas.evaluation import (
    EvaluationMetricRead,
    EvaluationRunDetailRead,
    EvaluationRunRowRead,
)
from app.services.evaluation_metrics import (
    EXPECTED_SELECTION_CODES,
    EvaluatedPredictionRow,
    EvaluatedSelection,
    SUPPORTED_EVALUATION_MARKETS,
    iter_metric_rows,
    probability_sum_is_valid,
)

logger = get_logger(__name__)

DEFAULT_PREDICTION_HORIZON = "pre_match"
DEFAULT_MIN_SAMPLE_SIZE_WARNING = 10
DEFAULT_SELECTION_STRATEGY = "latest_prediction_per_match_market_before_kickoff"
ALL_MARKETS_CODE = "ALL"


@dataclass(frozen=True)
class _AlignedPredictionContext:
    prediction_id: UUID
    match_id: UUID
    market_code: str
    competition_id: UUID | None
    competition_name: str | None
    season_id: UUID | None
    season_name: str | None
    model_version_id: UUID
    model_code: str
    model_version: str
    actual_selection_code: str


class EvaluationService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def _get_run_by_code(self, code: str) -> EvaluationRun | None:
        return self.db.execute(
            select(EvaluationRun).where(EvaluationRun.code == code)
        ).scalar_one_or_none()

    def run_backtest(
        self,
        *,
        period_start: datetime,
        period_end: datetime,
        market_code: str | None = None,
        markets: list[str] | None = None,
        model_version_id: UUID | None = None,
        prediction_horizon: str = DEFAULT_PREDICTION_HORIZON,
        code: str | None = None,
        name: str | None = None,
        min_sample_size_warning: int = DEFAULT_MIN_SAMPLE_SIZE_WARNING,
    ) -> dict[str, Any]:
        normalized_markets = self._normalize_markets(market_code=market_code, markets=markets)
        canonical_market_code = (
            normalized_markets[0] if len(normalized_markets) == 1 else ALL_MARKETS_CODE
        )
        period_start = self._coerce_utc(period_start)
        period_end = self._coerce_utc(period_end)
        if period_end < period_start:
            raise ValueError("invalid_period_range")

        if model_version_id is not None:
            self._ensure_model_version_exists(model_version_id)

        run_code = code or self._build_run_code(canonical_market_code, period_start, period_end)
        existing_run = self._get_run_by_code(run_code)
        if existing_run is not None:
            logger.info(
                "evaluation_run_reused",
                extra={
                    "evaluation_run_id": str(existing_run.id),
                    "code": run_code,
                    "market_code": existing_run.market_code,
                    "status": existing_run.status,
                },
            )
            return self.get_run_detail_dict(existing_run.id)

        started_at = datetime.now(UTC)
        evaluation_run = EvaluationRun(
            code=run_code,
            name=name or self._build_run_name(canonical_market_code, period_start, period_end),
            evaluation_type="backtest",
            model_version_id=model_version_id,
            market_code=canonical_market_code,
            period_start=period_start,
            period_end=period_end,
            started_at=started_at,
            finished_at=None,
            status="running",
            config_json={
                "requested_markets": normalized_markets,
                "prediction_horizon": prediction_horizon,
                "selection_strategy": DEFAULT_SELECTION_STRATEGY,
                "as_of_rules": [
                    "prediction.as_of_ts <= match.match_date",
                    "feature_snapshot.as_of_ts <= match.match_date",
                    "prediction.feature_snapshot_id references the snapshot used for evaluation",
                    "real outcome is derived only after match status finished",
                ],
                "source": "existing_predictions_only",
                "quality_checks": {},
                "limitations": [
                    "no_closing_line_value_engine",
                    "simulated_roi_uses_top_selection_and_market_best_odds_available_on_prediction_row",
                ],
            },
        )
        self.db.add(evaluation_run)
        self.db.commit()
        self.db.refresh(evaluation_run)

        try:
            aligned_rows, quality_checks = self._build_aligned_rows(
                period_start=period_start,
                period_end=period_end,
                markets=normalized_markets,
                model_version_id=model_version_id,
                prediction_horizon=prediction_horizon,
            )

            if not aligned_rows:
                raise ValueError("evaluation_sample_empty")

            metric_rows = [
                EvaluationMetric(
                    evaluation_run_id=evaluation_run.id,
                    metric_code=metric_code,
                    metric_value=float(metric_value),
                    segment_key=segment_key,
                )
                for segment_key, metric_code, metric_value in iter_metric_rows(aligned_rows)
            ]
            self.db.add_all(metric_rows)

            warnings: list[str] = []
            sample_size = len(aligned_rows)
            if sample_size < min_sample_size_warning:
                warnings.append("sample_size_low")

            config_json = dict(evaluation_run.config_json or {})
            config_json["quality_checks"] = quality_checks
            config_json["warnings"] = warnings
            config_json["sample_size"] = sample_size
            config_json["evaluated_prediction_ids"] = [
                str(row.prediction_id) for row in aligned_rows[:1000]
            ]
            config_json["segment_keys"] = sorted(
                {metric.segment_key for metric in metric_rows if metric.segment_key is not None}
            )

            evaluation_run.status = "success"
            evaluation_run.finished_at = datetime.now(UTC)
            evaluation_run.config_json = config_json
            self.db.commit()
            self.db.refresh(evaluation_run)

            logger.info(
                "evaluation_run_completed",
                extra={
                    "evaluation_run_id": str(evaluation_run.id),
                    "market_code": evaluation_run.market_code,
                    "model_version_id": str(model_version_id) if model_version_id else None,
                    "period_start": period_start.isoformat(),
                    "period_end": period_end.isoformat(),
                    "sample_size": sample_size,
                    "metric_rows_created": len(metric_rows),
                    "skipped": quality_checks.get("skipped_total", 0),
                    "errors": 0,
                    "warnings": warnings,
                },
            )
            return self.get_run_detail_dict(evaluation_run.id)
        except Exception as exc:  # noqa: BLE001
            self.db.rollback()
            evaluation_run.status = "failed"
            evaluation_run.finished_at = datetime.now(UTC)
            failed_config = dict(evaluation_run.config_json or {})
            failed_config["error"] = str(exc)
            evaluation_run.config_json = failed_config
            self.db.add(evaluation_run)
            self.db.commit()
            self.db.refresh(evaluation_run)
            logger.exception(
                "evaluation_run_failed",
                extra={
                    "evaluation_run_id": str(evaluation_run.id),
                    "market_code": evaluation_run.market_code,
                    "model_version_id": str(model_version_id) if model_version_id else None,
                    "period_start": period_start.isoformat(),
                    "period_end": period_end.isoformat(),
                    "sample_size": 0,
                    "metric_rows_created": 0,
                    "skipped": 0,
                    "errors": 1,
                },
            )
            raise

    def list_runs(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        market_code: str | None = None,
        status: str | None = None,
    ) -> list[EvaluationRunRowRead]:
        statement: Select[tuple[EvaluationRun]] = select(EvaluationRun)
        if market_code is not None:
            statement = statement.where(EvaluationRun.market_code == market_code)
        if status is not None:
            statement = statement.where(EvaluationRun.status == status)
        statement = (
            statement.order_by(EvaluationRun.started_at.desc(), EvaluationRun.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        runs = list(self.db.execute(statement).scalars().all())
        if not runs:
            return []

        metrics_by_run = self._load_metrics_by_run([run.id for run in runs])
        return [self._serialize_run_row(run, metrics_by_run.get(run.id, [])) for run in runs]

    def get_run_detail(self, run_id: UUID) -> EvaluationRunDetailRead:
        statement = select(EvaluationRun).where(EvaluationRun.id == run_id)
        run = self.db.execute(statement).scalar_one_or_none()
        if run is None:
            raise ValueError("evaluation_run_not_found")
        metrics = self._load_metrics_by_run([run.id]).get(run.id, [])
        return self._serialize_run_detail(run, metrics)

    def get_run_detail_dict(self, run_id: UUID) -> dict[str, Any]:
        return self.get_run_detail(run_id).model_dump()

    def _build_aligned_rows(
        self,
        *,
        period_start: datetime,
        period_end: datetime,
        markets: list[str],
        model_version_id: UUID | None,
        prediction_horizon: str,
    ) -> tuple[list[EvaluatedPredictionRow], dict[str, Any]]:
        raw_contexts = self._load_latest_prediction_contexts(
            period_start=period_start,
            period_end=period_end,
            markets=markets,
            model_version_id=model_version_id,
            prediction_horizon=prediction_horizon,
        )
        selections_by_prediction = self._load_selections_by_prediction(
            prediction_ids=[context.prediction_id for context in raw_contexts],
            market_codes_by_prediction={
                context.prediction_id: context.market_code for context in raw_contexts
            },
        )

        expected_finished_matches = self._count_finished_matches(
            period_start=period_start, period_end=period_end
        )
        finished_matches_with_scores = self._count_finished_matches_with_scores(
            period_start=period_start, period_end=period_end
        )
        expected_prediction_count = finished_matches_with_scores * len(markets)

        skip_counts: Counter[str] = Counter()
        aligned_rows: list[EvaluatedPredictionRow] = []

        for context in raw_contexts:
            selections = selections_by_prediction.get(context.prediction_id, [])
            if not selections:
                skip_counts["predictions_without_selections"] += 1
                continue

            if not self._selection_codes_match_market(context.market_code, selections):
                skip_counts["predictions_with_market_mismatch"] += 1
                continue

            if not probability_sum_is_valid(context.market_code, selections):
                skip_counts["predictions_with_invalid_probabilities"] += 1
                continue

            top_selection = self._select_top_selection(selections)
            aligned_rows.append(
                EvaluatedPredictionRow(
                    prediction_id=context.prediction_id,
                    match_id=context.match_id,
                    market_code=context.market_code,
                    competition_id=context.competition_id,
                    competition_name=context.competition_name,
                    season_id=context.season_id,
                    season_name=context.season_name,
                    model_version_id=context.model_version_id,
                    model_code=context.model_code,
                    model_version=context.model_version,
                    actual_selection_code=context.actual_selection_code,
                    top_selection_code=top_selection.selection_code,
                    top_probability=float(top_selection.predicted_probability),
                    top_market_best_odds=top_selection.market_best_odds,
                    top_edge_pct=top_selection.edge_pct,
                    top_confidence_score=top_selection.confidence_score,
                    selections=tuple(selections),
                )
            )

        missing_predictions = max(expected_prediction_count - len(raw_contexts), 0)
        if missing_predictions:
            skip_counts["predictions_missing_in_period"] += missing_predictions

        quality_checks = {
            "expected_finished_matches": expected_finished_matches,
            "finished_matches_with_valid_scores": finished_matches_with_scores,
            "finished_matches_without_scores": max(
                expected_finished_matches - finished_matches_with_scores, 0
            ),
            "expected_prediction_count": expected_prediction_count,
            "raw_prediction_context_count": len(raw_contexts),
            "aligned_prediction_count": len(aligned_rows),
            "skipped_total": int(sum(skip_counts.values())),
            "skip_reasons": dict(skip_counts),
            "model_version_filter_applied": str(model_version_id) if model_version_id else None,
        }
        return aligned_rows, quality_checks

    def _load_latest_prediction_contexts(
        self,
        *,
        period_start: datetime,
        period_end: datetime,
        markets: list[str],
        model_version_id: UUID | None,
        prediction_horizon: str,
    ) -> list[_AlignedPredictionContext]:
        partition_columns = [Prediction.match_id, Prediction.market_code]
        if model_version_id is not None:
            partition_columns.append(Prediction.model_version_id)

        ranked = (
            select(
                Prediction.id.label("prediction_id"),
                Prediction.match_id,
                Prediction.market_code,
                Match.competition_id,
                Competition.name.label("competition_name"),
                Match.season_id,
                Season.name.label("season_name"),
                Prediction.model_version_id,
                ModelRegistry.code.label("model_code"),
                ModelVersion.version.label("model_version"),
                self._actual_outcome_case(Prediction.market_code).label("actual_selection_code"),
                func.row_number()
                .over(
                    partition_by=tuple(partition_columns),
                    order_by=(
                        Prediction.as_of_ts.desc(),
                        Prediction.created_at.desc(),
                        Prediction.id.desc(),
                    ),
                )
                .label("rn"),
            )
            .join(Match, Match.id == Prediction.match_id)
            .join(FeatureSnapshot, FeatureSnapshot.id == Prediction.feature_snapshot_id)
            .join(ModelVersion, ModelVersion.id == Prediction.model_version_id)
            .join(ModelRegistry, ModelRegistry.id == ModelVersion.model_registry_id)
            .join(Competition, Competition.id == Match.competition_id)
            .outerjoin(Season, Season.id == Match.season_id)
            .where(
                Match.status == "finished",
                Match.match_date >= period_start,
                Match.match_date <= period_end,
                Prediction.market_code.in_(markets),
                Prediction.prediction_horizon == prediction_horizon,
                Prediction.as_of_ts <= Match.match_date,
                FeatureSnapshot.as_of_ts == Prediction.as_of_ts,
                FeatureSnapshot.as_of_ts <= Match.match_date,
                Match.home_goals.is_not(None),
                Match.away_goals.is_not(None),
            )
        )
        if model_version_id is not None:
            ranked = ranked.where(Prediction.model_version_id == model_version_id)

        ranked_subquery = ranked.subquery()
        rows = (
            self.db.execute(select(ranked_subquery).where(ranked_subquery.c.rn == 1))
            .mappings()
            .all()
        )

        payload: list[_AlignedPredictionContext] = []
        for row in rows:
            actual_selection_code = row["actual_selection_code"]
            if actual_selection_code is None:
                continue
            payload.append(
                _AlignedPredictionContext(
                    prediction_id=row["prediction_id"],
                    match_id=row["match_id"],
                    market_code=row["market_code"],
                    competition_id=row["competition_id"],
                    competition_name=row["competition_name"],
                    season_id=row["season_id"],
                    season_name=row["season_name"],
                    model_version_id=row["model_version_id"],
                    model_code=row["model_code"],
                    model_version=row["model_version"],
                    actual_selection_code=actual_selection_code,
                )
            )
        return payload

    def _load_selections_by_prediction(
        self,
        *,
        prediction_ids: list[UUID],
        market_codes_by_prediction: dict[UUID, str],
    ) -> dict[UUID, list[EvaluatedSelection]]:
        if not prediction_ids:
            return {}

        rows = (
            self.db.execute(
                select(PredictionSelection)
                .where(PredictionSelection.prediction_id.in_(prediction_ids))
                .order_by(
                    PredictionSelection.prediction_id.asc(),
                    PredictionSelection.selection_code.asc(),
                )
            )
            .scalars()
            .all()
        )

        payload: dict[UUID, list[EvaluatedSelection]] = defaultdict(list)
        for row in rows:
            payload[row.prediction_id].append(
                EvaluatedSelection(
                    selection_code=row.selection_code,
                    predicted_probability=float(row.predicted_probability),
                    fair_odds=float(row.fair_odds) if row.fair_odds is not None else None,
                    market_best_odds=float(row.market_best_odds)
                    if row.market_best_odds is not None
                    else None,
                    edge_pct=float(row.edge_pct) if row.edge_pct is not None else None,
                    confidence_score=float(row.confidence_score)
                    if row.confidence_score is not None
                    else None,
                )
            )

        ordered_payload: dict[UUID, list[EvaluatedSelection]] = {}
        for prediction_id, selections in payload.items():
            if not selections:
                ordered_payload[prediction_id] = selections
                continue
            market_code = market_codes_by_prediction.get(prediction_id)
            expected_order = EXPECTED_SELECTION_CODES.get(market_code or "", ())
            position = {code: index for index, code in enumerate(expected_order)}
            ordered_payload[prediction_id] = sorted(
                selections,
                key=lambda item: position.get(item.selection_code, 999),
            )
        return ordered_payload

    def _load_metrics_by_run(self, run_ids: list[UUID]) -> dict[UUID, list[EvaluationMetric]]:
        if not run_ids:
            return {}
        rows = (
            self.db.execute(
                select(EvaluationMetric)
                .where(EvaluationMetric.evaluation_run_id.in_(run_ids))
                .order_by(
                    EvaluationMetric.evaluation_run_id.asc(),
                    EvaluationMetric.segment_key.asc(),
                    EvaluationMetric.metric_code.asc(),
                )
            )
            .scalars()
            .all()
        )
        payload: dict[UUID, list[EvaluationMetric]] = defaultdict(list)
        for row in rows:
            payload[row.evaluation_run_id].append(row)
        return payload

    def _serialize_run_row(
        self,
        run: EvaluationRun,
        metrics: list[EvaluationMetric],
    ) -> EvaluationRunRowRead:
        metric_reads = [
            self._serialize_metric(metric) for metric in metrics if metric.segment_key is None
        ]
        sample_size = next(
            (
                int(metric.metric_value)
                for metric in metrics
                if metric.metric_code == "sample_size" and metric.segment_key is None
            ),
            None,
        )
        available_segments = sorted(
            {metric.segment_key for metric in metrics if metric.segment_key is not None}
        )
        return EvaluationRunRowRead(
            id=run.id,
            code=run.code,
            name=run.name,
            evaluation_type=run.evaluation_type,
            model_version_id=run.model_version_id,
            market_code=run.market_code,
            period_start=run.period_start,
            period_end=run.period_end,
            started_at=run.started_at,
            finished_at=run.finished_at,
            status=run.status,
            created_at=run.created_at,
            sample_size=sample_size,
            global_metrics=metric_reads,
            available_segments=available_segments,
        )

    def _serialize_run_detail(
        self,
        run: EvaluationRun,
        metrics: list[EvaluationMetric],
    ) -> EvaluationRunDetailRead:
        row = self._serialize_run_row(run, metrics)
        return EvaluationRunDetailRead(
            **row.model_dump(),
            config_json=run.config_json,
            metrics=[self._serialize_metric(metric) for metric in metrics],
        )

    def _serialize_metric(self, metric: EvaluationMetric) -> EvaluationMetricRead:
        return EvaluationMetricRead(
            id=metric.id,
            evaluation_run_id=metric.evaluation_run_id,
            metric_code=metric.metric_code,
            metric_value=float(metric.metric_value),
            segment_key=metric.segment_key,
            created_at=metric.created_at,
        )

    def _ensure_model_version_exists(self, model_version_id: UUID) -> None:
        statement = select(ModelVersion.id).where(ModelVersion.id == model_version_id)
        exists = self.db.execute(statement).scalar_one_or_none()
        if exists is None:
            raise ValueError("model_version_not_found")

    def _count_finished_matches(self, *, period_start: datetime, period_end: datetime) -> int:
        statement = select(func.count(Match.id)).where(
            Match.status == "finished",
            Match.match_date >= period_start,
            Match.match_date <= period_end,
        )
        return int(self.db.execute(statement).scalar_one())

    def _count_finished_matches_with_scores(
        self, *, period_start: datetime, period_end: datetime
    ) -> int:
        statement = select(func.count(Match.id)).where(
            Match.status == "finished",
            Match.match_date >= period_start,
            Match.match_date <= period_end,
            Match.home_goals.is_not(None),
            Match.away_goals.is_not(None),
        )
        return int(self.db.execute(statement).scalar_one())

    def _selection_codes_match_market(
        self,
        market_code: str,
        selections: list[EvaluatedSelection],
    ) -> bool:
        expected_codes = EXPECTED_SELECTION_CODES.get(market_code)
        if expected_codes is None:
            return False
        actual_codes = tuple(selection.selection_code for selection in selections)
        return actual_codes == expected_codes

    def _select_top_selection(self, selections: list[EvaluatedSelection]) -> EvaluatedSelection:
        return sorted(
            selections,
            key=lambda item: (
                -float(item.predicted_probability),
                -(float(item.edge_pct) if item.edge_pct is not None else -999999.0),
                -(float(item.confidence_score) if item.confidence_score is not None else -999999.0),
                item.selection_code,
            ),
        )[0]

    def _normalize_markets(
        self,
        *,
        market_code: str | None,
        markets: list[str] | None,
    ) -> list[str]:
        raw_markets: list[str]
        if markets is not None:
            raw_markets = markets
        elif market_code is not None:
            raw_markets = [market_code]
        else:
            raw_markets = list(SUPPORTED_EVALUATION_MARKETS)

        normalized: list[str] = []
        for item in raw_markets:
            value = item.strip().upper()
            if value not in SUPPORTED_EVALUATION_MARKETS:
                raise ValueError(f"unsupported_market_code:{value}")
            if value not in normalized:
                normalized.append(value)
        if not normalized:
            raise ValueError("no_market_codes_provided")
        return normalized

    def _build_run_code(
        self, market_code: str, period_start: datetime, period_end: datetime
    ) -> str:
        timestamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
        return f"eval_{market_code.lower()}_{period_start:%Y%m%d}_{period_end:%Y%m%d}_{timestamp}"

    def _build_run_name(
        self, market_code: str, period_start: datetime, period_end: datetime
    ) -> str:
        return f"Backtest {market_code} {period_start:%Y-%m-%d} -> {period_end:%Y-%m-%d}"

    def _coerce_utc(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def _actual_outcome_case(self, market_code_column):
        return case(
            (
                market_code_column == "1X2",
                case(
                    (Match.home_goals > Match.away_goals, "HOME"),
                    (Match.home_goals == Match.away_goals, "DRAW"),
                    else_="AWAY",
                ),
            ),
            (
                market_code_column == "OU25",
                case(
                    ((Match.home_goals + Match.away_goals) > 2, "OVER"),
                    else_="UNDER",
                ),
            ),
            (
                market_code_column == "BTTS",
                case(
                    ((Match.home_goals > 0) & (Match.away_goals > 0), "YES"),
                    else_="NO",
                ),
            ),
            else_=None,
        )
