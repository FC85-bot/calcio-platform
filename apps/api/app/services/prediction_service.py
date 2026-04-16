from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
import math
from typing import Any
from uuid import UUID

from sqlalchemy import Select, cast, func, literal_column, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.sql.sqltypes import Numeric

from app.core.logging import get_logger
from app.models.feature_snapshot import FeatureSnapshot
from app.models.market import Market
from app.models.match import Match
from app.models.model_registry import ModelRegistry
from app.models.model_version import ModelVersion
from app.models.odds import Odds
from app.models.prediction import Prediction
from app.models.prediction_selection import PredictionSelection

logger = get_logger(__name__)

SUPPORTED_MARKET_CODES: tuple[str, ...] = ("1X2", "OU25", "BTTS")
DEFAULT_PREDICTION_HORIZON = "pre_match"
DEFAULT_MAX_GOAL_GRID = 8
DEFAULT_PROBABILITY_TOLERANCE = 0.001
DEFAULT_MIN_COMPLETENESS_SCORE = 0.45
DEFAULT_FAIR_ODDS_DECIMALS = 4
DEFAULT_EDGE_DECIMALS = 4
DEFAULT_PROBABILITY_DECIMALS = 6
DEFAULT_CONFIDENCE_DECIMALS = 2
OU25_LINE_VALUE = Decimal("2.500")

MARKET_SELECTION_CODES: dict[str, tuple[str, ...]] = {
    "1X2": ("HOME", "DRAW", "AWAY"),
    "OU25": ("OVER", "UNDER"),
    "BTTS": ("YES", "NO"),
}

PREDICTION_MODEL_SEEDS: tuple[dict[str, Any], ...] = (
    {
        "code": "baseline_poisson_1x2",
        "name": "Baseline Poisson 1X2",
        "market_code": "1X2",
        "task_type": "probability_estimation",
        "version": "v1",
        "status": "active",
        "is_active": True,
        "artifact_path": None,
        "config_json": {
            "model_type": "poisson_baseline_v1",
            "min_completeness_score": 0.45,
            "probability_tolerance": 0.001,
            "max_goal_grid": 8,
            "baseline_home_goals": 0.32,
            "baseline_away_goals": 0.24,
            "home_advantage": 0.18,
            "attack_weight": 0.46,
            "opponent_defense_weight": 0.34,
            "ppg_weight": 0.20,
            "form_weight": 0.16,
            "position_weight": 0.015,
            "clean_sheet_weight": 0.08,
            "failed_to_score_weight": 0.10,
            "min_goal_rate": 0.2,
            "max_goal_rate": 3.2,
        },
    },
    {
        "code": "baseline_poisson_ou25",
        "name": "Baseline Poisson OU25",
        "market_code": "OU25",
        "task_type": "probability_estimation",
        "version": "v1",
        "status": "active",
        "is_active": True,
        "artifact_path": None,
        "config_json": {
            "model_type": "poisson_baseline_v1",
            "min_completeness_score": 0.45,
            "probability_tolerance": 0.001,
            "max_goal_grid": 8,
            "baseline_home_goals": 0.32,
            "baseline_away_goals": 0.24,
            "home_advantage": 0.18,
            "attack_weight": 0.46,
            "opponent_defense_weight": 0.34,
            "ppg_weight": 0.20,
            "form_weight": 0.16,
            "position_weight": 0.015,
            "clean_sheet_weight": 0.08,
            "failed_to_score_weight": 0.10,
            "min_goal_rate": 0.2,
            "max_goal_rate": 3.2,
        },
    },
    {
        "code": "baseline_poisson_btts",
        "name": "Baseline Poisson BTTS",
        "market_code": "BTTS",
        "task_type": "probability_estimation",
        "version": "v1",
        "status": "active",
        "is_active": True,
        "artifact_path": None,
        "config_json": {
            "model_type": "poisson_baseline_v1",
            "min_completeness_score": 0.45,
            "probability_tolerance": 0.001,
            "max_goal_grid": 8,
            "baseline_home_goals": 0.32,
            "baseline_away_goals": 0.24,
            "home_advantage": 0.18,
            "attack_weight": 0.46,
            "opponent_defense_weight": 0.34,
            "ppg_weight": 0.20,
            "form_weight": 0.16,
            "position_weight": 0.015,
            "clean_sheet_weight": 0.08,
            "failed_to_score_weight": 0.10,
            "min_goal_rate": 0.2,
            "max_goal_rate": 3.2,
        },
    },
)


@dataclass(frozen=True)
class ActiveModelVersion:
    model_version_id: UUID
    model_registry_id: UUID
    market_code: str
    model_code: str
    model_name: str
    version: str
    config_json: dict[str, Any]


class PredictionService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def _is_duplicate_prediction_integrity_error(self, exc: IntegrityError) -> bool:
        message = str(getattr(exc, "orig", exc)).lower()
        return "uq_predictions_identity" in message or (
            "unique constraint failed" in message
            and "predictions.match_id" in message
            and "predictions.feature_snapshot_id" in message
            and "predictions.model_version_id" in message
            and "predictions.market_code" in message
            and "predictions.prediction_horizon" in message
        )

    def build_predictions_for_match(
        self,
        *,
        match_id: UUID,
        prediction_horizon: str = DEFAULT_PREDICTION_HORIZON,
        feature_set_version: str | None = None,
        markets: Iterable[str] | None = None,
        persist: bool = True,
    ) -> dict[str, Any]:
        return self.build_predictions(
            match_id=match_id,
            prediction_horizon=prediction_horizon,
            feature_set_version=feature_set_version,
            markets=markets,
            persist=persist,
        )

    def build_predictions(
        self,
        *,
        match_id: UUID | None = None,
        competition_id: UUID | None = None,
        season_id: UUID | None = None,
        season: str | None = None,
        prediction_horizon: str = DEFAULT_PREDICTION_HORIZON,
        feature_set_version: str | None = None,
        future_only: bool = True,
        limit: int | None = None,
        markets: Iterable[str] | None = None,
        persist: bool = True,
    ) -> dict[str, Any]:
        selected_markets = self._normalize_markets(markets)
        target_matches = self._list_target_matches(
            match_id=match_id,
            competition_id=competition_id,
            season_id=season_id,
            season=season,
            future_only=future_only,
            limit=limit,
        )
        match_ids = [row.id for row in target_matches]

        active_model_versions = self._load_active_model_versions(selected_markets)
        missing_markets = sorted(set(selected_markets) - set(active_model_versions))
        if missing_markets:
            raise ValueError(
                f"active_model_version_missing_for_markets:{','.join(missing_markets)}"
            )

        snapshots_by_match = self._load_latest_feature_snapshots(
            match_ids=match_ids,
            prediction_horizon=prediction_horizon,
            feature_set_version=feature_set_version,
        )
        odds_map = self._load_best_odds_map(match_ids=match_ids)
        existing_keys = self._load_existing_prediction_keys(
            match_ids=match_ids,
            prediction_horizon=prediction_horizon,
            active_model_versions=active_model_versions,
        )

        results: list[dict[str, Any]] = []
        created = 0
        skipped = 0
        errors = 0
        warning_counts: Counter[str] = Counter()
        error_counts: Counter[str] = Counter()

        for match in target_matches:
            snapshot = snapshots_by_match.get(match.id)
            if snapshot is None:
                item = {
                    "match_id": match.id,
                    "status": "skipped",
                    "warning": "feature_snapshot_missing",
                }
                results.append(item)
                skipped += 1
                warning_counts[item["warning"]] += 1
                logger.warning(
                    "prediction_skipped",
                    extra={
                        "match_id": str(match.id),
                        "feature_snapshot_id": None,
                        "market_code": None,
                        "model_version_id": None,
                        "status": "skipped",
                        "warning": item["warning"],
                    },
                )
                continue

            for market_code in selected_markets:
                active_model = active_model_versions[market_code]
                prediction_key = (
                    match.id,
                    snapshot.id,
                    active_model.model_version_id,
                    market_code,
                    prediction_horizon,
                )
                if prediction_key in existing_keys:
                    item = {
                        "match_id": match.id,
                        "feature_snapshot_id": snapshot.id,
                        "market_code": market_code,
                        "model_version_id": active_model.model_version_id,
                        "status": "skipped",
                        "warning": "duplicate_prediction",
                    }
                    results.append(item)
                    skipped += 1
                    warning_counts[item["warning"]] += 1
                    logger.info(
                        "prediction_skipped",
                        extra={
                            "match_id": str(match.id),
                            "feature_snapshot_id": str(snapshot.id),
                            "market_code": market_code,
                            "model_version_id": str(active_model.model_version_id),
                            "status": "skipped",
                            "warning": item["warning"],
                        },
                    )
                    continue

                outcome = self._build_prediction_for_snapshot(
                    snapshot=snapshot,
                    market_code=market_code,
                    active_model=active_model,
                    odds_map=odds_map,
                    persist=persist,
                )
                results.append(outcome)

                if outcome["status"] == "created":
                    created += 1
                    existing_keys.add(prediction_key)
                elif outcome["status"] == "skipped":
                    skipped += 1
                    if outcome.get("warning"):
                        warning_counts[outcome["warning"]] += 1
                else:
                    errors += 1
                    if outcome.get("error"):
                        error_counts[outcome["error"]] += 1

        summary = {
            "warning_counts": dict(sorted(warning_counts.items())),
            "error_counts": dict(sorted(error_counts.items())),
        }
        logger.info(
            "prediction_batch_completed",
            extra={
                "target_count": len(target_matches),
                "market_count": len(selected_markets),
                "created_count": created,
                "skipped_count": skipped,
                "error_count": errors,
                "prediction_horizon": prediction_horizon,
                "feature_set_version": feature_set_version,
                "warning_counts": summary["warning_counts"],
                "error_counts": summary["error_counts"],
            },
        )
        return {
            "target_count": len(target_matches),
            "created": created,
            "skipped": skipped,
            "errors": errors,
            "summary": summary,
            "results": results,
        }

    def _build_prediction_for_snapshot(
        self,
        *,
        snapshot: FeatureSnapshot,
        market_code: str,
        active_model: ActiveModelVersion,
        odds_map: dict[tuple[UUID, str, str], float],
        persist: bool,
    ) -> dict[str, Any]:
        features = snapshot.features_json if isinstance(snapshot.features_json, dict) else {}
        config = active_model.config_json or {}
        audit = self._extract_feature_audit(features)
        min_completeness = float(
            config.get("min_completeness_score", DEFAULT_MIN_COMPLETENESS_SCORE)
        )
        data_quality_score = self._compute_data_quality_score(snapshot, audit)

        log_extra = {
            "match_id": str(snapshot.match_id),
            "feature_snapshot_id": str(snapshot.id),
            "market_code": market_code,
            "model_version_id": str(active_model.model_version_id),
            "data_quality_score": data_quality_score,
            "odds_available": False,
        }

        if snapshot.completeness_score < min_completeness:
            warning = "feature_snapshot_completeness_too_low"
            logger.warning(
                "prediction_skipped",
                extra={
                    **log_extra,
                    "status": "skipped",
                    "warning": warning,
                    "completeness_score": snapshot.completeness_score,
                    "min_completeness_score": min_completeness,
                },
            )
            return {
                "match_id": snapshot.match_id,
                "feature_snapshot_id": snapshot.id,
                "market_code": market_code,
                "model_version_id": active_model.model_version_id,
                "status": "skipped",
                "warning": warning,
                "data_quality_score": data_quality_score,
            }

        try:
            probability_vector, model_debug = self._generate_probabilities(
                features=features,
                market_code=market_code,
                config=config,
            )
            self._validate_probability_vector(
                market_code=market_code,
                probability_vector=probability_vector,
                tolerance=float(config.get("probability_tolerance", DEFAULT_PROBABILITY_TOLERANCE)),
            )
            confidence_score = self._compute_confidence_score(
                data_quality_score=data_quality_score,
                probability_vector=probability_vector,
            )
            selections = self._build_selection_payloads(
                match_id=snapshot.match_id,
                market_code=market_code,
                probability_vector=probability_vector,
                odds_map=odds_map,
                confidence_score=confidence_score,
            )
        except ValueError as exc:
            warning = str(exc)
            logger.warning(
                "prediction_skipped",
                extra={
                    **log_extra,
                    "status": "skipped",
                    "warning": warning,
                },
            )
            return {
                "match_id": snapshot.match_id,
                "feature_snapshot_id": snapshot.id,
                "market_code": market_code,
                "model_version_id": active_model.model_version_id,
                "status": "skipped",
                "warning": warning,
                "data_quality_score": data_quality_score,
            }
        except IntegrityError as exc:
            self.db.rollback()
            if self._is_duplicate_prediction_integrity_error(exc):
                logger.info(
                    "prediction_skipped_duplicate",
                    extra={
                        **log_extra,
                        "status": "skipped",
                        "warning": "duplicate_prediction",
                    },
                )
                return {
                    "match_id": snapshot.match_id,
                    "feature_snapshot_id": snapshot.id,
                    "market_code": market_code,
                    "model_version_id": active_model.model_version_id,
                    "status": "skipped",
                    "warning": "duplicate_prediction",
                    "data_quality_score": data_quality_score,
                }
            logger.exception(
                "prediction_failed",
                extra={
                    **log_extra,
                    "status": "error",
                    "error": str(exc),
                },
            )
            return {
                "match_id": snapshot.match_id,
                "feature_snapshot_id": snapshot.id,
                "market_code": market_code,
                "model_version_id": active_model.model_version_id,
                "status": "error",
                "error": str(exc),
                "data_quality_score": data_quality_score,
            }
        except Exception as exc:  # noqa: BLE001
            self.db.rollback()
            logger.exception(
                "prediction_failed",
                extra={
                    **log_extra,
                    "status": "error",
                    "error": str(exc),
                },
            )
            return {
                "match_id": snapshot.match_id,
                "feature_snapshot_id": snapshot.id,
                "market_code": market_code,
                "model_version_id": active_model.model_version_id,
                "status": "error",
                "error": str(exc),
                "data_quality_score": data_quality_score,
            }

        log_extra["odds_available"] = any(
            item["market_best_odds"] is not None for item in selections
        )

        if not persist:
            logger.info(
                "prediction_built_preview",
                extra={
                    **log_extra,
                    "status": "created",
                    "warnings": audit["data_warnings"],
                    "model_debug": model_debug,
                },
            )
            return {
                "match_id": snapshot.match_id,
                "feature_snapshot_id": snapshot.id,
                "market_code": market_code,
                "model_version_id": active_model.model_version_id,
                "model_version": active_model.version,
                "status": "created",
                "data_quality_score": data_quality_score,
                "selections": selections,
                "model_debug": model_debug,
            }

        try:
            prediction = Prediction(
                match_id=snapshot.match_id,
                feature_snapshot_id=snapshot.id,
                model_version_id=active_model.model_version_id,
                market_code=market_code,
                prediction_horizon=snapshot.prediction_horizon,
                as_of_ts=snapshot.as_of_ts,
                data_quality_score=data_quality_score,
            )
            self.db.add(prediction)
            self.db.flush()

            selection_rows: list[PredictionSelection] = []
            for item in selections:
                row = PredictionSelection(
                    prediction_id=prediction.id,
                    selection_code=item["selection_code"],
                    predicted_probability=item["predicted_probability"],
                    fair_odds=item["fair_odds"],
                    market_best_odds=item["market_best_odds"],
                    edge_pct=item["edge_pct"],
                    confidence_score=item["confidence_score"],
                )
                self.db.add(row)
                selection_rows.append(row)

            self.db.commit()
            self.db.refresh(prediction)
            for row in selection_rows:
                self.db.refresh(row)
        except IntegrityError as exc:
            self.db.rollback()
            if self._is_duplicate_prediction_integrity_error(exc):
                logger.info(
                    "prediction_skipped_duplicate",
                    extra={
                        **log_extra,
                        "status": "skipped",
                        "warning": "duplicate_prediction",
                    },
                )
                return {
                    "match_id": snapshot.match_id,
                    "feature_snapshot_id": snapshot.id,
                    "market_code": market_code,
                    "model_version_id": active_model.model_version_id,
                    "status": "skipped",
                    "warning": "duplicate_prediction",
                    "data_quality_score": data_quality_score,
                }
            logger.exception(
                "prediction_failed",
                extra={
                    **log_extra,
                    "status": "error",
                    "error": str(exc),
                },
            )
            return {
                "match_id": snapshot.match_id,
                "feature_snapshot_id": snapshot.id,
                "market_code": market_code,
                "model_version_id": active_model.model_version_id,
                "status": "error",
                "error": str(exc),
                "data_quality_score": data_quality_score,
            }
        except Exception as exc:  # noqa: BLE001
            self.db.rollback()
            logger.exception(
                "prediction_failed",
                extra={
                    **log_extra,
                    "status": "error",
                    "error": str(exc),
                },
            )
            return {
                "match_id": snapshot.match_id,
                "feature_snapshot_id": snapshot.id,
                "market_code": market_code,
                "model_version_id": active_model.model_version_id,
                "status": "error",
                "error": str(exc),
                "data_quality_score": data_quality_score,
            }

        logger.info(
            "prediction_created",
            extra={
                **log_extra,
                "status": "created",
                "prediction_id": str(prediction.id),
                "selection_count": len(selection_rows),
                "warnings": audit["data_warnings"],
                "model_debug": model_debug,
            },
        )
        return {
            "id": prediction.id,
            "match_id": prediction.match_id,
            "feature_snapshot_id": prediction.feature_snapshot_id,
            "model_version_id": prediction.model_version_id,
            "model_version": active_model.version,
            "market_code": prediction.market_code,
            "prediction_horizon": prediction.prediction_horizon,
            "as_of_ts": prediction.as_of_ts,
            "status": "created",
            "data_quality_score": prediction.data_quality_score,
            "selections": [self._serialize_selection(row) for row in selection_rows],
        }

    def _generate_probabilities(
        self,
        *,
        features: dict[str, Any],
        market_code: str,
        config: dict[str, Any],
    ) -> tuple[dict[str, float], dict[str, float]]:
        if market_code not in MARKET_SELECTION_CODES:
            raise ValueError("unsupported_market_code")

        home_lambda, away_lambda = self._estimate_goal_rates(features=features, config=config)
        goal_matrix = self._build_goal_matrix(
            home_lambda=home_lambda,
            away_lambda=away_lambda,
            max_goals=int(config.get("max_goal_grid", DEFAULT_MAX_GOAL_GRID)),
        )

        if market_code == "1X2":
            raw = {
                "HOME": sum(
                    prob
                    for (home_goals, away_goals), prob in goal_matrix.items()
                    if home_goals > away_goals
                ),
                "DRAW": sum(
                    prob
                    for (home_goals, away_goals), prob in goal_matrix.items()
                    if home_goals == away_goals
                ),
                "AWAY": sum(
                    prob
                    for (home_goals, away_goals), prob in goal_matrix.items()
                    if home_goals < away_goals
                ),
            }
        elif market_code == "OU25":
            over = sum(
                prob
                for (home_goals, away_goals), prob in goal_matrix.items()
                if (home_goals + away_goals) >= 3
            )
            raw = {
                "OVER": over,
                "UNDER": 1.0 - over,
            }
        else:
            yes = sum(
                prob
                for (home_goals, away_goals), prob in goal_matrix.items()
                if home_goals >= 1 and away_goals >= 1
            )
            raw = {
                "YES": yes,
                "NO": 1.0 - yes,
            }

        vector = self._stabilize_probabilities(market_code=market_code, raw_vector=raw)
        return vector, {
            "home_lambda": round(home_lambda, 4),
            "away_lambda": round(away_lambda, 4),
        }

    def _estimate_goal_rates(
        self, *, features: dict[str, Any], config: dict[str, Any]
    ) -> tuple[float, float]:
        home_attack = self._mean_available(
            self._feature_float(features, "home_team_avg_goals_scored_last_5", default=1.25),
            self._safe_divide(
                self._feature_float(features, "home_team_home_last_5_goals_scored", default=6.0),
                5.0,
            ),
        )
        away_attack = self._mean_available(
            self._feature_float(features, "away_team_avg_goals_scored_last_5", default=1.15),
            self._safe_divide(
                self._feature_float(features, "away_team_away_last_5_goals_scored", default=5.0),
                5.0,
            ),
        )
        home_defense = self._mean_available(
            self._feature_float(features, "home_team_avg_goals_conceded_last_5", default=1.15),
            self._safe_divide(
                self._feature_float(features, "home_team_home_last_5_goals_conceded", default=5.0),
                5.0,
            ),
        )
        away_defense = self._mean_available(
            self._feature_float(features, "away_team_avg_goals_conceded_last_5", default=1.25),
            self._safe_divide(
                self._feature_float(features, "away_team_away_last_5_goals_conceded", default=6.0),
                5.0,
            ),
        )

        home_ppg = self._feature_float(features, "home_team_points_per_game", default=1.35)
        away_ppg = self._feature_float(features, "away_team_points_per_game", default=1.35)
        home_form = self._safe_divide(
            self._feature_float(features, "home_team_last_5_points", default=6.75), 15.0
        )
        away_form = self._safe_divide(
            self._feature_float(features, "away_team_last_5_points", default=6.75), 15.0
        )
        home_position = self._feature_float(features, "home_team_league_position", default=10.0)
        away_position = self._feature_float(features, "away_team_league_position", default=10.0)
        home_clean_sheet = self._feature_float(
            features, "home_team_clean_sheet_rate_last_5", default=0.25
        )
        away_clean_sheet = self._feature_float(
            features, "away_team_clean_sheet_rate_last_5", default=0.25
        )
        home_failed_to_score = self._feature_float(
            features, "home_team_failed_to_score_rate_last_5", default=0.25
        )
        away_failed_to_score = self._feature_float(
            features, "away_team_failed_to_score_rate_last_5", default=0.25
        )

        ppg_diff = home_ppg - away_ppg
        form_diff = home_form - away_form
        position_diff = away_position - home_position
        clean_sheet_diff = home_clean_sheet - away_clean_sheet
        failed_to_score_diff = away_failed_to_score - home_failed_to_score

        home_lambda = (
            float(config.get("baseline_home_goals", 0.32))
            + float(config.get("attack_weight", 0.46)) * home_attack
            + float(config.get("opponent_defense_weight", 0.34)) * away_defense
            + float(config.get("ppg_weight", 0.20)) * max(min(ppg_diff, 1.5), -1.5)
            + float(config.get("form_weight", 0.16)) * max(min(form_diff, 1.0), -1.0)
            + float(config.get("position_weight", 0.015)) * max(min(position_diff, 12.0), -12.0)
            + float(config.get("clean_sheet_weight", 0.08)) * clean_sheet_diff
            + float(config.get("failed_to_score_weight", 0.10)) * failed_to_score_diff
            + float(config.get("home_advantage", 0.18))
        )
        away_lambda = (
            float(config.get("baseline_away_goals", 0.24))
            + float(config.get("attack_weight", 0.46)) * away_attack
            + float(config.get("opponent_defense_weight", 0.34)) * home_defense
            - float(config.get("ppg_weight", 0.20)) * max(min(ppg_diff, 1.5), -1.5)
            - float(config.get("form_weight", 0.16)) * max(min(form_diff, 1.0), -1.0)
            - float(config.get("position_weight", 0.015)) * max(min(position_diff, 12.0), -12.0)
            - float(config.get("clean_sheet_weight", 0.08)) * clean_sheet_diff
            - float(config.get("failed_to_score_weight", 0.10)) * failed_to_score_diff
        )

        min_goal_rate = float(config.get("min_goal_rate", 0.2))
        max_goal_rate = float(config.get("max_goal_rate", 3.2))
        return (
            round(self._clamp(home_lambda, min_goal_rate, max_goal_rate), 6),
            round(self._clamp(away_lambda, min_goal_rate, max_goal_rate), 6),
        )

    def _build_goal_matrix(
        self, *, home_lambda: float, away_lambda: float, max_goals: int
    ) -> dict[tuple[int, int], float]:
        home_probs = self._poisson_probabilities(lam=home_lambda, max_goals=max_goals)
        away_probs = self._poisson_probabilities(lam=away_lambda, max_goals=max_goals)
        matrix = {
            (home_goals, away_goals): home_prob * away_prob
            for home_goals, home_prob in enumerate(home_probs)
            for away_goals, away_prob in enumerate(away_probs)
        }
        total = sum(matrix.values())
        if total <= 0:
            raise ValueError("invalid_probability_vector")
        return {key: value / total for key, value in matrix.items()}

    def _poisson_probabilities(self, *, lam: float, max_goals: int) -> list[float]:
        probabilities: list[float] = []
        for goals in range(max_goals + 1):
            probabilities.append(math.exp(-lam) * (lam**goals) / math.factorial(goals))
        total = sum(probabilities)
        if total <= 0:
            raise ValueError("invalid_probability_vector")
        return [value / total for value in probabilities]

    def _validate_probability_vector(
        self,
        *,
        market_code: str,
        probability_vector: dict[str, float],
        tolerance: float,
    ) -> None:
        expected_codes = MARKET_SELECTION_CODES[market_code]
        if tuple(probability_vector) != expected_codes:
            raise ValueError("selection_code_market_mismatch")
        if any(
            probability_vector[selection] < 0 or probability_vector[selection] > 1
            for selection in expected_codes
        ):
            raise ValueError("invalid_probability_vector")
        total = sum(probability_vector.values())
        if abs(total - 1.0) > tolerance:
            raise ValueError("probability_sum_invalid")

    def _build_selection_payloads(
        self,
        *,
        match_id: UUID,
        market_code: str,
        probability_vector: dict[str, float],
        odds_map: dict[tuple[UUID, str, str], float],
        confidence_score: float,
    ) -> list[dict[str, Any]]:
        selections: list[dict[str, Any]] = []
        for selection_code in MARKET_SELECTION_CODES[market_code]:
            predicted_probability = round(
                probability_vector[selection_code], DEFAULT_PROBABILITY_DECIMALS
            )
            fair_odds = self._compute_fair_odds(predicted_probability)
            if fair_odds is None:
                raise ValueError("fair_odds_invalid")

            market_best_odds = odds_map.get((match_id, market_code, selection_code))
            edge_pct = self._compute_edge_pct(
                market_best_odds=market_best_odds, fair_odds=fair_odds
            )
            selections.append(
                {
                    "selection_code": selection_code,
                    "predicted_probability": predicted_probability,
                    "fair_odds": fair_odds,
                    "market_best_odds": market_best_odds,
                    "edge_pct": edge_pct,
                    "confidence_score": round(confidence_score, DEFAULT_CONFIDENCE_DECIMALS),
                }
            )
        return selections

    def _compute_fair_odds(self, predicted_probability: float) -> float | None:
        safe_probability = max(predicted_probability, 0.0)
        if safe_probability <= 0:
            return None
        return round(1.0 / safe_probability, DEFAULT_FAIR_ODDS_DECIMALS)

    def _compute_edge_pct(
        self, *, market_best_odds: float | None, fair_odds: float | None
    ) -> float | None:
        if market_best_odds is None or fair_odds is None or fair_odds <= 0:
            return None
        return round(((market_best_odds / fair_odds) - 1.0) * 100.0, DEFAULT_EDGE_DECIMALS)

    def _compute_data_quality_score(
        self, snapshot: FeatureSnapshot, audit: dict[str, list[str]]
    ) -> float:
        score = float(snapshot.completeness_score)
        score -= min(len(audit["missing_feature_groups"]) * 0.08, 0.32)
        score -= min(len(audit["data_warnings"]) * 0.03, 0.18)
        return round(self._clamp(score, 0.0, 1.0), 4)

    def _compute_confidence_score(
        self, *, data_quality_score: float, probability_vector: dict[str, float]
    ) -> float:
        ordered = sorted(probability_vector.values(), reverse=True)
        if len(ordered) >= 2:
            stability = ordered[0] - ordered[1]
        else:
            stability = abs((ordered[0] if ordered else 0.5) - 0.5) * 2
        confidence = (data_quality_score * 70.0) + (self._clamp(stability, 0.0, 1.0) * 30.0)
        return round(self._clamp(confidence, 0.0, 100.0), DEFAULT_CONFIDENCE_DECIMALS)

    def _load_active_model_versions(self, markets: list[str]) -> dict[str, ActiveModelVersion]:
        if not markets:
            return {}

        ranked = (
            select(
                ModelVersion.id.label("model_version_id"),
                ModelVersion.model_registry_id,
                ModelRegistry.market_code,
                ModelRegistry.code.label("model_code"),
                ModelRegistry.name.label("model_name"),
                ModelVersion.version,
                ModelVersion.config_json,
                func.row_number()
                .over(
                    partition_by=ModelRegistry.market_code,
                    order_by=(ModelVersion.created_at.desc(), ModelVersion.id.desc()),
                )
                .label("rn"),
            )
            .join(ModelRegistry, ModelRegistry.id == ModelVersion.model_registry_id)
            .where(
                ModelVersion.is_active.is_(True),
                ModelVersion.status == "active",
                ModelRegistry.market_code.in_(markets),
            )
            .subquery()
        )
        rows = self.db.execute(select(ranked).where(ranked.c.rn == 1)).mappings().all()
        return {
            row["market_code"]: ActiveModelVersion(
                model_version_id=row["model_version_id"],
                model_registry_id=row["model_registry_id"],
                market_code=row["market_code"],
                model_code=row["model_code"],
                model_name=row["model_name"],
                version=row["version"],
                config_json=dict(row["config_json"] or {}),
            )
            for row in rows
        }

    def _load_latest_feature_snapshots(
        self,
        *,
        match_ids: list[UUID],
        prediction_horizon: str,
        feature_set_version: str | None,
    ) -> dict[UUID, FeatureSnapshot]:
        if not match_ids:
            return {}

        ranked = select(
            FeatureSnapshot.id,
            FeatureSnapshot.match_id,
            func.row_number()
            .over(
                partition_by=FeatureSnapshot.match_id,
                order_by=(
                    FeatureSnapshot.as_of_ts.desc(),
                    FeatureSnapshot.created_at.desc(),
                    FeatureSnapshot.id.desc(),
                ),
            )
            .label("rn"),
        ).where(
            FeatureSnapshot.match_id.in_(match_ids),
            FeatureSnapshot.prediction_horizon == prediction_horizon,
        )
        if feature_set_version is not None:
            ranked = ranked.where(FeatureSnapshot.feature_set_version == feature_set_version)

        ranked_subquery = ranked.subquery()
        rows = (
            self.db.execute(
                select(FeatureSnapshot)
                .join(ranked_subquery, ranked_subquery.c.id == FeatureSnapshot.id)
                .where(ranked_subquery.c.rn == 1)
            )
            .scalars()
            .all()
        )
        return {row.match_id: row for row in rows}

    def _load_existing_prediction_keys(
        self,
        *,
        match_ids: list[UUID],
        prediction_horizon: str,
        active_model_versions: dict[str, ActiveModelVersion],
    ) -> set[tuple[UUID, UUID, UUID, str, str]]:
        if not match_ids or not active_model_versions:
            return set()

        statement = select(
            Prediction.match_id,
            Prediction.feature_snapshot_id,
            Prediction.model_version_id,
            Prediction.market_code,
            Prediction.prediction_horizon,
        ).where(
            Prediction.match_id.in_(match_ids),
            Prediction.prediction_horizon == prediction_horizon,
            Prediction.model_version_id.in_(
                [item.model_version_id for item in active_model_versions.values()]
            ),
            Prediction.market_code.in_(list(active_model_versions.keys())),
        )
        return {
            (
                row.match_id,
                row.feature_snapshot_id,
                row.model_version_id,
                row.market_code,
                row.prediction_horizon,
            )
            for row in self.db.execute(statement)
        }

    def _load_best_odds_map(self, *, match_ids: list[UUID]) -> dict[tuple[UUID, str, str], float]:
        if not match_ids:
            return {}

        line_key = func.coalesce(Odds.line_value, cast(literal_column("-999.000"), Numeric(10, 3)))
        latest_rows = (
            select(
                Odds.id,
                Odds.match_id,
                Market.code.label("odds_market_code"),
                Odds.selection_code,
                Odds.line_value,
                Odds.odds_value,
                Odds.snapshot_timestamp,
                Odds.bookmaker_id,
                Odds.provider_id,
                func.row_number()
                .over(
                    partition_by=(
                        Odds.match_id,
                        Odds.provider_id,
                        Odds.bookmaker_id,
                        Odds.market_id,
                        Odds.selection_code,
                        line_key,
                    ),
                    order_by=(
                        Odds.snapshot_timestamp.desc(),
                        Odds.ingested_at.desc(),
                        Odds.id.desc(),
                    ),
                )
                .label("latest_rank"),
            )
            .join(Market, Market.id == Odds.market_id)
            .where(
                Odds.match_id.in_(match_ids),
                or_(
                    Market.code == "1X2",
                    Market.code == "BTTS",
                    Market.code == "OU",
                ),
            )
            .subquery()
        )

        best_rows = (
            select(
                latest_rows,
                func.row_number()
                .over(
                    partition_by=(
                        latest_rows.c.match_id,
                        latest_rows.c.odds_market_code,
                        latest_rows.c.selection_code,
                        func.coalesce(
                            latest_rows.c.line_value,
                            cast(literal_column("-999.000"), Numeric(10, 3)),
                        ),
                    ),
                    order_by=(
                        latest_rows.c.odds_value.desc(),
                        latest_rows.c.snapshot_timestamp.desc(),
                        latest_rows.c.id.desc(),
                    ),
                )
                .label("best_rank"),
            )
            .where(latest_rows.c.latest_rank == 1)
            .subquery()
        )

        rows = self.db.execute(select(best_rows).where(best_rows.c.best_rank == 1)).mappings().all()
        best_odds_map: dict[tuple[UUID, str, str], float] = {}
        for row in rows:
            prediction_market_code = self._map_odds_market_to_prediction_market(
                odds_market_code=row["odds_market_code"],
                line_value=row["line_value"],
            )
            if prediction_market_code is None:
                continue
            key = (row["match_id"], prediction_market_code, row["selection_code"])
            best_odds_map[key] = round(float(row["odds_value"]), DEFAULT_FAIR_ODDS_DECIMALS)
        return best_odds_map

    def _map_odds_market_to_prediction_market(
        self, *, odds_market_code: str, line_value: Decimal | None
    ) -> str | None:
        if odds_market_code == "1X2":
            return "1X2"
        if odds_market_code == "BTTS":
            return "BTTS"
        if odds_market_code == "OU" and line_value == OU25_LINE_VALUE:
            return "OU25"
        return None

    def _list_target_matches(
        self,
        *,
        match_id: UUID | None,
        competition_id: UUID | None,
        season_id: UUID | None,
        season: str | None,
        future_only: bool,
        limit: int | None,
    ) -> list[Match]:
        statement: Select[tuple[Match]] = select(Match)
        if match_id is not None:
            statement = statement.where(Match.id == match_id)
        if competition_id is not None:
            statement = statement.where(Match.competition_id == competition_id)
        if season_id is not None:
            statement = statement.where(Match.season_id == season_id)
        if season is not None:
            statement = statement.where(Match.season == season)
        if future_only and match_id is None:
            now_utc = datetime.now(UTC)
            statement = statement.where(
                Match.match_date >= now_utc, Match.status.in_(("scheduled", "live"))
            )

        statement = statement.order_by(Match.match_date.asc(), Match.id.asc())
        if limit is not None:
            statement = statement.limit(limit)
        return list(self.db.execute(statement).scalars().all())

    def _normalize_markets(self, markets: Iterable[str] | None) -> list[str]:
        if markets is None:
            return list(SUPPORTED_MARKET_CODES)
        normalized: list[str] = []
        for market in markets:
            value = market.strip().upper()
            if value not in SUPPORTED_MARKET_CODES:
                raise ValueError(f"unsupported_market_code:{value}")
            if value not in normalized:
                normalized.append(value)
        if not normalized:
            raise ValueError("no_market_codes_provided")
        return normalized

    def _extract_feature_audit(self, features: dict[str, Any]) -> dict[str, list[str]]:
        raw = features.get("feature_audit", {}) if isinstance(features, dict) else {}
        if not isinstance(raw, dict):
            raw = {}
        return {
            "missing_fields": list(raw.get("missing_fields", [])),
            "missing_feature_groups": list(raw.get("missing_feature_groups", [])),
            "data_warnings": list(raw.get("data_warnings", [])),
        }

    def _feature_float(
        self, features: dict[str, Any], key: str, default: float | None = None
    ) -> float:
        value = features.get(key)
        if value is None:
            if default is None:
                raise ValueError(f"required_feature_missing:{key}")
            return float(default)
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"feature_not_numeric:{key}") from exc

    def _stabilize_probabilities(
        self, *, market_code: str, raw_vector: dict[str, float]
    ) -> dict[str, float]:
        epsilon = 1e-6
        normalized = {
            selection_code: max(float(raw_vector.get(selection_code, 0.0)), epsilon)
            for selection_code in MARKET_SELECTION_CODES[market_code]
        }
        total = sum(normalized.values())
        if total <= 0:
            raise ValueError("invalid_probability_vector")
        return {
            selection_code: round(normalized[selection_code] / total, DEFAULT_PROBABILITY_DECIMALS)
            for selection_code in MARKET_SELECTION_CODES[market_code]
        }

    def _serialize_selection(self, row: PredictionSelection) -> dict[str, Any]:
        return {
            "id": row.id,
            "selection_code": row.selection_code,
            "predicted_probability": row.predicted_probability,
            "fair_odds": row.fair_odds,
            "market_best_odds": row.market_best_odds,
            "edge_pct": row.edge_pct,
            "confidence_score": row.confidence_score,
            "created_at": row.created_at,
        }

    @staticmethod
    def _safe_divide(value: float, denominator: float) -> float:
        if denominator == 0:
            return 0.0
        return value / denominator

    @staticmethod
    def _mean_available(*values: float) -> float:
        available = [value for value in values if value is not None]
        if not available:
            return 0.0
        return sum(available) / len(available)

    @staticmethod
    def _clamp(value: float, minimum: float, maximum: float) -> float:
        return max(min(value, maximum), minimum)


def seed_prediction_model_registry(db: Session) -> dict[str, int]:
    created_registry = 0
    created_versions = 0
    updated_versions = 0

    for item in PREDICTION_MODEL_SEEDS:
        registry = db.execute(
            select(ModelRegistry).where(ModelRegistry.code == item["code"])
        ).scalar_one_or_none()
        if registry is None:
            registry = ModelRegistry(
                code=item["code"],
                name=item["name"],
                market_code=item["market_code"],
                task_type=item["task_type"],
            )
            db.add(registry)
            db.flush()
            created_registry += 1

        version = db.execute(
            select(ModelVersion).where(
                ModelVersion.model_registry_id == registry.id,
                ModelVersion.version == item["version"],
            )
        ).scalar_one_or_none()

        active_versions = (
            db.execute(
                select(ModelVersion).where(
                    ModelVersion.model_registry_id == registry.id,
                    ModelVersion.is_active.is_(True),
                    ModelVersion.id != (version.id if version is not None else None),
                )
            )
            .scalars()
            .all()
        )
        for active in active_versions:
            active.is_active = False
            if active.status == "active":
                active.status = "archived"
            updated_versions += 1

        if version is None:
            version = ModelVersion(
                model_registry_id=registry.id,
                version=item["version"],
                status=item["status"],
                is_active=item["is_active"],
                artifact_path=item["artifact_path"],
                config_json=item["config_json"],
            )
            db.add(version)
            created_versions += 1
        else:
            version.status = item["status"]
            version.is_active = item["is_active"]
            version.artifact_path = item["artifact_path"]
            version.config_json = item["config_json"]
            updated_versions += 1

    db.commit()
    return {
        "model_registry_created": created_registry,
        "model_versions_created": created_versions,
        "model_versions_updated": updated_versions,
    }


def load_prediction_rows(
    db: Session,
    *,
    match_id: UUID | None = None,
    market_code: str | None = None,
    prediction_horizon: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    statement = (
        select(
            Prediction,
            ModelVersion.version.label("model_version"),
            ModelRegistry.code.label("model_code"),
            ModelRegistry.name.label("model_name"),
        )
        .join(ModelVersion, ModelVersion.id == Prediction.model_version_id)
        .join(ModelRegistry, ModelRegistry.id == ModelVersion.model_registry_id)
    )
    if match_id is not None:
        statement = statement.where(Prediction.match_id == match_id)
    if market_code is not None:
        statement = statement.where(Prediction.market_code == market_code)
    if prediction_horizon is not None:
        statement = statement.where(Prediction.prediction_horizon == prediction_horizon)

    statement = (
        statement.order_by(
            Prediction.as_of_ts.desc(), Prediction.created_at.desc(), Prediction.id.desc()
        )
        .offset(offset)
        .limit(limit)
    )
    rows = db.execute(statement).all()
    if not rows:
        return []

    prediction_ids = [prediction.id for prediction, _, _, _ in rows]
    selection_rows = (
        db.execute(
            select(PredictionSelection)
            .where(PredictionSelection.prediction_id.in_(prediction_ids))
            .order_by(
                PredictionSelection.prediction_id.asc(), PredictionSelection.selection_code.asc()
            )
        )
        .scalars()
        .all()
    )

    selections_by_prediction: dict[UUID, list[PredictionSelection]] = defaultdict(list)
    for row in selection_rows:
        selections_by_prediction[row.prediction_id].append(row)

    payload: list[dict[str, Any]] = []
    for prediction, model_version, model_code, model_name in rows:
        payload.append(
            {
                "id": prediction.id,
                "match_id": prediction.match_id,
                "feature_snapshot_id": prediction.feature_snapshot_id,
                "model_version_id": prediction.model_version_id,
                "model_version": model_version,
                "model_code": model_code,
                "model_name": model_name,
                "market_code": prediction.market_code,
                "prediction_horizon": prediction.prediction_horizon,
                "as_of_ts": prediction.as_of_ts,
                "data_quality_score": prediction.data_quality_score,
                "created_at": prediction.created_at,
                "selections": [
                    {
                        "id": selection.id,
                        "selection_code": selection.selection_code,
                        "predicted_probability": selection.predicted_probability,
                        "fair_odds": selection.fair_odds,
                        "market_best_odds": selection.market_best_odds,
                        "edge_pct": selection.edge_pct,
                        "confidence_score": selection.confidence_score,
                        "created_at": selection.created_at,
                    }
                    for selection in selections_by_prediction.get(prediction.id, [])
                ],
            }
        )
    return payload
