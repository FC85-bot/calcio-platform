from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.models.feature_snapshot import FeatureSnapshot
from app.models.model_registry import ModelRegistry
from app.models.model_version import ModelVersion
from app.schemas.prediction import PredictionSelectionRowRead
from app.services.odds_query_service import OddsQueryService
from app.services.prediction_service import load_prediction_rows
from app.services.query_service import QueryService

logger = get_logger(__name__)

REPORT_VERSION = "sprint13_match_report_v1"
DEFAULT_PREDICTION_HORIZON = "pre_match"
REPORT_MARKETS: tuple[str, ...] = ("1X2", "OU25", "BTTS")
EXPECTED_SELECTIONS_BY_MARKET: dict[str, set[str]] = {
    "1X2": {"HOME", "DRAW", "AWAY"},
    "OU25": {"OVER", "UNDER"},
    "BTTS": {"YES", "NO"},
}
ODDS_MARKET_CODE_BY_REPORT_MARKET: dict[str, str] = {
    "1X2": "1X2",
    "OU25": "OU",
    "BTTS": "BTTS",
}
LOW_COMPLETENESS_THRESHOLD = 0.70
STALE_ODDS_THRESHOLD = timedelta(hours=24)
FEATURE_AUDIT_KEY = "feature_audit"


class MatchReportService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.query_service = QueryService(db)
        self.odds_service = OddsQueryService(db)

    def build_match_report(
        self,
        *,
        match_id: UUID,
        prediction_horizon: str = DEFAULT_PREDICTION_HORIZON,
        form_last_n: int = 5,
    ) -> dict[str, Any]:
        generated_at = datetime.now(UTC)

        match = self.query_service.get_match_by_id(match_id, include_latest_odds=False)
        if match is None:
            raise ValueError("match_not_found")

        if not match.get("competition") or not match.get("home_team") or not match.get("away_team"):
            raise ValueError("match_context_incomplete")

        home_team = self.query_service.get_team_detail(
            match["home_team_id"],
            season_id=match.get("season_id"),
            season=match.get("season"),
            form_last_n=form_last_n,
        )
        away_team = self.query_service.get_team_detail(
            match["away_team_id"],
            season_id=match.get("season_id"),
            season=match.get("season"),
            form_last_n=form_last_n,
        )

        if home_team is None or away_team is None:
            raise ValueError("team_reference_missing")

        standings_payload = self.query_service.get_standings(
            competition_id=match["competition_id"],
            season_id=match.get("season_id"),
            season=match.get("season"),
        )

        latest_feature_snapshot = self._load_latest_feature_snapshot(
            match_id=match_id,
            prediction_horizon=prediction_horizon,
        )
        active_model_versions = self._load_active_model_versions()
        prediction_rows = load_prediction_rows(
            self.db,
            match_id=match_id,
            prediction_horizon=prediction_horizon,
            limit=50,
        )

        latest_odds_rows = self.odds_service.get_latest_odds(match_id)
        best_odds_rows = self.odds_service.get_best_odds(match_id)
        opening_odds_rows = self.odds_service.get_opening_odds(match_id)

        warnings: list[dict[str, Any]] = []
        self._add_match_context_warnings(match=match, warnings=warnings)

        home_team_block = self._build_team_block(
            team_payload=home_team,
            venue_split_key="home",
            venue_split_label="home",
            warnings=warnings,
            warning_prefix="home",
            expected_form_length=form_last_n,
        )
        away_team_block = self._build_team_block(
            team_payload=away_team,
            venue_split_key="away",
            venue_split_label="away",
            warnings=warnings,
            warning_prefix="away",
            expected_form_length=form_last_n,
        )

        standings_context = self._build_standings_context(
            standings_payload=standings_payload,
            home_team_id=match["home_team_id"],
            away_team_id=match["away_team_id"],
            warnings=warnings,
        )

        odds_blocks = self._build_odds_blocks(
            latest_rows=latest_odds_rows,
            best_rows=best_odds_rows,
            opening_rows=opening_odds_rows,
            generated_at=generated_at,
            match_status=match["status"],
            warnings=warnings,
        )

        prediction_blocks = self._build_prediction_blocks(
            prediction_rows=prediction_rows,
            active_model_versions=active_model_versions,
            warnings=warnings,
        )

        if latest_feature_snapshot is None:
            self._add_warning(
                warnings,
                code="missing_feature_snapshot",
                section="predictions",
                detail="No feature snapshot available for the requested prediction horizon.",
            )
        else:
            feature_audit = self._extract_feature_audit(latest_feature_snapshot.features_json)
            if latest_feature_snapshot.completeness_score < LOW_COMPLETENESS_THRESHOLD:
                self._add_warning(
                    warnings,
                    code="low_feature_completeness",
                    section="predictions",
                    detail=(
                        f"Feature completeness {latest_feature_snapshot.completeness_score:.2f} is below the "
                        f"report threshold {LOW_COMPLETENESS_THRESHOLD:.2f}."
                    ),
                )

            for data_warning in feature_audit["data_warnings"]:
                normalized = self._normalize_feature_warning_code(data_warning)
                self._add_warning(
                    warnings,
                    code=normalized,
                    section="predictions",
                    detail=f"Feature audit warning: {data_warning}",
                )

        report = {
            "context": {
                "match_id": match["id"],
                "competition": match["competition"],
                "season": match.get("season_detail"),
                "season_label": match.get("season"),
                "match_date": match["match_date"],
                "home_team": match["home_team"],
                "away_team": match["away_team"],
                "status": match["status"],
                "score": match.get("score"),
            },
            "home_team": home_team_block,
            "away_team": away_team_block,
            "standings_context": standings_context,
            "odds": odds_blocks,
            "predictions": prediction_blocks,
            "warnings": warnings,
            "generated_at": generated_at,
            "report_version": REPORT_VERSION,
            "feature_set_version": latest_feature_snapshot.feature_set_version
            if latest_feature_snapshot
            else None,
        }

        logger.info(
            "match_report_generated",
            extra={
                "match_id": str(match_id),
                "report_version": REPORT_VERSION,
                "generated_at": generated_at.isoformat(),
                "warnings_count": len(warnings),
                "odds_availability": {
                    block["market_code"]: block["available"] for block in odds_blocks
                },
                "prediction_availability": {
                    block["market_code"]: block["available"] for block in prediction_blocks
                },
            },
        )
        return report

    def _build_team_block(
        self,
        *,
        team_payload: dict[str, Any],
        venue_split_key: str,
        venue_split_label: str,
        warnings: list[dict[str, Any]],
        warning_prefix: str,
        expected_form_length: int,
    ) -> dict[str, Any]:
        stats = team_payload.get("stats")
        form = team_payload.get("form")
        streak = team_payload.get("streak")

        if stats is None:
            self._add_warning(
                warnings,
                code=f"missing_team_stats_{warning_prefix}",
                section="team_form",
                detail=f"Missing base stats for {warning_prefix} team.",
            )

        if form is None or len(form.get("results", [])) == 0:
            self._add_warning(
                warnings,
                code=f"missing_form_{warning_prefix}",
                section="team_form",
                detail=f"Missing recent form for {warning_prefix} team.",
            )
        elif len(form.get("results", [])) < expected_form_length:
            self._add_warning(
                warnings,
                code=f"insufficient_form_{warning_prefix}",
                section="team_form",
                detail=(
                    f"Recent form for {warning_prefix} team contains {len(form.get('results', []))} "
                    f"results, expected {expected_form_length}."
                ),
            )

        venue_stats = None
        if stats is not None:
            venue_stats = stats.get(venue_split_key)

        return {
            "team": {
                "id": team_payload["id"],
                "name": team_payload["name"],
            },
            "last_results": form.get("results", []) if form is not None else [],
            "stats": stats,
            "streak": streak,
            "venue_split_label": venue_split_label,
            "venue_split": venue_stats,
        }

    def _build_standings_context(
        self,
        *,
        standings_payload: dict[str, Any] | None,
        home_team_id: UUID,
        away_team_id: UUID,
        warnings: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if standings_payload is None:
            self._add_warning(
                warnings,
                code="missing_standings",
                section="standings_context",
                detail="Standings are not available for this match.",
            )
            return {
                "available": False,
                "source": None,
                "snapshot_date": None,
                "home_team": None,
                "away_team": None,
            }

        rows = standings_payload.get("standings", []) or []
        home_row = next(
            (row for row in rows if row.get("team", {}).get("id") == home_team_id), None
        )
        away_row = next(
            (row for row in rows if row.get("team", {}).get("id") == away_team_id), None
        )

        if self._is_unreliable_standings_payload(standings_payload, home_row, away_row):
            self._add_warning(
                warnings,
                code="missing_standings",
                section="standings_context",
                detail="Standings snapshot is missing or not reliable enough for the selected teams.",
            )
            return {
                "available": False,
                "source": standings_payload.get("source"),
                "snapshot_date": standings_payload.get("snapshot_date"),
                "home_team": None,
                "away_team": None,
            }

        if home_row is None:
            self._add_warning(
                warnings,
                code="missing_standings_home",
                section="standings_context",
                detail="Home team standings row is missing.",
            )
        if away_row is None:
            self._add_warning(
                warnings,
                code="missing_standings_away",
                section="standings_context",
                detail="Away team standings row is missing.",
            )

        return {
            "available": home_row is not None and away_row is not None,
            "source": standings_payload.get("source"),
            "snapshot_date": standings_payload.get("snapshot_date"),
            "home_team": self._serialize_standings_row(home_row),
            "away_team": self._serialize_standings_row(away_row),
        }

    def _build_odds_blocks(
        self,
        *,
        latest_rows: list[dict[str, Any]],
        best_rows: list[dict[str, Any]],
        opening_rows: list[dict[str, Any]],
        generated_at: datetime,
        match_status: str,
        warnings: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        blocks: list[dict[str, Any]] = []

        for market_code in REPORT_MARKETS:
            latest_market_rows = self._filter_market_odds_rows(latest_rows, market_code)
            best_market_rows = self._filter_market_odds_rows(best_rows, market_code)
            opening_market_rows = self._filter_market_odds_rows(opening_rows, market_code)

            latest_snapshot_timestamp = None
            if latest_market_rows:
                latest_snapshot_timestamp = max(
                    row["snapshot_timestamp"]
                    for row in latest_market_rows
                    if row.get("snapshot_timestamp") is not None
                )

            selection_coverage = {row["selection_code"] for row in best_market_rows}
            expected_coverage = EXPECTED_SELECTIONS_BY_MARKET[market_code]

            if not latest_market_rows and not best_market_rows and not opening_market_rows:
                self._add_warning(
                    warnings,
                    code=f"missing_odds_{market_code.lower()}",
                    section="odds",
                    detail=f"No odds available for market {market_code}.",
                )
            elif selection_coverage and selection_coverage != expected_coverage:
                self._add_warning(
                    warnings,
                    code=f"partial_odds_{market_code.lower()}",
                    section="odds",
                    detail=(
                        f"Odds for market {market_code} are partial: "
                        f"{sorted(selection_coverage)} available, expected {sorted(expected_coverage)}."
                    ),
                )
            elif best_market_rows and not selection_coverage:
                self._add_warning(
                    warnings,
                    code=f"partial_odds_{market_code.lower()}",
                    section="odds",
                    detail=f"Odds rows exist for market {market_code} but selection coverage cannot be derived.",
                )

            if (
                latest_snapshot_timestamp is not None
                and match_status in {"scheduled", "live"}
                and generated_at - self._ensure_utc(latest_snapshot_timestamp)
                > STALE_ODDS_THRESHOLD
            ):
                self._add_warning(
                    warnings,
                    code=f"stale_odds_{market_code.lower()}",
                    section="odds",
                    detail=(
                        f"Latest odds snapshot for market {market_code} is older than "
                        f"{int(STALE_ODDS_THRESHOLD.total_seconds() // 3600)} hours."
                    ),
                )

            blocks.append(
                {
                    "market_code": market_code,
                    "available": bool(
                        latest_market_rows or best_market_rows or opening_market_rows
                    ),
                    "latest_snapshot_timestamp": latest_snapshot_timestamp,
                    "latest": latest_market_rows,
                    "best": best_market_rows,
                    "opening": opening_market_rows,
                }
            )

        return blocks

    def _build_prediction_blocks(
        self,
        *,
        prediction_rows: list[dict[str, Any]],
        active_model_versions: dict[str, dict[str, Any]],
        warnings: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        first_prediction_by_market: dict[str, dict[str, Any]] = {}
        for row in prediction_rows:
            market_code = row["market_code"]
            if market_code not in first_prediction_by_market:
                first_prediction_by_market[market_code] = row

        blocks: list[dict[str, Any]] = []
        for market_code in REPORT_MARKETS:
            prediction = first_prediction_by_market.get(market_code)
            active_model = active_model_versions.get(market_code)

            if active_model is None:
                self._add_warning(
                    warnings,
                    code=f"missing_active_model_version_{market_code.lower()}",
                    section="predictions",
                    detail=f"No active model version registered for market {market_code}.",
                )

            if prediction is None:
                self._add_warning(
                    warnings,
                    code=f"missing_prediction_{market_code.lower()}",
                    section="predictions",
                    detail=f"No persisted prediction available for market {market_code}.",
                )
                blocks.append(
                    {
                        "market_code": market_code,
                        "available": False,
                        "prediction_id": None,
                        "feature_snapshot_id": None,
                        "feature_set_version": None,
                        "model_version_id": active_model["model_version_id"]
                        if active_model
                        else None,
                        "model_version": active_model["version"] if active_model else None,
                        "model_code": active_model["model_code"] if active_model else None,
                        "model_name": active_model["model_name"] if active_model else None,
                        "prediction_horizon": None,
                        "as_of_ts": None,
                        "data_quality_score": None,
                        "selections": [],
                    }
                )
                continue

            selections = prediction.get("selections", [])
            selection_codes = {row["selection_code"] for row in selections}
            expected_coverage = EXPECTED_SELECTIONS_BY_MARKET[market_code]
            if selection_codes != expected_coverage:
                self._add_warning(
                    warnings,
                    code=f"partial_prediction_{market_code.lower()}",
                    section="predictions",
                    detail=(
                        f"Prediction for market {market_code} is partial: "
                        f"{sorted(selection_codes)} available, expected {sorted(expected_coverage)}."
                    ),
                )

            blocks.append(
                {
                    "market_code": market_code,
                    "available": True,
                    "prediction_id": prediction["id"],
                    "feature_snapshot_id": prediction["feature_snapshot_id"],
                    "feature_set_version": None,
                    "model_version_id": prediction["model_version_id"],
                    "model_version": prediction["model_version"],
                    "model_code": prediction["model_code"],
                    "model_name": prediction["model_name"],
                    "prediction_horizon": prediction["prediction_horizon"],
                    "as_of_ts": prediction["as_of_ts"],
                    "data_quality_score": prediction["data_quality_score"],
                    "selections": [
                        PredictionSelectionRowRead.model_validate(item).model_dump()
                        for item in selections
                    ],
                }
            )

        feature_set_versions_by_snapshot = self._load_feature_set_versions_by_snapshot(
            [
                block["feature_snapshot_id"]
                for block in blocks
                if block.get("feature_snapshot_id") is not None
            ]
        )
        for block in blocks:
            feature_snapshot_id = block.get("feature_snapshot_id")
            if feature_snapshot_id is not None:
                block["feature_set_version"] = feature_set_versions_by_snapshot.get(
                    feature_snapshot_id
                )

        return blocks

    def _load_latest_feature_snapshot(
        self, *, match_id: UUID, prediction_horizon: str
    ) -> FeatureSnapshot | None:
        statement = (
            select(FeatureSnapshot)
            .where(
                FeatureSnapshot.match_id == match_id,
                FeatureSnapshot.prediction_horizon == prediction_horizon,
            )
            .order_by(
                FeatureSnapshot.as_of_ts.desc(),
                FeatureSnapshot.created_at.desc(),
                FeatureSnapshot.id.desc(),
            )
            .limit(1)
        )
        return self.db.execute(statement).scalar_one_or_none()

    def _load_feature_set_versions_by_snapshot(self, snapshot_ids: list[UUID]) -> dict[UUID, str]:
        if not snapshot_ids:
            return {}
        statement = select(FeatureSnapshot.id, FeatureSnapshot.feature_set_version).where(
            FeatureSnapshot.id.in_(snapshot_ids)
        )
        return {row.id: row.feature_set_version for row in self.db.execute(statement)}

    def _load_active_model_versions(self) -> dict[str, dict[str, Any]]:
        statement = (
            select(
                ModelVersion.id.label("model_version_id"),
                ModelVersion.version.label("version"),
                ModelRegistry.code.label("model_code"),
                ModelRegistry.name.label("model_name"),
                ModelRegistry.market_code.label("market_code"),
            )
            .join(ModelRegistry, ModelRegistry.id == ModelVersion.model_registry_id)
            .where(ModelVersion.is_active.is_(True))
        )
        return {
            row.market_code: {
                "model_version_id": row.model_version_id,
                "version": row.version,
                "model_code": row.model_code,
                "model_name": row.model_name,
            }
            for row in self.db.execute(statement)
        }

    def _filter_market_odds_rows(
        self, rows: list[dict[str, Any]], report_market_code: str
    ) -> list[dict[str, Any]]:
        odds_market_code = ODDS_MARKET_CODE_BY_REPORT_MARKET[report_market_code]
        if report_market_code == "OU25":
            return [
                row
                for row in rows
                if row.get("line_value") == 2.5
                and row.get("market_code") in {"OU25", odds_market_code}
            ]
        return [row for row in rows if row.get("market_code") == odds_market_code]

    def _add_match_context_warnings(
        self, *, match: dict[str, Any], warnings: list[dict[str, Any]]
    ) -> None:
        required_keys = {
            "competition_id": match.get("competition_id"),
            "match_date": match.get("match_date"),
            "home_team_id": match.get("home_team_id"),
            "away_team_id": match.get("away_team_id"),
            "season": match.get("season"),
            "status": match.get("status"),
        }
        missing_fields = sorted(key for key, value in required_keys.items() if value in (None, ""))
        if missing_fields:
            self._add_warning(
                warnings,
                code="match_context_incomplete",
                section="context",
                detail=f"Missing required match fields: {', '.join(missing_fields)}.",
            )

    def _serialize_standings_row(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "position": row["position"],
            "points": row["points"],
            "goal_difference": row["goal_difference"],
            "played": row["played"],
        }

    def _is_unreliable_standings_payload(
        self,
        standings_payload: dict[str, Any],
        home_row: dict[str, Any] | None,
        away_row: dict[str, Any] | None,
    ) -> bool:
        if home_row is None and away_row is None:
            return True
        if standings_payload.get("source") == "computed_fallback":
            candidate_rows = [row for row in (home_row, away_row) if row is not None]
            if candidate_rows and all(
                (row.get("played", 0) == 0 and row.get("points", 0) == 0) for row in candidate_rows
            ):
                return True
        return False

    def _extract_feature_audit(self, features_json: dict[str, Any] | None) -> dict[str, list[str]]:
        if not isinstance(features_json, dict):
            return {
                "missing_fields": [],
                "missing_feature_groups": [],
                "data_warnings": [],
            }
        raw_audit = features_json.get(FEATURE_AUDIT_KEY, {})
        if not isinstance(raw_audit, dict):
            raw_audit = {}
        return {
            "missing_fields": list(raw_audit.get("missing_fields", [])),
            "missing_feature_groups": list(raw_audit.get("missing_feature_groups", [])),
            "data_warnings": list(raw_audit.get("data_warnings", [])),
        }

    def _normalize_feature_warning_code(self, warning_code: str) -> str:
        if "standings" in warning_code:
            return "missing_standings"
        if "insufficient_history" in warning_code:
            if warning_code.startswith("home_team"):
                return "insufficient_form_home"
            if warning_code.startswith("away_team"):
                return "insufficient_form_away"
            return "insufficient_form"
        if warning_code == "season_id_missing_on_match":
            return "missing_standings"
        return warning_code.replace(":", "_")

    def _add_warning(
        self,
        warnings: list[dict[str, Any]],
        *,
        code: str,
        section: str,
        detail: str,
        severity: str = "warning",
    ) -> None:
        exists = any(item["code"] == code and item["section"] == section for item in warnings)
        if exists:
            return
        warnings.append(
            {
                "code": code,
                "section": section,
                "severity": severity,
                "detail": detail,
            }
        )

    def _ensure_utc(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
