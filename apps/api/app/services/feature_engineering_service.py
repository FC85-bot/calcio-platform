from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.models.feature_snapshot import FeatureSnapshot
from app.models.match import Match
from app.models.standings_snapshot import StandingsSnapshot

logger = get_logger(__name__)

DEFAULT_FEATURE_SET_VERSION = "sprint11_mvp_v1"
SUPPORTED_PREDICTION_HORIZONS = ("pre_match",)

OVERALL_GROUP_WEIGHT = 10
VENUE_GROUP_WEIGHT = 3
STANDINGS_GROUP_WEIGHT = 3
EXPECTED_CORE_FEATURE_COUNT = 26
EXPECTED_TOTAL_FEATURE_COUNT_WITH_STANDINGS = 32
LAST_5_SAMPLE_SIZE = 5
FEATURE_AUDIT_KEY = "feature_audit"


@dataclass(frozen=True)
class TargetMatchContext:
    match_id: UUID
    competition_id: UUID
    season_id: UUID | None
    season_name: str
    home_team_id: UUID
    away_team_id: UUID
    match_date: datetime
    as_of_ts: datetime
    prediction_horizon: str
    feature_set_version: str

    @property
    def scope_key(self) -> tuple[UUID, str]:
        return (self.competition_id, self.season_name)

    @property
    def standings_scope_key(self) -> tuple[UUID, UUID] | None:
        if self.season_id is None:
            return None
        return (self.competition_id, self.season_id)

    @property
    def as_of_date(self) -> date:
        return self.as_of_ts.date()


@dataclass(frozen=True)
class TeamPerspectiveMatch:
    match_id: UUID
    match_date: datetime
    team_id: UUID
    venue: str
    goals_scored: int
    goals_conceded: int
    points: int
    win: int
    draw: int
    loss: int
    clean_sheet: int
    failed_to_score: int


class FeatureEngineeringService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def build_feature_snapshot_for_match(
        self,
        *,
        match_id: UUID,
        as_of_ts: datetime | None = None,
        prediction_horizon: str = "pre_match",
        feature_set_version: str = DEFAULT_FEATURE_SET_VERSION,
        persist: bool = True,
    ) -> dict[str, Any]:
        match = self.db.get(Match, match_id)
        if match is None:
            raise ValueError(f"Match not found: {match_id}")

        self._validate_prediction_horizon(prediction_horizon)

        try:
            context = self._build_target_context(
                match=match,
                as_of_ts=as_of_ts,
                prediction_horizon=prediction_horizon,
                feature_set_version=feature_set_version,
            )
        except ValueError as exc:
            result = self._build_skipped_result(
                match_id=match.id,
                as_of_ts=self._safe_ensure_utc(as_of_ts or match.match_date),
                prediction_horizon=prediction_horizon,
                feature_set_version=feature_set_version,
                warning=str(exc),
            )
            self._log_skipped_result(result)
            return result

        support_data = self._load_support_data([context])
        existing_keys = self._load_existing_snapshot_keys([context])
        return self._process_target(
            context=context,
            support_data=support_data,
            existing_keys=existing_keys,
            persist=persist,
        )

    def build_feature_snapshots(
        self,
        *,
        match_id: UUID | None = None,
        competition_id: UUID | None = None,
        season_id: UUID | None = None,
        season: str | None = None,
        prediction_horizon: str = "pre_match",
        feature_set_version: str = DEFAULT_FEATURE_SET_VERSION,
        future_only: bool = True,
        limit: int | None = None,
        persist: bool = True,
    ) -> dict[str, Any]:
        self._validate_prediction_horizon(prediction_horizon)

        matches = self._list_target_matches(
            match_id=match_id,
            competition_id=competition_id,
            season_id=season_id,
            season=season,
            future_only=future_only,
            limit=limit,
        )

        valid_contexts: list[TargetMatchContext] = []
        precheck_results: list[dict[str, Any]] = []

        for match in matches:
            try:
                valid_contexts.append(
                    self._build_target_context(
                        match=match,
                        as_of_ts=None,
                        prediction_horizon=prediction_horizon,
                        feature_set_version=feature_set_version,
                    )
                )
            except ValueError as exc:
                result = self._build_skipped_result(
                    match_id=match.id,
                    as_of_ts=self._safe_ensure_utc(match.match_date),
                    prediction_horizon=prediction_horizon,
                    feature_set_version=feature_set_version,
                    warning=str(exc),
                )
                self._log_skipped_result(result)
                precheck_results.append(result)

        support_data = self._load_support_data(valid_contexts)
        existing_keys = self._load_existing_snapshot_keys(valid_contexts)

        results = list(precheck_results)
        created = 0
        skipped = len(precheck_results)
        errors = 0
        created_with_warnings = 0
        warning_counts: Counter[str] = Counter()
        error_counts: Counter[str] = Counter()

        for item in precheck_results:
            if item.get("warning"):
                warning_counts[item["warning"]] += 1

        for context in valid_contexts:
            result = self._process_target(
                context=context,
                support_data=support_data,
                existing_keys=existing_keys,
                persist=persist,
            )
            results.append(result)

            status = result["status"]
            if status == "created":
                created += 1
                if result.get("data_warnings"):
                    created_with_warnings += 1
            elif status == "skipped":
                skipped += 1
            else:
                errors += 1

            if result.get("warning"):
                warning_counts[result["warning"]] += 1
            if result.get("error"):
                error_counts[result["error"]] += 1

        summary = {
            "warning_counts": dict(sorted(warning_counts.items())),
            "error_counts": dict(sorted(error_counts.items())),
            "created_with_warnings": created_with_warnings,
        }

        logger.info(
            "feature_snapshot_batch_completed",
            extra={
                "target_count": len(matches),
                "created_count": created,
                "skipped_count": skipped,
                "error_count": errors,
                "created_with_warnings_count": created_with_warnings,
                "warning_counts": summary["warning_counts"],
                "error_counts": summary["error_counts"],
                "feature_set_version": feature_set_version,
                "prediction_horizon": prediction_horizon,
            },
        )

        return {
            "target_count": len(matches),
            "created": created,
            "skipped": skipped,
            "errors": errors,
            "summary": summary,
            "results": results,
        }

    def _process_target(
        self,
        *,
        context: TargetMatchContext,
        support_data: dict[str, Any],
        existing_keys: set[tuple[UUID, datetime, str, str]],
        persist: bool,
    ) -> dict[str, Any]:
        log_extra = {
            "match_id": str(context.match_id),
            "as_of_ts": context.as_of_ts.isoformat(),
            "feature_set_version": context.feature_set_version,
            "prediction_horizon": context.prediction_horizon,
        }

        snapshot_key = (
            context.match_id,
            context.as_of_ts,
            context.prediction_horizon,
            context.feature_set_version,
        )
        if snapshot_key in existing_keys:
            result = self._build_skipped_result(
                match_id=context.match_id,
                as_of_ts=context.as_of_ts,
                prediction_horizon=context.prediction_horizon,
                feature_set_version=context.feature_set_version,
                warning="duplicate_snapshot",
            )
            self._log_skipped_result(result)
            return result

        try:
            payload = self._build_snapshot_payload(context=context, support_data=support_data)
            audit = self._extract_feature_audit(payload["features_json"])
        except ValueError as exc:
            result = self._build_skipped_result(
                match_id=context.match_id,
                as_of_ts=context.as_of_ts,
                prediction_horizon=context.prediction_horizon,
                feature_set_version=context.feature_set_version,
                warning=str(exc),
            )
            self._log_skipped_result(result)
            return result
        except Exception as exc:  # noqa: BLE001
            self.db.rollback()
            logger.exception(
                "feature_snapshot_failed",
                extra={**log_extra, "status": "error", "error": str(exc)},
            )
            return {
                "match_id": context.match_id,
                "as_of_ts": context.as_of_ts,
                "feature_set_version": context.feature_set_version,
                "prediction_horizon": context.prediction_horizon,
                "status": "error",
                "completeness_score": None,
                "missing_fields": [],
                "missing_feature_groups": [],
                "data_warnings": [],
                "error": str(exc),
            }

        if not persist:
            logger.info(
                "feature_snapshot_built_preview",
                extra={
                    **log_extra,
                    "status": "created",
                    "completeness_score": payload["completeness_score"],
                    "missing_feature_groups": audit["missing_feature_groups"],
                    "data_warnings": audit["data_warnings"],
                },
            )
            return {
                "match_id": context.match_id,
                "as_of_ts": context.as_of_ts,
                "feature_set_version": context.feature_set_version,
                "prediction_horizon": context.prediction_horizon,
                "status": "created",
                "completeness_score": payload["completeness_score"],
                "missing_fields": audit["missing_fields"],
                "missing_feature_groups": audit["missing_feature_groups"],
                "data_warnings": audit["data_warnings"],
                "snapshot": payload,
            }

        try:
            snapshot = FeatureSnapshot(**payload)
            self.db.add(snapshot)
            self.db.commit()
            self.db.refresh(snapshot)
            existing_keys.add(snapshot_key)
        except IntegrityError as exc:
            self.db.rollback()
            if self._is_duplicate_snapshot_integrity_error(exc):
                existing_keys.add(snapshot_key)
                result = self._build_skipped_result(
                    match_id=context.match_id,
                    as_of_ts=context.as_of_ts,
                    prediction_horizon=context.prediction_horizon,
                    feature_set_version=context.feature_set_version,
                    warning="duplicate_snapshot",
                )
                self._log_skipped_result(result)
                return result
            logger.exception(
                "feature_snapshot_failed",
                extra={**log_extra, "status": "error", "error": str(exc)},
            )
            return {
                "match_id": context.match_id,
                "as_of_ts": context.as_of_ts,
                "feature_set_version": context.feature_set_version,
                "prediction_horizon": context.prediction_horizon,
                "status": "error",
                "completeness_score": None,
                "missing_fields": audit["missing_fields"],
                "missing_feature_groups": audit["missing_feature_groups"],
                "data_warnings": audit["data_warnings"],
                "error": str(exc),
            }
        except Exception as exc:  # noqa: BLE001
            self.db.rollback()
            logger.exception(
                "feature_snapshot_failed",
                extra={**log_extra, "status": "error", "error": str(exc)},
            )
            return {
                "match_id": context.match_id,
                "as_of_ts": context.as_of_ts,
                "feature_set_version": context.feature_set_version,
                "prediction_horizon": context.prediction_horizon,
                "status": "error",
                "completeness_score": None,
                "missing_fields": audit["missing_fields"],
                "missing_feature_groups": audit["missing_feature_groups"],
                "data_warnings": audit["data_warnings"],
                "error": str(exc),
            }

        logger.info(
            "feature_snapshot_created",
            extra={
                **log_extra,
                "status": "created",
                "completeness_score": snapshot.completeness_score,
                "missing_feature_groups": audit["missing_feature_groups"],
                "data_warnings": audit["data_warnings"],
            },
        )
        return {
            "match_id": snapshot.match_id,
            "as_of_ts": snapshot.as_of_ts,
            "feature_set_version": snapshot.feature_set_version,
            "prediction_horizon": snapshot.prediction_horizon,
            "status": "created",
            "completeness_score": snapshot.completeness_score,
            "missing_fields": audit["missing_fields"],
            "missing_feature_groups": audit["missing_feature_groups"],
            "data_warnings": audit["data_warnings"],
            "snapshot_id": snapshot.id,
        }

    def _build_snapshot_payload(
        self,
        *,
        context: TargetMatchContext,
        support_data: dict[str, Any],
    ) -> dict[str, Any]:
        self._assert_strict_pre_match_window(context)

        history_overall = support_data["overall_history_by_team"]
        history_home = support_data["home_history_by_team"]
        history_away = support_data["away_history_by_team"]
        standings_by_team = support_data["standings_by_team"]

        home_overall = self._take_matches_strictly_before_cutoff(
            history_overall.get((context.scope_key, context.home_team_id), []),
            context.as_of_ts,
        )
        away_overall = self._take_matches_strictly_before_cutoff(
            history_overall.get((context.scope_key, context.away_team_id), []),
            context.as_of_ts,
        )
        home_home = self._take_matches_strictly_before_cutoff(
            history_home.get((context.scope_key, context.home_team_id), []),
            context.as_of_ts,
        )
        away_away = self._take_matches_strictly_before_cutoff(
            history_away.get((context.scope_key, context.away_team_id), []),
            context.as_of_ts,
        )

        features = {
            **self._build_overall_team_features(prefix="home_team", matches=home_overall),
            **self._build_overall_team_features(prefix="away_team", matches=away_overall),
            **self._build_home_split_features(prefix="home_team", matches=home_home),
            **self._build_away_split_features(prefix="away_team", matches=away_away),
        }

        audit = self._init_feature_audit()
        completeness_numerator = 0.0
        completeness_denominator = float(EXPECTED_CORE_FEATURE_COUNT)

        home_overall_ratio = min(len(home_overall), LAST_5_SAMPLE_SIZE) / LAST_5_SAMPLE_SIZE
        away_overall_ratio = min(len(away_overall), LAST_5_SAMPLE_SIZE) / LAST_5_SAMPLE_SIZE
        home_home_ratio = min(len(home_home), LAST_5_SAMPLE_SIZE) / LAST_5_SAMPLE_SIZE
        away_away_ratio = min(len(away_away), LAST_5_SAMPLE_SIZE) / LAST_5_SAMPLE_SIZE

        completeness_numerator += OVERALL_GROUP_WEIGHT * home_overall_ratio
        completeness_numerator += OVERALL_GROUP_WEIGHT * away_overall_ratio
        completeness_numerator += VENUE_GROUP_WEIGHT * home_home_ratio
        completeness_numerator += VENUE_GROUP_WEIGHT * away_away_ratio

        self._register_history_group_audit(
            audit=audit,
            context=context,
            team_id=context.home_team_id,
            group_name="home_team_overall_last_5",
            available_matches=len(home_overall),
            affected_fields=self._overall_feature_names("home_team"),
        )
        self._register_history_group_audit(
            audit=audit,
            context=context,
            team_id=context.away_team_id,
            group_name="away_team_overall_last_5",
            available_matches=len(away_overall),
            affected_fields=self._overall_feature_names("away_team"),
        )
        self._register_history_group_audit(
            audit=audit,
            context=context,
            team_id=context.home_team_id,
            group_name="home_team_home_last_5",
            available_matches=len(home_home),
            affected_fields=self._home_split_feature_names("home_team"),
        )
        self._register_history_group_audit(
            audit=audit,
            context=context,
            team_id=context.away_team_id,
            group_name="away_team_away_last_5",
            available_matches=len(away_away),
            affected_fields=self._away_split_feature_names("away_team"),
        )

        standings_scope_key = context.standings_scope_key
        if standings_scope_key is not None:
            completeness_denominator += float(STANDINGS_GROUP_WEIGHT * 2)

            home_standings, home_cutoff_blocked = self._resolve_standings_snapshot(
                snapshots=standings_by_team.get((standings_scope_key, context.home_team_id), []),
                as_of_date=context.as_of_date,
            )
            away_standings, away_cutoff_blocked = self._resolve_standings_snapshot(
                snapshots=standings_by_team.get((standings_scope_key, context.away_team_id), []),
                as_of_date=context.as_of_date,
            )

            features.update(
                self._build_standings_features(prefix="home_team", snapshot=home_standings)
            )
            features.update(
                self._build_standings_features(prefix="away_team", snapshot=away_standings)
            )

            self._register_standings_audit(
                audit=audit,
                context=context,
                team_id=context.home_team_id,
                prefix="home_team",
                snapshot=home_standings,
                cutoff_blocked=home_cutoff_blocked,
            )
            self._register_standings_audit(
                audit=audit,
                context=context,
                team_id=context.away_team_id,
                prefix="away_team",
                snapshot=away_standings,
                cutoff_blocked=away_cutoff_blocked,
            )

            if home_standings is not None:
                completeness_numerator += STANDINGS_GROUP_WEIGHT
            if away_standings is not None:
                completeness_numerator += STANDINGS_GROUP_WEIGHT
        else:
            features.update(self._build_standings_features(prefix="home_team", snapshot=None))
            features.update(self._build_standings_features(prefix="away_team", snapshot=None))
            self._add_missing_group(audit, "home_team_standings")
            self._add_missing_group(audit, "away_team_standings")
            self._add_missing_fields(audit, self._standings_feature_names("home_team"))
            self._add_missing_fields(audit, self._standings_feature_names("away_team"))
            warning_code = "season_id_missing_on_match"
            self._add_warning(audit, warning_code)
            logger.warning(
                "feature_snapshot_standings_unavailable",
                extra={
                    "match_id": str(context.match_id),
                    "team_id": None,
                    "as_of_ts": context.as_of_ts.isoformat(),
                    "warning": warning_code,
                },
            )

        completeness_score = (
            round(completeness_numerator / completeness_denominator, 4)
            if completeness_denominator
            else 0.0
        )

        features[FEATURE_AUDIT_KEY] = self._finalize_feature_audit(audit)

        self._validate_feature_ranges(match_id=context.match_id, features=features)

        return {
            "match_id": context.match_id,
            "as_of_ts": context.as_of_ts,
            "prediction_horizon": context.prediction_horizon,
            "feature_set_version": context.feature_set_version,
            "home_team_id": context.home_team_id,
            "away_team_id": context.away_team_id,
            "features_json": features,
            "completeness_score": completeness_score,
        }

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
        statement = select(Match)
        if match_id is not None:
            statement = statement.where(Match.id == match_id)
        if competition_id is not None:
            statement = statement.where(Match.competition_id == competition_id)
        if season_id is not None:
            statement = statement.where(Match.season_id == season_id)
        if season is not None:
            statement = statement.where(Match.season == season)
        if future_only:
            now_utc = datetime.now(UTC)
            statement = statement.where(
                Match.match_date >= now_utc,
                Match.status.in_(("scheduled", "live")),
            )

        statement = statement.order_by(Match.match_date.asc(), Match.id.asc())
        if limit is not None:
            statement = statement.limit(limit)
        return list(self.db.execute(statement).scalars().all())

    def _build_target_context(
        self,
        *,
        match: Match,
        as_of_ts: datetime | None,
        prediction_horizon: str,
        feature_set_version: str,
    ) -> TargetMatchContext:
        self._validate_prediction_horizon(prediction_horizon)

        if match.competition_id is None:
            raise ValueError("match_missing_competition_id")
        if match.match_date is None:
            raise ValueError("match_missing_match_date")
        if not match.season:
            raise ValueError("match_missing_season")
        if match.home_team_id is None:
            raise ValueError("match_missing_home_team_id")
        if match.away_team_id is None:
            raise ValueError("match_missing_away_team_id")

        match_date_utc = self._ensure_utc(match.match_date)
        resolved_as_of_ts = self._ensure_utc(as_of_ts or match.match_date)
        if resolved_as_of_ts > match_date_utc:
            raise ValueError("as_of_ts_after_match_kickoff")

        return TargetMatchContext(
            match_id=match.id,
            competition_id=match.competition_id,
            season_id=match.season_id,
            season_name=match.season,
            home_team_id=match.home_team_id,
            away_team_id=match.away_team_id,
            match_date=match_date_utc,
            as_of_ts=resolved_as_of_ts,
            prediction_horizon=prediction_horizon,
            feature_set_version=feature_set_version,
        )

    def _load_existing_snapshot_keys(
        self,
        contexts: list[TargetMatchContext],
    ) -> set[tuple[UUID, datetime, str, str]]:
        if not contexts:
            return set()

        match_ids = sorted({context.match_id for context in contexts}, key=str)
        feature_set_versions = sorted({context.feature_set_version for context in contexts})
        prediction_horizons = sorted({context.prediction_horizon for context in contexts})

        statement = select(
            FeatureSnapshot.match_id,
            FeatureSnapshot.as_of_ts,
            FeatureSnapshot.prediction_horizon,
            FeatureSnapshot.feature_set_version,
        ).where(
            FeatureSnapshot.match_id.in_(match_ids),
            FeatureSnapshot.feature_set_version.in_(feature_set_versions),
            FeatureSnapshot.prediction_horizon.in_(prediction_horizons),
        )
        return {
            (
                row.match_id,
                self._ensure_utc(row.as_of_ts),
                row.prediction_horizon,
                row.feature_set_version,
            )
            for row in self.db.execute(statement)
        }

    def _load_support_data(self, contexts: list[TargetMatchContext]) -> dict[str, Any]:
        overall_history_by_team: dict[
            tuple[tuple[UUID, str], UUID], list[TeamPerspectiveMatch]
        ] = {}
        home_history_by_team: dict[tuple[tuple[UUID, str], UUID], list[TeamPerspectiveMatch]] = {}
        away_history_by_team: dict[tuple[tuple[UUID, str], UUID], list[TeamPerspectiveMatch]] = {}
        standings_by_team: dict[tuple[tuple[UUID, UUID], UUID], list[StandingsSnapshot]] = {}

        if not contexts:
            return {
                "overall_history_by_team": overall_history_by_team,
                "home_history_by_team": home_history_by_team,
                "away_history_by_team": away_history_by_team,
                "standings_by_team": standings_by_team,
            }

        scope_map: dict[tuple[UUID, str], dict[str, Any]] = {}
        standings_scope_map: dict[tuple[UUID, UUID], dict[str, Any]] = {}

        for context in contexts:
            scope_bucket = scope_map.setdefault(
                context.scope_key,
                {
                    "team_ids": set(),
                    "max_as_of": context.as_of_ts,
                },
            )
            scope_bucket["team_ids"].update({context.home_team_id, context.away_team_id})
            if context.as_of_ts > scope_bucket["max_as_of"]:
                scope_bucket["max_as_of"] = context.as_of_ts

            standings_scope_key = context.standings_scope_key
            if standings_scope_key is not None:
                standings_bucket = standings_scope_map.setdefault(
                    standings_scope_key,
                    {
                        "team_ids": set(),
                        "max_as_of_date": context.as_of_date,
                    },
                )
                standings_bucket["team_ids"].update({context.home_team_id, context.away_team_id})
                if context.as_of_date > standings_bucket["max_as_of_date"]:
                    standings_bucket["max_as_of_date"] = context.as_of_date

        for scope_key, bucket in scope_map.items():
            competition_id, season_name = scope_key
            team_ids = sorted(bucket["team_ids"], key=str)
            strict_history_upper_bound = self._ensure_utc(bucket["max_as_of"])

            statement = (
                select(Match)
                .where(
                    Match.competition_id == competition_id,
                    Match.season == season_name,
                    Match.status == "finished",
                    Match.home_goals.is_not(None),
                    Match.away_goals.is_not(None),
                    Match.match_date < strict_history_upper_bound,
                    or_(Match.home_team_id.in_(team_ids), Match.away_team_id.in_(team_ids)),
                )
                .order_by(Match.match_date.desc(), Match.id.desc())
            )
            for match in self.db.execute(statement).scalars().all():
                if match.home_team_id in bucket["team_ids"]:
                    home_perspective = self._to_team_perspective_match(
                        match=match,
                        team_id=match.home_team_id,
                        venue="home",
                    )
                    overall_history_by_team.setdefault((scope_key, match.home_team_id), []).append(
                        home_perspective
                    )
                    home_history_by_team.setdefault((scope_key, match.home_team_id), []).append(
                        home_perspective
                    )
                if match.away_team_id in bucket["team_ids"]:
                    away_perspective = self._to_team_perspective_match(
                        match=match,
                        team_id=match.away_team_id,
                        venue="away",
                    )
                    overall_history_by_team.setdefault((scope_key, match.away_team_id), []).append(
                        away_perspective
                    )
                    away_history_by_team.setdefault((scope_key, match.away_team_id), []).append(
                        away_perspective
                    )

        for standings_scope_key, bucket in standings_scope_map.items():
            competition_id, season_id = standings_scope_key
            inclusive_prefetch_cutoff_date = bucket["max_as_of_date"]
            statement = (
                select(StandingsSnapshot)
                .where(
                    StandingsSnapshot.competition_id == competition_id,
                    StandingsSnapshot.season_id == season_id,
                    StandingsSnapshot.team_id.in_(sorted(bucket["team_ids"], key=str)),
                    StandingsSnapshot.snapshot_date <= inclusive_prefetch_cutoff_date,
                )
                .order_by(
                    StandingsSnapshot.team_id.asc(),
                    StandingsSnapshot.snapshot_date.desc(),
                    StandingsSnapshot.position.asc(),
                )
            )
            for snapshot in self.db.execute(statement).scalars().all():
                standings_by_team.setdefault((standings_scope_key, snapshot.team_id), []).append(
                    snapshot
                )

        return {
            "overall_history_by_team": overall_history_by_team,
            "home_history_by_team": home_history_by_team,
            "away_history_by_team": away_history_by_team,
            "standings_by_team": standings_by_team,
        }

    def _take_matches_strictly_before_cutoff(
        self,
        matches: list[TeamPerspectiveMatch],
        as_of_ts: datetime,
    ) -> list[TeamPerspectiveMatch]:
        strict_cutoff_utc = self._ensure_utc(as_of_ts)
        filtered = [
            match
            for match in matches
            if self._is_strictly_before_timestamp(match.match_date, strict_cutoff_utc)
        ]
        return filtered[:LAST_5_SAMPLE_SIZE]

    def _build_overall_team_features(
        self,
        *,
        prefix: str,
        matches: list[TeamPerspectiveMatch],
    ) -> dict[str, Any]:
        sample_size = len(matches)
        total_points = sum(match.points for match in matches)
        total_wins = sum(match.win for match in matches)
        total_draws = sum(match.draw for match in matches)
        total_losses = sum(match.loss for match in matches)
        total_goals_scored = sum(match.goals_scored for match in matches)
        total_goals_conceded = sum(match.goals_conceded for match in matches)
        clean_sheet_rate = (
            round(sum(match.clean_sheet for match in matches) / sample_size, 4)
            if sample_size
            else 0.0
        )
        failed_to_score_rate = (
            round(sum(match.failed_to_score for match in matches) / sample_size, 4)
            if sample_size
            else 0.0
        )
        avg_goals_scored = round(total_goals_scored / sample_size, 4) if sample_size else 0.0
        avg_goals_conceded = round(total_goals_conceded / sample_size, 4) if sample_size else 0.0

        return {
            f"{prefix}_last_5_points": total_points,
            f"{prefix}_last_5_wins": total_wins,
            f"{prefix}_last_5_draws": total_draws,
            f"{prefix}_last_5_losses": total_losses,
            f"{prefix}_last_5_goals_scored": total_goals_scored,
            f"{prefix}_last_5_goals_conceded": total_goals_conceded,
            f"{prefix}_avg_goals_scored_last_5": avg_goals_scored,
            f"{prefix}_avg_goals_conceded_last_5": avg_goals_conceded,
            f"{prefix}_clean_sheet_rate_last_5": clean_sheet_rate,
            f"{prefix}_failed_to_score_rate_last_5": failed_to_score_rate,
        }

    def _build_home_split_features(
        self,
        *,
        prefix: str,
        matches: list[TeamPerspectiveMatch],
    ) -> dict[str, Any]:
        return {
            f"{prefix}_home_last_5_points": sum(match.points for match in matches),
            f"{prefix}_home_last_5_goals_scored": sum(match.goals_scored for match in matches),
            f"{prefix}_home_last_5_goals_conceded": sum(match.goals_conceded for match in matches),
        }

    def _build_away_split_features(
        self,
        *,
        prefix: str,
        matches: list[TeamPerspectiveMatch],
    ) -> dict[str, Any]:
        return {
            f"{prefix}_away_last_5_points": sum(match.points for match in matches),
            f"{prefix}_away_last_5_goals_scored": sum(match.goals_scored for match in matches),
            f"{prefix}_away_last_5_goals_conceded": sum(match.goals_conceded for match in matches),
        }

    def _build_standings_features(
        self,
        *,
        prefix: str,
        snapshot: StandingsSnapshot | None,
    ) -> dict[str, Any]:
        if snapshot is None:
            return {
                f"{prefix}_league_position": None,
                f"{prefix}_points_per_game": None,
                f"{prefix}_goal_difference": None,
            }

        played = int(snapshot.played or 0)
        points_per_game = round(snapshot.points / played, 4) if played else 0.0
        goal_difference = int(snapshot.goals_for - snapshot.goals_against)
        return {
            f"{prefix}_league_position": int(snapshot.position),
            f"{prefix}_points_per_game": points_per_game,
            f"{prefix}_goal_difference": goal_difference,
        }

    def _resolve_standings_snapshot(
        self,
        *,
        snapshots: list[StandingsSnapshot],
        as_of_date: date,
    ) -> tuple[StandingsSnapshot | None, bool]:
        cutoff_blocked = False
        for snapshot in snapshots:
            if self._is_strictly_before_date(snapshot.snapshot_date, as_of_date):
                return snapshot, cutoff_blocked
            cutoff_blocked = True
        return None, cutoff_blocked

    def _validate_feature_ranges(self, *, match_id: UUID, features: dict[str, Any]) -> None:
        for feature_name, value in features.items():
            if feature_name == FEATURE_AUDIT_KEY or value is None:
                continue
            if feature_name.endswith("_rate_last_5") and not 0 <= float(value) <= 1:
                logger.warning(
                    "feature_snapshot_value_out_of_range",
                    extra={
                        "match_id": str(match_id),
                        "feature_name": feature_name,
                        "feature_value": value,
                    },
                )
            if feature_name.endswith("_league_position") and int(value) < 1:
                logger.warning(
                    "feature_snapshot_value_out_of_range",
                    extra={
                        "match_id": str(match_id),
                        "feature_name": feature_name,
                        "feature_value": value,
                    },
                )
            if (
                "goals" in feature_name
                or feature_name.endswith("_points")
                or feature_name.endswith("_wins")
                or feature_name.endswith("_draws")
                or feature_name.endswith("_losses")
            ) and float(value) < 0:
                logger.warning(
                    "feature_snapshot_value_out_of_range",
                    extra={
                        "match_id": str(match_id),
                        "feature_name": feature_name,
                        "feature_value": value,
                    },
                )

    def _to_team_perspective_match(
        self,
        *,
        match: Match,
        team_id: UUID,
        venue: str,
    ) -> TeamPerspectiveMatch:
        if venue == "home":
            goals_scored = int(match.home_goals or 0)
            goals_conceded = int(match.away_goals or 0)
        else:
            goals_scored = int(match.away_goals or 0)
            goals_conceded = int(match.home_goals or 0)

        if goals_scored > goals_conceded:
            points = 3
            win = 1
            draw = 0
            loss = 0
        elif goals_scored == goals_conceded:
            points = 1
            win = 0
            draw = 1
            loss = 0
        else:
            points = 0
            win = 0
            draw = 0
            loss = 1

        return TeamPerspectiveMatch(
            match_id=match.id,
            match_date=self._ensure_utc(match.match_date),
            team_id=team_id,
            venue=venue,
            goals_scored=goals_scored,
            goals_conceded=goals_conceded,
            points=points,
            win=win,
            draw=draw,
            loss=loss,
            clean_sheet=1 if goals_conceded == 0 else 0,
            failed_to_score=1 if goals_scored == 0 else 0,
        )

    def _assert_strict_pre_match_window(self, context: TargetMatchContext) -> None:
        if context.prediction_horizon != "pre_match":
            raise ValueError(f"unsupported_prediction_horizon:{context.prediction_horizon}")
        if context.as_of_ts > context.match_date:
            raise ValueError("as_of_ts_after_match_kickoff")

    def _validate_prediction_horizon(self, prediction_horizon: str) -> None:
        if prediction_horizon not in SUPPORTED_PREDICTION_HORIZONS:
            raise ValueError(f"unsupported_prediction_horizon:{prediction_horizon}")

    def _is_strictly_before_timestamp(self, candidate_ts: datetime, cutoff_ts: datetime) -> bool:
        return self._ensure_utc(candidate_ts) < self._ensure_utc(cutoff_ts)

    def _is_strictly_before_date(self, candidate_date: date, cutoff_date: date) -> bool:
        return candidate_date < cutoff_date

    def _overall_feature_names(self, prefix: str) -> list[str]:
        return [
            f"{prefix}_last_5_points",
            f"{prefix}_last_5_wins",
            f"{prefix}_last_5_draws",
            f"{prefix}_last_5_losses",
            f"{prefix}_last_5_goals_scored",
            f"{prefix}_last_5_goals_conceded",
            f"{prefix}_avg_goals_scored_last_5",
            f"{prefix}_avg_goals_conceded_last_5",
            f"{prefix}_clean_sheet_rate_last_5",
            f"{prefix}_failed_to_score_rate_last_5",
        ]

    def _home_split_feature_names(self, prefix: str) -> list[str]:
        return [
            f"{prefix}_home_last_5_points",
            f"{prefix}_home_last_5_goals_scored",
            f"{prefix}_home_last_5_goals_conceded",
        ]

    def _away_split_feature_names(self, prefix: str) -> list[str]:
        return [
            f"{prefix}_away_last_5_points",
            f"{prefix}_away_last_5_goals_scored",
            f"{prefix}_away_last_5_goals_conceded",
        ]

    def _standings_feature_names(self, prefix: str) -> list[str]:
        return [
            f"{prefix}_league_position",
            f"{prefix}_points_per_game",
            f"{prefix}_goal_difference",
        ]

    def _init_feature_audit(self) -> dict[str, list[str]]:
        return {
            "missing_fields": [],
            "missing_feature_groups": [],
            "data_warnings": [],
        }

    def _finalize_feature_audit(self, audit: dict[str, list[str]]) -> dict[str, list[str]]:
        return {
            "missing_fields": sorted(audit["missing_fields"]),
            "missing_feature_groups": sorted(audit["missing_feature_groups"]),
            "data_warnings": sorted(audit["data_warnings"]),
        }

    def _extract_feature_audit(self, features_json: dict[str, Any]) -> dict[str, list[str]]:
        raw_audit = (
            features_json.get(FEATURE_AUDIT_KEY, {}) if isinstance(features_json, dict) else {}
        )
        if not isinstance(raw_audit, dict):
            raw_audit = {}
        return {
            "missing_fields": list(raw_audit.get("missing_fields", [])),
            "missing_feature_groups": list(raw_audit.get("missing_feature_groups", [])),
            "data_warnings": list(raw_audit.get("data_warnings", [])),
        }

    def _add_missing_fields(self, audit: dict[str, list[str]], field_names: list[str]) -> None:
        for field_name in field_names:
            if field_name not in audit["missing_fields"]:
                audit["missing_fields"].append(field_name)

    def _add_missing_group(self, audit: dict[str, list[str]], group_name: str) -> None:
        if group_name not in audit["missing_feature_groups"]:
            audit["missing_feature_groups"].append(group_name)

    def _add_warning(self, audit: dict[str, list[str]], warning_code: str) -> None:
        if warning_code not in audit["data_warnings"]:
            audit["data_warnings"].append(warning_code)

    def _register_history_group_audit(
        self,
        *,
        audit: dict[str, list[str]],
        context: TargetMatchContext,
        team_id: UUID,
        group_name: str,
        available_matches: int,
        affected_fields: list[str],
    ) -> None:
        if available_matches >= LAST_5_SAMPLE_SIZE:
            return

        warning_code = (
            f"{group_name}_insufficient_history:{available_matches}_of_{LAST_5_SAMPLE_SIZE}"
        )
        self._add_missing_group(audit, group_name)
        self._add_missing_fields(audit, affected_fields)
        self._add_warning(audit, warning_code)

        logger.warning(
            "feature_snapshot_insufficient_history",
            extra={
                "match_id": str(context.match_id),
                "team_id": str(team_id),
                "history_scope": group_name,
                "available_matches": available_matches,
                "required_matches": LAST_5_SAMPLE_SIZE,
                "warning": warning_code,
            },
        )

    def _register_standings_audit(
        self,
        *,
        audit: dict[str, list[str]],
        context: TargetMatchContext,
        team_id: UUID,
        prefix: str,
        snapshot: StandingsSnapshot | None,
        cutoff_blocked: bool,
    ) -> None:
        if snapshot is not None:
            return

        group_name = f"{prefix}_standings"
        warning_code = f"{group_name}_missing"
        if cutoff_blocked:
            warning_code = f"{group_name}_excluded_by_cutoff"

        self._add_missing_group(audit, group_name)
        self._add_missing_fields(audit, self._standings_feature_names(prefix))
        self._add_warning(audit, warning_code)

        logger.warning(
            "feature_snapshot_standings_unavailable",
            extra={
                "match_id": str(context.match_id),
                "team_id": str(team_id),
                "as_of_ts": context.as_of_ts.isoformat(),
                "warning": warning_code,
            },
        )

    def _build_skipped_result(
        self,
        *,
        match_id: UUID,
        as_of_ts: datetime | None,
        prediction_horizon: str,
        feature_set_version: str,
        warning: str,
    ) -> dict[str, Any]:
        return {
            "match_id": match_id,
            "as_of_ts": as_of_ts,
            "feature_set_version": feature_set_version,
            "prediction_horizon": prediction_horizon,
            "status": "skipped",
            "completeness_score": None,
            "missing_fields": [],
            "missing_feature_groups": [],
            "data_warnings": [],
            "warning": warning,
        }

    def _log_skipped_result(self, result: dict[str, Any]) -> None:
        logger.warning(
            "feature_snapshot_skipped",
            extra={
                "match_id": str(result["match_id"]),
                "as_of_ts": result["as_of_ts"].isoformat() if result.get("as_of_ts") else None,
                "feature_set_version": result["feature_set_version"],
                "prediction_horizon": result["prediction_horizon"],
                "status": result["status"],
                "warning": result.get("warning"),
            },
        )

    def _safe_ensure_utc(self, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return self._ensure_utc(value)

    def _ensure_utc(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
