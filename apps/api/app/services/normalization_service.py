from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import Select, func, or_, select
from sqlalchemy.orm import Session, aliased

from app.core.config import get_settings
from app.core.logging import get_logger
from app.models.bookmaker import Bookmaker
from app.models.competition import Competition
from app.models.competition_season import CompetitionSeason
from app.models.ingestion_run import IngestionRun
from app.models.market import Market
from app.models.match import Match
from app.models.odds import Odds
from app.models.provider import Provider
from app.models.provider_entity import ProviderEntity
from app.models.raw_ingestion import RawIngestion
from app.models.season import Season
from app.models.team import Team
from app.services.odds_mapping_service import (
    CANONICAL_MARKET_DEFINITIONS,
    coerce_decimal,
    normalize_bookmaker_name,
    normalize_line_value,
    normalize_name_key,
    provider_market_to_canonical,
    provider_selection_to_canonical,
    validate_line_value,
    validate_odds_value,
)

logger = get_logger(__name__)

VALID_RAW_ENTITY_TYPES = ("competitions", "seasons", "teams", "matches", "odds")
VALID_MATCH_STATUSES = {"scheduled", "live", "finished", "postponed", "cancelled"}
_FINISHED_PROVIDER_STATUSES = {"FINISHED", "AWARDED"}
_LIVE_PROVIDER_STATUSES = {"IN_PLAY", "LIVE", "PAUSED"}
_POSTPONED_PROVIDER_STATUSES = {"POSTPONED", "SUSPENDED"}
_CANCELLED_PROVIDER_STATUSES = {"CANCELLED"}


@dataclass(slots=True)
class UpsertResult:
    entity_id: UUID
    state: str


@dataclass(slots=True)
class NormalizationCounters:
    raw_record_count: int = 0
    row_count: int = 0
    created_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    error_count: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "raw_record_count": self.raw_record_count,
            "row_count": self.row_count,
            "created_count": self.created_count,
            "updated_count": self.updated_count,
            "skipped_count": self.skipped_count,
            "error_count": self.error_count,
        }


class NormalizationService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.settings = get_settings()

    def run(
        self,
        *,
        entity_types: list[str] | None = None,
        provider: str | None = None,
        include_processed: bool = False,
    ) -> list[dict[str, Any]]:
        entity_types = entity_types or list(VALID_RAW_ENTITY_TYPES)
        results: list[dict[str, Any]] = []
        for entity_type in entity_types:
            if entity_type not in VALID_RAW_ENTITY_TYPES:
                raise ValueError(f"Unsupported entity_type={entity_type}")

            provider_names = self._resolve_provider_names(
                entity_type=entity_type, provider=provider
            )
            if not provider_names:
                results.append(
                    {
                        "provider": provider,
                        "entity_type": entity_type,
                        "status": "skipped",
                        "run_id": None,
                        "raw_record_count": 0,
                        "row_count": 0,
                        "created_count": 0,
                        "updated_count": 0,
                        "skipped_count": 0,
                        "error_count": 0,
                        "reason": "no_raw_records_found",
                    }
                )
                continue

            for provider_name in provider_names:
                results.append(
                    self._run_provider_entity_normalization(
                        provider_name=provider_name,
                        entity_type=entity_type,
                        include_processed=include_processed,
                    )
                )

        return results

    def get_normalization_status(self, *, limit_runs: int = 20) -> dict[str, Any]:
        raw_status_rows = self.db.execute(
            select(
                RawIngestion.entity_type,
                RawIngestion.normalization_status,
                func.count(RawIngestion.id),
            )
            .group_by(RawIngestion.entity_type, RawIngestion.normalization_status)
            .order_by(RawIngestion.entity_type.asc(), RawIngestion.normalization_status.asc())
        ).all()

        by_entity: dict[str, dict[str, int]] = {}
        for entity_type, normalization_status, count in raw_status_rows:
            entity_bucket = by_entity.setdefault(
                entity_type,
                {
                    "pending": 0,
                    "success": 0,
                    "failed": 0,
                    "skipped": 0,
                    "total": 0,
                },
            )
            entity_bucket[normalization_status] = int(count)
            entity_bucket["total"] += int(count)

        run_rows = self.db.execute(
            select(IngestionRun, Provider.name.label("provider_name"))
            .outerjoin(Provider, IngestionRun.provider_id == Provider.id)
            .where(IngestionRun.run_type == "normalization")
            .order_by(IngestionRun.started_at.desc())
            .limit(limit_runs)
        ).all()

        return {
            "raw_pending_count": self._count_raw_by_status("pending"),
            "raw_failed_count": self._count_raw_by_status("failed"),
            "raw_success_count": self._count_raw_by_status("success"),
            "by_entity_type": [
                {
                    "entity_type": entity_type,
                    **counts,
                }
                for entity_type, counts in sorted(by_entity.items())
            ],
            "recent_runs": [
                {
                    "id": run.id,
                    "provider": provider_name,
                    "run_type": run.run_type,
                    "entity_type": run.entity_type,
                    "started_at": run.started_at,
                    "finished_at": run.finished_at,
                    "status": run.status,
                    "row_count": run.row_count,
                    "raw_record_count": run.raw_record_count,
                    "created_count": run.created_count,
                    "updated_count": run.updated_count,
                    "skipped_count": run.skipped_count,
                    "error_count": run.error_count,
                    "error_message": run.error_message,
                }
                for run, provider_name in run_rows
            ],
            "quality_checks": self.get_quality_summary(),
        }

    def get_quality_summary(self) -> dict[str, int]:
        invalid_status_count = int(
            self.db.execute(
                select(func.count(Match.id)).where(
                    Match.status.not_in(sorted(VALID_MATCH_STATUSES))
                )
            ).scalar_one()
            or 0
        )
        match_missing_team_count = int(
            self.db.execute(
                select(func.count(Match.id)).where(
                    or_(
                        Match.home_team_id.is_(None),
                        Match.away_team_id.is_(None),
                        Match.home_team_id == Match.away_team_id,
                    )
                )
            ).scalar_one()
            or 0
        )
        match_missing_competition_count = int(
            self.db.execute(
                select(func.count(Match.id))
                .select_from(Match)
                .outerjoin(Competition, Competition.id == Match.competition_id)
                .where(or_(Match.competition_id.is_(None), Competition.id.is_(None)))
            ).scalar_one()
            or 0
        )
        odds_inconsistent_count = int(
            self.db.execute(
                select(func.count(Odds.id))
                .select_from(Odds)
                .outerjoin(Match, Match.id == Odds.match_id)
                .outerjoin(Provider, Provider.id == Odds.provider_id)
                .outerjoin(Bookmaker, Bookmaker.id == Odds.bookmaker_id)
                .outerjoin(Market, Market.id == Odds.market_id)
                .where(
                    or_(
                        Match.id.is_(None),
                        Provider.id.is_(None),
                        Bookmaker.id.is_(None),
                        Market.id.is_(None),
                    )
                )
            ).scalar_one()
            or 0
        )

        duplicate_team_rows = self.db.execute(
            select(
                func.lower(func.trim(Team.name)).label("normalized_name"),
                Team.competition_id,
                func.count(Team.id),
            )
            .group_by(func.lower(func.trim(Team.name)), Team.competition_id)
            .having(func.count(Team.id) > 1)
        ).all()
        team_duplicates_in_scope_count = sum(int(row[2]) for row in duplicate_team_rows)

        mapping_missing_count = 0
        mapping_missing_count += self._count_missing_mappings(Competition, "competition")
        mapping_missing_count += self._count_missing_mappings(Season, "season")
        mapping_missing_count += self._count_missing_mappings(Team, "team")
        mapping_missing_count += self._count_missing_mappings(Match, "match")
        mapping_missing_count += self._count_missing_mappings(Bookmaker, "bookmaker")
        mapping_missing_count += self._count_missing_mappings(Market, "market")

        raw_not_normalized_count = int(
            self.db.execute(
                select(func.count(RawIngestion.id)).where(
                    RawIngestion.normalization_status.in_(["pending", "failed"])
                )
            ).scalar_one()
            or 0
        )

        return {
            "match_missing_team_count": match_missing_team_count,
            "match_missing_competition_count": match_missing_competition_count,
            "team_duplicates_in_scope_count": team_duplicates_in_scope_count,
            "mapping_missing_count": mapping_missing_count,
            "invalid_match_status_count": invalid_status_count,
            "odds_inconsistent_count": odds_inconsistent_count,
            "raw_not_normalized_count": raw_not_normalized_count,
        }

    def _run_provider_entity_normalization(
        self,
        *,
        provider_name: str,
        entity_type: str,
        include_processed: bool,
    ) -> dict[str, Any]:
        provider_record = self._get_or_create_provider(provider_name)
        raw_records = self._get_raw_records(
            provider_name=provider_name,
            entity_type=entity_type,
            include_processed=include_processed,
        )
        counters = NormalizationCounters(raw_record_count=len(raw_records))
        run = self._create_run(
            provider_id=provider_record.id, entity_type=entity_type, counters=counters
        )

        logger.info(
            "normalization_run_started",
            extra={
                "provider": provider_name,
                "entity_type": entity_type,
                "run_id": str(run.id),
                "run_type": run.run_type,
                "raw_record_count": counters.raw_record_count,
                "created_count": 0,
                "updated_count": 0,
                "skipped_count": 0,
                "error_count": 0,
            },
        )

        try:
            if entity_type == "odds":
                self._normalize_odds(
                    raw_records=raw_records,
                    provider_record=provider_record,
                    run=run,
                    counters=counters,
                )
            else:
                handler = {
                    "competitions": self._normalize_competitions,
                    "seasons": self._normalize_seasons,
                    "teams": self._normalize_teams,
                    "matches": self._normalize_matches,
                }[entity_type]
                handler(
                    raw_records=raw_records,
                    provider_record=provider_record,
                    run=run,
                    counters=counters,
                )

            status = "failed" if counters.error_count > 0 else "success"
            self._finish_run(run, status=status, counters=counters, error_message=None)
            logger.info(
                "normalization_run_completed",
                extra={
                    "provider": provider_name,
                    "entity_type": entity_type,
                    "run_id": str(run.id),
                    "run_type": run.run_type,
                    "status": status,
                    **counters.as_dict(),
                },
            )
            return {
                "provider": provider_name,
                "entity_type": entity_type,
                "run_id": str(run.id),
                "status": status,
                **counters.as_dict(),
            }
        except Exception as exc:
            self.db.rollback()
            self._finish_run(run, status="failed", counters=counters, error_message=str(exc))
            logger.exception(
                "normalization_run_failed",
                extra={
                    "provider": provider_name,
                    "entity_type": entity_type,
                    "run_id": str(run.id),
                    "run_type": run.run_type,
                    "status": "failed",
                    **counters.as_dict(),
                    "error": str(exc),
                },
            )
            raise

    def _normalize_competitions(
        self,
        *,
        raw_records: list[RawIngestion],
        provider_record: Provider,
        run: IngestionRun,
        counters: NormalizationCounters,
    ) -> None:
        for raw_record in raw_records:
            record_errors: list[str] = []
            try:
                payload = self._load_payload(raw_record)
                items = self._extract_competition_items(payload)
                if not items:
                    self._mark_raw_record_skipped(
                        raw_record, run_id=run.id, reason="no_competitions_found"
                    )
                    counters.skipped_count += 1
                    self.db.commit()
                    continue

                for item in items:
                    try:
                        external_id = self._require_external_id(item)
                        name = self._clean_name(item.get("name") or external_id)
                        country = self._clean_name(self._competition_country(item) or "Unknown")
                        result = self._upsert_competition(
                            provider_id=provider_record.id,
                            external_id=external_id,
                            name=name,
                            country=country,
                        )
                        counters.row_count += 1
                        self._count_upsert_state(result.state, counters)
                    except Exception as exc:
                        record_errors.append(str(exc))
                        counters.error_count += 1
                        logger.warning(
                            "normalization_item_failed",
                            extra={
                                "provider": provider_record.name,
                                "entity_type": "competitions",
                                "run_id": str(run.id),
                                "raw_record_id": str(raw_record.id),
                                "error": str(exc),
                            },
                        )
                self._finalize_raw_record(raw_record, run_id=run.id, record_errors=record_errors)
                self.db.commit()
            except Exception as exc:
                self.db.rollback()
                counters.error_count += 1
                self._mark_raw_record_failed(raw_record, run_id=run.id, reason=str(exc))
                self.db.commit()

    def _normalize_seasons(
        self,
        *,
        raw_records: list[RawIngestion],
        provider_record: Provider,
        run: IngestionRun,
        counters: NormalizationCounters,
    ) -> None:
        for raw_record in raw_records:
            record_errors: list[str] = []
            try:
                payload = self._load_payload(raw_record)
                items = self._extract_season_items(payload)
                if not items:
                    self._mark_raw_record_skipped(
                        raw_record, run_id=run.id, reason="no_seasons_found"
                    )
                    counters.skipped_count += 1
                    self.db.commit()
                    continue

                for item in items:
                    try:
                        competition_external_id = self._require_external_id(item["competition"])
                        competition_name = self._clean_name(
                            item["competition"].get("name") or competition_external_id
                        )
                        competition_country = self._clean_name(
                            self._competition_country(item["competition"]) or "Unknown"
                        )
                        competition_result = self._upsert_competition(
                            provider_id=provider_record.id,
                            external_id=competition_external_id,
                            name=competition_name,
                            country=competition_country,
                        )
                        self._count_upsert_state(competition_result.state, counters)

                        season_external_id = self._require_external_id(item["season"])
                        start_date = self._parse_date(item["season"]["startDate"])
                        end_date = self._parse_date(item["season"]["endDate"])
                        season_name = self._build_season_name(
                            start_date=start_date, end_date=end_date
                        )
                        season_result = self._upsert_season(
                            provider_id=provider_record.id,
                            external_id=season_external_id,
                            name=season_name,
                            start_date=start_date,
                            end_date=end_date,
                        )
                        self._count_upsert_state(season_result.state, counters)
                        link_state = self._link_competition_season(
                            competition_id=competition_result.entity_id,
                            season_id=season_result.entity_id,
                        )
                        self._count_upsert_state(link_state, counters)
                        counters.row_count += 1
                    except Exception as exc:
                        record_errors.append(str(exc))
                        counters.error_count += 1
                        logger.warning(
                            "normalization_item_failed",
                            extra={
                                "provider": provider_record.name,
                                "entity_type": "seasons",
                                "run_id": str(run.id),
                                "raw_record_id": str(raw_record.id),
                                "error": str(exc),
                            },
                        )
                self._finalize_raw_record(raw_record, run_id=run.id, record_errors=record_errors)
                self.db.commit()
            except Exception as exc:
                self.db.rollback()
                counters.error_count += 1
                self._mark_raw_record_failed(raw_record, run_id=run.id, reason=str(exc))
                self.db.commit()

    def _normalize_teams(
        self,
        *,
        raw_records: list[RawIngestion],
        provider_record: Provider,
        run: IngestionRun,
        counters: NormalizationCounters,
    ) -> None:
        for raw_record in raw_records:
            record_errors: list[str] = []
            try:
                payload = self._load_payload(raw_record)
                competition_payload = self._extract_team_competition_payload(payload)
                competition_external_id = self._require_external_id(competition_payload)
                competition_name = self._clean_name(
                    competition_payload.get("name") or competition_external_id
                )
                competition_country = self._clean_name(
                    self._competition_country(competition_payload) or "Unknown"
                )
                competition_result = self._upsert_competition(
                    provider_id=provider_record.id,
                    external_id=competition_external_id,
                    name=competition_name,
                    country=competition_country,
                )
                self._count_upsert_state(competition_result.state, counters)

                items = self._extract_team_items(payload)
                if not items:
                    self._mark_raw_record_skipped(
                        raw_record, run_id=run.id, reason="no_teams_found"
                    )
                    counters.skipped_count += 1
                    self.db.commit()
                    continue

                seen_names: set[str] = set()
                for item in items:
                    try:
                        external_id = self._require_external_id(item)
                        name = self._clean_name(
                            item.get("name") or item.get("shortName") or external_id
                        )
                        scope_key = f"{competition_result.entity_id}:{name.casefold()}"
                        if scope_key in seen_names:
                            counters.skipped_count += 1
                            logger.warning(
                                "normalization_duplicate_team_in_raw_scope",
                                extra={
                                    "provider": provider_record.name,
                                    "entity_type": "teams",
                                    "run_id": str(run.id),
                                    "raw_record_id": str(raw_record.id),
                                    "team_name": name,
                                },
                            )
                            continue
                        seen_names.add(scope_key)

                        team_result = self._upsert_team(
                            provider_id=provider_record.id,
                            external_id=external_id,
                            name=name,
                            competition_id=competition_result.entity_id,
                        )
                        self._count_upsert_state(team_result.state, counters)
                        counters.row_count += 1
                    except Exception as exc:
                        record_errors.append(str(exc))
                        counters.error_count += 1
                        logger.warning(
                            "normalization_item_failed",
                            extra={
                                "provider": provider_record.name,
                                "entity_type": "teams",
                                "run_id": str(run.id),
                                "raw_record_id": str(raw_record.id),
                                "error": str(exc),
                            },
                        )
                self._finalize_raw_record(raw_record, run_id=run.id, record_errors=record_errors)
                self.db.commit()
            except Exception as exc:
                self.db.rollback()
                counters.error_count += 1
                self._mark_raw_record_failed(raw_record, run_id=run.id, reason=str(exc))
                self.db.commit()

    def _normalize_matches(
        self,
        *,
        raw_records: list[RawIngestion],
        provider_record: Provider,
        run: IngestionRun,
        counters: NormalizationCounters,
    ) -> None:
        for raw_record in raw_records:
            record_errors: list[str] = []
            try:
                payload = self._load_payload(raw_record)
                competition_payload = self._extract_match_competition_payload(payload)
                competition_external_id = self._require_external_id(competition_payload)
                competition_name = self._clean_name(
                    competition_payload.get("name") or competition_external_id
                )
                competition_country = self._clean_name(
                    self._competition_country(competition_payload) or "Unknown"
                )
                competition_result = self._upsert_competition(
                    provider_id=provider_record.id,
                    external_id=competition_external_id,
                    name=competition_name,
                    country=competition_country,
                )
                self._count_upsert_state(competition_result.state, counters)

                items = self._extract_match_items(payload)
                if not items:
                    self._mark_raw_record_skipped(
                        raw_record, run_id=run.id, reason="no_matches_found"
                    )
                    counters.skipped_count += 1
                    self.db.commit()
                    continue

                for item in items:
                    try:
                        match_item = self._coerce_match_item(
                            item, competition_payload=competition_payload
                        )
                        match_external_id = self._require_external_id(match_item)
                        home_team_payload = match_item.get("homeTeam") or {}
                        away_team_payload = match_item.get("awayTeam") or {}
                        if not home_team_payload or not away_team_payload:
                            raise ValueError(
                                f"match external_id={match_external_id} missing home/away teams"
                            )

                        season_payload = match_item.get("season") or {}
                        season_id: UUID | None = None
                        season_name = self._fallback_season_name_from_match_date(
                            self._parse_datetime(match_item["utcDate"])
                        )
                        if (
                            season_payload.get("id") is not None
                            and season_payload.get("startDate")
                            and season_payload.get("endDate")
                        ):
                            season_start = self._parse_date(season_payload["startDate"])
                            season_end = self._parse_date(season_payload["endDate"])
                            season_name = self._build_season_name(season_start, season_end)
                            season_result = self._upsert_season(
                                provider_id=provider_record.id,
                                external_id=str(season_payload["id"]),
                                name=season_name,
                                start_date=season_start,
                                end_date=season_end,
                            )
                            season_id = season_result.entity_id
                            self._count_upsert_state(season_result.state, counters)
                            link_state = self._link_competition_season(
                                competition_id=competition_result.entity_id,
                                season_id=season_id,
                            )
                            self._count_upsert_state(link_state, counters)

                        home_team_result = self._upsert_team(
                            provider_id=provider_record.id,
                            external_id=self._require_external_id(home_team_payload),
                            name=self._clean_name(
                                home_team_payload.get("name")
                                or home_team_payload.get("shortName")
                                or str(home_team_payload.get("id"))
                            ),
                            competition_id=competition_result.entity_id,
                        )
                        away_team_result = self._upsert_team(
                            provider_id=provider_record.id,
                            external_id=self._require_external_id(away_team_payload),
                            name=self._clean_name(
                                away_team_payload.get("name")
                                or away_team_payload.get("shortName")
                                or str(away_team_payload.get("id"))
                            ),
                            competition_id=competition_result.entity_id,
                        )
                        self._count_upsert_state(home_team_result.state, counters)
                        self._count_upsert_state(away_team_result.state, counters)

                        status = self._normalize_match_status(match_item.get("status"))
                        score = match_item.get("score") or {}
                        full_time = score.get("fullTime") or {}
                        match_result = self._upsert_match(
                            provider_id=provider_record.id,
                            external_id=match_external_id,
                            competition_id=competition_result.entity_id,
                            season_id=season_id,
                            season_name=season_name,
                            match_date=self._parse_datetime(match_item["utcDate"]),
                            home_team_id=home_team_result.entity_id,
                            away_team_id=away_team_result.entity_id,
                            home_goals=full_time.get("home"),
                            away_goals=full_time.get("away"),
                            status=status,
                        )
                        self._count_upsert_state(match_result.state, counters)
                        counters.row_count += 1
                    except Exception as exc:
                        record_errors.append(str(exc))
                        counters.error_count += 1
                        logger.warning(
                            "normalization_item_failed",
                            extra={
                                "provider": provider_record.name,
                                "entity_type": "matches",
                                "run_id": str(run.id),
                                "raw_record_id": str(raw_record.id),
                                "error": str(exc),
                            },
                        )
                self._finalize_raw_record(raw_record, run_id=run.id, record_errors=record_errors)
                self.db.commit()
            except Exception as exc:
                self.db.rollback()
                counters.error_count += 1
                self._mark_raw_record_failed(raw_record, run_id=run.id, reason=str(exc))
                self.db.commit()

    def _normalize_odds(
        self,
        *,
        raw_records: list[RawIngestion],
        provider_record: Provider,
        run: IngestionRun,
        counters: NormalizationCounters,
    ) -> None:
        for raw_record in raw_records:
            payload = self._load_payload(raw_record)
            items = payload.get("odds") or []
            if not items:
                self._mark_raw_record_skipped(
                    raw_record, run_id=run.id, reason="odds_not_available"
                )
                counters.skipped_count += 1
                self.db.commit()
                continue

            record_errors: list[str] = []
            for item in items:
                try:
                    match_internal_id = self._resolve_match_for_odds_item(
                        provider_id=provider_record.id, item=item
                    )

                    bookmaker_result = self._upsert_bookmaker(
                        provider_id=provider_record.id,
                        external_id=str(item["bookmaker_key"]),
                        name=normalize_bookmaker_name(str(item["bookmaker_name"])),
                    )
                    self._count_upsert_state(bookmaker_result.state, counters)

                    market_key = str(item["market_key"])
                    market_code = provider_market_to_canonical(
                        provider_name=provider_record.name, market_key=market_key
                    )
                    if market_code is None:
                        raise ValueError(f"market_mapping_missing:{market_key}")

                    market_result = self._upsert_market(
                        provider_id=provider_record.id,
                        external_id=market_key,
                        code=market_code,
                    )
                    self._count_upsert_state(market_result.state, counters)

                    selection_code = provider_selection_to_canonical(
                        canonical_market_code=market_code,
                        selection_name=str(item["selection_name"]),
                        home_team_name=item.get("home_team_name"),
                        away_team_name=item.get("away_team_name"),
                    )
                    if selection_code is None:
                        raise ValueError(f"selection_mapping_missing:{item.get('selection_name')}")

                    line_value = normalize_line_value(
                        canonical_market_code=market_code,
                        raw_line_value=item.get("line_value"),
                    )
                    if not validate_line_value(
                        canonical_market_code=market_code, line_value=line_value
                    ):
                        raise ValueError(f"line_value_inconsistent_for_market:{market_code}")

                    snapshot_timestamp = self._parse_datetime(item["snapshot_timestamp"])
                    odds_value = coerce_decimal(item["odds_value"], places="0.0001")
                    if not validate_odds_value(odds_value):
                        raise ValueError(f"odds_value_out_of_range:{item.get('odds_value')}")

                    existing = self.db.execute(
                        select(Odds).where(
                            Odds.match_id == match_internal_id,
                            Odds.provider_id == provider_record.id,
                            Odds.bookmaker_id == bookmaker_result.entity_id,
                            Odds.market_id == market_result.entity_id,
                            Odds.selection_code == selection_code,
                            Odds.line_value == line_value,
                            Odds.odds_value == odds_value,
                            Odds.snapshot_timestamp == snapshot_timestamp,
                        )
                    ).scalar_one_or_none()
                    if existing is not None:
                        counters.skipped_count += 1
                        continue

                    self.db.add(
                        Odds(
                            match_id=match_internal_id,
                            provider_id=provider_record.id,
                            bookmaker_id=bookmaker_result.entity_id,
                            market_id=market_result.entity_id,
                            selection_code=selection_code,
                            line_value=line_value,
                            odds_value=odds_value,
                            snapshot_timestamp=snapshot_timestamp,
                        )
                    )
                    counters.created_count += 1
                    counters.row_count += 1
                except Exception as exc:
                    record_errors.append(str(exc))
                    counters.error_count += 1
                    logger.warning(
                        "normalization_item_failed",
                        extra={
                            "provider": provider_record.name,
                            "entity_type": "odds",
                            "run_id": str(run.id),
                            "raw_record_id": str(raw_record.id),
                            "error": str(exc),
                        },
                    )
            self._finalize_raw_record(raw_record, run_id=run.id, record_errors=record_errors)
            self.db.commit()

    def _resolve_match_for_odds_item(self, *, provider_id: UUID, item: dict[str, Any]) -> UUID:
        external_id = str(item["match_external_id"])
        mapping = self._get_provider_mapping(
            provider_id=provider_id, entity_type="match", external_id=external_id
        )
        if mapping is not None:
            return mapping.internal_id

        home_team = aliased(Team)
        away_team = aliased(Team)
        match_date = self._parse_datetime(item["match_date"])
        lower_bound = match_date - timedelta(hours=12)
        upper_bound = match_date + timedelta(hours=12)
        home_key = normalize_name_key(item.get("home_team_name"))
        away_key = normalize_name_key(item.get("away_team_name"))
        if not home_key or not away_key:
            raise ValueError("match_team_names_missing")

        rows = self.db.execute(
            select(
                Match,
                home_team.name.label("home_team_name"),
                away_team.name.label("away_team_name"),
            )
            .join(home_team, home_team.id == Match.home_team_id)
            .join(away_team, away_team.id == Match.away_team_id)
            .where(Match.match_date >= lower_bound, Match.match_date <= upper_bound)
        ).all()

        candidates = [
            row[0]
            for row in rows
            if normalize_name_key(row.home_team_name) == home_key
            and normalize_name_key(row.away_team_name) == away_key
        ]
        if len(candidates) != 1:
            if not candidates:
                raise ValueError(f"match_mapping_missing:{external_id}")
            raise ValueError(f"match_mapping_ambiguous:{external_id}")

        match = candidates[0]
        self._create_provider_mapping(
            provider_id=provider_id,
            entity_type="match",
            external_id=external_id,
            internal_id=match.id,
        )
        return match.id

    def _upsert_bookmaker(
        self,
        *,
        provider_id: UUID,
        external_id: str,
        name: str,
    ) -> UpsertResult:
        mapping = self._get_provider_mapping(
            provider_id=provider_id, entity_type="bookmaker", external_id=external_id
        )
        if mapping is not None:
            bookmaker = self.db.get(Bookmaker, mapping.internal_id)
            if bookmaker is None:
                raise ValueError(
                    f"Invalid provider mapping for bookmaker external_id={external_id}: internal entity not found"
                )
            state = "unchanged"
            if bookmaker.name != name:
                bookmaker.name = name
                state = "updated"
            return UpsertResult(entity_id=bookmaker.id, state=state)

        bookmaker = self.db.execute(
            select(Bookmaker).where(Bookmaker.name == name)
        ).scalar_one_or_none()
        state = "created"
        if bookmaker is None:
            bookmaker = Bookmaker(name=name)
            self.db.add(bookmaker)
            self.db.flush()
        else:
            state = "unchanged"

        self._create_provider_mapping(
            provider_id=provider_id,
            entity_type="bookmaker",
            external_id=external_id,
            internal_id=bookmaker.id,
        )
        return UpsertResult(entity_id=bookmaker.id, state=state)

    def _upsert_market(
        self,
        *,
        provider_id: UUID,
        external_id: str,
        code: str,
    ) -> UpsertResult:
        mapping = self._get_provider_mapping(
            provider_id=provider_id, entity_type="market", external_id=external_id
        )
        expected_name = CANONICAL_MARKET_DEFINITIONS[code]["name"]
        if mapping is not None:
            market = self.db.get(Market, mapping.internal_id)
            if market is None:
                raise ValueError(
                    f"Invalid provider mapping for market external_id={external_id}: internal entity not found"
                )
            state = "unchanged"
            if market.code != code or market.name != expected_name:
                market.code = code
                market.name = expected_name
                state = "updated"
            return UpsertResult(entity_id=market.id, state=state)

        market = self.db.execute(select(Market).where(Market.code == code)).scalar_one_or_none()
        state = "created"
        if market is None:
            market = Market(code=code, name=expected_name)
            self.db.add(market)
            self.db.flush()
        else:
            if market.name != expected_name:
                market.name = expected_name
                state = "updated"
            else:
                state = "unchanged"

        self._create_provider_mapping(
            provider_id=provider_id,
            entity_type="market",
            external_id=external_id,
            internal_id=market.id,
        )
        return UpsertResult(entity_id=market.id, state=state)

    def _resolve_provider_names(
        self,
        *,
        entity_type: str,
        provider: str | None,
    ) -> list[str]:
        statement = select(RawIngestion.provider).where(RawIngestion.entity_type == entity_type)
        if provider:
            statement = statement.where(RawIngestion.provider == provider)
        statement = statement.distinct().order_by(RawIngestion.provider.asc())
        return list(self.db.execute(statement).scalars().all())

    def _get_raw_records(
        self,
        *,
        provider_name: str,
        entity_type: str,
        include_processed: bool,
    ) -> list[RawIngestion]:
        statement: Select[tuple[RawIngestion]] = (
            select(RawIngestion)
            .where(
                RawIngestion.provider == provider_name,
                RawIngestion.entity_type == entity_type,
            )
            .order_by(RawIngestion.ingested_at.asc(), RawIngestion.id.asc())
        )
        if not include_processed:
            statement = statement.where(
                RawIngestion.normalization_status.in_(["pending", "failed"])
            )
        return list(self.db.execute(statement).scalars().all())

    def _get_or_create_provider(self, name: str) -> Provider:
        provider = self.db.execute(
            select(Provider).where(Provider.name == name)
        ).scalar_one_or_none()
        if provider is not None:
            return provider
        provider = Provider(name=name)
        self.db.add(provider)
        self.db.flush()
        return provider

    def _create_run(
        self, *, provider_id: UUID, entity_type: str, counters: NormalizationCounters
    ) -> IngestionRun:
        run = IngestionRun(
            provider_id=provider_id,
            run_type="normalization",
            entity_type=entity_type,
            started_at=datetime.now(UTC),
            status="running",
            row_count=0,
            raw_record_count=counters.raw_record_count,
            created_count=0,
            updated_count=0,
            skipped_count=0,
            error_count=0,
            error_message=None,
        )
        self.db.add(run)
        self.db.commit()
        return run

    def _finish_run(
        self,
        run: IngestionRun,
        *,
        status: str,
        counters: NormalizationCounters,
        error_message: str | None,
    ) -> None:
        persisted = self.db.get(IngestionRun, run.id)
        if persisted is None:
            return
        persisted.status = status
        persisted.row_count = counters.row_count
        persisted.raw_record_count = counters.raw_record_count
        persisted.created_count = counters.created_count
        persisted.updated_count = counters.updated_count
        persisted.skipped_count = counters.skipped_count
        persisted.error_count = counters.error_count
        persisted.error_message = error_message
        persisted.finished_at = datetime.now(UTC)
        self.db.commit()

    def _load_payload(self, raw_record: RawIngestion) -> dict[str, Any]:
        if raw_record.raw_path:
            file_path = Path(raw_record.raw_path)
            if not file_path.exists() and "data/raw" in raw_record.raw_path.replace("\\", "/"):
                relative_part = raw_record.raw_path.replace("\\", "/").split(
                    "data/raw/", maxsplit=1
                )[-1]
                candidate = self.settings.raw_storage_abs_path / relative_part
                if candidate.exists():
                    file_path = candidate
            if file_path.exists():
                return json.loads(file_path.read_text(encoding="utf-8"))
        if raw_record.payload:
            return raw_record.payload
        raise ValueError(f"Missing readable payload for raw_record_id={raw_record.id}")

    def _extract_competition_items(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        return list(payload.get("competitions") or payload.get("items") or [])

    def _extract_season_items(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        legacy_items = payload.get("items") or []
        if legacy_items:
            season_rows: list[dict[str, Any]] = []
            for item in legacy_items:
                competition_payload = {
                    "id": item.get("competition_external_id")
                    or item.get("competition", {}).get("id"),
                    "name": item.get("competition_name") or item.get("competition", {}).get("name"),
                    "area": {"name": item.get("competition_country")},
                    "country": item.get("competition_country"),
                }
                season_payload = {
                    "id": item.get("external_id") or item.get("id"),
                    "startDate": item.get("start_date") or item.get("startDate"),
                    "endDate": item.get("end_date") or item.get("endDate"),
                }
                if (
                    season_payload["id"] is not None
                    and season_payload["startDate"]
                    and season_payload["endDate"]
                ):
                    season_rows.append(
                        {"competition": competition_payload, "season": season_payload}
                    )
            return season_rows

        seasons = payload.get("seasons") or []
        if not seasons and payload.get("currentSeason"):
            seasons = [payload["currentSeason"]]

        competition_payload = {
            "id": payload.get("id") or payload.get("competition", {}).get("id"),
            "name": payload.get("name") or payload.get("competition", {}).get("name"),
            "area": payload.get("area") or payload.get("competition", {}).get("area"),
            "country": payload.get("country") or payload.get("competition", {}).get("country"),
        }
        return [
            {"competition": competition_payload, "season": season}
            for season in seasons
            if season.get("id") is not None and season.get("startDate") and season.get("endDate")
        ]

    def _extract_team_competition_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        competition_payload = payload.get("competition") or {}
        if competition_payload:
            return competition_payload

        items = payload.get("teams") or payload.get("items") or []
        if not items:
            return {}

        first_item = items[0]
        return {
            "id": first_item.get("competition_external_id")
            or first_item.get("competition", {}).get("id"),
            "name": first_item.get("competition_name")
            or first_item.get("competition", {}).get("name"),
            "area": {"name": first_item.get("competition_country")},
            "country": first_item.get("competition_country"),
        }

    def _extract_team_items(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        return list(payload.get("teams") or payload.get("items") or [])

    def _extract_match_competition_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        competition_payload = payload.get("competition") or {}
        if competition_payload:
            return competition_payload

        items = payload.get("matches") or payload.get("items") or []
        if not items:
            return {}

        first_item = items[0]
        return {
            "id": first_item.get("competition_external_id")
            or first_item.get("competition", {}).get("id"),
            "name": first_item.get("competition_name")
            or first_item.get("competition", {}).get("name"),
            "area": {"name": first_item.get("competition_country")},
            "country": first_item.get("competition_country"),
        }

    def _extract_match_items(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        return list(payload.get("matches") or payload.get("items") or [])

    def _coerce_match_item(
        self, item: dict[str, Any], *, competition_payload: dict[str, Any]
    ) -> dict[str, Any]:
        if item.get("utcDate") and item.get("homeTeam") and item.get("awayTeam"):
            return item

        return {
            "id": item.get("id") or item.get("external_id"),
            "utcDate": item.get("utcDate") or item.get("match_date"),
            "status": item.get("status") or item.get("provider_status") or "SCHEDULED",
            "season": {
                "id": item.get("season", {}).get("id") or item.get("season_external_id"),
                "startDate": item.get("season", {}).get("startDate")
                or item.get("season_start_date"),
                "endDate": item.get("season", {}).get("endDate") or item.get("season_end_date"),
            },
            "competition": competition_payload,
            "homeTeam": item.get("homeTeam")
            or {
                "id": item.get("home_team_external_id"),
                "name": item.get("home_team_name"),
                "shortName": item.get("home_team_name"),
            },
            "awayTeam": item.get("awayTeam")
            or {
                "id": item.get("away_team_external_id"),
                "name": item.get("away_team_name"),
                "shortName": item.get("away_team_name"),
            },
            "score": item.get("score")
            or {
                "fullTime": {
                    "home": item.get("home_goals"),
                    "away": item.get("away_goals"),
                }
            },
        }

    def _upsert_competition(
        self,
        *,
        provider_id: UUID,
        external_id: str,
        name: str,
        country: str,
    ) -> UpsertResult:
        mapping = self._get_provider_mapping(
            provider_id=provider_id, entity_type="competition", external_id=external_id
        )
        if mapping is not None:
            competition = self.db.get(Competition, mapping.internal_id)
            if competition is None:
                raise ValueError(
                    f"Invalid provider mapping for competition external_id={external_id}: internal entity not found"
                )
            state = "unchanged"
            if competition.name != name or competition.country != country:
                competition.name = name
                competition.country = country
                state = "updated"
            return UpsertResult(entity_id=competition.id, state=state)

        competition = self.db.execute(
            select(Competition).where(Competition.name == name, Competition.country == country)
        ).scalar_one_or_none()
        state = "created"
        if competition is None:
            competition = Competition(name=name, country=country)
            self.db.add(competition)
            self.db.flush()
        else:
            state = "unchanged"

        self._create_provider_mapping(
            provider_id=provider_id,
            entity_type="competition",
            external_id=external_id,
            internal_id=competition.id,
        )
        return UpsertResult(entity_id=competition.id, state=state)

    def _upsert_season(
        self,
        *,
        provider_id: UUID,
        external_id: str,
        name: str,
        start_date: date,
        end_date: date,
    ) -> UpsertResult:
        mapping = self._get_provider_mapping(
            provider_id=provider_id, entity_type="season", external_id=external_id
        )
        if mapping is not None:
            season = self.db.get(Season, mapping.internal_id)
            if season is None:
                raise ValueError(
                    f"Invalid provider mapping for season external_id={external_id}: internal entity not found"
                )
            state = "unchanged"
            if (
                season.name != name
                or season.start_date != start_date
                or season.end_date != end_date
            ):
                season.name = name
                season.start_date = start_date
                season.end_date = end_date
                state = "updated"
            return UpsertResult(entity_id=season.id, state=state)

        season = self.db.execute(select(Season).where(Season.name == name)).scalar_one_or_none()
        state = "created"
        if season is None:
            season = Season(name=name, start_date=start_date, end_date=end_date)
            self.db.add(season)
            self.db.flush()
        else:
            if season.start_date != start_date or season.end_date != end_date:
                season.start_date = start_date
                season.end_date = end_date
                state = "updated"
            else:
                state = "unchanged"

        self._create_provider_mapping(
            provider_id=provider_id,
            entity_type="season",
            external_id=external_id,
            internal_id=season.id,
        )
        return UpsertResult(entity_id=season.id, state=state)

    def _upsert_team(
        self,
        *,
        provider_id: UUID,
        external_id: str,
        name: str,
        competition_id: UUID,
    ) -> UpsertResult:
        mapping = self._get_provider_mapping(
            provider_id=provider_id, entity_type="team", external_id=external_id
        )
        if mapping is not None:
            team = self.db.get(Team, mapping.internal_id)
            if team is None:
                raise ValueError(
                    f"Invalid provider mapping for team external_id={external_id}: internal entity not found"
                )
            state = "unchanged"
            if team.name != name or team.competition_id != competition_id:
                team.name = name
                team.competition_id = competition_id
                state = "updated"
            return UpsertResult(entity_id=team.id, state=state)

        team = self.db.execute(
            select(Team).where(Team.name == name, Team.competition_id == competition_id)
        ).scalar_one_or_none()
        state = "created"
        if team is None:
            team = Team(name=name, competition_id=competition_id)
            self.db.add(team)
            self.db.flush()
        else:
            state = "unchanged"

        self._create_provider_mapping(
            provider_id=provider_id,
            entity_type="team",
            external_id=external_id,
            internal_id=team.id,
        )
        return UpsertResult(entity_id=team.id, state=state)

    def _upsert_match(
        self,
        *,
        provider_id: UUID,
        external_id: str,
        competition_id: UUID,
        season_id: UUID | None,
        season_name: str,
        match_date: datetime,
        home_team_id: UUID,
        away_team_id: UUID,
        home_goals: int | None,
        away_goals: int | None,
        status: str,
    ) -> UpsertResult:
        mapping = self._get_provider_mapping(
            provider_id=provider_id, entity_type="match", external_id=external_id
        )
        if mapping is not None:
            match = self.db.get(Match, mapping.internal_id)
            if match is None:
                raise ValueError(
                    f"Invalid provider mapping for match external_id={external_id}: internal entity not found"
                )
            state = "unchanged"
            if (
                match.competition_id != competition_id
                or match.season_id != season_id
                or match.season != season_name
                or match.match_date != match_date
                or match.home_team_id != home_team_id
                or match.away_team_id != away_team_id
                or match.home_goals != home_goals
                or match.away_goals != away_goals
                or match.status != status
            ):
                match.competition_id = competition_id
                match.season_id = season_id
                match.season = season_name
                match.match_date = match_date
                match.home_team_id = home_team_id
                match.away_team_id = away_team_id
                match.home_goals = home_goals
                match.away_goals = away_goals
                match.status = status
                state = "updated"
            return UpsertResult(entity_id=match.id, state=state)

        match = self.db.execute(
            select(Match).where(
                Match.competition_id == competition_id,
                Match.season == season_name,
                Match.home_team_id == home_team_id,
                Match.away_team_id == away_team_id,
                Match.match_date == match_date,
            )
        ).scalar_one_or_none()
        state = "created"
        if match is None:
            match = Match(
                competition_id=competition_id,
                season_id=season_id,
                season=season_name,
                match_date=match_date,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                home_goals=home_goals,
                away_goals=away_goals,
                status=status,
            )
            self.db.add(match)
            self.db.flush()
        else:
            if (
                match.season_id != season_id
                or match.home_goals != home_goals
                or match.away_goals != away_goals
                or match.status != status
            ):
                match.season_id = season_id
                match.home_goals = home_goals
                match.away_goals = away_goals
                match.status = status
                state = "updated"
            else:
                state = "unchanged"

        self._create_provider_mapping(
            provider_id=provider_id,
            entity_type="match",
            external_id=external_id,
            internal_id=match.id,
        )
        return UpsertResult(entity_id=match.id, state=state)

    def _link_competition_season(self, *, competition_id: UUID, season_id: UUID) -> str:
        link = self.db.execute(
            select(CompetitionSeason).where(
                CompetitionSeason.competition_id == competition_id,
                CompetitionSeason.season_id == season_id,
            )
        ).scalar_one_or_none()
        if link is not None:
            return "unchanged"
        self.db.add(CompetitionSeason(competition_id=competition_id, season_id=season_id))
        self.db.flush()
        return "created"

    def _get_provider_mapping(
        self,
        *,
        provider_id: UUID,
        entity_type: str,
        external_id: str,
    ) -> ProviderEntity | None:
        return self.db.execute(
            select(ProviderEntity).where(
                ProviderEntity.provider_id == provider_id,
                ProviderEntity.entity_type == entity_type,
                ProviderEntity.external_id == external_id,
            )
        ).scalar_one_or_none()

    def _create_provider_mapping(
        self,
        *,
        provider_id: UUID,
        entity_type: str,
        external_id: str,
        internal_id: UUID,
    ) -> ProviderEntity:
        mapping = self._get_provider_mapping(
            provider_id=provider_id,
            entity_type=entity_type,
            external_id=external_id,
        )
        if mapping is not None:
            return mapping
        mapping = ProviderEntity(
            provider_id=provider_id,
            entity_type=entity_type,
            external_id=external_id,
            internal_id=internal_id,
        )
        self.db.add(mapping)
        self.db.flush()
        return mapping

    def _require_internal_id(
        self, *, provider_id: UUID, entity_type: str, external_id: str
    ) -> UUID:
        mapping = self._get_provider_mapping(
            provider_id=provider_id,
            entity_type=entity_type,
            external_id=external_id,
        )
        if mapping is None:
            raise ValueError(
                f"Missing provider mapping for entity_type={entity_type} external_id={external_id}"
            )
        return mapping.internal_id

    def _count_missing_mappings(self, model: Any, entity_type: str) -> int:
        return int(
            self.db.execute(
                select(func.count(model.id)).where(
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

    def _count_raw_by_status(self, status: str) -> int:
        return int(
            self.db.execute(
                select(func.count(RawIngestion.id)).where(
                    RawIngestion.normalization_status == status
                )
            ).scalar_one()
            or 0
        )

    def _require_external_id(self, payload: dict[str, Any]) -> str:
        value = payload.get("id")
        if value is None:
            value = payload.get("external_id")
        if value is None:
            raise ValueError("Missing provider external id")
        return str(value)

    def _normalize_match_status(self, provider_status: str | None) -> str:
        status = str(provider_status or "SCHEDULED").upper()
        if status in _FINISHED_PROVIDER_STATUSES:
            return "finished"
        if status in _LIVE_PROVIDER_STATUSES:
            return "live"
        if status in _POSTPONED_PROVIDER_STATUSES:
            return "postponed"
        if status in _CANCELLED_PROVIDER_STATUSES:
            return "cancelled"
        return "scheduled"

    def _parse_date(self, raw_value: str | date) -> date:
        if isinstance(raw_value, date) and not isinstance(raw_value, datetime):
            return raw_value
        return date.fromisoformat(str(raw_value)[:10])

    def _parse_datetime(self, raw_value: str | datetime) -> datetime:
        if isinstance(raw_value, datetime):
            parsed = raw_value
        else:
            parsed = datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    def _build_season_name(self, start_date: date, end_date: date) -> str:
        return f"{start_date.year}/{end_date.year}"

    def _fallback_season_name_from_match_date(self, match_date: datetime) -> str:
        year = match_date.year
        if match_date.month < 7:
            return f"{year - 1}/{year}"
        return f"{year}/{year + 1}"

    def _competition_country(self, payload: dict[str, Any]) -> str:
        area = payload.get("area") or {}
        return area.get("name") or payload.get("country") or ""

    def _clean_name(self, value: str) -> str:
        return " ".join(str(value).split()).strip()

    def _count_upsert_state(self, state: str, counters: NormalizationCounters) -> None:
        if state == "created":
            counters.created_count += 1
        elif state == "updated":
            counters.updated_count += 1

    def _finalize_raw_record(
        self, raw_record: RawIngestion, *, run_id: UUID, record_errors: list[str]
    ) -> None:
        if record_errors:
            self._mark_raw_record_failed(
                raw_record, run_id=run_id, reason="; ".join(record_errors[:5])
            )
            return
        raw_record.normalization_run_id = run_id
        raw_record.normalization_status = "success"
        raw_record.normalized_at = datetime.now(UTC)
        raw_record.normalization_error = None

    def _mark_raw_record_skipped(
        self, raw_record: RawIngestion, *, run_id: UUID, reason: str
    ) -> None:
        raw_record.normalization_run_id = run_id
        raw_record.normalization_status = "skipped"
        raw_record.normalized_at = datetime.now(UTC)
        raw_record.normalization_error = reason

    def _mark_raw_record_failed(
        self, raw_record: RawIngestion, *, run_id: UUID, reason: str
    ) -> None:
        raw_record.normalization_run_id = run_id
        raw_record.normalization_status = "failed"
        raw_record.normalized_at = datetime.now(UTC)
        raw_record.normalization_error = reason[:2000]
