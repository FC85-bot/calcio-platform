from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

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
from app.providers.base import BaseProvider, ProviderFetchResult
from app.services.odds_mapping_service import (
    CANONICAL_MARKET_DEFINITIONS,
    coerce_decimal,
    normalize_bookmaker_name,
    normalize_line_value,
    provider_market_to_canonical,
    provider_selection_to_canonical,
    validate_line_value,
    validate_odds_value,
)
from app.services.raw_storage_service import RawStorageService

logger = get_logger(__name__)

_FINISHED_PROVIDER_STATUSES = {"FINISHED", "AWARDED"}


class IngestionService:
    def __init__(
        self,
        db: Session,
        provider: BaseProvider,
        raw_storage_service: RawStorageService | None = None,
    ) -> None:
        self.db = db
        self.provider = provider
        self.raw_storage_service = raw_storage_service or RawStorageService()
        self.provider_record = self._get_or_create_provider(provider.name)
        self.db.rollback()

    def ingest_competitions(self) -> dict[str, Any]:
        return self._run_entity_ingestion(
            entity_type="competitions",
            fetcher=self.provider.fetch_competitions,
            processor=self._process_competition_items,
        )

    def ingest_seasons(self) -> dict[str, Any]:
        return self._run_entity_ingestion(
            entity_type="seasons",
            fetcher=self.provider.fetch_seasons,
            processor=self._process_season_items,
        )

    def ingest_teams(self) -> dict[str, Any]:
        return self._run_entity_ingestion(
            entity_type="teams",
            fetcher=self.provider.fetch_teams,
            processor=self._process_team_items,
        )

    def ingest_matches(self) -> dict[str, Any]:
        return self._run_entity_ingestion(
            entity_type="matches",
            fetcher=self.provider.fetch_matches,
            processor=self._process_match_items,
        )

    def ingest_odds(self) -> dict[str, Any]:
        if not self.provider.supports_odds:
            logger.warning(
                "ingestion_entity_not_supported",
                extra={
                    "provider": self.provider.name,
                    "entity_type": "odds",
                },
            )
            return {
                "provider": self.provider.name,
                "entity_type": "odds",
                "status": "skipped",
                "row_count": 0,
                "raw_records": 0,
                "reason": "provider_does_not_expose_odds",
            }

        return self._run_entity_ingestion(
            entity_type="odds",
            fetcher=self.provider.fetch_odds,
            processor=self._process_odds_items,
        )

    def run_full_ingestion(self, *, include_odds: bool = False) -> dict[str, dict[str, Any]]:
        logger.info("ingestion_started", extra={"provider": self.provider.name})
        results = {
            "competitions": self.ingest_competitions(),
            "seasons": self.ingest_seasons(),
            "teams": self.ingest_teams(),
            "matches": self.ingest_matches(),
        }
        if include_odds:
            results["odds"] = self.ingest_odds()
        logger.info("ingestion_finished", extra={"provider": self.provider.name})
        return results

    def _run_entity_ingestion(
        self,
        *,
        entity_type: str,
        fetcher: Callable[[], list[ProviderFetchResult]],
        processor: Callable[[list[dict[str, Any]]], int],
    ) -> dict[str, Any]:
        self.db.rollback()
        run = self._create_run(entity_type)
        total_processed = 0
        total_raw_records = 0

        logger.info(
            "ingestion_run_started",
            extra={
                "provider": self.provider.name,
                "entity_type": entity_type,
                "run_id": str(run.id),
                "run_type": run.run_type,
                "status": run.status,
            },
        )

        try:
            fetch_results = fetcher()
            self.db.rollback()

            for fetch_result in fetch_results:
                raw_metadata = self.raw_storage_service.save_payload(
                    provider=self.provider.name,
                    entity_type=entity_type,
                    run_id=run.id,
                    payload=fetch_result.payload,
                )

                raw_record = RawIngestion(
                    run_id=run.id,
                    provider=self.provider.name,
                    entity_type=entity_type,
                    endpoint=fetch_result.endpoint,
                    raw_path=raw_metadata.raw_path,
                    payload_sha256=raw_metadata.payload_sha256,
                    payload_size_bytes=raw_metadata.payload_size_bytes,
                    request_params=self._serialize_value(fetch_result.request_params)
                    if fetch_result.request_params
                    else None,
                    response_metadata=self._serialize_value(fetch_result.response_metadata)
                    if fetch_result.response_metadata
                    else None,
                    payload=raw_metadata.payload_summary,
                    normalization_status="success",
                    normalized_at=datetime.now(UTC),
                )
                self.db.add(raw_record)

                batch_processor = (
                    processor
                    if fetch_result.entity_type == entity_type
                    else self._resolve_processor(fetch_result.entity_type)
                )
                processed = batch_processor(fetch_result.items)
                total_processed += processed
                total_raw_records += 1

                current_run = self.db.get(IngestionRun, run.id)
                if current_run is not None:
                    current_run.row_count = total_processed
                    current_run.raw_record_count = total_raw_records

                self.db.commit()

                logger.info(
                    "ingestion_batch_completed",
                    extra={
                        "provider": self.provider.name,
                        "entity_type": entity_type,
                        "run_id": str(run.id),
                        "run_type": run.run_type,
                        "status": "running",
                        "row_count": total_processed,
                        "raw_record_count": total_raw_records,
                        "raw_path": raw_metadata.raw_path,
                    },
                )

            self._finish_run(
                run.id,
                status="success",
                row_count=total_processed,
                raw_record_count=total_raw_records,
            )
            logger.info(
                "ingestion_run_completed",
                extra={
                    "provider": self.provider.name,
                    "entity_type": entity_type,
                    "run_id": str(run.id),
                    "run_type": run.run_type,
                    "status": "success",
                    "row_count": total_processed,
                    "raw_record_count": total_raw_records,
                },
            )
            return {
                "provider": self.provider.name,
                "entity_type": entity_type,
                "run_id": str(run.id),
                "status": "success",
                "row_count": total_processed,
                "raw_records": total_raw_records,
            }
        except Exception as exc:
            self.db.rollback()
            self._finish_run(
                run.id,
                status="failed",
                row_count=total_processed,
                raw_record_count=total_raw_records,
                error_message=str(exc),
            )
            logger.exception(
                "ingestion_run_failed",
                extra={
                    "provider": self.provider.name,
                    "entity_type": entity_type,
                    "run_id": str(run.id),
                    "run_type": run.run_type,
                    "status": "failed",
                    "row_count": total_processed,
                    "raw_record_count": total_raw_records,
                    "error": str(exc),
                },
            )
            raise

    def _get_or_create_provider(self, name: str) -> Provider:
        provider = self.db.execute(
            select(Provider).where(Provider.name == name)
        ).scalar_one_or_none()
        if provider is not None:
            self.db.rollback()
            return provider

        provider = Provider(name=name)
        self.db.add(provider)
        self.db.commit()
        return provider

    def _create_run(self, entity_type: str) -> IngestionRun:
        run = IngestionRun(
            provider_id=self.provider_record.id,
            run_type="raw_ingestion",
            entity_type=entity_type,
            started_at=datetime.now(UTC),
            status="running",
            row_count=0,
            raw_record_count=0,
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
        run_id: UUID,
        *,
        status: str,
        row_count: int,
        raw_record_count: int,
        error_message: str | None = None,
    ) -> None:
        run = self.db.get(IngestionRun, run_id)
        if run is None:
            return
        run.status = status
        run.row_count = row_count
        run.raw_record_count = raw_record_count
        run.error_message = error_message
        run.finished_at = datetime.now(UTC)
        self.db.commit()

    def _resolve_processor(self, fetch_entity_type: str) -> Callable[[list[dict[str, Any]]], int]:
        processors: dict[str, Callable[[list[dict[str, Any]]], int]] = {
            "competitions": self._process_competition_items,
            "seasons": self._process_season_items,
            "teams": self._process_team_items,
            "matches": self._process_match_items,
            "odds": self._process_odds_items,
        }
        processor = processors.get(fetch_entity_type)
        if processor is None:
            raise ValueError(f"Unsupported fetch entity_type={fetch_entity_type}")
        return processor

    def _process_competition_items(self, items: list[dict[str, Any]]) -> int:
        processed = 0
        for item in items:
            self._upsert_competition(
                external_id=item["external_id"],
                name=item["name"],
                country=item["country"],
            )
            processed += 1
        return processed

    def _process_season_items(self, items: list[dict[str, Any]]) -> int:
        processed = 0
        for item in items:
            competition = self._upsert_competition(
                external_id=item["competition_external_id"],
                name=item["competition_name"],
                country=item["competition_country"],
            )
            season = self._upsert_season(
                external_id=item["external_id"],
                name=item["name"],
                start_date=item["start_date"],
                end_date=item["end_date"],
            )
            self._link_competition_season(competition_id=competition.id, season_id=season.id)
            processed += 1
        return processed

    def _process_team_items(self, items: list[dict[str, Any]]) -> int:
        processed = 0
        for item in items:
            competition = self._upsert_competition(
                external_id=item["competition_external_id"],
                name=item["competition_name"],
                country=item["competition_country"],
            )
            self._upsert_team(
                external_id=item["external_id"],
                name=item["name"],
                competition_id=competition.id,
            )
            processed += 1
        return processed

    def _process_match_items(self, items: list[dict[str, Any]]) -> int:
        processed = 0
        for item in items:
            competition = self._upsert_competition(
                external_id=item["competition_external_id"],
                name=item["competition_name"],
                country=item["competition_country"],
            )
            season_name = item.get("season_name")
            season_id: UUID | None = None
            if (
                item.get("season_external_id")
                and season_name
                and item.get("season_start_date")
                and item.get("season_end_date")
            ):
                season = self._upsert_season(
                    external_id=item["season_external_id"],
                    name=season_name,
                    start_date=item["season_start_date"],
                    end_date=item["season_end_date"],
                )
                season_id = season.id
                self._link_competition_season(competition_id=competition.id, season_id=season.id)
            elif season_name and item.get("season_start_date") and item.get("season_end_date"):
                season = self._find_or_create_season_by_name(
                    name=season_name,
                    start_date=item["season_start_date"],
                    end_date=item["season_end_date"],
                )
                season_id = season.id
                self._link_competition_season(competition_id=competition.id, season_id=season.id)

            home_team = self._upsert_team(
                external_id=item["home_team_external_id"],
                name=item["home_team_name"],
                competition_id=competition.id,
            )
            away_team = self._upsert_team(
                external_id=item["away_team_external_id"],
                name=item["away_team_name"],
                competition_id=competition.id,
            )

            self._upsert_match(
                external_id=item["external_id"],
                competition_id=competition.id,
                season_name=season_name
                or self._fallback_season_name_from_match_date(item["match_date"]),
                match_date=item["match_date"],
                home_team_id=home_team.id,
                away_team_id=away_team.id,
                home_goals=item.get("home_goals"),
                away_goals=item.get("away_goals"),
                status=self._normalize_match_status(item.get("provider_status")),
                season_id=season_id,
            )
            processed += 1
        return processed

    def _process_odds_items(self, items: list[dict[str, Any]]) -> int:
        processed = 0
        for item in items:
            match_id = self._require_internal_id("match", item["match_external_id"])

            bookmaker = self._resolve_or_create_bookmaker(
                external_id=str(item["bookmaker_key"]),
                name=normalize_bookmaker_name(str(item["bookmaker_name"])),
            )

            market_code = provider_market_to_canonical(
                provider_name=self.provider_record.name,
                market_key=str(item["market_key"]),
            )
            if market_code is None:
                raise ValueError(f"market_mapping_missing:{item['market_key']}")

            market = self._resolve_or_create_market(
                external_id=str(item["market_key"]),
                code=market_code,
            )

            selection_code = provider_selection_to_canonical(
                canonical_market_code=market_code,
                selection_name=str(item.get("selection_name") or ""),
                home_team_name=item.get("home_team_name"),
                away_team_name=item.get("away_team_name"),
            )
            if selection_code is None:
                raise ValueError(f"selection_mapping_missing:{item.get('selection_name')}")

            line_value = normalize_line_value(
                canonical_market_code=market_code,
                raw_line_value=item.get("line_value"),
            )
            if not validate_line_value(canonical_market_code=market_code, line_value=line_value):
                raise ValueError(f"line_value_inconsistent_for_market:{market_code}")

            odds_value = coerce_decimal(item["odds_value"], places="0.0001")
            if not validate_odds_value(odds_value):
                raise ValueError(f"odds_value_out_of_range:{item['odds_value']}")

            existing = self.db.execute(
                select(Odds).where(
                    Odds.match_id == match_id,
                    Odds.provider_id == self.provider_record.id,
                    Odds.bookmaker_id == bookmaker.id,
                    Odds.market_id == market.id,
                    Odds.selection_code == selection_code,
                    Odds.line_value == line_value,
                    Odds.odds_value == odds_value,
                    Odds.snapshot_timestamp == item["snapshot_timestamp"],
                )
            ).scalar_one_or_none()
            if existing is not None:
                continue

            self.db.add(
                Odds(
                    match_id=match_id,
                    provider_id=self.provider_record.id,
                    bookmaker_id=bookmaker.id,
                    market_id=market.id,
                    selection_code=selection_code,
                    line_value=line_value,
                    odds_value=odds_value,
                    snapshot_timestamp=item["snapshot_timestamp"],
                )
            )
            processed += 1
        return processed

    def _resolve_or_create_bookmaker(self, *, external_id: str, name: str) -> Bookmaker:
        mapping = self._get_provider_entity("bookmaker", external_id)
        if mapping is not None:
            bookmaker = self.db.get(Bookmaker, mapping.internal_id)
            if bookmaker is None:
                raise ValueError(
                    f"Invalid provider mapping for bookmaker external_id={external_id}"
                )
            if bookmaker.name != name:
                bookmaker.name = name
            return bookmaker

        bookmaker = self.db.execute(
            select(Bookmaker).where(Bookmaker.name == name)
        ).scalar_one_or_none()
        if bookmaker is None:
            bookmaker = Bookmaker(name=name)
            self.db.add(bookmaker)
            self.db.flush()
        self._create_provider_entity(
            entity_type="bookmaker", external_id=external_id, internal_id=bookmaker.id
        )
        return bookmaker

    def _resolve_or_create_market(self, *, external_id: str, code: str) -> Market:
        mapping = self._get_provider_entity("market", external_id)
        expected_name = CANONICAL_MARKET_DEFINITIONS[code]["name"]
        if mapping is not None:
            market = self.db.get(Market, mapping.internal_id)
            if market is None:
                raise ValueError(f"Invalid provider mapping for market external_id={external_id}")
            if market.code != code:
                market.code = code
            if market.name != expected_name:
                market.name = expected_name
            return market

        market = self.db.execute(select(Market).where(Market.code == code)).scalar_one_or_none()
        if market is None:
            market = Market(code=code, name=expected_name)
            self.db.add(market)
            self.db.flush()
        elif market.name != expected_name:
            market.name = expected_name

        self._create_provider_entity(
            entity_type="market", external_id=external_id, internal_id=market.id
        )
        return market

    def _upsert_competition(self, *, external_id: str, name: str, country: str) -> Competition:
        mapping = self._get_provider_entity("competition", external_id)
        if mapping is not None:
            competition = self.db.get(Competition, mapping.internal_id)
            if competition is None:
                raise ValueError(
                    f"Invalid provider mapping for competition external_id={external_id}: internal entity not found"
                )
            competition.name = name
            competition.country = country
            return competition

        competition = self.db.execute(
            select(Competition).where(Competition.name == name, Competition.country == country)
        ).scalar_one_or_none()
        if competition is None:
            competition = Competition(name=name, country=country)
            self.db.add(competition)
            self.db.flush()

        self._create_provider_entity(
            entity_type="competition",
            external_id=external_id,
            internal_id=competition.id,
        )
        return competition

    def _upsert_season(
        self,
        *,
        external_id: str,
        name: str,
        start_date: date,
        end_date: date,
    ) -> Season:
        mapping = self._get_provider_entity("season", external_id)
        if mapping is not None:
            season = self.db.get(Season, mapping.internal_id)
            if season is None:
                raise ValueError(
                    f"Invalid provider mapping for season external_id={external_id}: internal entity not found"
                )
            season.name = name
            season.start_date = start_date
            season.end_date = end_date
            return season

        season = self.db.execute(select(Season).where(Season.name == name)).scalar_one_or_none()
        if season is None:
            season = Season(name=name, start_date=start_date, end_date=end_date)
            self.db.add(season)
            self.db.flush()
        else:
            season.start_date = start_date
            season.end_date = end_date

        self._create_provider_entity(
            entity_type="season",
            external_id=external_id,
            internal_id=season.id,
        )
        return season

    def _find_or_create_season_by_name(
        self, *, name: str, start_date: date, end_date: date
    ) -> Season:
        season = self.db.execute(select(Season).where(Season.name == name)).scalar_one_or_none()
        if season is None:
            season = Season(name=name, start_date=start_date, end_date=end_date)
            self.db.add(season)
            self.db.flush()
            return season
        season.start_date = start_date
        season.end_date = end_date
        return season

    def _link_competition_season(
        self, *, competition_id: UUID, season_id: UUID
    ) -> CompetitionSeason:
        link = self.db.execute(
            select(CompetitionSeason).where(
                CompetitionSeason.competition_id == competition_id,
                CompetitionSeason.season_id == season_id,
            )
        ).scalar_one_or_none()
        if link is not None:
            return link
        link = CompetitionSeason(competition_id=competition_id, season_id=season_id)
        self.db.add(link)
        self.db.flush()
        return link

    def _upsert_team(self, *, external_id: str, name: str, competition_id: UUID) -> Team:
        mapping = self._get_provider_entity("team", external_id)
        if mapping is not None:
            team = self.db.get(Team, mapping.internal_id)
            if team is None:
                raise ValueError(
                    f"Invalid provider mapping for team external_id={external_id}: internal entity not found"
                )
            team.name = name
            team.competition_id = competition_id
            return team

        team = self.db.execute(
            select(Team).where(Team.name == name, Team.competition_id == competition_id)
        ).scalar_one_or_none()
        if team is None:
            team = Team(name=name, competition_id=competition_id)
            self.db.add(team)
            self.db.flush()

        self._create_provider_entity(
            entity_type="team",
            external_id=external_id,
            internal_id=team.id,
        )
        return team

    def _upsert_match(
        self,
        *,
        external_id: str,
        competition_id: UUID,
        season_name: str,
        match_date: datetime,
        home_team_id: UUID,
        away_team_id: UUID,
        home_goals: int | None,
        away_goals: int | None,
        status: str,
        season_id: UUID | None = None,
    ) -> Match:
        mapping = self._get_provider_entity("match", external_id)
        if mapping is not None:
            match = self.db.get(Match, mapping.internal_id)
            if match is None:
                raise ValueError(
                    f"Invalid provider mapping for match external_id={external_id}: internal entity not found"
                )
            match.competition_id = competition_id
            match.season_id = season_id
            match.season = season_name
            match.match_date = match_date
            match.home_team_id = home_team_id
            match.away_team_id = away_team_id
            match.home_goals = home_goals
            match.away_goals = away_goals
            match.status = status
            return match

        match = self.db.execute(
            select(Match).where(
                Match.competition_id == competition_id,
                Match.season == season_name,
                Match.home_team_id == home_team_id,
                Match.away_team_id == away_team_id,
                Match.match_date == match_date,
            )
        ).scalar_one_or_none()
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
            match.season_id = season_id
            match.home_goals = home_goals
            match.away_goals = away_goals
            match.status = status

        self._create_provider_entity(
            entity_type="match",
            external_id=external_id,
            internal_id=match.id,
        )
        return match

    def _get_provider_entity(self, entity_type: str, external_id: str) -> ProviderEntity | None:
        return self.db.execute(
            select(ProviderEntity).where(
                ProviderEntity.provider_id == self.provider_record.id,
                ProviderEntity.entity_type == entity_type,
                ProviderEntity.external_id == external_id,
            )
        ).scalar_one_or_none()

    def _create_provider_entity(
        self, *, entity_type: str, external_id: str, internal_id: UUID
    ) -> ProviderEntity:
        provider_entity = self._get_provider_entity(entity_type, external_id)
        if provider_entity is not None:
            return provider_entity

        provider_entity = ProviderEntity(
            provider_id=self.provider_record.id,
            entity_type=entity_type,
            external_id=external_id,
            internal_id=internal_id,
        )
        self.db.add(provider_entity)
        self.db.flush()
        return provider_entity

    def _require_internal_id(self, entity_type: str, external_id: str) -> UUID:
        provider_entity = self._get_provider_entity(entity_type, external_id)
        if provider_entity is None:
            raise ValueError(
                f"Missing provider mapping for entity_type={entity_type} external_id={external_id}"
            )
        return provider_entity.internal_id

    def _normalize_match_status(self, provider_status: str | None) -> str:
        normalized_status = (provider_status or "SCHEDULED").upper()
        if normalized_status in _FINISHED_PROVIDER_STATUSES:
            return "finished"
        return "scheduled"

    def _fallback_season_name_from_match_date(self, match_date: datetime) -> str:
        year = match_date.year
        if match_date.month < 7:
            return f"{year - 1}/{year}"
        return f"{year}/{year + 1}"

    def _serialize_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {key: self._serialize_value(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._serialize_value(item) for item in value]
        if isinstance(value, tuple):
            return [self._serialize_value(item) for item in value]
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, Decimal):
            return float(value)
        return value
