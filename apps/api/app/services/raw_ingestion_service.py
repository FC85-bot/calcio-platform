from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.models.ingestion_run import IngestionRun
from app.models.provider import Provider
from app.models.raw_ingestion import RawIngestion
from app.providers.base import BaseProvider, ProviderFetchResult
from app.services.raw_storage_service import RawStorageService

logger = get_logger(__name__)


class RawIngestionService:
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
        )

    def ingest_seasons(self) -> dict[str, Any]:
        return self._run_entity_ingestion(
            entity_type="seasons",
            fetcher=self.provider.fetch_seasons,
        )

    def ingest_teams(self) -> dict[str, Any]:
        return self._run_entity_ingestion(
            entity_type="teams",
            fetcher=self.provider.fetch_teams,
        )

    def ingest_matches(self) -> dict[str, Any]:
        return self._run_entity_ingestion(
            entity_type="matches",
            fetcher=self.provider.fetch_matches,
        )

    def ingest_odds(self) -> dict[str, Any]:
        if not self.provider.supports_odds:
            logger.warning(
                "raw_ingestion_entity_not_supported",
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
                "raw_record_count": 0,
                "created_count": 0,
                "updated_count": 0,
                "skipped_count": 0,
                "error_count": 0,
                "reason": "provider_does_not_expose_odds",
            }

        return self._run_entity_ingestion(
            entity_type="odds",
            fetcher=self.provider.fetch_odds,
        )

    def run_full_ingestion(self, *, include_odds: bool = False) -> dict[str, dict[str, Any]]:
        logger.info("raw_ingestion_started", extra={"provider": self.provider.name})
        results = {
            "competitions": self.ingest_competitions(),
            "seasons": self.ingest_seasons(),
            "teams": self.ingest_teams(),
            "matches": self.ingest_matches(),
        }
        if include_odds:
            results["odds"] = self.ingest_odds()
        logger.info("raw_ingestion_finished", extra={"provider": self.provider.name})
        return results

    def _run_entity_ingestion(
        self,
        *,
        entity_type: str,
        fetcher: Callable[[], list[ProviderFetchResult]],
    ) -> dict[str, Any]:
        self.db.rollback()
        run = self._create_run(entity_type)
        total_items = 0
        total_raw_records = 0
        created_count = 0
        skipped_count = 0
        error_count = 0

        logger.info(
            "raw_ingestion_run_started",
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
                items_in_batch = len(fetch_result.items)
                total_items += items_in_batch
                prepared_payload = self.raw_storage_service.prepare_payload(
                    provider=self.provider.name,
                    entity_type=fetch_result.entity_type,
                    run_id=run.id,
                    payload=fetch_result.payload,
                )

                if self._raw_payload_exists(
                    provider=self.provider.name,
                    entity_type=fetch_result.entity_type,
                    endpoint=fetch_result.endpoint,
                    payload_sha256=prepared_payload.metadata.payload_sha256,
                ):
                    skipped_count += 1
                    self._update_run_counters(
                        run.id,
                        row_count=total_items,
                        raw_record_count=total_raw_records,
                        created_count=created_count,
                        skipped_count=skipped_count,
                        error_count=error_count,
                    )
                    logger.info(
                        "raw_ingestion_batch_skipped_duplicate",
                        extra={
                            "provider": self.provider.name,
                            "entity_type": fetch_result.entity_type,
                            "requested_entity_type": entity_type,
                            "run_id": str(run.id),
                            "run_type": "raw_ingestion",
                            "status": "skipped",
                            "row_count": total_items,
                            "raw_record_count": total_raw_records,
                            "created_count": created_count,
                            "skipped_count": skipped_count,
                            "payload_sha256": prepared_payload.metadata.payload_sha256,
                            "endpoint": fetch_result.endpoint,
                            "skip_reason": "duplicate_payload",
                        },
                    )
                    continue

                raw_metadata = self.raw_storage_service.save_prepared_payload(prepared_payload)
                raw_record = RawIngestion(
                    run_id=run.id,
                    provider=self.provider.name,
                    entity_type=fetch_result.entity_type,
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
                    normalization_status="pending",
                )
                self.db.add(raw_record)
                self.db.commit()

                created_count += 1
                total_raw_records += 1
                self._update_run_counters(
                    run.id,
                    row_count=total_items,
                    raw_record_count=total_raw_records,
                    created_count=created_count,
                    skipped_count=skipped_count,
                    error_count=error_count,
                )

                logger.info(
                    "raw_ingestion_batch_completed",
                    extra={
                        "provider": self.provider.name,
                        "entity_type": fetch_result.entity_type,
                        "requested_entity_type": entity_type,
                        "run_id": str(run.id),
                        "run_type": "raw_ingestion",
                        "status": "created",
                        "row_count": total_items,
                        "raw_record_count": total_raw_records,
                        "created_count": created_count,
                        "skipped_count": skipped_count,
                        "raw_path": raw_metadata.raw_path,
                        "payload_sha256": raw_metadata.payload_sha256,
                        "latency_ms": (fetch_result.response_metadata or {}).get("latency_ms"),
                    },
                )

            self._finish_run(
                run.id,
                status="success",
                row_count=total_items,
                raw_record_count=total_raw_records,
                created_count=created_count,
                skipped_count=skipped_count,
                error_count=error_count,
            )
            logger.info(
                "raw_ingestion_run_completed",
                extra={
                    "provider": self.provider.name,
                    "entity_type": entity_type,
                    "run_id": str(run.id),
                    "run_type": "raw_ingestion",
                    "status": "success",
                    "row_count": total_items,
                    "raw_record_count": total_raw_records,
                    "created_count": created_count,
                    "skipped_count": skipped_count,
                    "error_count": error_count,
                },
            )
            return {
                "provider": self.provider.name,
                "entity_type": entity_type,
                "run_id": str(run.id),
                "status": "success",
                "row_count": total_items,
                "raw_record_count": total_raw_records,
                "created_count": created_count,
                "updated_count": 0,
                "skipped_count": skipped_count,
                "error_count": error_count,
            }
        except Exception as exc:
            self.db.rollback()
            error_count += 1
            self._finish_run(
                run.id,
                status="failed",
                row_count=total_items,
                raw_record_count=total_raw_records,
                created_count=created_count,
                skipped_count=skipped_count,
                error_count=error_count,
                error_message=str(exc),
            )
            logger.exception(
                "raw_ingestion_run_failed",
                extra={
                    "provider": self.provider.name,
                    "entity_type": entity_type,
                    "run_id": str(run.id),
                    "run_type": "raw_ingestion",
                    "status": "failed",
                    "row_count": total_items,
                    "raw_record_count": total_raw_records,
                    "created_count": created_count,
                    "skipped_count": skipped_count,
                    "error_count": error_count,
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

    def _update_run_counters(
        self,
        run_id: UUID,
        *,
        row_count: int,
        raw_record_count: int,
        created_count: int,
        skipped_count: int,
        error_count: int,
    ) -> None:
        run = self.db.get(IngestionRun, run_id)
        if run is None:
            return
        run.row_count = row_count
        run.raw_record_count = raw_record_count
        run.created_count = created_count
        run.updated_count = 0
        run.skipped_count = skipped_count
        run.error_count = error_count
        self.db.commit()

    def _finish_run(
        self,
        run_id: UUID,
        *,
        status: str,
        row_count: int,
        raw_record_count: int,
        created_count: int,
        skipped_count: int,
        error_count: int,
        error_message: str | None = None,
    ) -> None:
        run = self.db.get(IngestionRun, run_id)
        if run is None:
            return
        run.status = status
        run.row_count = row_count
        run.raw_record_count = raw_record_count
        run.created_count = created_count
        run.updated_count = 0
        run.skipped_count = skipped_count
        run.error_count = error_count
        run.error_message = error_message
        run.finished_at = datetime.now(UTC)
        self.db.commit()

    def _raw_payload_exists(
        self,
        *,
        provider: str,
        entity_type: str,
        endpoint: str,
        payload_sha256: str | None,
    ) -> bool:
        if payload_sha256 is None:
            return False
        statement = select(RawIngestion.id).where(
            RawIngestion.provider == provider,
            RawIngestion.entity_type == entity_type,
            RawIngestion.endpoint == endpoint,
            RawIngestion.payload_sha256 == payload_sha256,
        )
        return self.db.execute(statement).first() is not None

    def _serialize_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {key: self._serialize_value(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._serialize_value(item) for item in value]
        if isinstance(value, tuple):
            return [self._serialize_value(item) for item in value]
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, Decimal):
            return float(value)
        return value
