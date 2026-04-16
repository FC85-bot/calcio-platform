from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
API_DIR = ROOT_DIR / "apps" / "api"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.db.session import SessionLocal
from app.providers.factory import build_provider
from app.services.raw_ingestion_service import RawIngestionService

configure_logging()
logger = get_logger(__name__)

VALID_ENTITY_TYPES = ("competitions", "seasons", "teams", "matches", "odds")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run tracked raw ingestion against the configured provider"
    )
    parser.add_argument(
        "--entity-type",
        choices=VALID_ENTITY_TYPES,
        action="append",
        default=None,
        help="Run one or more entity-types instead of the default full ingestion flow.",
    )
    parser.add_argument(
        "--include-odds",
        action="store_true",
        help="Include odds in a full ingestion run when supported by the configured provider.",
    )
    return parser.parse_args()


def run_single_entity(service: RawIngestionService, entity_type: str) -> dict[str, Any]:
    if entity_type == "competitions":
        return service.ingest_competitions()
    if entity_type == "seasons":
        return service.ingest_seasons()
    if entity_type == "teams":
        return service.ingest_teams()
    if entity_type == "matches":
        return service.ingest_matches()
    if entity_type == "odds":
        return service.ingest_odds()
    raise ValueError(f"Unsupported entity_type={entity_type}")


def _return_code(results: dict[str, dict[str, Any]]) -> int:
    for value in results.values():
        if value.get("status") == "failed" or int(value.get("error_count") or 0) > 0:
            return 1
    return 0


def main() -> int:
    settings = get_settings()
    args = parse_args()
    provider = None
    try:
        with SessionLocal() as db:
            provider = build_provider(settings)
            service = RawIngestionService(db=db, provider=provider)
            if args.entity_type:
                results = {
                    entity_type: run_single_entity(service, entity_type)
                    for entity_type in args.entity_type
                }
            else:
                results = service.run_full_ingestion(include_odds=args.include_odds)
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "raw_ingestion_script_failed",
            extra={
                "provider": settings.ingestion_provider,
                "error": str(exc),
            },
        )
        print(f"Raw ingestion failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if provider is not None:
            provider.close()

    logger.info(
        "raw_ingestion_script_completed",
        extra={"provider": settings.ingestion_provider},
    )
    print("Raw ingestion completed:")
    for key, value in results.items():
        print(
            "- {key}: status={status} row_count={row_count} raw_record_count={raw_record_count} "
            "created_count={created_count} skipped_count={skipped_count} error_count={error_count}".format(
                key=key,
                status=value.get("status"),
                row_count=value.get("row_count", 0),
                raw_record_count=value.get("raw_record_count", 0),
                created_count=value.get("created_count", 0),
                skipped_count=value.get("skipped_count", 0),
                error_count=value.get("error_count", 0),
            )
        )
        if value.get("reason"):
            print(f"  reason={value['reason']}")
    return _return_code(results)


if __name__ == "__main__":
    raise SystemExit(main())
