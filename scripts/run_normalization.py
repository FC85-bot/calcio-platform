from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
API_DIR = ROOT_DIR / "apps" / "api"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from app.core.logging import configure_logging, get_logger
from app.db.session import SessionLocal
from app.services.normalization_service import (
    VALID_RAW_ENTITY_TYPES,
    NormalizationService,
)

configure_logging()
logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize raw provider payloads into canonical entities"
    )
    parser.add_argument(
        "--entity-type",
        choices=VALID_RAW_ENTITY_TYPES,
        action="append",
        default=None,
        help="Normalize one or more entity types. Repeat the flag to run multiple types.",
    )
    parser.add_argument(
        "--provider",
        default=None,
        help="Restrict normalization to one provider name.",
    )
    parser.add_argument(
        "--include-processed",
        action="store_true",
        help="Re-run normalization also for raw records already marked success/skipped.",
    )
    return parser.parse_args()


def _log_results(results: list[dict]) -> bool:
    has_errors = False
    logger.info(
        "normalization_script_completed",
        extra={"result_count": len(results)},
    )
    for result in results:
        payload = {
            "provider": result.get("provider"),
            "entity_type": result.get("entity_type"),
            "status": result.get("status"),
            "run_id": result.get("run_id"),
            "raw_record_count": result.get("raw_record_count", 0),
            "row_count": result.get("row_count", 0),
            "created_count": result.get("created_count", 0),
            "updated_count": result.get("updated_count", 0),
            "skipped_count": result.get("skipped_count", 0),
            "error_count": result.get("error_count", 0),
        }
        if result.get("reason"):
            payload["reason"] = result["reason"]
        logger.info("normalization_script_result", extra=payload)
        if result.get("status") == "failed" or int(result.get("error_count") or 0) > 0:
            has_errors = True
    return has_errors


def main() -> int:
    args = parse_args()
    try:
        with SessionLocal() as db:
            service = NormalizationService(db)
            results = service.run(
                entity_types=args.entity_type,
                provider=args.provider,
                include_processed=args.include_processed,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("normalization_script_failed", extra={"error": str(exc)})
        return 1

    has_errors = _log_results(results)
    return 1 if has_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
