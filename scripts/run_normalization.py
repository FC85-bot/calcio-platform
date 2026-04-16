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
        print(f"Normalization failed: {exc}", file=sys.stderr)
        return 1

    print("Normalization completed:")
    has_errors = False
    for result in results:
        print(
            "- provider={provider} entity_type={entity_type} status={status} run_id={run_id} "
            "raw_record_count={raw_record_count} row_count={row_count} "
            "created_count={created_count} updated_count={updated_count} "
            "skipped_count={skipped_count} error_count={error_count}".format(**result)
        )
        if result.get("reason"):
            print(f"  reason={result['reason']}")
        if result.get("status") == "failed" or int(result.get("error_count") or 0) > 0:
            has_errors = True
    return 1 if has_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
