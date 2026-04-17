from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
import sys
from uuid import UUID

ROOT_DIR = Path(__file__).resolve().parents[1]
API_DIR = ROOT_DIR / "apps" / "api"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from app.core.logging import configure_logging, get_logger
from app.db.session import SessionLocal
from app.services.feature_engineering_service import (
    DEFAULT_FEATURE_SET_VERSION,
    FeatureEngineeringService,
    SUPPORTED_PREDICTION_HORIZONS,
)

configure_logging()
logger = get_logger(__name__)


def parse_uuid(value: str) -> UUID:
    return UUID(value)


def parse_as_of_ts(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value_must_be_positive")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build and persist as-of feature snapshots"
    )
    parser.add_argument(
        "--match-id",
        type=parse_uuid,
        default=None,
        help="Build one snapshot for one match",
    )
    parser.add_argument(
        "--competition-id",
        type=parse_uuid,
        default=None,
        help="Restrict batch generation to one competition",
    )
    parser.add_argument(
        "--season-id",
        type=parse_uuid,
        default=None,
        help="Restrict batch to one season_id",
    )
    parser.add_argument(
        "--season", default=None, help="Restrict batch to one season name"
    )
    parser.add_argument(
        "--prediction-horizon",
        choices=SUPPORTED_PREDICTION_HORIZONS,
        default="pre_match",
        help="Prediction horizon to materialize",
    )
    parser.add_argument(
        "--feature-set-version",
        default=DEFAULT_FEATURE_SET_VERSION,
        help="Feature set version label stored in feature_snapshots",
    )
    parser.add_argument(
        "--as-of-ts",
        type=parse_as_of_ts,
        default=None,
        help="Optional explicit as-of timestamp for --match-id only. ISO 8601 expected.",
    )
    parser.add_argument(
        "--include-finished",
        action="store_true",
        help="Include past/finished matches in batch mode. By default only future scheduled/live targets.",
    )
    parser.add_argument(
        "--limit", type=positive_int, default=None, help="Optional cap on batch size"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute snapshots without persisting rows",
    )
    return parser.parse_args()


def _build_item_payload(item: dict) -> dict:
    payload = {
        "match_id": item.get("match_id"),
        "status": item.get("status"),
        "as_of_ts": item.get("as_of_ts"),
        "prediction_horizon": item.get("prediction_horizon"),
        "feature_set_version": item.get("feature_set_version"),
        "completeness_score": item.get("completeness_score"),
    }
    if item.get("warning"):
        payload["warning"] = item["warning"]
    if item.get("error"):
        payload["error"] = item["error"]
    missing_feature_groups = item.get("missing_feature_groups") or []
    data_warnings = item.get("data_warnings") or []
    missing_fields = item.get("missing_fields") or []
    if missing_feature_groups:
        payload["missing_feature_groups"] = missing_feature_groups
    if data_warnings:
        payload["data_warnings"] = data_warnings
    if missing_fields:
        payload["missing_fields"] = missing_fields
    return payload


def main() -> int:
    args = parse_args()
    try:
        with SessionLocal() as db:
            service = FeatureEngineeringService(db)
            if args.match_id is not None:
                result = service.build_feature_snapshot_for_match(
                    match_id=args.match_id,
                    as_of_ts=args.as_of_ts,
                    prediction_horizon=args.prediction_horizon,
                    feature_set_version=args.feature_set_version,
                    persist=not args.dry_run,
                )
                logger.info(
                    "feature_snapshot_script_completed", extra={"mode": "single"}
                )
                logger.info(
                    "feature_snapshot_script_result", extra=_build_item_payload(result)
                )
                return 1 if result.get("status") == "error" else 0

            batch_result = service.build_feature_snapshots(
                competition_id=args.competition_id,
                season_id=args.season_id,
                season=args.season,
                prediction_horizon=args.prediction_horizon,
                feature_set_version=args.feature_set_version,
                future_only=not args.include_finished,
                limit=args.limit,
                persist=not args.dry_run,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("feature_snapshot_script_failed", extra={"error": str(exc)})
        return 1

    logger.info(
        "feature_snapshot_script_completed",
        extra={
            "mode": "batch",
            "target_count": batch_result.get("target_count", 0),
            "created": batch_result.get("created", 0),
            "skipped": batch_result.get("skipped", 0),
            "errors": batch_result.get("errors", 0),
            "summary": batch_result.get("summary", {}),
        },
    )
    for item in batch_result["results"]:
        logger.info("feature_snapshot_script_result", extra=_build_item_payload(item))
    return 1 if int(batch_result.get("errors") or 0) > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
