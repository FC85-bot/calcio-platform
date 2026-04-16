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


def _print_audit_lines(item: dict) -> None:
    if item.get("warning"):
        print(f"  warning={item['warning']}")
    if item.get("error"):
        print(f"  error={item['error']}")

    missing_feature_groups = item.get("missing_feature_groups") or []
    data_warnings = item.get("data_warnings") or []
    missing_fields = item.get("missing_fields") or []

    if missing_feature_groups:
        print(f"  missing_feature_groups={','.join(missing_feature_groups)}")
    if data_warnings:
        print(f"  data_warnings={','.join(data_warnings)}")
    if missing_fields:
        print(f"  missing_fields={','.join(missing_fields)}")


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
                print("Feature snapshot run completed:")
                print(
                    "- match_id={match_id} status={status} as_of_ts={as_of_ts} "
                    "prediction_horizon={prediction_horizon} feature_set_version={feature_set_version} "
                    "completeness_score={completeness_score}".format(**result)
                )
                _print_audit_lines(result)
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
        print(f"Feature snapshots failed: {exc}", file=sys.stderr)
        return 1

    print("Feature snapshot batch completed:")
    print(f"- target_count: {batch_result['target_count']}")
    print(f"- created: {batch_result['created']}")
    print(f"- skipped: {batch_result['skipped']}")
    print(f"- errors: {batch_result['errors']}")
    summary = batch_result.get("summary", {})
    if summary:
        print(f"- created_with_warnings: {summary.get('created_with_warnings', 0)}")
        print(f"- warning_counts: {summary.get('warning_counts', {})}")
        print(f"- error_counts: {summary.get('error_counts', {})}")
    for item in batch_result["results"]:
        print(
            "- match_id={match_id} status={status} as_of_ts={as_of_ts} "
            "feature_set_version={feature_set_version} prediction_horizon={prediction_horizon} "
            "completeness_score={completeness_score}".format(**item)
        )
        _print_audit_lines(item)
    return 1 if int(batch_result.get("errors") or 0) > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
