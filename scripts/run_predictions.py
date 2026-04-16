from __future__ import annotations

import argparse
from pathlib import Path
import sys
from uuid import UUID

ROOT_DIR = Path(__file__).resolve().parents[1]
API_DIR = ROOT_DIR / "apps" / "api"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from app.core.logging import configure_logging, get_logger
from app.db.session import SessionLocal
from app.services.prediction_service import (
    DEFAULT_PREDICTION_HORIZON,
    PredictionService,
    SUPPORTED_MARKET_CODES,
)

configure_logging()
logger = get_logger(__name__)


def parse_uuid(value: str) -> UUID:
    return UUID(value)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value_must_be_positive")
    return parsed


def parse_markets(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    markets = [item.strip().upper() for item in raw.split(",") if item.strip()]
    if not markets:
        return None
    invalid = [item for item in markets if item not in SUPPORTED_MARKET_CODES]
    if invalid:
        raise argparse.ArgumentTypeError(
            f"Unsupported market codes: {','.join(invalid)}. Allowed: {','.join(SUPPORTED_MARKET_CODES)}"
        )
    return markets


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run MVP predictions from existing feature snapshots"
    )
    parser.add_argument(
        "--match-id",
        type=parse_uuid,
        default=None,
        help="Run predictions for one match",
    )
    parser.add_argument(
        "--competition-id",
        type=parse_uuid,
        default=None,
        help="Restrict batch to one competition",
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
        default=DEFAULT_PREDICTION_HORIZON,
        help="Prediction horizon to read from feature_snapshots",
    )
    parser.add_argument(
        "--feature-set-version",
        default=None,
        help="Optional feature_set_version filter when selecting latest snapshots",
    )
    parser.add_argument(
        "--markets",
        type=parse_markets,
        default=None,
        help="Comma-separated market list: 1X2,OU25,BTTS",
    )
    parser.add_argument(
        "--limit", type=positive_int, default=None, help="Optional batch cap"
    )
    parser.add_argument(
        "--include-finished",
        action="store_true",
        help="Include finished/past matches in batch mode. Default: scheduled/live future matches only.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute predictions without saving rows to DB",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        with SessionLocal() as db:
            service = PredictionService(db)
            result = service.build_predictions(
                match_id=args.match_id,
                competition_id=args.competition_id,
                season_id=args.season_id,
                season=args.season,
                prediction_horizon=args.prediction_horizon,
                feature_set_version=args.feature_set_version,
                future_only=not args.include_finished,
                limit=args.limit,
                markets=args.markets,
                persist=not args.dry_run,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("prediction_script_failed", extra={"error": str(exc)})
        print(f"Prediction batch failed: {exc}", file=sys.stderr)
        return 1

    print("Prediction batch completed:")
    print(f"- target_count: {result['target_count']}")
    print(f"- created: {result['created']}")
    print(f"- skipped: {result['skipped']}")
    print(f"- errors: {result['errors']}")
    summary = result.get("summary", {})
    if summary:
        print(f"- warning_counts: {summary.get('warning_counts', {})}")
        print(f"- error_counts: {summary.get('error_counts', {})}")
    for item in result["results"]:
        print(
            "- match_id={match_id} market_code={market_code} status={status} model_version_id={model_version_id} "
            "feature_snapshot_id={feature_snapshot_id}".format(
                match_id=item.get("match_id"),
                market_code=item.get("market_code"),
                status=item.get("status"),
                model_version_id=item.get("model_version_id"),
                feature_snapshot_id=item.get("feature_snapshot_id"),
            )
        )
        if item.get("warning"):
            print(f"  warning={item['warning']}")
        if item.get("error"):
            print(f"  error={item['error']}")
    return 1 if int(result.get("errors") or 0) > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
