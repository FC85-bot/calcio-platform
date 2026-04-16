from __future__ import annotations

import argparse
from datetime import UTC, datetime, time
from pathlib import Path
import sys
from uuid import UUID

ROOT_DIR = Path(__file__).resolve().parents[1]
API_DIR = ROOT_DIR / "apps" / "api"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from app.core.logging import configure_logging, get_logger
from app.db.session import SessionLocal
from app.services.evaluation_service import (
    ALL_MARKETS_CODE,
    DEFAULT_PREDICTION_HORIZON,
    EvaluationService,
)

configure_logging()
logger = get_logger(__name__)
SUPPORTED_MARKETS = ("1X2", "OU25", "BTTS")


def parse_uuid(value: str) -> UUID:
    return UUID(value)


def parse_period_start(raw: str) -> datetime:
    value = datetime.fromisoformat(raw)
    if value.tzinfo is None and raw.count(":") == 0:
        value = datetime.combine(value.date(), time.min, tzinfo=UTC)
    elif value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def parse_period_end(raw: str) -> datetime:
    value = datetime.fromisoformat(raw)
    if value.tzinfo is None and raw.count(":") == 0:
        value = datetime.combine(value.date(), time.max, tzinfo=UTC)
    elif value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run historical backtest/evaluation on stored predictions"
    )
    parser.add_argument(
        "--period-start",
        required=True,
        type=parse_period_start,
        help="ISO datetime or date",
    )
    parser.add_argument(
        "--period-end",
        required=True,
        type=parse_period_end,
        help="ISO datetime or date",
    )
    parser.add_argument(
        "--market-code", default=ALL_MARKETS_CODE, help="1X2, OU25, BTTS or ALL"
    )
    parser.add_argument(
        "--prediction-horizon",
        default=DEFAULT_PREDICTION_HORIZON,
        help="Prediction horizon filter. Default: pre_match",
    )
    parser.add_argument("--model-version-id", type=parse_uuid, default=None)
    parser.add_argument(
        "--code", default=None, help="Optional explicit evaluation run code"
    )
    parser.add_argument(
        "--name", default=None, help="Optional explicit evaluation run name"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    raw_market = args.market_code.strip().upper()
    if raw_market == ALL_MARKETS_CODE:
        markets = list(SUPPORTED_MARKETS)
        market_code = None
    else:
        if raw_market not in SUPPORTED_MARKETS:
            print(
                f"Unsupported market code: {raw_market}. Allowed: ALL,{','.join(SUPPORTED_MARKETS)}",
                file=sys.stderr,
            )
            return 2
        markets = None
        market_code = raw_market

    try:
        with SessionLocal() as db:
            service = EvaluationService(db)
            result = service.run_backtest(
                period_start=args.period_start,
                period_end=args.period_end,
                market_code=market_code,
                markets=markets,
                model_version_id=args.model_version_id,
                prediction_horizon=args.prediction_horizon,
                code=args.code,
                name=args.name,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("evaluation_script_failed", extra={"error": str(exc)})
        print(f"Evaluation failed: {exc}", file=sys.stderr)
        return 1

    print("Evaluation completed:")
    print(f"- evaluation_run_id: {result['id']}")
    print(f"- code: {result['code']}")
    print(f"- name: {result['name']}")
    print(f"- market_code: {result['market_code']}")
    print(f"- status: {result['status']}")
    print(f"- sample_size: {result.get('sample_size')}")
    print(f"- period_start: {result['period_start']}")
    print(f"- period_end: {result['period_end']}")

    metrics = result.get("global_metrics", [])
    if metrics:
        print("- global_metrics:")
        for metric in metrics:
            print(f"  * {metric['metric_code']}={metric['metric_value']}")

    config = result.get("config_json") or {}
    quality_checks = config.get("quality_checks", {})
    if quality_checks:
        print("- quality_checks:")
        for key, value in quality_checks.items():
            print(f"  * {key}={value}")

    warnings = config.get("warnings", [])
    if warnings:
        print(f"- warnings: {warnings}")

    return 0 if result.get("status") == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
