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
            logger.error(
                "evaluation_script_invalid_market_code",
                extra={
                    "market_code": raw_market,
                    "supported_markets": list(SUPPORTED_MARKETS),
                },
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
        return 1

    logger.info(
        "evaluation_script_completed",
        extra={
            "evaluation_run_id": result.get("id"),
            "code": result.get("code"),
            "name": result.get("name"),
            "market_code": result.get("market_code"),
            "status": result.get("status"),
            "sample_size": result.get("sample_size"),
            "period_start": result.get("period_start"),
            "period_end": result.get("period_end"),
            "global_metrics": result.get("global_metrics", []),
            "quality_checks": (result.get("config_json") or {}).get(
                "quality_checks", {}
            ),
            "warnings": (result.get("config_json") or {}).get("warnings", []),
        },
    )
    return 0 if result.get("status") == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
