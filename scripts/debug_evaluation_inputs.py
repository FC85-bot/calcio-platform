from __future__ import annotations

import argparse
from datetime import UTC, datetime, time
from pathlib import Path
import sys

from sqlalchemy import func, select

ROOT_DIR = Path(__file__).resolve().parents[1]
API_DIR = ROOT_DIR / "apps" / "api"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from app.core.logging import configure_logging
from app.db.session import SessionLocal
from app.models.feature_snapshot import FeatureSnapshot
from app.models.match import Match
from app.models.prediction import Prediction
from app.models.prediction_selection import PredictionSelection

configure_logging()


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
    parser = argparse.ArgumentParser(description="Debug evaluation input coverage")
    parser.add_argument("--period-start", required=True, type=parse_period_start)
    parser.add_argument("--period-end", required=True, type=parse_period_end)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    with SessionLocal() as db:
        global_min_max = db.execute(
            select(
                func.min(Match.match_date),
                func.max(Match.match_date),
            )
        ).one()

        finished_matches = db.execute(
            select(func.count(Match.id)).where(
                Match.status == "finished",
                Match.match_date >= args.period_start,
                Match.match_date <= args.period_end,
            )
        ).scalar_one()

        finished_matches_with_scores = db.execute(
            select(func.count(Match.id)).where(
                Match.status == "finished",
                Match.match_date >= args.period_start,
                Match.match_date <= args.period_end,
                Match.home_goals.is_not(None),
                Match.away_goals.is_not(None),
            )
        ).scalar_one()

        feature_snapshots = db.execute(
            select(func.count(FeatureSnapshot.id))
            .join(Match, Match.id == FeatureSnapshot.match_id)
            .where(
                Match.status == "finished",
                Match.match_date >= args.period_start,
                Match.match_date <= args.period_end,
                FeatureSnapshot.prediction_horizon == "pre_match",
                FeatureSnapshot.as_of_ts <= Match.match_date,
            )
        ).scalar_one()

        prediction_rows = db.execute(
            select(
                Prediction.market_code,
                func.count(Prediction.id),
            )
            .join(Match, Match.id == Prediction.match_id)
            .where(
                Match.status == "finished",
                Match.match_date >= args.period_start,
                Match.match_date <= args.period_end,
                Prediction.prediction_horizon == "pre_match",
                Prediction.as_of_ts <= Match.match_date,
            )
            .group_by(Prediction.market_code)
            .order_by(Prediction.market_code.asc())
        ).all()

        selection_rows = db.execute(
            select(
                Prediction.market_code,
                func.count(PredictionSelection.id),
            )
            .join(Prediction, Prediction.id == PredictionSelection.prediction_id)
            .join(Match, Match.id == Prediction.match_id)
            .where(
                Match.status == "finished",
                Match.match_date >= args.period_start,
                Match.match_date <= args.period_end,
                Prediction.prediction_horizon == "pre_match",
                Prediction.as_of_ts <= Match.match_date,
            )
            .group_by(Prediction.market_code)
            .order_by(Prediction.market_code.asc())
        ).all()

    print("DEBUG EVALUATION INPUTS")
    print(f"- requested_period_start: {args.period_start.isoformat()}")
    print(f"- requested_period_end:   {args.period_end.isoformat()}")
    print(f"- global_match_date_min:  {global_min_max[0]}")
    print(f"- global_match_date_max:  {global_min_max[1]}")
    print(f"- finished_matches:       {finished_matches}")
    print(f"- finished_with_scores:   {finished_matches_with_scores}")
    print(f"- feature_snapshots:      {feature_snapshots}")

    print("- predictions_by_market:")
    if prediction_rows:
        for market_code, count_value in prediction_rows:
            print(f"  * {market_code}: {count_value}")
    else:
        print("  * none")

    print("- prediction_selections_by_market:")
    if selection_rows:
        for market_code, count_value in selection_rows:
            print(f"  * {market_code}: {count_value}")
    else:
        print("  * none")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
