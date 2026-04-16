from __future__ import annotations

from decimal import Decimal

from sqlalchemy import select

from app.models.bookmaker import Bookmaker
from app.models.feature_snapshot import FeatureSnapshot
from app.models.market import Market
from app.models.odds import Odds
from app.models.prediction import Prediction
from app.models.prediction_selection import PredictionSelection
from app.models.provider import Provider
from app.services.feature_engineering_service import FeatureEngineeringService
from app.services.prediction_service import PredictionService, seed_prediction_model_registry
from tests.helpers.feature_factory import utc_dt
from tests.test_feature_engineering_service import _build_full_history_context


def _seed_odds_context(db_session, match_id):
    provider = Provider(name="the_odds_api")
    bookmaker_a = Bookmaker(name="Book A")
    bookmaker_b = Bookmaker(name="Book B")
    market_1x2 = Market(code="1X2", name="1X2")
    market_ou = Market(code="OU", name="Over/Under")
    market_btts = Market(code="BTTS", name="Both Teams To Score")
    db_session.add_all([provider, bookmaker_a, bookmaker_b, market_1x2, market_ou, market_btts])
    db_session.commit()

    snapshot_ts = utc_dt(2026, 2, 9, 12)
    later_snapshot_ts = utc_dt(2026, 2, 9, 13)

    odds_rows = [
        Odds(
            match_id=match_id,
            provider_id=provider.id,
            bookmaker_id=bookmaker_a.id,
            market_id=market_1x2.id,
            selection_code="HOME",
            line_value=None,
            odds_value=Decimal("2.10"),
            snapshot_timestamp=snapshot_ts,
        ),
        Odds(
            match_id=match_id,
            provider_id=provider.id,
            bookmaker_id=bookmaker_a.id,
            market_id=market_1x2.id,
            selection_code="DRAW",
            line_value=None,
            odds_value=Decimal("3.20"),
            snapshot_timestamp=snapshot_ts,
        ),
        Odds(
            match_id=match_id,
            provider_id=provider.id,
            bookmaker_id=bookmaker_a.id,
            market_id=market_1x2.id,
            selection_code="AWAY",
            line_value=None,
            odds_value=Decimal("3.60"),
            snapshot_timestamp=snapshot_ts,
        ),
        Odds(
            match_id=match_id,
            provider_id=provider.id,
            bookmaker_id=bookmaker_b.id,
            market_id=market_1x2.id,
            selection_code="HOME",
            line_value=None,
            odds_value=Decimal("2.22"),
            snapshot_timestamp=later_snapshot_ts,
        ),
        Odds(
            match_id=match_id,
            provider_id=provider.id,
            bookmaker_id=bookmaker_b.id,
            market_id=market_1x2.id,
            selection_code="DRAW",
            line_value=None,
            odds_value=Decimal("3.10"),
            snapshot_timestamp=later_snapshot_ts,
        ),
        Odds(
            match_id=match_id,
            provider_id=provider.id,
            bookmaker_id=bookmaker_b.id,
            market_id=market_1x2.id,
            selection_code="AWAY",
            line_value=None,
            odds_value=Decimal("3.50"),
            snapshot_timestamp=later_snapshot_ts,
        ),
        Odds(
            match_id=match_id,
            provider_id=provider.id,
            bookmaker_id=bookmaker_a.id,
            market_id=market_ou.id,
            selection_code="OVER",
            line_value=Decimal("2.500"),
            odds_value=Decimal("1.95"),
            snapshot_timestamp=snapshot_ts,
        ),
        Odds(
            match_id=match_id,
            provider_id=provider.id,
            bookmaker_id=bookmaker_a.id,
            market_id=market_ou.id,
            selection_code="UNDER",
            line_value=Decimal("2.500"),
            odds_value=Decimal("1.90"),
            snapshot_timestamp=snapshot_ts,
        ),
        Odds(
            match_id=match_id,
            provider_id=provider.id,
            bookmaker_id=bookmaker_b.id,
            market_id=market_ou.id,
            selection_code="OVER",
            line_value=Decimal("2.500"),
            odds_value=Decimal("2.02"),
            snapshot_timestamp=later_snapshot_ts,
        ),
        Odds(
            match_id=match_id,
            provider_id=provider.id,
            bookmaker_id=bookmaker_b.id,
            market_id=market_ou.id,
            selection_code="UNDER",
            line_value=Decimal("2.500"),
            odds_value=Decimal("1.88"),
            snapshot_timestamp=later_snapshot_ts,
        ),
        Odds(
            match_id=match_id,
            provider_id=provider.id,
            bookmaker_id=bookmaker_a.id,
            market_id=market_btts.id,
            selection_code="YES",
            line_value=None,
            odds_value=Decimal("1.80"),
            snapshot_timestamp=snapshot_ts,
        ),
        Odds(
            match_id=match_id,
            provider_id=provider.id,
            bookmaker_id=bookmaker_a.id,
            market_id=market_btts.id,
            selection_code="NO",
            line_value=None,
            odds_value=Decimal("1.95"),
            snapshot_timestamp=snapshot_ts,
        ),
        Odds(
            match_id=match_id,
            provider_id=provider.id,
            bookmaker_id=bookmaker_b.id,
            market_id=market_btts.id,
            selection_code="YES",
            line_value=None,
            odds_value=Decimal("1.86"),
            snapshot_timestamp=later_snapshot_ts,
        ),
        Odds(
            match_id=match_id,
            provider_id=provider.id,
            bookmaker_id=bookmaker_b.id,
            market_id=market_btts.id,
            selection_code="NO",
            line_value=None,
            odds_value=Decimal("1.99"),
            snapshot_timestamp=later_snapshot_ts,
        ),
    ]
    db_session.add_all(odds_rows)
    db_session.commit()


def test_seed_prediction_model_registry_creates_versions(db_session):
    result = seed_prediction_model_registry(db_session)

    assert result["model_registry_created"] == 3
    assert result["model_versions_created"] == 3


def test_prediction_service_creates_predictions_and_selections(db_session):
    context = _build_full_history_context(db_session)
    target_match = context["target_match"]

    feature_service = FeatureEngineeringService(db_session)
    feature_result = feature_service.build_feature_snapshot_for_match(
        match_id=target_match.id, persist=True
    )
    assert feature_result["status"] == "created"

    seed_prediction_model_registry(db_session)
    _seed_odds_context(db_session, target_match.id)

    service = PredictionService(db_session)
    result = service.build_predictions_for_match(match_id=target_match.id, persist=True)

    assert result["created"] == 3
    assert result["errors"] == 0
    assert result["skipped"] == 0

    predictions = (
        db_session.execute(select(Prediction).order_by(Prediction.market_code.asc()))
        .scalars()
        .all()
    )
    selections = db_session.execute(select(PredictionSelection)).scalars().all()

    assert len(predictions) == 3
    assert len(selections) == 7

    one_x_two = next(item for item in result["results"] if item["market_code"] == "1X2")
    assert (
        abs(sum(selection["predicted_probability"] for selection in one_x_two["selections"]) - 1.0)
        < 0.001
    )
    home_selection = next(
        item for item in one_x_two["selections"] if item["selection_code"] == "HOME"
    )
    assert home_selection["market_best_odds"] == 2.22
    assert home_selection["fair_odds"] >= 1.0
    assert home_selection["edge_pct"] is not None

    ou25 = next(item for item in result["results"] if item["market_code"] == "OU25")
    over_selection = next(item for item in ou25["selections"] if item["selection_code"] == "OVER")
    assert over_selection["market_best_odds"] == 2.02

    btts = next(item for item in result["results"] if item["market_code"] == "BTTS")
    yes_selection = next(item for item in btts["selections"] if item["selection_code"] == "YES")
    assert yes_selection["market_best_odds"] == 1.86


def test_prediction_service_skips_low_completeness_snapshot(db_session):
    context = _build_full_history_context(db_session)
    target_match = context["target_match"]

    feature_service = FeatureEngineeringService(db_session)
    feature_service.build_feature_snapshot_for_match(match_id=target_match.id, persist=True)
    feature_snapshot = db_session.execute(select(FeatureSnapshot)).scalar_one()
    feature_snapshot.completeness_score = 0.20
    db_session.commit()

    seed_prediction_model_registry(db_session)
    service = PredictionService(db_session)
    result = service.build_predictions_for_match(match_id=target_match.id, persist=True)

    assert result["created"] == 0
    assert result["skipped"] == 3
    assert all(
        item["warning"] == "feature_snapshot_completeness_too_low" for item in result["results"]
    )
