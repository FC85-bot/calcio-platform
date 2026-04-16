from __future__ import annotations

from decimal import Decimal

from app.models.bookmaker import Bookmaker
from app.models.market import Market
from app.models.odds import Odds
from app.models.provider import Provider
from app.services.feature_engineering_service import FeatureEngineeringService
from app.services.match_report_service import MatchReportService
from app.services.prediction_service import PredictionService, seed_prediction_model_registry
from tests.helpers.feature_factory import (
    create_competition,
    create_match,
    create_season,
    create_team,
    utc_dt,
)
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


def test_match_report_service_builds_full_report(db_session):
    context = _build_full_history_context(db_session)
    target_match = context["target_match"]

    FeatureEngineeringService(db_session).build_feature_snapshot_for_match(
        match_id=target_match.id, persist=True
    )
    seed_prediction_model_registry(db_session)
    _seed_odds_context(db_session, target_match.id)
    PredictionService(db_session).build_predictions_for_match(
        match_id=target_match.id, persist=True
    )

    report = MatchReportService(db_session).build_match_report(match_id=target_match.id)

    assert report["report_version"] == "sprint13_match_report_v1"
    assert report["context"]["match_id"] == target_match.id
    assert report["standings_context"]["available"] is True
    assert len(report["odds"]) == 3
    assert all(block["available"] is True for block in report["odds"])
    assert len(report["predictions"]) == 3
    assert all(block["available"] is True for block in report["predictions"])

    codes = {warning["code"] for warning in report["warnings"]}
    assert "missing_standings" not in codes
    assert "missing_prediction_1x2" not in codes
    assert "missing_odds_1x2" not in codes
    assert "missing_feature_snapshot" not in codes


def test_match_report_service_returns_partial_report_with_warnings(db_session):
    competition = create_competition(db_session, name="Sparse League")
    season = create_season(db_session, name="2027/2028")
    home_team = create_team(db_session, competition_id=competition.id, name="Sparse Home")
    away_team = create_team(db_session, competition_id=competition.id, name="Sparse Away")
    target_match = create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 6, 10, 18),
        home_team_id=home_team.id,
        away_team_id=away_team.id,
        status="scheduled",
    )

    report = MatchReportService(db_session).build_match_report(match_id=target_match.id)

    assert report["context"]["match_id"] == target_match.id
    assert report["standings_context"]["available"] is False

    warning_codes = {warning["code"] for warning in report["warnings"]}
    assert "missing_standings" in warning_codes
    assert "missing_odds_1x2" in warning_codes
    assert "missing_prediction_1x2" in warning_codes
    assert "missing_feature_snapshot" in warning_codes
    assert "missing_form_home" in warning_codes
    assert "missing_form_away" in warning_codes
