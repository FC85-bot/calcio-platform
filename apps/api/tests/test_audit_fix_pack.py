from __future__ import annotations

from decimal import Decimal

from sqlalchemy import func, select

from app.models.bookmaker import Bookmaker
from app.models.competition import Competition
from app.models.market import Market
from app.models.match import Match
from app.models.odds import Odds
from app.models.provider import Provider
from app.models.provider_entity import ProviderEntity
from app.models.season import Season
from app.models.team import Team
from app.providers.mock_provider import MockProvider
from app.services.normalization_service import NormalizationService
from app.services.raw_ingestion_service import RawIngestionService
from tests.helpers.feature_factory import (
    create_competition,
    create_match,
    create_season,
    create_team,
    utc_dt,
)


def _seed_match_with_odds(db_session):
    competition = create_competition(db_session, name="Serie A", country="Italy")
    season = create_season(db_session, name="2026/2027")
    home_team = create_team(db_session, competition_id=competition.id, name="Inter")
    away_team = create_team(db_session, competition_id=competition.id, name="Milan")
    match = create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 8, 20, 18, 45),
        home_team_id=home_team.id,
        away_team_id=away_team.id,
        status="scheduled",
    )

    provider = Provider(name="the_odds_api")
    bookmaker = Bookmaker(name="Book A")
    market_ou = Market(code="OU", name="Over/Under")
    db_session.add_all([provider, bookmaker, market_ou])
    db_session.commit()

    snapshot_ts = utc_dt(2026, 8, 19, 10, 0)
    later_snapshot_ts = utc_dt(2026, 8, 19, 18, 0)
    db_session.add_all(
        [
            Odds(
                match_id=match.id,
                provider_id=provider.id,
                bookmaker_id=bookmaker.id,
                market_id=market_ou.id,
                selection_code="OVER",
                line_value=Decimal("2.500"),
                odds_value=Decimal("1.95"),
                snapshot_timestamp=snapshot_ts,
            ),
            Odds(
                match_id=match.id,
                provider_id=provider.id,
                bookmaker_id=bookmaker.id,
                market_id=market_ou.id,
                selection_code="UNDER",
                line_value=Decimal("2.500"),
                odds_value=Decimal("1.90"),
                snapshot_timestamp=snapshot_ts,
            ),
            Odds(
                match_id=match.id,
                provider_id=provider.id,
                bookmaker_id=bookmaker.id,
                market_id=market_ou.id,
                selection_code="OVER",
                line_value=Decimal("2.500"),
                odds_value=Decimal("2.01"),
                snapshot_timestamp=later_snapshot_ts,
            ),
            Odds(
                match_id=match.id,
                provider_id=provider.id,
                bookmaker_id=bookmaker.id,
                market_id=market_ou.id,
                selection_code="UNDER",
                line_value=Decimal("2.500"),
                odds_value=Decimal("1.86"),
                snapshot_timestamp=later_snapshot_ts,
            ),
        ]
    )
    db_session.commit()
    return match


def test_mock_provider_pipeline_populates_provider_entities_and_is_idempotent(
    db_session, monkeypatch, tmp_path
):
    monkeypatch.setenv("RAW_STORAGE_PATH", str(tmp_path / "raw"))

    provider = MockProvider()
    raw_service = RawIngestionService(db=db_session, provider=provider)

    first_ingestion = raw_service.run_full_ingestion(include_odds=True)
    second_ingestion = raw_service.run_full_ingestion(include_odds=True)

    assert first_ingestion["competitions"]["created_count"] == 1
    assert first_ingestion["seasons"]["created_count"] == 1
    assert first_ingestion["teams"]["created_count"] == 1
    assert first_ingestion["matches"]["created_count"] == 1
    assert first_ingestion["odds"]["created_count"] == 1
    assert second_ingestion["competitions"]["skipped_count"] == 1
    assert second_ingestion["seasons"]["skipped_count"] == 1
    assert second_ingestion["teams"]["skipped_count"] == 1
    assert second_ingestion["matches"]["skipped_count"] == 1
    assert second_ingestion["odds"]["skipped_count"] == 1

    normalization_service = NormalizationService(db_session)
    first_normalization = normalization_service.run(
        entity_types=["competitions", "seasons", "teams", "matches", "odds"],
        provider="mock_provider",
    )
    second_normalization = normalization_service.run(
        entity_types=["competitions", "seasons", "teams", "matches", "odds"],
        provider="mock_provider",
        include_processed=True,
    )
    provider.close()

    assert all(item["status"] == "success" for item in first_normalization)
    assert all(item["error_count"] == 0 for item in first_normalization)
    assert all(item["error_count"] == 0 for item in second_normalization)

    assert db_session.execute(select(func.count(Competition.id))).scalar_one() == 1
    assert db_session.execute(select(func.count(Season.id))).scalar_one() == 1
    assert db_session.execute(select(func.count(Team.id))).scalar_one() == 6
    assert db_session.execute(select(func.count(Match.id))).scalar_one() == 15
    assert db_session.execute(select(func.count(Odds.id))).scalar_one() > 0

    mapping_counts = dict(
        db_session.execute(
            select(ProviderEntity.entity_type, func.count(ProviderEntity.id)).group_by(
                ProviderEntity.entity_type
            )
        ).all()
    )
    assert db_session.execute(select(func.count(ProviderEntity.id))).scalar_one() > 0
    assert mapping_counts["competition"] == 1
    assert mapping_counts["season"] == 1
    assert mapping_counts["team"] == 6
    assert mapping_counts["match"] == 15
    assert mapping_counts["bookmaker"] == 2
    assert mapping_counts["market"] == 3


def test_odds_endpoints_accept_ou25_public_contract(client, db_session):
    match = _seed_match_with_odds(db_session)

    for endpoint in ("latest", "history", "best", "opening"):
        response = client.get(
            f"/api/v1/matches/{match.id}/odds/{endpoint}",
            params={"market_code": "OU25"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload
        assert all(item["market_code"] == "OU25" for item in payload)
        assert all(item["line_value"] == 2.5 for item in payload)
