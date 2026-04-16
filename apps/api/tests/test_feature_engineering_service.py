from __future__ import annotations

from datetime import date

from app.services.feature_engineering_service import FeatureEngineeringService
from tests.helpers.feature_factory import (
    count_feature_snapshots,
    create_competition,
    create_match,
    create_season,
    create_standings_snapshot,
    create_team,
    utc_dt,
)


def _build_full_history_context(db_session):
    competition = create_competition(db_session)
    season = create_season(db_session)

    home_team = create_team(db_session, competition_id=competition.id, name="Home FC")
    away_team = create_team(db_session, competition_id=competition.id, name="Away FC")
    home_opponents = [
        create_team(db_session, competition_id=competition.id, name=f"Home Opponent {idx}")
        for idx in range(1, 6)
    ]
    away_opponents = [
        create_team(db_session, competition_id=competition.id, name=f"Away Opponent {idx}")
        for idx in range(1, 6)
    ]

    create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 5, 20),
        home_team_id=home_team.id,
        away_team_id=home_opponents[0].id,
        home_goals=2,
        away_goals=0,
        status="finished",
    )
    create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 4, 20),
        home_team_id=home_opponents[1].id,
        away_team_id=home_team.id,
        home_goals=1,
        away_goals=1,
        status="finished",
    )
    create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 3, 20),
        home_team_id=home_team.id,
        away_team_id=home_opponents[2].id,
        home_goals=0,
        away_goals=1,
        status="finished",
    )
    create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 2, 20),
        home_team_id=home_opponents[3].id,
        away_team_id=home_team.id,
        home_goals=0,
        away_goals=2,
        status="finished",
    )
    create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 1, 20),
        home_team_id=home_team.id,
        away_team_id=home_opponents[4].id,
        home_goals=3,
        away_goals=2,
        status="finished",
    )

    create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 5, 18),
        home_team_id=away_opponents[0].id,
        away_team_id=away_team.id,
        home_goals=0,
        away_goals=2,
        status="finished",
    )
    create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 4, 18),
        home_team_id=away_team.id,
        away_team_id=away_opponents[1].id,
        home_goals=1,
        away_goals=1,
        status="finished",
    )
    create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 3, 18),
        home_team_id=away_opponents[2].id,
        away_team_id=away_team.id,
        home_goals=2,
        away_goals=0,
        status="finished",
    )
    create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 2, 18),
        home_team_id=away_team.id,
        away_team_id=away_opponents[3].id,
        home_goals=3,
        away_goals=0,
        status="finished",
    )
    create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 1, 18),
        home_team_id=away_opponents[4].id,
        away_team_id=away_team.id,
        home_goals=1,
        away_goals=0,
        status="finished",
    )

    create_standings_snapshot(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        team_id=home_team.id,
        snapshot_date=date(2026, 2, 9),
        position=3,
        points=42,
        played=22,
        won=13,
        drawn=3,
        lost=6,
        goals_for=36,
        goals_against=21,
    )
    create_standings_snapshot(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        team_id=away_team.id,
        snapshot_date=date(2026, 2, 9),
        position=8,
        points=31,
        played=22,
        won=9,
        drawn=4,
        lost=9,
        goals_for=28,
        goals_against=29,
    )

    target_match = create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 10, 18),
        home_team_id=home_team.id,
        away_team_id=away_team.id,
        status="scheduled",
    )

    return {
        "competition": competition,
        "season": season,
        "home_team": home_team,
        "away_team": away_team,
        "target_match": target_match,
    }


def test_feature_form_last_5_is_calculated_correctly(db_session):
    context = _build_full_history_context(db_session)
    service = FeatureEngineeringService(db_session)

    result = service.build_feature_snapshot_for_match(
        match_id=context["target_match"].id,
        persist=False,
    )

    features = result["snapshot"]["features_json"]

    assert result["status"] == "created"
    assert features["home_team_last_5_points"] == 10
    assert features["home_team_last_5_wins"] == 3
    assert features["home_team_last_5_draws"] == 1
    assert features["home_team_last_5_losses"] == 1
    assert features["home_team_last_5_goals_scored"] == 8
    assert features["home_team_last_5_goals_conceded"] == 4


def test_no_leakage_ignores_matches_after_as_of_ts(db_session):
    competition = create_competition(db_session, name="Leakage League")
    season = create_season(db_session, name="2026")
    home_team = create_team(db_session, competition_id=competition.id, name="Leak Home")
    away_team = create_team(db_session, competition_id=competition.id, name="Leak Away")
    opponent_1 = create_team(db_session, competition_id=competition.id, name="Leak Opponent 1")
    opponent_2 = create_team(db_session, competition_id=competition.id, name="Leak Opponent 2")

    create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 10, 9),
        home_team_id=home_team.id,
        away_team_id=opponent_1.id,
        home_goals=1,
        away_goals=0,
        status="finished",
    )
    create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 10, 13),
        home_team_id=home_team.id,
        away_team_id=opponent_2.id,
        home_goals=9,
        away_goals=0,
        status="finished",
    )
    create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 9, 18),
        home_team_id=away_team.id,
        away_team_id=opponent_1.id,
        home_goals=1,
        away_goals=1,
        status="finished",
    )

    target_match = create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 10, 18),
        home_team_id=home_team.id,
        away_team_id=away_team.id,
        status="scheduled",
    )

    service = FeatureEngineeringService(db_session)
    result = service.build_feature_snapshot_for_match(
        match_id=target_match.id,
        as_of_ts=utc_dt(2026, 2, 10, 12),
        persist=False,
    )

    features = result["snapshot"]["features_json"]

    assert result["status"] == "created"
    assert features["home_team_last_5_points"] == 3
    assert features["home_team_last_5_goals_scored"] == 1
    assert features["home_team_last_5_goals_conceded"] == 0


def test_duplicate_snapshot_is_not_recreated(db_session):
    context = _build_full_history_context(db_session)
    service = FeatureEngineeringService(db_session)

    first = service.build_feature_snapshot_for_match(
        match_id=context["target_match"].id, persist=True
    )
    second = service.build_feature_snapshot_for_match(
        match_id=context["target_match"].id, persist=True
    )

    assert first["status"] == "created"
    assert second["status"] == "skipped"
    assert second["warning"] == "duplicate_snapshot"
    assert count_feature_snapshots(db_session) == 1


def test_standings_conservative_cutoff_excludes_same_day_snapshots(db_session):
    competition = create_competition(db_session, name="Standings League")
    season = create_season(db_session, name="2027")
    home_team = create_team(db_session, competition_id=competition.id, name="Standings Home")
    away_team = create_team(db_session, competition_id=competition.id, name="Standings Away")
    opponent = create_team(db_session, competition_id=competition.id, name="Standings Opponent")

    create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 8, 18),
        home_team_id=home_team.id,
        away_team_id=opponent.id,
        home_goals=1,
        away_goals=0,
        status="finished",
    )
    create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 8, 20),
        home_team_id=opponent.id,
        away_team_id=away_team.id,
        home_goals=0,
        away_goals=1,
        status="finished",
    )

    create_standings_snapshot(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        team_id=home_team.id,
        snapshot_date=date(2026, 2, 10),
        position=1,
        points=51,
        played=24,
        won=16,
        drawn=3,
        lost=5,
        goals_for=44,
        goals_against=20,
    )
    create_standings_snapshot(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        team_id=away_team.id,
        snapshot_date=date(2026, 2, 10),
        position=9,
        points=30,
        played=24,
        won=8,
        drawn=6,
        lost=10,
        goals_for=29,
        goals_against=33,
    )

    target_match = create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 10, 18),
        home_team_id=home_team.id,
        away_team_id=away_team.id,
        status="scheduled",
    )

    service = FeatureEngineeringService(db_session)
    result = service.build_feature_snapshot_for_match(match_id=target_match.id, persist=False)

    features = result["snapshot"]["features_json"]
    audit = features["feature_audit"]

    assert features["home_team_league_position"] is None
    assert features["away_team_league_position"] is None
    assert "home_team_standings_excluded_by_cutoff" in audit["data_warnings"]
    assert "away_team_standings_excluded_by_cutoff" in audit["data_warnings"]


def test_completeness_and_missing_breakdown_with_insufficient_history(db_session):
    competition = create_competition(db_session, name="Incomplete League")
    season = create_season(db_session, name="2028")
    home_team = create_team(db_session, competition_id=competition.id, name="Incomplete Home")
    away_team = create_team(db_session, competition_id=competition.id, name="Incomplete Away")
    opponent = create_team(db_session, competition_id=competition.id, name="Incomplete Opponent")

    create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 1, 18),
        home_team_id=home_team.id,
        away_team_id=opponent.id,
        home_goals=2,
        away_goals=1,
        status="finished",
    )

    target_match = create_match(
        db_session,
        competition_id=competition.id,
        season_id=season.id,
        season=season.name,
        match_date=utc_dt(2026, 2, 10, 18),
        home_team_id=home_team.id,
        away_team_id=away_team.id,
        status="scheduled",
    )

    service = FeatureEngineeringService(db_session)
    result = service.build_feature_snapshot_for_match(match_id=target_match.id, persist=False)

    features = result["snapshot"]["features_json"]
    audit = features["feature_audit"]

    assert result["completeness_score"] < 1.0
    assert "home_team_overall_last_5" in audit["missing_feature_groups"]
    assert "away_team_overall_last_5" in audit["missing_feature_groups"]
    assert "home_team_overall_last_5_insufficient_history:1_of_5" in audit["data_warnings"]
    assert "away_team_overall_last_5_insufficient_history:0_of_5" in audit["data_warnings"]
    assert "home_team_last_5_points" in audit["missing_fields"]
    assert "away_team_last_5_points" in audit["missing_fields"]
