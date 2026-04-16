from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy.orm import Session

from app.models.competition import Competition
from app.models.feature_snapshot import FeatureSnapshot
from app.models.match import Match
from app.models.season import Season
from app.models.standings_snapshot import StandingsSnapshot
from app.models.team import Team


def utc_dt(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


def create_competition(db: Session, *, name: str = "Serie A", country: str = "IT") -> Competition:
    row = Competition(name=name, country=country)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def create_season(
    db: Session,
    *,
    name: str = "2025/2026",
    start_date: date = date(2025, 8, 1),
    end_date: date = date(2026, 5, 31),
) -> Season:
    row = Season(name=name, start_date=start_date, end_date=end_date)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def create_team(db: Session, *, competition_id, name: str) -> Team:
    row = Team(name=name, competition_id=competition_id)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def create_match(
    db: Session,
    *,
    competition_id,
    season: str,
    match_date: datetime,
    home_team_id,
    away_team_id,
    season_id=None,
    home_goals: int | None = None,
    away_goals: int | None = None,
    status: str = "scheduled",
) -> Match:
    row = Match(
        competition_id=competition_id,
        season_id=season_id,
        season=season,
        match_date=match_date,
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        home_goals=home_goals,
        away_goals=away_goals,
        status=status,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def create_standings_snapshot(
    db: Session,
    *,
    competition_id,
    season_id,
    team_id,
    snapshot_date: date,
    position: int,
    points: int,
    played: int,
    won: int,
    drawn: int,
    lost: int,
    goals_for: int,
    goals_against: int,
) -> StandingsSnapshot:
    row = StandingsSnapshot(
        competition_id=competition_id,
        season_id=season_id,
        team_id=team_id,
        snapshot_date=snapshot_date,
        position=position,
        points=points,
        played=played,
        won=won,
        drawn=drawn,
        lost=lost,
        goals_for=goals_for,
        goals_against=goals_against,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def count_feature_snapshots(db: Session) -> int:
    return db.query(FeatureSnapshot).count()
