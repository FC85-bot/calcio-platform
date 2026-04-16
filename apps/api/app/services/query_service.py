from __future__ import annotations

from datetime import date, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import case, desc, func, or_, select
from sqlalchemy.orm import Session, aliased

from app.models.competition import Competition
from app.models.competition_season import CompetitionSeason
from app.models.ingestion_run import IngestionRun
from app.models.match import Match
from app.models.provider import Provider
from app.models.season import Season
from app.models.standings_snapshot import StandingsSnapshot
from app.models.team import Team
from app.services.odds_query_service import OddsQueryService
from app.services.stats_service import StatsService


class QueryService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def list_competitions(
        self,
        *,
        season_id: UUID | None = None,
        season: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        statement = select(Competition)

        if season_id is not None or season is not None:
            statement = statement.join(
                CompetitionSeason,
                CompetitionSeason.competition_id == Competition.id,
            )
            if season is not None:
                statement = statement.join(Season, Season.id == CompetitionSeason.season_id)
            if season_id is not None:
                statement = statement.where(CompetitionSeason.season_id == season_id)
            if season is not None:
                statement = statement.where(Season.name == season)
            statement = statement.distinct()

        statement = (
            statement.order_by(Competition.name.asc(), Competition.id.asc())
            .offset(offset)
            .limit(limit)
        )
        competitions = self.db.execute(statement).scalars().all()
        return [self._serialize_competition(competition) for competition in competitions]

    def list_seasons(
        self,
        *,
        competition_id: UUID | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        statement = select(Season)
        if competition_id is not None:
            statement = (
                statement.join(CompetitionSeason, CompetitionSeason.season_id == Season.id)
                .where(CompetitionSeason.competition_id == competition_id)
                .distinct()
            )

        statement = (
            statement.order_by(Season.end_date.desc(), Season.start_date.desc(), Season.name.desc())
            .offset(offset)
            .limit(limit)
        )
        seasons = self.db.execute(statement).scalars().all()
        return [self._serialize_season(season) for season in seasons]

    def list_teams(
        self,
        *,
        competition_id: UUID | None = None,
        search: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        statement = select(Team, Competition).join(
            Competition, Competition.id == Team.competition_id
        )

        if competition_id is not None:
            statement = statement.where(Team.competition_id == competition_id)
        if search is not None:
            statement = statement.where(Team.name.ilike(f"%{search.strip()}%"))

        statement = (
            statement.order_by(Competition.name.asc(), Team.name.asc(), Team.id.asc())
            .offset(offset)
            .limit(limit)
        )

        rows = self.db.execute(statement).all()
        return [
            {
                "id": team.id,
                "name": team.name,
                "competition_id": team.competition_id,
                "created_at": team.created_at,
                "competition": self._serialize_competition_summary(competition),
            }
            for team, competition in rows
        ]

    def get_matches(
        self,
        *,
        competition_id: UUID | None = None,
        season_id: UUID | None = None,
        season: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        team_id: UUID | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
        include_latest_odds: bool = False,
    ) -> list[dict[str, Any]]:
        home_team = aliased(Team)
        away_team = aliased(Team)
        statement = (
            select(Match, Competition, Season, home_team, away_team)
            .join(Competition, Competition.id == Match.competition_id)
            .outerjoin(Season, Season.id == Match.season_id)
            .join(home_team, home_team.id == Match.home_team_id)
            .join(away_team, away_team.id == Match.away_team_id)
        )

        if competition_id is not None:
            statement = statement.where(Match.competition_id == competition_id)
        if season_id is not None:
            statement = statement.where(Match.season_id == season_id)
        if season is not None:
            statement = statement.where(Match.season == season)
        if date_from is not None:
            statement = statement.where(Match.match_date >= date_from)
        if date_to is not None:
            statement = statement.where(Match.match_date <= date_to)
        if team_id is not None:
            statement = statement.where(
                or_(Match.home_team_id == team_id, Match.away_team_id == team_id)
            )
        if status is not None:
            statement = statement.where(Match.status == status)

        statement = (
            statement.order_by(Match.match_date.desc(), Match.id.desc()).offset(offset).limit(limit)
        )
        rows = self.db.execute(statement).all()

        latest_odds_map: dict[UUID, list[dict[str, Any]]] = {}
        if include_latest_odds and rows:
            latest_odds_map = self.get_latest_odds_map([row[0].id for row in rows])

        return [
            self._serialize_match_row(
                match=row[0],
                competition=row[1],
                season=row[2],
                home_team=row[3],
                away_team=row[4],
                latest_odds=latest_odds_map.get(row[0].id) if include_latest_odds else None,
            )
            for row in rows
        ]

    def get_match_by_id(
        self, match_id: UUID, *, include_latest_odds: bool = True
    ) -> dict[str, Any] | None:
        home_team = aliased(Team)
        away_team = aliased(Team)
        statement = (
            select(Match, Competition, Season, home_team, away_team)
            .join(Competition, Competition.id == Match.competition_id)
            .outerjoin(Season, Season.id == Match.season_id)
            .join(home_team, home_team.id == Match.home_team_id)
            .join(away_team, away_team.id == Match.away_team_id)
            .where(Match.id == match_id)
        )
        row = self.db.execute(statement).one_or_none()
        if row is None:
            return None

        latest_odds: list[dict[str, Any]] | None = None
        if include_latest_odds:
            latest_odds = self.get_latest_odds_map([match_id]).get(match_id, [])

        return self._serialize_match_row(
            match=row[0],
            competition=row[1],
            season=row[2],
            home_team=row[3],
            away_team=row[4],
            latest_odds=latest_odds,
        )

    def get_team_detail(
        self,
        team_id: UUID,
        *,
        season_id: UUID | None = None,
        season: str | None = None,
        form_last_n: int = 5,
    ) -> dict[str, Any] | None:
        statement = (
            select(Team, Competition)
            .join(Competition, Competition.id == Team.competition_id)
            .where(Team.id == team_id)
        )
        row = self.db.execute(statement).one_or_none()
        if row is None:
            return None

        team, competition = row
        resolved_season, resolved_season_name = self._resolve_team_season(
            team_id=team.id,
            competition_id=team.competition_id,
            season_id=season_id,
            season=season,
        )

        stats = None
        form = None
        streak = None
        if resolved_season_name is not None:
            stats_service = StatsService(self.db)
            stats = stats_service.get_team_stats(
                team_id,
                competition_id=team.competition_id,
                season=resolved_season_name,
            )
            form = stats_service.get_team_form(
                team_id,
                competition_id=team.competition_id,
                season=resolved_season_name,
                last_n=form_last_n,
            )
            streak = stats_service.get_team_streak(
                team_id,
                competition_id=team.competition_id,
                season=resolved_season_name,
            )

        return {
            "id": team.id,
            "name": team.name,
            "competition_id": team.competition_id,
            "created_at": team.created_at,
            "competition": self._serialize_competition_summary(competition),
            "season_id": resolved_season.id if resolved_season is not None else None,
            "season": resolved_season_name,
            "season_detail": self._serialize_season_summary(resolved_season)
            if resolved_season is not None
            else None,
            "stats": stats,
            "form": form,
            "streak": streak,
        }

    def get_standings(
        self,
        *,
        competition_id: UUID,
        season_id: UUID | None = None,
        season: str | None = None,
    ) -> dict[str, Any] | None:
        competition = self.db.get(Competition, competition_id)
        if competition is None:
            return None

        resolved_season, resolved_season_name = self._resolve_competition_season(
            competition_id=competition_id,
            season_id=season_id,
            season=season,
            infer_latest=True,
        )

        snapshot_date: date | None = None
        standings: list[dict[str, Any]] = []
        source = "computed_fallback"

        if resolved_season is not None:
            snapshot_date = self.db.execute(
                select(func.max(StandingsSnapshot.snapshot_date)).where(
                    StandingsSnapshot.competition_id == competition_id,
                    StandingsSnapshot.season_id == resolved_season.id,
                )
            ).scalar_one()

            if snapshot_date is not None:
                statement = (
                    select(StandingsSnapshot, Team)
                    .join(Team, Team.id == StandingsSnapshot.team_id)
                    .where(
                        StandingsSnapshot.competition_id == competition_id,
                        StandingsSnapshot.season_id == resolved_season.id,
                        StandingsSnapshot.snapshot_date == snapshot_date,
                    )
                    .order_by(StandingsSnapshot.position.asc(), Team.name.asc())
                )
                standings = [
                    {
                        "position": int(snapshot.position),
                        "team": self._serialize_team_summary(team),
                        "points": int(snapshot.points),
                        "played": int(snapshot.played),
                        "won": int(snapshot.won),
                        "drawn": int(snapshot.drawn),
                        "lost": int(snapshot.lost),
                        "goals_for": int(snapshot.goals_for),
                        "goals_against": int(snapshot.goals_against),
                        "goal_difference": int(snapshot.goals_for - snapshot.goals_against),
                    }
                    for snapshot, team in self.db.execute(statement).all()
                ]
                source = "snapshot"

        if not standings and resolved_season_name is not None:
            source = "computed_fallback"
            standings = self._compute_standings_rows_from_matches(
                competition_id=competition_id,
                season_name=resolved_season_name,
            )

        return {
            "competition": self._serialize_competition_summary(competition),
            "season": self._serialize_season_summary(resolved_season)
            if resolved_season is not None
            else None,
            "season_name": resolved_season_name,
            "source": source,
            "snapshot_date": snapshot_date if source == "snapshot" else None,
            "standings": standings,
        }

    def get_latest_odds_for_match(self, match_id: UUID) -> list[dict[str, Any]]:
        odds_service = OddsQueryService(self.db)
        return odds_service.get_latest_compact_map([match_id]).get(match_id, [])

    def get_latest_odds_map(self, match_ids: list[UUID]) -> dict[UUID, list[dict[str, Any]]]:
        odds_service = OddsQueryService(self.db)
        return odds_service.get_latest_compact_map(match_ids)

    def list_ingestion_runs(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        provider: str | None = None,
        status: str | None = None,
        run_type: str | None = None,
        entity_type: str | None = None,
    ) -> list[dict[str, Any]]:
        statement = select(IngestionRun, Provider.name.label("provider_name")).outerjoin(
            Provider,
            IngestionRun.provider_id == Provider.id,
        )

        if provider:
            statement = statement.where(Provider.name == provider)
        if status:
            statement = statement.where(IngestionRun.status == status)
        if run_type:
            statement = statement.where(IngestionRun.run_type == run_type)
        if entity_type:
            statement = statement.where(IngestionRun.entity_type == entity_type)

        statement = (
            statement.order_by(IngestionRun.started_at.desc(), IngestionRun.id.desc())
            .offset(offset)
            .limit(limit)
        )

        rows = self.db.execute(statement).all()
        return [
            {
                "id": run.id,
                "provider": provider_name,
                "run_type": run.run_type,
                "entity_type": run.entity_type,
                "started_at": run.started_at,
                "finished_at": run.finished_at,
                "status": run.status,
                "row_count": run.row_count,
                "raw_record_count": run.raw_record_count,
                "created_count": run.created_count,
                "updated_count": run.updated_count,
                "skipped_count": run.skipped_count,
                "error_count": run.error_count,
                "error_message": run.error_message,
            }
            for run, provider_name in rows
        ]

    def _compute_standings_rows_from_matches(
        self,
        *,
        competition_id: UUID,
        season_name: str,
    ) -> list[dict[str, Any]]:
        finished_matches_filter = (
            Match.competition_id == competition_id,
            Match.season == season_name,
            Match.status == "finished",
            Match.home_goals.is_not(None),
            Match.away_goals.is_not(None),
        )

        home_stats = (
            select(
                Match.home_team_id.label("team_id"),
                func.count(Match.id).label("played"),
                func.coalesce(
                    func.sum(case((Match.home_goals > Match.away_goals, 1), else_=0)), 0
                ).label("won"),
                func.coalesce(
                    func.sum(case((Match.home_goals == Match.away_goals, 1), else_=0)), 0
                ).label("drawn"),
                func.coalesce(
                    func.sum(case((Match.home_goals < Match.away_goals, 1), else_=0)), 0
                ).label("lost"),
                func.coalesce(func.sum(Match.home_goals), 0).label("goals_for"),
                func.coalesce(func.sum(Match.away_goals), 0).label("goals_against"),
            )
            .where(*finished_matches_filter)
            .group_by(Match.home_team_id)
        )

        away_stats = (
            select(
                Match.away_team_id.label("team_id"),
                func.count(Match.id).label("played"),
                func.coalesce(
                    func.sum(case((Match.away_goals > Match.home_goals, 1), else_=0)), 0
                ).label("won"),
                func.coalesce(
                    func.sum(case((Match.home_goals == Match.away_goals, 1), else_=0)), 0
                ).label("drawn"),
                func.coalesce(
                    func.sum(case((Match.away_goals < Match.home_goals, 1), else_=0)), 0
                ).label("lost"),
                func.coalesce(func.sum(Match.away_goals), 0).label("goals_for"),
                func.coalesce(func.sum(Match.home_goals), 0).label("goals_against"),
            )
            .where(*finished_matches_filter)
            .group_by(Match.away_team_id)
        )

        unioned = home_stats.union_all(away_stats).subquery()
        aggregated = (
            select(
                unioned.c.team_id,
                func.coalesce(func.sum(unioned.c.played), 0).label("played"),
                func.coalesce(func.sum(unioned.c.won), 0).label("won"),
                func.coalesce(func.sum(unioned.c.drawn), 0).label("drawn"),
                func.coalesce(func.sum(unioned.c.lost), 0).label("lost"),
                func.coalesce(func.sum(unioned.c.goals_for), 0).label("goals_for"),
                func.coalesce(func.sum(unioned.c.goals_against), 0).label("goals_against"),
            )
            .group_by(unioned.c.team_id)
            .subquery()
        )

        points = aggregated.c.won * 3 + aggregated.c.drawn
        goal_difference = aggregated.c.goals_for - aggregated.c.goals_against

        statement = (
            select(
                Team.id,
                Team.name,
                func.coalesce(aggregated.c.played, 0).label("played"),
                func.coalesce(aggregated.c.won, 0).label("won"),
                func.coalesce(aggregated.c.drawn, 0).label("drawn"),
                func.coalesce(aggregated.c.lost, 0).label("lost"),
                func.coalesce(aggregated.c.goals_for, 0).label("goals_for"),
                func.coalesce(aggregated.c.goals_against, 0).label("goals_against"),
                func.coalesce(goal_difference, 0).label("goal_difference"),
                func.coalesce(points, 0).label("points"),
            )
            .select_from(Team)
            .outerjoin(aggregated, aggregated.c.team_id == Team.id)
            .where(Team.competition_id == competition_id)
            .order_by(
                desc(func.coalesce(points, 0)),
                desc(func.coalesce(goal_difference, 0)),
                desc(func.coalesce(aggregated.c.goals_for, 0)),
                Team.name.asc(),
            )
        )

        rows = self.db.execute(statement).all()
        standings: list[dict[str, Any]] = []
        for position, row in enumerate(rows, start=1):
            standings.append(
                {
                    "position": position,
                    "team": {"id": row.id, "name": row.name},
                    "points": int(row.points or 0),
                    "played": int(row.played or 0),
                    "won": int(row.won or 0),
                    "drawn": int(row.drawn or 0),
                    "lost": int(row.lost or 0),
                    "goals_for": int(row.goals_for or 0),
                    "goals_against": int(row.goals_against or 0),
                    "goal_difference": int(row.goal_difference or 0),
                }
            )
        return standings

    def _resolve_team_season(
        self,
        *,
        team_id: UUID,
        competition_id: UUID,
        season_id: UUID | None,
        season: str | None,
    ) -> tuple[Season | None, str | None]:
        resolved_season, resolved_season_name = self._resolve_competition_season(
            competition_id=competition_id,
            season_id=season_id,
            season=season,
            infer_latest=False,
        )
        if resolved_season is not None or resolved_season_name is not None:
            return resolved_season, resolved_season_name

        row = self.db.execute(
            select(Match.season_id, Match.season)
            .where(or_(Match.home_team_id == team_id, Match.away_team_id == team_id))
            .order_by(Match.match_date.desc(), Match.id.desc())
            .limit(1)
        ).one_or_none()
        if row is None:
            return None, None

        inferred_season = self.db.get(Season, row.season_id) if row.season_id is not None else None
        inferred_season_name = inferred_season.name if inferred_season is not None else row.season
        return inferred_season, inferred_season_name

    def _resolve_competition_season(
        self,
        *,
        competition_id: UUID,
        season_id: UUID | None,
        season: str | None,
        infer_latest: bool,
    ) -> tuple[Season | None, str | None]:
        if season_id is not None:
            season_record = self.db.get(Season, season_id)
            season_name = season_record.name if season_record is not None else season
            return season_record, season_name

        if season is not None:
            season_record = self.db.execute(
                select(Season)
                .join(CompetitionSeason, CompetitionSeason.season_id == Season.id)
                .where(
                    CompetitionSeason.competition_id == competition_id,
                    Season.name == season,
                )
                .limit(1)
            ).scalar_one_or_none()
            return season_record, season

        if not infer_latest:
            return None, None

        season_record = self.db.execute(
            select(Season)
            .join(CompetitionSeason, CompetitionSeason.season_id == Season.id)
            .where(CompetitionSeason.competition_id == competition_id)
            .order_by(Season.end_date.desc(), Season.start_date.desc(), Season.name.desc())
            .limit(1)
        ).scalar_one_or_none()
        if season_record is not None:
            return season_record, season_record.name

        row = self.db.execute(
            select(Match.season_id, Match.season)
            .where(Match.competition_id == competition_id)
            .order_by(Match.match_date.desc(), Match.id.desc())
            .limit(1)
        ).one_or_none()
        if row is None:
            return None, None

        inferred_season = self.db.get(Season, row.season_id) if row.season_id is not None else None
        inferred_season_name = inferred_season.name if inferred_season is not None else row.season
        return inferred_season, inferred_season_name

    def _serialize_competition(self, competition: Competition) -> dict[str, Any]:
        return {
            "id": competition.id,
            "name": competition.name,
            "country": competition.country,
            "created_at": competition.created_at,
        }

    def _serialize_competition_summary(self, competition: Competition) -> dict[str, Any]:
        return {
            "id": competition.id,
            "name": competition.name,
            "country": competition.country,
        }

    def _serialize_season(self, season: Season) -> dict[str, Any]:
        return {
            "id": season.id,
            "name": season.name,
            "start_date": season.start_date,
            "end_date": season.end_date,
            "created_at": season.created_at,
        }

    def _serialize_season_summary(self, season: Season) -> dict[str, Any]:
        return {
            "id": season.id,
            "name": season.name,
            "start_date": season.start_date,
            "end_date": season.end_date,
        }

    def _serialize_team_summary(self, team: Team) -> dict[str, Any]:
        return {
            "id": team.id,
            "name": team.name,
        }

    def _serialize_match_row(
        self,
        *,
        match: Match,
        competition: Competition,
        season: Season | None,
        home_team: Team,
        away_team: Team,
        latest_odds: list[dict[str, Any]] | None,
    ) -> dict[str, Any]:
        score = None
        if match.home_goals is not None or match.away_goals is not None:
            score = {
                "home": match.home_goals,
                "away": match.away_goals,
            }

        return {
            "id": match.id,
            "competition_id": match.competition_id,
            "season_id": match.season_id,
            "season": match.season,
            "match_date": match.match_date,
            "home_team_id": match.home_team_id,
            "away_team_id": match.away_team_id,
            "home_goals": match.home_goals,
            "away_goals": match.away_goals,
            "status": match.status,
            "created_at": match.created_at,
            "competition": self._serialize_competition_summary(competition),
            "season_detail": self._serialize_season_summary(season) if season is not None else None,
            "home_team": self._serialize_team_summary(home_team),
            "away_team": self._serialize_team_summary(away_team),
            "score": score,
            "latest_odds": latest_odds,
        }
