from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.orm import Session

from app.models.match import Match


class StatsService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_team_stats(self, team_id: UUID, *, competition_id: UUID, season: str) -> dict[str, Any]:
        is_home = Match.home_team_id == team_id
        is_away = Match.away_team_id == team_id

        wins_case = case(
            (and_(is_home, Match.home_goals > Match.away_goals), 1),
            (and_(is_away, Match.away_goals > Match.home_goals), 1),
            else_=0,
        )
        draws_case = case((Match.home_goals == Match.away_goals, 1), else_=0)
        losses_case = case(
            (and_(is_home, Match.home_goals < Match.away_goals), 1),
            (and_(is_away, Match.away_goals < Match.home_goals), 1),
            else_=0,
        )
        goals_scored_case = case((is_home, Match.home_goals), (is_away, Match.away_goals), else_=0)
        goals_conceded_case = case(
            (is_home, Match.away_goals), (is_away, Match.home_goals), else_=0
        )

        statement = select(
            func.count(Match.id).label("matches_played"),
            func.coalesce(func.sum(wins_case), 0).label("wins"),
            func.coalesce(func.sum(draws_case), 0).label("draws"),
            func.coalesce(func.sum(losses_case), 0).label("losses"),
            func.coalesce(func.sum(goals_scored_case), 0).label("goals_scored"),
            func.coalesce(func.sum(goals_conceded_case), 0).label("goals_conceded"),
            func.coalesce(func.sum(case((is_home, 1), else_=0)), 0).label("home_matches"),
            func.coalesce(
                func.sum(case((and_(is_home, Match.home_goals > Match.away_goals), 1), else_=0)), 0
            ).label("home_wins"),
            func.coalesce(
                func.sum(case((and_(is_home, Match.home_goals == Match.away_goals), 1), else_=0)), 0
            ).label("home_draws"),
            func.coalesce(
                func.sum(case((and_(is_home, Match.home_goals < Match.away_goals), 1), else_=0)), 0
            ).label("home_losses"),
            func.coalesce(func.sum(case((is_home, Match.home_goals), else_=0)), 0).label(
                "home_goals_scored"
            ),
            func.coalesce(func.sum(case((is_home, Match.away_goals), else_=0)), 0).label(
                "home_goals_conceded"
            ),
            func.coalesce(func.sum(case((is_away, 1), else_=0)), 0).label("away_matches"),
            func.coalesce(
                func.sum(case((and_(is_away, Match.away_goals > Match.home_goals), 1), else_=0)), 0
            ).label("away_wins"),
            func.coalesce(
                func.sum(case((and_(is_away, Match.away_goals == Match.home_goals), 1), else_=0)), 0
            ).label("away_draws"),
            func.coalesce(
                func.sum(case((and_(is_away, Match.away_goals < Match.home_goals), 1), else_=0)), 0
            ).label("away_losses"),
            func.coalesce(func.sum(case((is_away, Match.away_goals), else_=0)), 0).label(
                "away_goals_scored"
            ),
            func.coalesce(func.sum(case((is_away, Match.home_goals), else_=0)), 0).label(
                "away_goals_conceded"
            ),
        ).where(
            Match.competition_id == competition_id,
            Match.season == season,
            Match.status == "finished",
            Match.home_goals.is_not(None),
            Match.away_goals.is_not(None),
            or_(is_home, is_away),
        )

        row = self.db.execute(statement).one()

        matches_played = int(row.matches_played or 0)
        goals_scored = int(row.goals_scored or 0)
        goals_conceded = int(row.goals_conceded or 0)

        avg_goals_scored = round(goals_scored / matches_played, 2) if matches_played else 0.0
        avg_goals_conceded = round(goals_conceded / matches_played, 2) if matches_played else 0.0

        return {
            "team_id": team_id,
            "competition_id": competition_id,
            "season": season,
            "matches_played": matches_played,
            "wins": int(row.wins or 0),
            "draws": int(row.draws or 0),
            "losses": int(row.losses or 0),
            "goals_scored": goals_scored,
            "goals_conceded": goals_conceded,
            "avg_goals_scored": avg_goals_scored,
            "avg_goals_conceded": avg_goals_conceded,
            "home": {
                "matches": int(row.home_matches or 0),
                "wins": int(row.home_wins or 0),
                "draws": int(row.home_draws or 0),
                "losses": int(row.home_losses or 0),
                "goals_scored": int(row.home_goals_scored or 0),
                "goals_conceded": int(row.home_goals_conceded or 0),
            },
            "away": {
                "matches": int(row.away_matches or 0),
                "wins": int(row.away_wins or 0),
                "draws": int(row.away_draws or 0),
                "losses": int(row.away_losses or 0),
                "goals_scored": int(row.away_goals_scored or 0),
                "goals_conceded": int(row.away_goals_conceded or 0),
            },
        }

    def get_team_form(
        self,
        team_id: UUID,
        *,
        competition_id: UUID,
        season: str,
        last_n: int = 5,
    ) -> dict[str, Any]:
        statement = (
            select(
                Match.id,
                Match.match_date,
                Match.home_team_id,
                Match.away_team_id,
                Match.home_goals,
                Match.away_goals,
            )
            .where(
                Match.competition_id == competition_id,
                Match.season == season,
                Match.status == "finished",
                Match.home_goals.is_not(None),
                Match.away_goals.is_not(None),
                or_(Match.home_team_id == team_id, Match.away_team_id == team_id),
            )
            .order_by(Match.match_date.desc(), Match.id.desc())
            .limit(last_n)
        )

        results: list[str] = []
        for row in self.db.execute(statement):
            if row.home_goals is None or row.away_goals is None:
                continue

            if row.home_team_id == team_id:
                if row.home_goals > row.away_goals:
                    results.append("W")
                elif row.home_goals == row.away_goals:
                    results.append("D")
                else:
                    results.append("L")
            else:
                if row.away_goals > row.home_goals:
                    results.append("W")
                elif row.away_goals == row.home_goals:
                    results.append("D")
                else:
                    results.append("L")

        return {
            "team_id": team_id,
            "competition_id": competition_id,
            "season": season,
            "last_n": last_n,
            "results": results,
        }

    def get_team_streak(
        self, team_id: UUID, *, competition_id: UUID, season: str
    ) -> dict[str, Any]:
        result_type = case(
            (and_(Match.home_team_id == team_id, Match.home_goals > Match.away_goals), "win"),
            (and_(Match.away_team_id == team_id, Match.away_goals > Match.home_goals), "win"),
            (Match.home_goals == Match.away_goals, "draw"),
            (and_(Match.home_team_id == team_id, Match.home_goals < Match.away_goals), "loss"),
            (and_(Match.away_team_id == team_id, Match.away_goals < Match.home_goals), "loss"),
            else_=None,
        )

        ordered_matches = (
            select(
                Match.id.label("match_id"),
                Match.match_date.label("match_date"),
                result_type.label("result_type"),
                func.lag(result_type)
                .over(order_by=(Match.match_date.desc(), Match.id.desc()))
                .label("previous_result"),
            )
            .where(
                Match.competition_id == competition_id,
                Match.season == season,
                Match.status == "finished",
                Match.home_goals.is_not(None),
                Match.away_goals.is_not(None),
                or_(Match.home_team_id == team_id, Match.away_team_id == team_id),
            )
            .cte("ordered_matches")
        )

        streak_groups = (
            select(
                ordered_matches.c.result_type,
                func.sum(
                    case(
                        (ordered_matches.c.previous_result.is_(None), 0),
                        (ordered_matches.c.result_type != ordered_matches.c.previous_result, 1),
                        else_=0,
                    )
                )
                .over(
                    order_by=(
                        ordered_matches.c.match_date.desc(),
                        ordered_matches.c.match_id.desc(),
                    )
                )
                .label("streak_group"),
            )
            .where(ordered_matches.c.result_type.is_not(None))
            .cte("streak_groups")
        )

        statement = (
            select(
                streak_groups.c.result_type.label("current_streak_type"),
                func.count().label("current_streak_length"),
            )
            .where(streak_groups.c.streak_group == 0)
            .group_by(streak_groups.c.result_type)
        )

        row = self.db.execute(statement).one_or_none()

        return {
            "team_id": team_id,
            "competition_id": competition_id,
            "season": season,
            "current_streak_type": row.current_streak_type if row is not None else None,
            "current_streak_length": int(row.current_streak_length or 0) if row is not None else 0,
        }
