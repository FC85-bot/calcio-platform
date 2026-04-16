from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import case, desc, func, select, union_all
from sqlalchemy.orm import Session

from app.models.match import Match
from app.models.team import Team


class StandingsService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_standings(self, competition_id: UUID, season: str) -> dict[str, Any]:
        finished_matches_filter = (
            Match.competition_id == competition_id,
            Match.season == season,
            Match.status == "finished",
            Match.home_goals.is_not(None),
            Match.away_goals.is_not(None),
        )

        home_stats = (
            select(
                Match.home_team_id.label("team_id"),
                func.count(Match.id).label("matches_played"),
                func.coalesce(
                    func.sum(case((Match.home_goals > Match.away_goals, 1), else_=0)), 0
                ).label("wins"),
                func.coalesce(
                    func.sum(case((Match.home_goals == Match.away_goals, 1), else_=0)), 0
                ).label("draws"),
                func.coalesce(
                    func.sum(case((Match.home_goals < Match.away_goals, 1), else_=0)), 0
                ).label("losses"),
                func.coalesce(func.sum(Match.home_goals), 0).label("goals_scored"),
                func.coalesce(func.sum(Match.away_goals), 0).label("goals_conceded"),
            )
            .where(*finished_matches_filter)
            .group_by(Match.home_team_id)
        )

        away_stats = (
            select(
                Match.away_team_id.label("team_id"),
                func.count(Match.id).label("matches_played"),
                func.coalesce(
                    func.sum(case((Match.away_goals > Match.home_goals, 1), else_=0)), 0
                ).label("wins"),
                func.coalesce(
                    func.sum(case((Match.home_goals == Match.away_goals, 1), else_=0)), 0
                ).label("draws"),
                func.coalesce(
                    func.sum(case((Match.away_goals < Match.home_goals, 1), else_=0)), 0
                ).label("losses"),
                func.coalesce(func.sum(Match.away_goals), 0).label("goals_scored"),
                func.coalesce(func.sum(Match.home_goals), 0).label("goals_conceded"),
            )
            .where(*finished_matches_filter)
            .group_by(Match.away_team_id)
        )

        unioned_stats = union_all(home_stats, away_stats).subquery()

        aggregated_stats = (
            select(
                unioned_stats.c.team_id,
                func.coalesce(func.sum(unioned_stats.c.matches_played), 0).label("matches_played"),
                func.coalesce(func.sum(unioned_stats.c.wins), 0).label("wins"),
                func.coalesce(func.sum(unioned_stats.c.draws), 0).label("draws"),
                func.coalesce(func.sum(unioned_stats.c.losses), 0).label("losses"),
                func.coalesce(func.sum(unioned_stats.c.goals_scored), 0).label("goals_scored"),
                func.coalesce(func.sum(unioned_stats.c.goals_conceded), 0).label("goals_conceded"),
            )
            .group_by(unioned_stats.c.team_id)
            .subquery()
        )

        matches_played = func.coalesce(aggregated_stats.c.matches_played, 0)
        wins = func.coalesce(aggregated_stats.c.wins, 0)
        draws = func.coalesce(aggregated_stats.c.draws, 0)
        losses = func.coalesce(aggregated_stats.c.losses, 0)
        goals_scored = func.coalesce(aggregated_stats.c.goals_scored, 0)
        goals_conceded = func.coalesce(aggregated_stats.c.goals_conceded, 0)
        goal_difference = goals_scored - goals_conceded
        points = wins * 3 + draws

        statement = (
            select(
                Team.id.label("team_id"),
                Team.name.label("team_name"),
                matches_played.label("matches_played"),
                wins.label("wins"),
                draws.label("draws"),
                losses.label("losses"),
                goals_scored.label("goals_scored"),
                goals_conceded.label("goals_conceded"),
                goal_difference.label("goal_difference"),
                points.label("points"),
            )
            .select_from(Team)
            .outerjoin(aggregated_stats, aggregated_stats.c.team_id == Team.id)
            .where(Team.competition_id == competition_id)
            .order_by(
                desc(points),
                desc(goal_difference),
                desc(goals_scored),
                Team.name.asc(),
            )
        )

        standings = [
            {
                "team_id": row.team_id,
                "team_name": row.team_name,
                "matches_played": int(row.matches_played or 0),
                "wins": int(row.wins or 0),
                "draws": int(row.draws or 0),
                "losses": int(row.losses or 0),
                "goals_scored": int(row.goals_scored or 0),
                "goals_conceded": int(row.goals_conceded or 0),
                "goal_difference": int(row.goal_difference or 0),
                "points": int(row.points or 0),
            }
            for row in self.db.execute(statement)
        ]

        return {
            "competition_id": competition_id,
            "season": season,
            "standings": standings,
        }
