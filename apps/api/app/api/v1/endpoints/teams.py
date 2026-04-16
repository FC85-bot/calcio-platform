from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.team import Team
from app.schemas.team import (
    TeamDetailRead,
    TeamFormRead,
    TeamListItemRead,
    TeamStatsRead,
    TeamStreakRead,
)
from app.services.query_service import QueryService
from app.services.stats_service import StatsService

router = APIRouter()


@router.get("/teams", response_model=list[TeamListItemRead])
def list_teams(
    competition_id: UUID | None = Query(default=None),
    search: str | None = Query(default=None, min_length=1),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> list[dict]:
    query_service = QueryService(db)
    return query_service.list_teams(
        competition_id=competition_id,
        search=search,
        limit=limit,
        offset=offset,
    )


@router.get("/teams/{team_id}", response_model=TeamDetailRead)
def get_team_detail(
    team_id: UUID,
    season_id: UUID | None = Query(default=None),
    season: str | None = Query(default=None, min_length=1),
    form_last_n: int = Query(default=5, ge=1, le=20),
    db: Session = Depends(get_db),
) -> dict:
    query_service = QueryService(db)
    team_detail = query_service.get_team_detail(
        team_id,
        season_id=season_id,
        season=season,
        form_last_n=form_last_n,
    )
    if team_detail is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Team not found")
    return team_detail


@router.get("/teams/{team_id}/stats", response_model=TeamStatsRead)
def get_team_stats(
    team_id: UUID,
    season: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
) -> dict:
    team = db.get(Team, team_id)
    if team is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Team not found")

    stats_service = StatsService(db)
    return stats_service.get_team_stats(team_id, competition_id=team.competition_id, season=season)


@router.get("/teams/{team_id}/form", response_model=TeamFormRead)
def get_team_form(
    team_id: UUID,
    season: str = Query(..., min_length=1),
    last_n: int = Query(default=5, ge=1, le=20),
    db: Session = Depends(get_db),
) -> dict:
    team = db.get(Team, team_id)
    if team is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Team not found")

    stats_service = StatsService(db)
    return stats_service.get_team_form(
        team_id,
        competition_id=team.competition_id,
        season=season,
        last_n=last_n,
    )


@router.get("/teams/{team_id}/streak", response_model=TeamStreakRead)
def get_team_streak(
    team_id: UUID,
    season: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
) -> dict:
    team = db.get(Team, team_id)
    if team is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Team not found")

    stats_service = StatsService(db)
    return stats_service.get_team_streak(team_id, competition_id=team.competition_id, season=season)
