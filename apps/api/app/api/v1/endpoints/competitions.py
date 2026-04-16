from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.competition import CompetitionRead, CompetitionStandingsRead
from app.services.query_service import QueryService

router = APIRouter()


@router.get("/competitions", response_model=list[CompetitionRead])
def list_competitions(
    season_id: UUID | None = Query(default=None),
    season: str | None = Query(default=None, min_length=1),
    limit: int = Query(default=100, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> list[dict]:
    query_service = QueryService(db)
    return query_service.list_competitions(
        season_id=season_id,
        season=season,
        limit=limit,
        offset=offset,
    )


@router.get("/competitions/{competition_id}/standings", response_model=CompetitionStandingsRead)
def get_competition_standings(
    competition_id: UUID,
    season_id: UUID | None = Query(default=None),
    season: str | None = Query(default=None, min_length=1),
    db: Session = Depends(get_db),
) -> dict:
    query_service = QueryService(db)
    standings = query_service.get_standings(
        competition_id=competition_id,
        season_id=season_id,
        season=season,
    )
    if standings is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Competition not found")
    return standings
