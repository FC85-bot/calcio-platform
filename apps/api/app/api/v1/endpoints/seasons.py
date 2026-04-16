from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.season import SeasonRead
from app.services.query_service import QueryService

router = APIRouter()


@router.get("/seasons", response_model=list[SeasonRead])
def list_seasons(
    competition_id: UUID | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> list[dict]:
    query_service = QueryService(db)
    return query_service.list_seasons(
        competition_id=competition_id,
        limit=limit,
        offset=offset,
    )
