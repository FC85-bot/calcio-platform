from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.odds import OddsSnapshotRowRead
from app.services.odds_query_service import OddsQueryService
from app.services.query_service import QueryService

router = APIRouter()


@router.get("/odds/{match_id}", response_model=list[OddsSnapshotRowRead])
def list_match_odds(
    match_id: UUID,
    market_code: str | None = Query(default=None, pattern="^(1X2|OU25|OU|BTTS)$"),
    limit: int = Query(default=200, ge=1, le=2000),
    db: Session = Depends(get_db),
) -> list[dict]:
    query_service = QueryService(db)
    match = query_service.get_match_by_id(match_id, include_latest_odds=False)
    if match is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Match not found")
    return OddsQueryService(db).get_history_odds(match_id, market_code=market_code, limit=limit)
