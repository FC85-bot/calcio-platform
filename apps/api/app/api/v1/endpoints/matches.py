from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.match import MatchRead
from app.schemas.match_report import MatchReportRead
from app.schemas.odds import LatestOddsRead, OddsBestRowRead, OddsSnapshotRowRead
from app.services.match_report_service import MatchReportService
from app.services.odds_query_service import OddsQueryService
from app.services.query_service import QueryService

router = APIRouter()


@router.get("/matches", response_model=list[MatchRead])
def list_matches(
    competition_id: UUID | None = Query(default=None),
    season_id: UUID | None = Query(default=None),
    season: str | None = Query(default=None, min_length=1),
    date_from: datetime | None = Query(default=None),
    date_to: datetime | None = Query(default=None),
    team_id: UUID | None = Query(default=None),
    status_filter: str | None = Query(
        default=None,
        alias="status",
        pattern="^(scheduled|live|finished|postponed|cancelled)$",
    ),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    include_latest_odds: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> list[dict]:
    query_service = QueryService(db)
    return query_service.get_matches(
        competition_id=competition_id,
        season_id=season_id,
        season=season,
        date_from=date_from,
        date_to=date_to,
        team_id=team_id,
        status=status_filter,
        limit=limit,
        offset=offset,
        include_latest_odds=include_latest_odds,
    )


@router.get("/matches/{match_id}", response_model=MatchRead)
def get_match(
    match_id: UUID,
    include_latest_odds: bool = Query(default=True),
    db: Session = Depends(get_db),
) -> dict:
    query_service = QueryService(db)
    match = query_service.get_match_by_id(
        match_id,
        include_latest_odds=include_latest_odds,
    )
    if match is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Match not found",
        )
    return match


@router.get("/matches/{match_id}/report", response_model=MatchReportRead)
def get_match_report(
    match_id: UUID,
    prediction_horizon: str = Query(default="pre_match", min_length=1),
    form_last_n: int = Query(default=5, ge=1, le=10),
    db: Session = Depends(get_db),
) -> dict:
    service = MatchReportService(db)
    try:
        return service.build_match_report(
            match_id=match_id,
            prediction_horizon=prediction_horizon,
            form_last_n=form_last_n,
        )
    except ValueError as exc:
        detail = str(exc)
        if detail == "match_not_found":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Match not found",
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=detail,
        ) from exc


@router.get("/matches/{match_id}/odds/latest", response_model=list[OddsSnapshotRowRead])
def get_latest_match_odds(
    match_id: UUID,
    market_code: str | None = Query(default=None, pattern="^(1X2|OU25|OU|BTTS)$"),
    db: Session = Depends(get_db),
) -> list[dict]:
    _ensure_match_exists(db, match_id)
    return OddsQueryService(db).get_latest_odds(match_id, market_code=market_code)


@router.get("/matches/{match_id}/odds/history", response_model=list[OddsSnapshotRowRead])
def get_match_odds_history(
    match_id: UUID,
    market_code: str | None = Query(default=None, pattern="^(1X2|OU25|OU|BTTS)$"),
    bookmaker_id: UUID | None = Query(default=None),
    selection_code: str | None = Query(
        default=None,
        pattern="^(HOME|DRAW|AWAY|OVER|UNDER|YES|NO)$",
    ),
    limit: int = Query(default=500, ge=1, le=2000),
    db: Session = Depends(get_db),
) -> list[dict]:
    _ensure_match_exists(db, match_id)
    return OddsQueryService(db).get_history_odds(
        match_id,
        market_code=market_code,
        bookmaker_id=bookmaker_id,
        selection_code=selection_code,
        limit=limit,
    )


@router.get("/matches/{match_id}/odds/best", response_model=list[OddsBestRowRead])
def get_best_match_odds(
    match_id: UUID,
    market_code: str | None = Query(default=None, pattern="^(1X2|OU25|OU|BTTS)$"),
    db: Session = Depends(get_db),
) -> list[dict]:
    _ensure_match_exists(db, match_id)
    return OddsQueryService(db).get_best_odds(match_id, market_code=market_code)


@router.get("/matches/{match_id}/odds/opening", response_model=list[OddsSnapshotRowRead])
def get_opening_match_odds(
    match_id: UUID,
    market_code: str | None = Query(default=None, pattern="^(1X2|OU25|OU|BTTS)$"),
    db: Session = Depends(get_db),
) -> list[dict]:
    _ensure_match_exists(db, match_id)
    return OddsQueryService(db).get_opening_odds(match_id, market_code=market_code)


@router.get(
    "/matches/{match_id}/odds/latest-compact",
    response_model=list[LatestOddsRead],
)
def get_latest_match_odds_compact(
    match_id: UUID,
    db: Session = Depends(get_db),
) -> list[dict]:
    _ensure_match_exists(db, match_id)
    return OddsQueryService(db).get_latest_compact_map([match_id]).get(match_id, [])


def _ensure_match_exists(db: Session, match_id: UUID) -> None:
    query_service = QueryService(db)
    match = query_service.get_match_by_id(match_id, include_latest_odds=False)
    if match is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Match not found",
        )
