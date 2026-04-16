from __future__ import annotations

from datetime import datetime, timezone
from time import perf_counter

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.request_context import get_request_id
from app.db.session import get_db

router = APIRouter()
settings = get_settings()


@router.get("/health")
def readiness_health(db: Session = Depends(get_db)) -> dict[str, object]:
    started_at = perf_counter()
    db.execute(text("SELECT 1"))
    database_latency_ms = round((perf_counter() - started_at) * 1000, 2)

    return {
        "status": "ok",
        "service": settings.project_name,
        "environment": settings.environment,
        "database": "ok",
        "database_latency_ms": database_latency_ms,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "request_id": get_request_id(),
    }


@router.get("/readiness")
def readiness_alias(db: Session = Depends(get_db)) -> dict[str, object]:
    return readiness_health(db=db)
