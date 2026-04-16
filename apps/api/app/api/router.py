from fastapi import APIRouter

from app.api.v1.endpoints.admin import router as admin_router
from app.api.v1.endpoints.competitions import router as competitions_router
from app.api.v1.endpoints.health import router as health_router
from app.api.v1.endpoints.matches import router as matches_router
from app.api.v1.endpoints.odds import router as odds_router
from app.api.v1.endpoints.seasons import router as seasons_router
from app.api.v1.endpoints.teams import router as teams_router
from app.core.config import get_settings

settings = get_settings()

api_router = APIRouter()
api_router.include_router(health_router, prefix=settings.api_v1_prefix, tags=["health"])
api_router.include_router(admin_router, prefix=settings.api_v1_prefix, tags=["admin"])
api_router.include_router(competitions_router, prefix=settings.api_v1_prefix, tags=["competitions"])
api_router.include_router(seasons_router, prefix=settings.api_v1_prefix, tags=["seasons"])
api_router.include_router(matches_router, prefix=settings.api_v1_prefix, tags=["matches"])
api_router.include_router(odds_router, prefix=settings.api_v1_prefix, tags=["odds"])
api_router.include_router(teams_router, prefix=settings.api_v1_prefix, tags=["teams"])
