from __future__ import annotations

from contextlib import asynccontextmanager
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError

from app.api.router import api_router
from app.core.config import get_settings
from app.core.error_handlers import unhandled_exception_handler, validation_exception_handler
from app.core.logging import configure_logging, get_logger
from app.core.request_context import (
    bind_log_context,
    reset_log_context,
    reset_request_id,
    set_request_id,
)

configure_logging()
logger = get_logger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info("application_starting", extra={"environment": settings.environment})
    logger.info("application_started", extra={"project_name": settings.project_name})
    yield
    logger.info("application_stopped")


app = FastAPI(
    title=settings.project_name,
    debug=settings.debug,
    version="0.1.0",
    lifespan=lifespan,
)
app.include_router(api_router)
app.add_exception_handler(RequestValidationError, validation_exception_handler)
app.add_exception_handler(Exception, unhandled_exception_handler)


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or uuid4().hex
    request_id_token = set_request_id(request_id)
    log_context_token = bind_log_context(request_id=request_id)
    started_at = perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        duration_ms = round((perf_counter() - started_at) * 1000, 2)
        logger.exception(
            "http_request_failed",
            extra={
                "method": request.method,
                "path": request.url.path,
                "duration_ms": duration_ms,
            },
        )
        raise
    else:
        duration_ms = round((perf_counter() - started_at) * 1000, 2)
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "http_request_completed",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": duration_ms,
            },
        )
        return response
    finally:
        reset_log_context(log_context_token)
        reset_request_id(request_id_token)


@app.get("/health", tags=["system"])
def liveness_health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readiness", tags=["system"])
def readiness_alias() -> dict[str, str]:
    return {"status": "ok"}
