from __future__ import annotations

from time import perf_counter

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette import status

from app.core.logging import get_logger
from app.core.request_context import get_request_id

logger = get_logger(__name__)


def _get_duration_ms(request: Request) -> float | None:
    started_at = getattr(request.state, "request_started_at", None)
    if started_at is None:
        return None
    return round((perf_counter() - started_at) * 1000, 2)


async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    request_id = get_request_id()
    response = JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "detail": "request_validation_error",
            "errors": exc.errors(),
            "request_id": request_id,
        },
    )
    response.headers["X-Request-ID"] = request_id
    logger.warning(
        "request_validation_failed",
        extra={
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "duration_ms": _get_duration_ms(request),
            "error_count": len(exc.errors()),
        },
    )
    return response


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    request_id = get_request_id()
    response = JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "internal_server_error",
            "request_id": request_id,
        },
    )
    response.headers["X-Request-ID"] = request_id
    logger.exception(
        "http_request_failed",
        extra={
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "duration_ms": _get_duration_ms(request),
            "error": str(exc),
        },
    )
    return response
