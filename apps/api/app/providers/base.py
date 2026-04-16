from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from app.core.config import Settings, get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)


@dataclass(slots=True)
class ProviderFetchResult:
    entity_type: str
    endpoint: str
    payload: dict[str, Any]
    items: list[dict[str, Any]]
    request_params: dict[str, Any] | None = None
    response_metadata: dict[str, Any] | None = None


class BaseProvider(ABC):
    name: str
    supports_odds: bool = False

    def __init__(
        self, settings: Settings | None = None, client: httpx.Client | None = None
    ) -> None:
        self.settings = settings or get_settings()
        self._client = client or httpx.Client()

    @abstractmethod
    def fetch_competitions(self) -> list[ProviderFetchResult]:
        raise NotImplementedError

    @abstractmethod
    def fetch_seasons(self) -> list[ProviderFetchResult]:
        raise NotImplementedError

    @abstractmethod
    def fetch_teams(self) -> list[ProviderFetchResult]:
        raise NotImplementedError

    @abstractmethod
    def fetch_matches(self) -> list[ProviderFetchResult]:
        raise NotImplementedError

    @abstractmethod
    def fetch_odds(self) -> list[ProviderFetchResult]:
        raise NotImplementedError

    def close(self) -> None:
        self._client.close()

    def _get_json(
        self,
        *,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        url = self._build_url(endpoint)
        max_attempts = max(1, self.settings.provider_retry_attempts)
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            started_at = datetime.now(UTC)
            started_perf = time.perf_counter()
            try:
                response = self._client.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.settings.provider_timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()
                metadata = {
                    "request_url": str(response.request.url),
                    "method": response.request.method,
                    "status_code": response.status_code,
                    "response_headers": self._select_response_headers(response.headers),
                    "latency_ms": round((time.perf_counter() - started_perf) * 1000, 2),
                    "attempt": attempt,
                    "requested_at": started_at.isoformat(),
                }
                return payload, metadata
            except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError) as exc:
                last_error = exc
                retryable = self._is_retryable_exception(exc)
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                logger.warning(
                    "provider_request_failed",
                    extra={
                        "provider": self.name,
                        "endpoint": endpoint,
                        "attempt": attempt,
                        "max_attempts": max_attempts,
                        "status_code": status_code,
                        "retryable": retryable,
                        "error": str(exc),
                    },
                )
                if not retryable or attempt >= max_attempts:
                    break
                backoff_seconds = self.settings.provider_retry_backoff_seconds * (
                    2 ** (attempt - 1)
                )
                time.sleep(backoff_seconds)

        if last_error is None:
            raise RuntimeError(
                f"Provider request failed without exception: provider={self.name} endpoint={endpoint}"
            )
        raise last_error

    def _build_url(self, endpoint: str) -> str:
        base_url = self._base_url.rstrip("/")
        normalized_endpoint = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        return f"{base_url}{normalized_endpoint}"

    def _is_retryable_exception(self, exc: Exception) -> bool:
        if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
            return True
        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code
            return status_code == 429 or status_code >= 500
        return False

    def _select_response_headers(self, headers: httpx.Headers) -> dict[str, str]:
        selected = {}
        allowed = {
            "content-type",
            "x-api-version",
            "x-authenticated-client",
            "x-requestcounter-reset",
            "x-requests-available-minute",
            "x-requests-last",
            "x-requests-remaining",
            "x-requests-used",
        }
        for key, value in headers.items():
            if key.lower() in allowed:
                selected[key] = value
        return selected

    @property
    @abstractmethod
    def _base_url(self) -> str:
        raise NotImplementedError
