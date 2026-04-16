from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any

_request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)
_log_context_var: ContextVar[dict[str, Any]] = ContextVar("log_context", default={})


def set_request_id(request_id: str | None) -> Token[str | None]:
    return _request_id_var.set(request_id)


def get_request_id() -> str | None:
    return _request_id_var.get()


def reset_request_id(token: Token[str | None]) -> None:
    _request_id_var.reset(token)


def clear_request_id() -> None:
    _request_id_var.set(None)


def bind_log_context(**kwargs: Any) -> Token[dict[str, Any]]:
    payload = dict(_log_context_var.get())
    for key, value in kwargs.items():
        if value is None:
            payload.pop(key, None)
        else:
            payload[key] = value
    return _log_context_var.set(payload)


def reset_log_context(token: Token[dict[str, Any]]) -> None:
    _log_context_var.reset(token)


def clear_log_context() -> None:
    _log_context_var.set({})


def get_log_context() -> dict[str, Any]:
    payload = dict(_log_context_var.get())
    request_id = get_request_id()
    if request_id is not None and "request_id" not in payload:
        payload["request_id"] = request_id
    return payload
