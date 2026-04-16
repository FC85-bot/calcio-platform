from __future__ import annotations

import json
import logging
from logging.config import dictConfig
from typing import Any

from app.core.config import get_settings
from app.core.request_context import get_log_context

_RESERVED_LOG_RECORD_FIELDS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
}


class StructuredExtraFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base_message = super().format(record)
        extra = _extract_extra_fields(record)
        if not extra:
            return base_message

        parts = [f"{key}={_stringify_value(value)}" for key, value in sorted(extra.items())]
        return f"{base_message} | {' '.join(parts)}"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(_extract_extra_fields(record))
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=_stringify_value)


def configure_logging() -> None:
    settings = get_settings()
    formatter_name = "json" if settings.log_json else "standard"

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "()": "app.core.logging.StructuredExtraFormatter",
                    "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                },
                "json": {
                    "()": "app.core.logging.JsonFormatter",
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": formatter_name,
                    "level": settings.log_level,
                }
            },
            "root": {
                "handlers": ["console"],
                "level": settings.log_level,
            },
        }
    )


def _extract_extra_fields(record: logging.LogRecord) -> dict[str, Any]:
    payload = get_log_context()
    payload.update(
        {
            key: value
            for key, value in record.__dict__.items()
            if key not in _RESERVED_LOG_RECORD_FIELDS and not key.startswith("_")
        }
    )
    return payload


def _stringify_value(value: Any) -> str:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return str(value)
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except TypeError:
        return str(value)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
