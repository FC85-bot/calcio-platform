from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

ROOT_DIR = Path(__file__).resolve().parents[1]
API_DIR = ROOT_DIR / "apps" / "api"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from app.core.config import get_settings  # noqa: E402
from app.services.monitoring_service import MonitoringService  # noqa: E402


EXIT_OK = 0
EXIT_BROKEN = 2


SIGNAL_NAMES = [
    "matches_missing_team_count",
    "matches_missing_competition_count",
    "provider_mapping_missing_count",
    "odds_inconsistent_count",
    "predictions_without_selections_count",
]


def _with_connect_timeout(database_url: str, timeout_seconds: int = 5) -> tuple[str, dict[str, Any]]:
    """Add a short connect timeout for PostgreSQL-style URLs used by the CLI check.

    Keeps SQLite untouched. Returns (url, connect_args).
    """
    lowered = database_url.lower()
    if lowered.startswith("sqlite"):
        return database_url, {}

    if lowered.startswith("postgresql"):
        split = urlsplit(database_url)
        query = dict(parse_qsl(split.query, keep_blank_values=True))
        query.setdefault("connect_timeout", str(timeout_seconds))
        safe_url = urlunsplit(
            (split.scheme, split.netloc, split.path, urlencode(query), split.fragment)
        )
        return safe_url, {}

    return database_url, {}



def _redact_database_url(database_url: str) -> str:
    try:
        url = URL.create(database_url) if "://" not in database_url else None
        if url is not None:
            return url.render_as_string(hide_password=True)
    except Exception:
        pass

    if "://" not in database_url:
        return database_url

    split = urlsplit(database_url)
    if "@" not in split.netloc:
        return database_url
    userinfo, hostinfo = split.netloc.rsplit("@", 1)
    username = userinfo.split(":", 1)[0]
    safe_netloc = f"{username}:***@{hostinfo}"
    return urlunsplit((split.scheme, safe_netloc, split.path, split.query, split.fragment))



def _normalize_summary(raw_summary: dict[str, Any]) -> dict[str, Any]:
    raw_status = raw_summary.get("status") or raw_summary.get("data_confidence_status") or "BROKEN"
    normalized_signals: list[dict[str, int | str]] = []

    for signal in raw_summary.get("signals", []):
        name = signal.get("name") or signal.get("signal_name")
        value = signal.get("value")
        if value is None:
            value = signal.get("observed_value")
        if name is None:
            continue
        normalized_signals.append({"name": str(name), "value": int(value or 0)})

    by_name = {item["name"]: item for item in normalized_signals}
    normalized_signals = [by_name[name] for name in SIGNAL_NAMES if name in by_name]

    critical_signal_count = int(
        raw_summary.get("critical_signal_count")
        if raw_summary.get("critical_signal_count") is not None
        else sum(1 for item in normalized_signals if int(item["value"]) > 0)
    )

    if critical_signal_count == 0 and raw_status != "OK":
        raw_status = "OK"
    elif critical_signal_count > 0 and raw_status != "BROKEN":
        raw_status = "BROKEN"

    return {
        "status": raw_status,
        "critical_signal_count": critical_signal_count,
        "signals": normalized_signals,
    }



def main() -> int:
    settings = get_settings()
    database_url = settings.database_url
    runtime_url, connect_args = _with_connect_timeout(database_url)

    print("data_quality_check_started", flush=True)
    print(
        json.dumps(
            {
                "database": _redact_database_url(runtime_url),
                "expected_signals": SIGNAL_NAMES,
            },
            indent=2,
        ),
        flush=True,
    )

    try:
        engine = create_engine(
            runtime_url,
            pool_pre_ping=settings.database_pool_pre_ping,
            pool_size=settings.database_pool_size,
            max_overflow=settings.database_max_overflow,
            echo=settings.database_echo,
            connect_args=connect_args,
        )
        with Session(engine) as db:
            db.execute(text("SELECT 1"))
            raw_summary = MonitoringService(db).get_data_confidence_summary()
    except SQLAlchemyError as exc:
        print(
            json.dumps(
                {
                    "status": "BROKEN",
                    "critical_signal_count": len(SIGNAL_NAMES),
                    "signals": [],
                    "error": str(exc),
                },
                indent=2,
            ),
            flush=True,
        )
        return EXIT_BROKEN
    except Exception as exc:  # pragma: no cover - defensive CLI guard
        print(
            json.dumps(
                {
                    "status": "BROKEN",
                    "critical_signal_count": len(SIGNAL_NAMES),
                    "signals": [],
                    "error": repr(exc),
                },
                indent=2,
            ),
            flush=True,
        )
        return EXIT_BROKEN

    summary = _normalize_summary(raw_summary)
    print("data_quality_check", flush=True)
    print(json.dumps(summary, indent=2), flush=True)
    return EXIT_OK if summary["status"] == "OK" else EXIT_BROKEN


if __name__ == "__main__":
    raise SystemExit(main())
