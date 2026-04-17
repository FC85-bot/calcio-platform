from __future__ import annotations

import os
import time
from pathlib import Path
import sys
from typing import Final

ROOT_DIR = Path(__file__).resolve().parents[1]
API_DIR = ROOT_DIR / "apps" / "api"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

import psycopg

from app.core.logging import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)

DEFAULT_TIMEOUT_SECONDS: Final[int] = 60
DEFAULT_INTERVAL_SECONDS: Final[float] = 2.0


def build_dsn() -> str:
    host = os.getenv("POSTGRES_SERVER", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "calcio")
    password = os.getenv("POSTGRES_PASSWORD", "calcio")
    database = os.getenv("POSTGRES_DB", "calcio_platform")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def main() -> int:
    timeout_seconds = int(os.getenv("DB_WAIT_TIMEOUT", DEFAULT_TIMEOUT_SECONDS))
    interval_seconds = float(os.getenv("DB_WAIT_INTERVAL", DEFAULT_INTERVAL_SECONDS))
    deadline = time.monotonic() + timeout_seconds
    dsn = build_dsn()
    attempt = 0

    logger.info(
        "database_readiness_check_started",
        extra={
            "timeout_seconds": timeout_seconds,
            "interval_seconds": interval_seconds,
        },
    )

    while time.monotonic() < deadline:
        attempt += 1
        try:
            with psycopg.connect(dsn, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            logger.info(
                "database_readiness_check_succeeded",
                extra={"attempt": attempt},
            )
            return 0
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "database_not_ready_yet",
                extra={
                    "attempt": attempt,
                    "error": str(exc),
                },
            )
            time.sleep(interval_seconds)

    logger.error(
        "database_readiness_check_timed_out",
        extra={
            "attempts": attempt,
            "timeout_seconds": timeout_seconds,
        },
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
