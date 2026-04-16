from __future__ import annotations

import os
import sys
import time
from typing import Final

import psycopg

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

    while time.monotonic() < deadline:
        try:
            with psycopg.connect(dsn, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            print("Database is ready.")
            return 0
        except Exception as exc:  # noqa: BLE001
            print(f"Database not ready yet: {exc}")
            time.sleep(interval_seconds)

    print("Database readiness check timed out.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
