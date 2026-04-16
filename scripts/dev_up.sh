#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f .env ]]; then
  cp .env.example .env
fi

python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ./apps/api
python scripts/wait_for_db.py
alembic -c apps/api/alembic.ini upgrade head
uvicorn app.main:app --app-dir apps/api --reload --host 0.0.0.0 --port 8000
