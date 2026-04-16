@echo off
setlocal
cd /d %~dp0\..

if not exist .env copy .env.example .env

python -m venv .venv
call .venv\Scripts\activate.bat
python -m pip install -U pip
python -m pip install -e .\apps\api
python scripts\wait_for_db.py
alembic -c apps\api\alembic.ini upgrade head
uvicorn app.main:app --app-dir apps\api --reload --host 0.0.0.0 --port 8000
