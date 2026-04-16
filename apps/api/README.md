# Calcio Platform API

README tecnico minimo allineato al fix pack audit blocker post-Sprint 15.

## Target supportato
- FastAPI
- PostgreSQL + Alembic
- provider mock per bootstrap locale auditabile
- football-data per dominio canonical reale
- the-odds-api per odds reali

## Avvio rapido

### Install
```bash
pip install -e "./apps/api[dev]"
```

### Migration Postgres
```bash
alembic -c apps/api/alembic.ini upgrade head
```

### Run API
```bash
uvicorn app.main:app --app-dir apps/api --reload
```

## Health / readiness
```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/readiness
curl http://127.0.0.1:8000/api/v1/health
curl http://127.0.0.1:8000/api/v1/readiness
```

## Monitoring tecnico
```bash
curl http://127.0.0.1:8000/api/v1/admin/monitoring/summary
curl http://127.0.0.1:8000/api/v1/admin/normalization-status
curl "http://127.0.0.1:8000/api/v1/admin/ingestion-runs?limit=20&offset=0"
curl "http://127.0.0.1:8000/api/v1/admin/provider-mappings?limit=100&offset=0"
```

## Mock provider locale

### Raw ingestion
```bash
python scripts/run_raw_ingestion.py --include-odds
```

### Normalization
```bash
python scripts/run_normalization.py --provider mock_provider --entity-type competitions --entity-type seasons --entity-type teams --entity-type matches --entity-type odds
```

## Odds public contract
Endpoint principali:
- `GET /api/v1/matches/{match_id}/odds/latest`
- `GET /api/v1/matches/{match_id}/odds/history`
- `GET /api/v1/matches/{match_id}/odds/best`
- `GET /api/v1/matches/{match_id}/odds/opening`

Market code pubblico supportato:
- `1X2`
- `OU25`
- `BTTS`
- `OU` accettato solo per retrocompatibilità semplice

Esempio:
```bash
curl "http://127.0.0.1:8000/api/v1/matches/<MATCH_UUID>/odds/latest?market_code=OU25"
```

## Test / quality checks
```bash
pytest -q apps/api/tests
python scripts/check_repo_hygiene.py
python scripts/check_migrations.py
```

## Nota importante
SQLite non è il path di validazione completo delle migration storiche.
Il target reale supportato per upgrade Alembic è PostgreSQL.
