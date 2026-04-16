# Calcio Platform

Baseline tecnica aggiornata post-Sprint 15 con fix pack audit blocker.

Perimetro incluso:
- backend FastAPI
- PostgreSQL + Alembic
- raw ingestion
- normalization + provider mapping
- query layer / API
- frontend Next.js
- odds layer
- feature snapshots
- prediction engine MVP
- match report
- evaluation/backtest MVP

Questo pacchetto non introduce nuove feature di prodotto.
Chiude solo fix obbligatori su repo hygiene, bootstrap, normalizzazione mock provider, contratto odds `OU25`, Docker full-stack, CI e documentazione operativa minima.

## 1. Prerequisiti reali

### Backend
- Python 3.11+
- PostgreSQL 15+ o 17+

### Frontend
- Node 22+
- npm 10+

### Docker bootstrap
- Docker Engine + Docker Compose plugin

## 2. Repo pulito

Il repo consegnabile deve contenere:
- `.env.example`
- `apps/web/.env.example`

Il repo **non** deve contenere:
- `.env` reale
- `.venv`
- `node_modules`
- `.next`
- `__pycache__`
- `.pytest_cache`
- `.ruff_cache`

Se in passato è stato committato un `.env` reale, trattalo come incidente:
- rimuovi il file dal versionamento
- ruota tutte le credenziali/API key coinvolte

## 3. Configurazione ambiente

Copia il file esempio e personalizzalo localmente fuori dal versionamento:

```bash
cp .env.example .env
```

Variabili principali:
- `POSTGRES_SERVER`
- `POSTGRES_PORT`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `INGESTION_PROVIDER`
- `FOOTBALL_DATA_API_KEY`
- `THE_ODDS_API_API_KEY`

Per bootstrap locale con provider mock:

```env
INGESTION_PROVIDER=mock_provider
```

Per il frontend locale non è richiesto alcun file `.env.local` nel pacchetto consegnato.
Puoi passare `CALCIO_API_BASE_URL` via environment oppure usare il fallback locale incorporato.
Se crei `apps/web/.env.local` per uso personale, deve restare fuori dal versionamento.

Se non imposti `CALCIO_API_BASE_URL`, il frontend usa il fallback locale:

```text
http://127.0.0.1:8000/api/v1
```

## 4. Installazione locale backend

```bash
pip install -e "./apps/api[dev]"
```

## 5. Migration su PostgreSQL pulito

Il target supportato è PostgreSQL.
SQLite non è il path pienamente supportato per validare tutte le migration Alembic storiche.

Configura `.env` con il DB Postgres reale e poi esegui:

```bash
alembic -c apps/api/alembic.ini upgrade head
```

## 6. Avvio backend locale

```bash
uvicorn app.main:app --app-dir apps/api --reload
```

Health / readiness:

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/readiness
curl http://127.0.0.1:8000/api/v1/health
curl http://127.0.0.1:8000/api/v1/readiness
```

Monitoring tecnico minimo:

```bash
curl http://127.0.0.1:8000/api/v1/admin/monitoring/summary
curl http://127.0.0.1:8000/api/v1/admin/normalization-status
curl "http://127.0.0.1:8000/api/v1/admin/ingestion-runs?limit=20&offset=0"
```

## 7. Avvio frontend locale

Da clone pulito:

```bash
cd apps/web
npm ci
npm run build
npm run start -- --hostname 0.0.0.0 --port 3000
```

Pagine smoke minime:
- `/`
- `/competitions`
- `/matches`
- `/matches/<MATCH_ID>`
- `/teams/<TEAM_ID>`
- `/admin/freshness`

## 8. Bootstrap Docker full-stack

Il compose porta su l'intero stack minimo reale:
- `db`
- `api`
- `web`

Comandi:

```bash
docker compose config
docker compose up --build
```

Porte default:
- Postgres: `5432`
- API: `8000`
- Web: `3000`

Health check rapido:

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:3000
```

Nel compose il frontend punta al backend interno via:

```text
CALCIO_API_BASE_URL=http://api:8000/api/v1
```

## 9. Raw ingestion

### Flow locale mock provider

```bash
python scripts/run_raw_ingestion.py --include-odds
```

oppure per singoli entity type:

```bash
python scripts/run_raw_ingestion.py --entity-type competitions
python scripts/run_raw_ingestion.py --entity-type seasons
python scripts/run_raw_ingestion.py --entity-type teams
python scripts/run_raw_ingestion.py --entity-type matches
python scripts/run_raw_ingestion.py --entity-type odds
```

### Flow provider reali

Dominio calcistico canonical:

```bash
set INGESTION_PROVIDER=football_data
python scripts/run_raw_ingestion.py --entity-type competitions --entity-type seasons --entity-type teams --entity-type matches
```

Odds:

```bash
set INGESTION_PROVIDER=the_odds_api
python scripts/run_raw_ingestion.py --entity-type odds
```

## 10. Normalization

### Flow locale mock provider

```bash
python scripts/run_normalization.py --provider mock_provider --entity-type competitions --entity-type seasons --entity-type teams --entity-type matches --entity-type odds
```

### Rerun completo controllato

```bash
python scripts/run_normalization.py --provider mock_provider --entity-type competitions --entity-type seasons --entity-type teams --entity-type matches --entity-type odds --include-processed
```

### Flow provider reali

```bash
python scripts/run_normalization.py --entity-type competitions --entity-type seasons --entity-type teams --entity-type matches
python scripts/run_normalization.py --provider the_odds_api --entity-type odds
```

## 11. Verifiche minime post-normalization

Provider mapping reale:

```bash
curl "http://127.0.0.1:8000/api/v1/admin/provider-mappings?limit=200&offset=0"
```

Atteso su flow mock locale:
- `provider_entities` popolata
- `competition` mappata
- `season` mappata
- `team` mappate
- `match` mappati
- `bookmaker` e `market` mappati per odds

Tabelle canonical attese popolate:
- `competitions`
- `seasons`
- `teams`
- `matches`
- `odds`

## 12. Contratto odds pubblico: OU25

Il contratto pubblico supportato è:
- `1X2`
- `OU25`
- `BTTS`

Retrocompatibilità semplice mantenuta:
- `OU` continua a essere accettato sugli endpoint odds pubblici
- storage canonical interno resta `OU` + `line_value = 2.5`

Smoke minimi:

```bash
curl "http://127.0.0.1:8000/api/v1/matches/<MATCH_UUID>/odds/latest?market_code=OU25"
curl "http://127.0.0.1:8000/api/v1/matches/<MATCH_UUID>/odds/history?market_code=OU25&limit=50"
curl "http://127.0.0.1:8000/api/v1/matches/<MATCH_UUID>/odds/best?market_code=OU25"
curl "http://127.0.0.1:8000/api/v1/matches/<MATCH_UUID>/odds/opening?market_code=OU25"
```

Atteso:
- status `200`
- mercato restituito come `OU25`
- line value `2.5`

## 13. Test suite

```bash
pytest -q apps/api/tests
```

## 14. CI locali equivalenti

```bash
python scripts/check_repo_hygiene.py
python -m ruff format --check apps/api/app apps/api/tests scripts run_feature_snapshots.py
python -m ruff check --select E,F --ignore E402,E501 apps/api/app apps/api/tests scripts run_feature_snapshots.py
python -m compileall apps/api/app apps/api/tests scripts run_feature_snapshots.py
python scripts/check_migrations.py
```

Frontend bootstrap check:

```bash
cd apps/web
npm ci
npm run build
npm run start -- --hostname 0.0.0.0 --port 3000
```

## 15. Rerun finale end-to-end minimo

Su ambiente Postgres pulito:

```bash
alembic -c apps/api/alembic.ini upgrade head
python scripts/run_raw_ingestion.py --include-odds
python scripts/run_normalization.py --provider mock_provider --entity-type competitions --entity-type seasons --entity-type teams --entity-type matches --entity-type odds
pytest -q apps/api/tests
```

Poi smoke API / frontend:

```bash
curl http://127.0.0.1:8000/api/v1/health
curl http://127.0.0.1:8000/api/v1/readiness
curl http://127.0.0.1:8000/api/v1/admin/monitoring/summary
curl http://127.0.0.1:8000/api/v1/admin/normalization-status
curl "http://127.0.0.1:8000/api/v1/matches/<MATCH_UUID>/odds/latest?market_code=OU25"
```

## 16. Stato supportato dopo fix pack

Supportato:
- Postgres + Alembic come path reale
- bootstrap backend locale
- bootstrap frontend locale da clone pulito
- compose full-stack `db + api + web`
- mock provider compatibile con normalization reale
- provider mappings come source of truth reale
- odds public contract `OU25`
- monitoring tecnico minimo
- CI con backend checks + frontend build

Non supportato come path principale:
- validazione piena migration su SQLite storico
- estensioni PRO/intermedie
- nuove feature di prodotto
